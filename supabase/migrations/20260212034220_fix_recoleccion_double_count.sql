-- Fix cantidad_despues negativos: 3 causas
--
-- 1. RECOLECCION legacy double-counting: cantidad_salida incluye lo vendido,
--    pero VENTA ya lo restó → usar GREATEST(current_stock, 0) en vez de cantidad_salida
-- 2. VENTA > stock disponible: capear a MIN(cantidad, stock disponible)
-- 3. Falta PERMANENCIA: legacy RECOLECCION con cantidad_permanencia > 0 necesita
--    un movimiento que sume esos items al stock del siguiente ciclo

-- Paso 1: Agregar PERMANENCIA al enum
ALTER TYPE tipo_movimiento_botiquin ADD VALUE IF NOT EXISTS 'PERMANENCIA';

-- Paso 2: Reescribir rebuild con fixes
CREATE OR REPLACE FUNCTION public.rebuild_movimientos_inventario()
 RETURNS TABLE(movimientos_creados bigint, inventario_final bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  saga_rec RECORD;
  item_rec RECORD;
  current_stock INTEGER;
  new_stock INTEGER;
  tipo_mov tipo_movimiento_botiquin;
  item_cantidad INTEGER;
  mov_count BIGINT := 0;
  inv_count BIGINT := 0;
BEGIN
  -- Limpiar tablas
  TRUNCATE TABLE movimientos_inventario RESTART IDENTITY CASCADE;
  TRUNCATE TABLE inventario_botiquin RESTART IDENTITY CASCADE;

  -- Procesar cada SAGA en orden cronológico
  -- Excluir sagas canceladas/fallidas
  -- CREACION primero en mismo timestamp (refleja que el levantamiento establece el stock
  -- antes de que se procesen ventas/recolecciones)
  FOR saga_rec IN
    SELECT id, id_cliente, tipo, created_at, items
    FROM saga_transactions
    WHERE items IS NOT NULL
      AND jsonb_array_length(items) > 0
      AND estado NOT IN ('CANCELADA', 'FALLIDA')
    ORDER BY created_at,
             CASE WHEN tipo IN ('LEVANTAMIENTO_INICIAL','LEV_POST_CORTE','CORTE_RENOVACION') THEN 0
                  WHEN tipo IN ('VENTA','VENTA_ODV') THEN 1
                  ELSE 2 END,
             id
  LOOP
    -- Determinar tipo de movimiento basado en tipo de saga
    IF saga_rec.tipo IN ('LEVANTAMIENTO_INICIAL', 'LEV_POST_CORTE', 'CORTE_RENOVACION') THEN
      tipo_mov := 'CREACION';
    ELSIF saga_rec.tipo IN ('VENTA', 'VENTA_ODV') THEN
      tipo_mov := 'VENTA';
    ELSIF saga_rec.tipo = 'RECOLECCION' THEN
      tipo_mov := 'RECOLECCION';
    ELSE
      -- Tipos no reconocidos (CORTE, DEVOLUCION_ODV, etc.) — skip
      CONTINUE;
    END IF;

    -- Procesar cada item en el SAGA
    FOR item_rec IN
      SELECT
        item->>'sku' as sku,
        -- New format: {sku, cantidad}
        (item->>'cantidad')::int as cantidad,
        -- Legacy format: {sku, cantidad_salida, cantidad_entrada, cantidad_permanencia}
        (item->>'cantidad_salida')::int as cantidad_salida,
        (item->>'cantidad_entrada')::int as cantidad_entrada,
        (item->>'cantidad_permanencia')::int as cantidad_permanencia
      FROM jsonb_array_elements(saga_rec.items) as item
    LOOP
      -- Skip SKUs que no existen en medicamentos (FK constraint)
      IF NOT EXISTS (SELECT 1 FROM medicamentos WHERE sku = item_rec.sku) THEN
        CONTINUE;
      END IF;

      -- Obtener stock actual
      SELECT COALESCE(cantidad_disponible, 0)
      INTO current_stock
      FROM inventario_botiquin
      WHERE id_cliente = saga_rec.id_cliente AND sku = item_rec.sku;

      IF current_stock IS NULL THEN
        current_stock := 0;
      END IF;

      -- ============================================================
      -- Determinar item_cantidad según tipo de movimiento
      -- ============================================================
      IF tipo_mov = 'CREACION' THEN
        -- CREACION: cantidad directa (no cap)
        IF item_rec.cantidad IS NOT NULL THEN
          item_cantidad := item_rec.cantidad;
        ELSE
          item_cantidad := COALESCE(item_rec.cantidad_entrada, 0);
        END IF;

      ELSIF tipo_mov = 'VENTA' THEN
        -- VENTA: capeado a stock disponible
        IF item_rec.cantidad IS NOT NULL THEN
          item_cantidad := LEAST(item_rec.cantidad, GREATEST(current_stock, 0));
        ELSE
          item_cantidad := LEAST(COALESCE(item_rec.cantidad_salida, 0), GREATEST(current_stock, 0));
        END IF;

      ELSIF tipo_mov = 'RECOLECCION' THEN
        IF item_rec.cantidad IS NOT NULL THEN
          -- New format: capeado a stock disponible
          item_cantidad := LEAST(item_rec.cantidad, GREATEST(current_stock, 0));
        ELSE
          -- Legacy: usar current_stock (cantidad_salida incluye VENTAs = double-count)
          item_cantidad := GREATEST(current_stock, 0);
        END IF;
      END IF;

      -- Skip items con cantidad 0 (e.g. legacy items sin movimiento real)
      IF item_cantidad IS NULL OR item_cantidad = 0 THEN
        CONTINUE;
      END IF;

      -- Calcular nuevo stock
      IF tipo_mov = 'CREACION' THEN
        new_stock := current_stock + item_cantidad;
      ELSE
        new_stock := current_stock - item_cantidad;
      END IF;

      -- Insertar movimiento
      INSERT INTO movimientos_inventario (
        id_saga_transaction,
        id_cliente,
        sku,
        tipo,
        cantidad,
        cantidad_antes,
        cantidad_despues,
        fecha_movimiento
      ) VALUES (
        saga_rec.id,
        saga_rec.id_cliente,
        item_rec.sku,
        tipo_mov,
        item_cantidad,
        current_stock,
        new_stock,
        saga_rec.created_at
      );

      mov_count := mov_count + 1;

      -- Actualizar inventario
      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
      VALUES (saga_rec.id_cliente, item_rec.sku, GREATEST(new_stock, 0))
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET cantidad_disponible = GREATEST(new_stock, 0);

      -- ============================================================
      -- PERMANENCIA: solo legacy RECOLECCION con permanencia > 0
      -- ============================================================
      IF tipo_mov = 'RECOLECCION'
         AND item_rec.cantidad IS NULL  -- legacy format
         AND COALESCE(item_rec.cantidad_permanencia, 0) > 0 THEN

        current_stock := GREATEST(new_stock, 0);  -- post-RECOLECCION (should be 0)
        new_stock := current_stock + item_rec.cantidad_permanencia;

        INSERT INTO movimientos_inventario (
          id_saga_transaction, id_cliente, sku, tipo, cantidad,
          cantidad_antes, cantidad_despues, fecha_movimiento
        ) VALUES (
          saga_rec.id, saga_rec.id_cliente, item_rec.sku, 'PERMANENCIA',
          item_rec.cantidad_permanencia, current_stock, new_stock, saga_rec.created_at
        );
        mov_count := mov_count + 1;

        INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
        VALUES (saga_rec.id_cliente, item_rec.sku, GREATEST(new_stock, 0))
        ON CONFLICT (id_cliente, sku)
        DO UPDATE SET cantidad_disponible = GREATEST(new_stock, 0);
      END IF;

    END LOOP;
  END LOOP;

  -- Contar inventario final
  SELECT COUNT(*) INTO inv_count FROM inventario_botiquin;

  RETURN QUERY SELECT mov_count, inv_count;
END;
$function$;

-- Paso 3: Re-ejecutar rebuild
SELECT * FROM rebuild_movimientos_inventario();
