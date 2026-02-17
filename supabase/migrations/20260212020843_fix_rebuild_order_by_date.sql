-- Fix rebuild ORDER BY: priorizar CREACION dentro del mismo DÍA, no solo mismo timestamp
--
-- Problema: LEV_POST_CORTE y RECOLECCION/VENTA del mismo día tienen timestamps distintos
-- (e.g., RECOLECCION 10:11 vs LEV_POST_CORTE 12:00). El ORDER BY anterior solo priorizaba
-- CREACION con el mismo timestamp exacto. Ahora se agrupa por fecha (DATE) primero.
--
-- Esto afecta ~14 pares saga en múltiples clientes donde RECOLECCION/VENTA
-- se registró antes que LEV_POST_CORTE el mismo día.

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
  -- Dentro del mismo DÍA, CREACION se procesa primero (LEV_POST_CORTE → VENTA → RECOLECCION)
  -- Esto refleja el flujo real: primero se entrega producto, luego se vende/recolecta
  FOR saga_rec IN
    SELECT id, id_cliente, tipo, created_at, items
    FROM saga_transactions
    WHERE items IS NOT NULL
      AND jsonb_array_length(items) > 0
      AND estado NOT IN ('CANCELADA', 'FALLIDA')
    ORDER BY created_at::date,
             CASE WHEN tipo IN ('LEVANTAMIENTO_INICIAL','LEV_POST_CORTE','CORTE_RENOVACION') THEN 0 ELSE 1 END,
             created_at,
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
      -- Determinar la cantidad del movimiento
      IF item_rec.cantidad IS NOT NULL THEN
        -- New format: usar cantidad directamente
        item_cantidad := item_rec.cantidad;
      ELSE
        -- Legacy format: extraer cantidad según tipo de movimiento
        IF tipo_mov = 'CREACION' THEN
          item_cantidad := COALESCE(item_rec.cantidad_entrada, 0);
        ELSIF tipo_mov = 'VENTA' THEN
          item_cantidad := COALESCE(item_rec.cantidad_salida, 0);
        ELSIF tipo_mov = 'RECOLECCION' THEN
          item_cantidad := COALESCE(item_rec.cantidad_salida, 0);
        ELSE
          item_cantidad := 0;
        END IF;
      END IF;

      -- Skip items con cantidad 0 (e.g. legacy PERMANENCIA-only items en VENTA sagas)
      IF item_cantidad IS NULL OR item_cantidad = 0 THEN
        CONTINUE;
      END IF;

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

      -- Actualizar inventario (clamp a 0 para respetar CHECK constraint;
      -- movimientos_inventario guarda el valor real para auditoría)
      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
      VALUES (saga_rec.id_cliente, item_rec.sku, GREATEST(new_stock, 0))
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET cantidad_disponible = GREATEST(new_stock, 0);

    END LOOP;
  END LOOP;

  -- Contar inventario final
  SELECT COUNT(*) INTO inv_count FROM inventario_botiquin;

  RETURN QUERY SELECT mov_count, inv_count;
END;
$function$;

-- Re-ejecutar rebuild con el ORDER BY corregido
SELECT * FROM rebuild_movimientos_inventario();
