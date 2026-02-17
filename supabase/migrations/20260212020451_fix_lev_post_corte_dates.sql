-- Fix cantidad_despues negativos: corregir fechas de LEV_POST_CORTE y priorizar CREACION en rebuild
--
-- Causa raíz: 9 sagas LEV_POST_CORTE fueron creadas con fecha 2026-01-27 (fecha de setup),
-- pero representan entregas reales de Oct-Nov 2025. El rebuild procesa cronológicamente,
-- así que las VENTA/RECOLECCION de Oct-Dec 2025 se procesaban ANTES de estas entradas → stock negativo.
--
-- Además, el ORDER BY (created_at, id) podía poner VENTA antes de LEV_POST_CORTE
-- cuando ambas ocurrían el mismo día, dependiendo del timestamp/UUID.
-- Esto es sistémico: el flujo real es LEV_POST_CORTE → VENTA → RECOLECCION dentro del mismo día.

-- ============================================================
-- Paso 1: Corregir fechas de las 9 sagas LEV_POST_CORTE
-- ============================================================
-- Usar 00:00:00 UTC para asegurar que se procesan ANTES de VENTA/RECOLECCION del mismo día

-- MEXAB19703 (3 sagas)
UPDATE saga_transactions SET created_at = '2025-10-31 00:00:00+00'
WHERE id = '317e0871-b1a3-4fa1-a5b6-c4280d032c63';

UPDATE saga_transactions SET created_at = '2025-11-14 00:00:00+00'
WHERE id = 'daa30369-a87d-4e40-a47a-1c15b4b8b3e3';

UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = '1f82f37d-667f-45fa-b131-607c3429661c';

-- MEXAF10018
UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = '01904b65-83d2-412f-82c3-f56f2f917d7a';

-- MEXAP10933
UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = 'b9dd1fee-a93f-4a0c-b0b7-d098f978e920';

-- MEXFS22989
UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = '788fcace-c511-41b0-a60c-4754e4b25a8e';

-- MEXHR15497
UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = '3673d37a-6986-4606-aa84-a52aa94d3fc9';

-- MEXJG20850
UPDATE saga_transactions SET created_at = '2025-11-28 00:00:00+00'
WHERE id = '0b89372c-20b5-4244-a866-87ab0db263a0';

-- MEXPF13496
UPDATE saga_transactions SET created_at = '2025-11-27 00:00:00+00'
WHERE id = 'de9596b4-6dd1-435f-93cd-c34db6d30b31';

-- ============================================================
-- Paso 2: Actualizar rebuild_movimientos_inventario()
-- Priorizar CREACION dentro del mismo DÍA (no solo mismo timestamp)
-- ============================================================
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

-- ============================================================
-- Paso 3: Re-ejecutar rebuild con las fechas corregidas
-- ============================================================
SELECT * FROM rebuild_movimientos_inventario();
