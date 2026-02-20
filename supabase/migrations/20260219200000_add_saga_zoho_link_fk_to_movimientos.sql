-- Migration: Add id_saga_zoho_link FK to movimientos_inventario
--
-- Establishes direct link: visitas → saga_transactions → saga_zoho_links → movimientos_inventario
-- This resolves ambiguity when a saga has multiple ODVs (e.g., MEXBR172 Nov 28 with 3 BOTIQUIN ODVs).
--
-- SAFETY: This migration NEVER creates new movimientos. It only:
--   1. Adds FK column + index
--   2. Fixes incorrect items in saga_zoho_links
--   3. Creates missing saga_zoho_links (REC- generics, MEXAF10018)
--   4. Populates items NULL in single-link VENTA links
--   5. Moves 2 misplaced movimientos between sagas (MEXPF13496)
--   6. Backfills the FK on existing movimientos (UPDATE, not INSERT)
--   7. Updates operational functions to use the new FK

-- ============================================================
-- PART 1: DDL — Add column + index
-- ============================================================

ALTER TABLE movimientos_inventario
  ADD COLUMN id_saga_zoho_link INTEGER
  REFERENCES saga_zoho_links(id) ON DELETE SET NULL;

CREATE INDEX idx_movimientos_saga_zoho_link
  ON movimientos_inventario(id_saga_zoho_link)
  WHERE id_saga_zoho_link IS NOT NULL;

-- ============================================================
-- PART 2: Data fixes — correct incorrect items
-- ============================================================

-- MEXRUP21941 LEV_INICIAL (saga 14470f75):
-- DCOdV-36092 (id=59) only has X952+Y399, not all 18 SKUs
UPDATE saga_zoho_links SET items = '[{"sku":"X952","cantidad":2},{"sku":"Y399","cantidad":1}]'::jsonb
WHERE id = 59;

-- DCOdV-35942 (id=58): remove X952 and Y399 (keep remaining 16 SKUs)
UPDATE saga_zoho_links SET items = (
  SELECT jsonb_agg(elem) FROM jsonb_array_elements(items) elem
  WHERE elem->>'sku' NOT IN ('X952', 'Y399')
) WHERE id = 58;

-- MEXPF13496 VENTA (saga b6642345):
-- 1. Move movimientos P165+P077 from wrong saga to correct saga
UPDATE movimientos_inventario
SET id_saga_transaction = 'b6642345-8291-4794-8cbc-4e34bd1846a2'
WHERE id_saga_transaction = 'a527da97-93f6-4d6a-9a71-ed4d8be1d68c'
  AND sku IN ('P165', 'P077') AND tipo = 'VENTA';

-- 2. Populate items per ODV for MEXPF13496
UPDATE saga_zoho_links SET items = '[{"sku":"P032","cantidad":1},{"sku":"R846","cantidad":1}]'::jsonb
WHERE id = 45;  -- DCOdV-33477
UPDATE saga_zoho_links SET items = '[{"sku":"P165","cantidad":1},{"sku":"P077","cantidad":2}]'::jsonb
WHERE id = 119; -- DCOdV-34390

-- ============================================================
-- PART 3: Populate items NULL in single-link VENTA links
-- ============================================================

UPDATE saga_zoho_links szl
SET items = sub.derived_items
FROM (
  SELECT szl2.id, jsonb_agg(
    jsonb_build_object('sku', m.sku, 'cantidad', m.cantidad)
    ORDER BY m.sku
  ) as derived_items
  FROM saga_zoho_links szl2
  JOIN movimientos_inventario m ON m.id_saga_transaction = szl2.id_saga_transaction
    AND m.tipo = 'VENTA'
  WHERE szl2.tipo = 'VENTA'
    AND szl2.items IS NULL
    AND (SELECT COUNT(*) FROM saga_zoho_links s3
         WHERE s3.id_saga_transaction = szl2.id_saga_transaction
           AND s3.tipo = 'VENTA') = 1
  GROUP BY szl2.id
) sub
WHERE szl.id = sub.id;

-- ============================================================
-- PART 4: Create missing saga_zoho_links
-- ============================================================

-- 4a. RECOLECCION sagas (34 sagas, 304 movimientos) — generic REC- links
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo, items, zoho_sync_status, created_at, updated_at)
SELECT
  st.id,
  'REC-' || st.id_cliente || '-' || TO_CHAR(st.created_at, 'YYYY-MM-DD'),
  'DEVOLUCION'::tipo_zoho_link,
  (SELECT jsonb_agg(jsonb_build_object('sku', m.sku, 'cantidad', m.cantidad) ORDER BY m.sku)
   FROM movimientos_inventario m
   WHERE m.id_saga_transaction = st.id AND m.tipo = 'RECOLECCION'),
  'synced',
  st.created_at,
  NOW()
FROM saga_transactions st
WHERE st.tipo = 'RECOLECCION'
  AND NOT EXISTS (SELECT 1 FROM saga_zoho_links szl WHERE szl.id_saga_transaction = st.id)
  AND EXISTS (SELECT 1 FROM movimientos_inventario m WHERE m.id_saga_transaction = st.id AND m.tipo = 'RECOLECCION');

-- 4b. MEXAF10018 LEV_POST_CORTE (saga ba1ae11f, Nov 15)
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo, items, zoho_sync_status, created_at, updated_at)
VALUES (
  'ba1ae11f-ff86-4afe-83a8-70213bad9de5',
  'DCOdV-32453',
  'BOTIQUIN'::tipo_zoho_link,
  '[{"sku":"P070","cantidad":1},{"sku":"P113","cantidad":1},{"sku":"Y399","cantidad":1}]'::jsonb,
  'pending',
  '2025-11-15 00:00:00+00',
  NOW()
);

-- ============================================================
-- PART 5: Backfill — sagas with 1 zoho_link
-- ============================================================

-- Excludes PERMANENCIA — only CREACION, VENTA, RECOLECCION
UPDATE movimientos_inventario m
SET id_saga_zoho_link = (
  SELECT szl.id FROM saga_zoho_links szl
  WHERE szl.id_saga_transaction = m.id_saga_transaction
)
WHERE m.tipo != 'PERMANENCIA'
  AND m.id_saga_transaction IN (
    SELECT id_saga_transaction FROM saga_zoho_links
    GROUP BY id_saga_transaction HAVING COUNT(*) = 1
  );

-- ============================================================
-- PART 6: Backfill — sagas with multiple zoho_links
-- ============================================================

-- Match by SKU in szl.items. Overlaps → first link by szl.id
UPDATE movimientos_inventario m
SET id_saga_zoho_link = (
  SELECT szl.id
  FROM saga_zoho_links szl
  WHERE szl.id_saga_transaction = m.id_saga_transaction
    AND szl.items IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM jsonb_array_elements(szl.items) elem
      WHERE elem->>'sku' = m.sku
    )
  ORDER BY szl.id
  LIMIT 1
)
WHERE m.id_saga_zoho_link IS NULL
  AND m.tipo != 'PERMANENCIA'
  AND m.id_saga_transaction IN (
    SELECT id_saga_transaction FROM saga_zoho_links
    GROUP BY id_saga_transaction HAVING COUNT(*) > 1
  );

-- ============================================================
-- PART 7: Update operational functions
-- ============================================================

-- 7.1 rpc_confirm_saga_pivot — anti-duplication for "already confirmed" path
CREATE OR REPLACE FUNCTION public.rpc_confirm_saga_pivot(p_saga_id uuid, p_zoho_id text DEFAULT NULL::text, p_zoho_items jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_zoho_link_id integer;
  v_task_tipo text;
  v_zoho_link_tipo tipo_zoho_link;
  v_item record;
  v_cantidad_antes integer;
  v_cantidad_despues integer;
  v_tipo_movimiento tipo_movimiento_botiquin;
  v_already_confirmed boolean := false;
BEGIN
  -- 1. Get and validate saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  IF v_saga.estado = 'CONFIRMADO' THEN
    v_already_confirmed := true;
    IF p_zoho_id IS NULL THEN
      -- No zoho_id, nothing to do
      SELECT id INTO v_zoho_link_id
      FROM public.saga_zoho_links
      WHERE id_saga_transaction = p_saga_id
      LIMIT 1;

      RETURN jsonb_build_object(
        'success', true,
        'already_confirmed', true,
        'saga_id', p_saga_id,
        'zoho_link_id', v_zoho_link_id
      );
    END IF;
    -- With zoho_id: skip state change, go directly to create link
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determine tipo de zoho_link, task_tipo and tipo_movimiento
  CASE v_saga.tipo::text
    WHEN 'LEVANTAMIENTO_INICIAL' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';
    WHEN 'LEV_POST_CORTE' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';
    WHEN 'VENTA' THEN
      v_zoho_link_tipo := 'VENTA';
      v_task_tipo := 'VENTA_ODV';
      v_tipo_movimiento := 'VENTA';
    WHEN 'RECOLECCION' THEN
      v_zoho_link_tipo := 'DEVOLUCION';
      v_task_tipo := 'RECOLECCION';
      v_tipo_movimiento := 'RECOLECCION';
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado: %', v_saga.tipo;
  END CASE;

  -- 3. Change saga state to CONFIRMADO (ONLY on first confirmation)
  IF NOT v_already_confirmed THEN
    UPDATE public.saga_transactions
    SET
      estado = 'CONFIRMADO'::estado_saga_transaction,
      updated_at = now()
    WHERE id = p_saga_id;
  END IF;

  -- 4. Create saga_zoho_link
  IF p_zoho_id IS NOT NULL THEN
    INSERT INTO public.saga_zoho_links (
      id_saga_transaction,
      zoho_id,
      tipo,
      items,
      zoho_sync_status,
      created_at,
      updated_at
    )
    VALUES (
      p_saga_id,
      p_zoho_id,
      v_zoho_link_tipo,
      COALESCE(p_zoho_items, v_saga.items),
      'pending',
      now(),
      now()
    )
    ON CONFLICT (id_saga_transaction, zoho_id) DO NOTHING
    RETURNING id INTO v_zoho_link_id;

    -- If ON CONFLICT hit, get the existing id
    IF v_zoho_link_id IS NULL THEN
      SELECT id INTO v_zoho_link_id
      FROM public.saga_zoho_links
      WHERE id_saga_transaction = p_saga_id AND zoho_id = p_zoho_id;
    END IF;
  END IF;

  -- 5. Create movements OR assign FK (anti-duplication)
  IF v_already_confirmed THEN
    -- ANTI-DUPLICATION: Do NOT create new movements
    -- Only assign FK to existing movements that match by SKU
    IF v_zoho_link_id IS NOT NULL THEN
      IF p_zoho_items IS NOT NULL THEN
        -- Link by SKU match from zoho_items
        UPDATE movimientos_inventario
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND tipo != 'PERMANENCIA'
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_zoho_items) elem
            WHERE elem->>'sku' = movimientos_inventario.sku
          );
      ELSE
        -- No specific items: link all unlinked non-PERMANENCIA movements
        UPDATE movimientos_inventario
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND tipo != 'PERMANENCIA';
      END IF;
    END IF;

    RETURN jsonb_build_object(
      'success', true,
      'already_confirmed', true,
      'saga_id', p_saga_id,
      'zoho_link_id', v_zoho_link_id
    );
  ELSE
    -- First confirmation: create movements normally (+ new id_saga_zoho_link field)
    FOR v_item IN
      SELECT
        (item->>'sku')::varchar as sku,
        (item->>'cantidad')::int as cantidad
      FROM jsonb_array_elements(v_saga.items) as item
    LOOP
      -- Get cantidad_antes
      SELECT COALESCE(cantidad_disponible, 0)
      INTO v_cantidad_antes
      FROM public.inventario_botiquin
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;

      IF v_cantidad_antes IS NULL THEN
        v_cantidad_antes := 0;
      END IF;

      -- Calculate cantidad_despues
      IF v_tipo_movimiento = 'CREACION' THEN
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      ELSE
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      END IF;

      -- Insert movement with id_saga_zoho_link
      INSERT INTO public.movimientos_inventario (
        id_saga_transaction,
        id_saga_zoho_link,
        id_cliente,
        sku,
        tipo,
        cantidad,
        cantidad_antes,
        cantidad_despues,
        fecha_movimiento
      )
      VALUES (
        p_saga_id,
        v_zoho_link_id,
        v_saga.id_cliente,
        v_item.sku,
        v_tipo_movimiento,
        v_item.cantidad,
        v_cantidad_antes,
        v_cantidad_despues,
        now()
      );

      -- Update inventario_botiquin
      IF v_cantidad_despues > 0 THEN
        INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
        VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues, now())
        ON CONFLICT (id_cliente, sku)
        DO UPDATE SET
          cantidad_disponible = v_cantidad_despues,
          ultima_actualizacion = now();
      ELSE
        DELETE FROM public.inventario_botiquin
        WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
      END IF;

      -- If VENTA, remove from botiquin_clientes_sku_disponibles
      IF v_saga.tipo::text = 'VENTA' THEN
        DELETE FROM public.botiquin_clientes_sku_disponibles
        WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
      END IF;
    END LOOP;
  END IF;

  -- 6. Update visit_tasks (ONLY on first confirmation)
  IF NOT v_already_confirmed THEN
    IF v_zoho_link_id IS NOT NULL THEN
      UPDATE public.visit_tasks
      SET
        estado = 'COMPLETADO',
        completed_at = COALESCE(completed_at, now()),
        reference_table = 'saga_zoho_links',
        reference_id = v_zoho_link_id::text,
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_tipo = v_task_tipo::visit_task_tipo;
    ELSE
      UPDATE public.visit_tasks
      SET
        estado = 'COMPLETADO',
        completed_at = COALESCE(completed_at, now()),
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_tipo = v_task_tipo::visit_task_tipo;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'zoho_link_id', v_zoho_link_id,
    'tipo', v_saga.tipo,
    'items_count', jsonb_array_length(v_saga.items)
  );
END;
$function$;

-- 7.2 rebuild_movimientos_inventario — include id_saga_zoho_link resolution
-- DROP first: PROD return type has 'inventario_actualizado', we rename to 'inventario_final'
DROP FUNCTION IF EXISTS public.rebuild_movimientos_inventario();
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
  mov_count BIGINT := 0;
  inv_count BIGINT := 0;
  v_zoho_link_id INTEGER;
  v_link_count INTEGER;
BEGIN
  TRUNCATE TABLE movimientos_inventario RESTART IDENTITY CASCADE;
  TRUNCATE TABLE inventario_botiquin RESTART IDENTITY CASCADE;

  FOR saga_rec IN
    SELECT id, id_cliente, created_at, items
    FROM saga_transactions
    WHERE items IS NOT NULL
    ORDER BY created_at, id
  LOOP
    -- Pre-count links for this saga
    SELECT COUNT(*) INTO v_link_count
    FROM saga_zoho_links WHERE id_saga_transaction = saga_rec.id;

    FOR item_rec IN
      SELECT
        item->>'sku' as sku,
        (item->>'cantidad')::int as cantidad,
        item->>'tipo_movimiento' as tipo_movimiento
      FROM jsonb_array_elements(saga_rec.items) as item
      WHERE item->>'tipo_movimiento' != 'PERMANENCIA'
    LOOP
      IF item_rec.tipo_movimiento = 'CREACION' THEN
        tipo_mov := 'CREACION';
      ELSIF item_rec.tipo_movimiento = 'VENTA' THEN
        tipo_mov := 'VENTA';
      ELSIF item_rec.tipo_movimiento = 'RECOLECCION' THEN
        tipo_mov := 'RECOLECCION';
      ELSE
        CONTINUE;
      END IF;

      SELECT COALESCE(cantidad_disponible, 0)
      INTO current_stock
      FROM inventario_botiquin
      WHERE id_cliente = saga_rec.id_cliente AND sku = item_rec.sku;

      IF current_stock IS NULL THEN
        current_stock := 0;
      END IF;

      IF tipo_mov = 'CREACION' THEN
        new_stock := current_stock + item_rec.cantidad;
      ELSE
        new_stock := current_stock - item_rec.cantidad;
      END IF;

      -- Resolve id_saga_zoho_link
      v_zoho_link_id := NULL;
      IF v_link_count = 1 THEN
        SELECT szl.id INTO v_zoho_link_id
        FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = saga_rec.id;
      ELSIF v_link_count > 1 THEN
        SELECT szl.id INTO v_zoho_link_id
        FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = saga_rec.id
          AND szl.items IS NOT NULL
          AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = item_rec.sku)
        ORDER BY szl.id LIMIT 1;
      END IF;

      INSERT INTO movimientos_inventario (
        id_saga_transaction,
        id_saga_zoho_link,
        id_cliente,
        sku,
        tipo,
        cantidad,
        cantidad_antes,
        cantidad_despues,
        fecha_movimiento
      ) VALUES (
        saga_rec.id,
        v_zoho_link_id,
        saga_rec.id_cliente,
        item_rec.sku,
        tipo_mov,
        item_rec.cantidad,
        current_stock,
        new_stock,
        saga_rec.created_at
      );

      mov_count := mov_count + 1;

      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
      VALUES (saga_rec.id_cliente, item_rec.sku, new_stock)
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET cantidad_disponible = new_stock;

    END LOOP;
  END LOOP;

  SELECT COUNT(*) INTO inv_count FROM inventario_botiquin;

  RETURN QUERY SELECT mov_count, inv_count;
END;
$function$;

-- 7.3 regenerar_movimientos_desde_saga — include id_saga_zoho_link resolution
CREATE OR REPLACE FUNCTION public.regenerar_movimientos_desde_saga(p_saga_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_tipo_movimiento tipo_movimiento_botiquin;
  v_zoho_link_id integer;
  v_link_count integer;
BEGIN
  SELECT * INTO v_saga FROM saga_transactions WHERE id = p_saga_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'SAGA % no encontrada', p_saga_id;
  END IF;

  -- Pre-count links for this saga
  SELECT COUNT(*) INTO v_link_count
  FROM saga_zoho_links WHERE id_saga_transaction = p_saga_id;

  FOR v_item IN
    SELECT
      item->>'sku' as sku,
      (item->>'cantidad')::int as cantidad,
      item->>'tipo_movimiento' as tipo_movimiento
    FROM jsonb_array_elements(v_saga.items) as item
  LOOP
    SELECT COALESCE(cantidad_disponible, 0)
    INTO v_cantidad_antes
    FROM inventario_botiquin
    WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;

    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    CASE v_item.tipo_movimiento
      WHEN 'CREACION' THEN
        v_tipo_movimiento := 'CREACION';
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      WHEN 'VENTA' THEN
        v_tipo_movimiento := 'VENTA';
        v_cantidad_despues := v_cantidad_antes - v_item.cantidad;
      WHEN 'RECOLECCION' THEN
        v_tipo_movimiento := 'RECOLECCION';
        v_cantidad_despues := v_cantidad_antes - v_item.cantidad;
      WHEN 'PERMANENCIA' THEN
        CONTINUE;
      ELSE
        RAISE EXCEPTION 'Tipo de movimiento desconocido: %', v_item.tipo_movimiento;
    END CASE;

    -- Resolve id_saga_zoho_link
    v_zoho_link_id := NULL;
    IF v_link_count = 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = p_saga_id;
    ELSIF v_link_count > 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = p_saga_id
        AND szl.items IS NOT NULL
        AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = v_item.sku)
      ORDER BY szl.id LIMIT 1;
    END IF;

    INSERT INTO movimientos_inventario (
      id_saga_transaction,
      id_saga_zoho_link,
      id_cliente,
      sku,
      tipo,
      cantidad,
      cantidad_antes,
      cantidad_despues,
      fecha_movimiento
    ) VALUES (
      p_saga_id,
      v_zoho_link_id,
      v_saga.id_cliente,
      v_item.sku,
      v_tipo_movimiento,
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      v_saga.created_at
    );

    INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
    VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues)
    ON CONFLICT (id_cliente, sku)
    DO UPDATE SET cantidad_disponible = v_cantidad_despues;
  END LOOP;
END;
$function$;

-- 7.4 trigger_generate_movements_from_saga — update signature for compatibility
CREATE OR REPLACE FUNCTION public.trigger_generate_movements_from_saga()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_tipo_movimiento tipo_movimiento_botiquin;
  v_zoho_link_id integer;
  v_link_count integer;
BEGIN
  -- Only process if estado is CONFIRMADO
  IF NEW.estado != 'CONFIRMADO' THEN
    RETURN NEW;
  END IF;

  -- Skip if this saga already has movements (avoid duplicates)
  IF EXISTS (SELECT 1 FROM movimientos_inventario WHERE id_saga_transaction = NEW.id) THEN
    RETURN NEW;
  END IF;

  -- Skip if no items
  IF NEW.items IS NULL OR jsonb_array_length(NEW.items) = 0 THEN
    RETURN NEW;
  END IF;

  -- Pre-count links for this saga
  SELECT COUNT(*) INTO v_link_count
  FROM saga_zoho_links WHERE id_saga_transaction = NEW.id;

  -- Process each item in the saga
  FOR v_item IN
    SELECT
      item->>'sku' as sku,
      (item->>'cantidad')::int as cantidad,
      item->>'tipo_movimiento' as tipo_movimiento
    FROM jsonb_array_elements(NEW.items) as item
  LOOP
    -- Skip PERMANENCIA movements
    IF v_item.tipo_movimiento = 'PERMANENCIA' THEN
      CONTINUE;
    END IF;

    -- Get cantidad_antes
    SELECT COALESCE(cantidad_despues, 0)
    INTO v_cantidad_antes
    FROM movimientos_inventario
    WHERE id_cliente = NEW.id_cliente
      AND sku = v_item.sku
    ORDER BY fecha_movimiento DESC, id DESC
    LIMIT 1;

    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    -- Determine movement type
    CASE v_item.tipo_movimiento
      WHEN 'CREACION' THEN
        v_tipo_movimiento := 'CREACION';
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      WHEN 'VENTA' THEN
        v_tipo_movimiento := 'VENTA';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      WHEN 'RECOLECCION' THEN
        v_tipo_movimiento := 'RECOLECCION';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      ELSE
        CONTINUE;
    END CASE;

    -- Resolve id_saga_zoho_link
    v_zoho_link_id := NULL;
    IF v_link_count = 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = NEW.id;
    ELSIF v_link_count > 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = NEW.id
        AND szl.items IS NOT NULL
        AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = v_item.sku)
      ORDER BY szl.id LIMIT 1;
    END IF;

    -- Insert movement
    INSERT INTO movimientos_inventario (
      id_saga_transaction,
      id_saga_zoho_link,
      id_cliente,
      sku,
      tipo,
      cantidad,
      cantidad_antes,
      cantidad_despues,
      fecha_movimiento
    ) VALUES (
      NEW.id,
      v_zoho_link_id,
      NEW.id_cliente,
      v_item.sku,
      v_tipo_movimiento,
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      COALESCE(NEW.created_at, now())
    );

    -- Update inventario_botiquin
    IF v_cantidad_despues > 0 THEN
      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      VALUES (NEW.id_cliente, v_item.sku, v_cantidad_despues, now())
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET
        cantidad_disponible = v_cantidad_despues,
        ultima_actualizacion = now();
    ELSE
      DELETE FROM inventario_botiquin
      WHERE id_cliente = NEW.id_cliente AND sku = v_item.sku;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$function$;

-- ============================================================
-- PART 8: Update analytics RPCs
-- ============================================================

-- 8.1 get_auditoria_cliente — use direct id_saga_zoho_link join for graph edges
CREATE OR REPLACE FUNCTION analytics.get_auditoria_cliente(p_cliente varchar)
RETURNS json
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_cliente json;
  v_visitas json;
  v_ciclo json;
  v_grafo_nodos json;
  v_grafo_aristas json;
  v_resumen json;
  v_anomalias_count int := 0;
BEGIN
  -- 1. Client Info
  SELECT json_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)
  INTO v_cliente
  FROM clientes c
  WHERE c.id_cliente = p_cliente;

  IF v_cliente IS NULL THEN
    RETURN json_build_object('error', 'Cliente no encontrado');
  END IF;

  -- Temp: ordered visits for this client
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    MIN(m.fecha_movimiento)::date as fecha_visita,
    COALESCE(v.tipo::text, 'DESCONOCIDO') as tipo_visita,
    ROW_NUMBER() OVER (ORDER BY MIN(m.fecha_movimiento), v.visit_id) as visit_num
  FROM visitas v
  JOIN saga_transactions st ON st.visit_id = v.visit_id
  JOIN movimientos_inventario m ON m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
  GROUP BY v.visit_id, v.tipo;

  -- 2. Visitas with sagas, movements, anomalies
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'fecha', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'visita_tipo', av.tipo_visita,
      'sagas', (
        SELECT COALESCE(json_agg(sr ORDER BY sr->>'saga_tipo'), '[]'::json)
        FROM (
          SELECT json_build_object(
            'saga_tipo', st.tipo::text,
            'saga_estado', st.estado::text,
            'odv_botiquin', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.tipo = 'BOTIQUIN'
                AND szl.zoho_id IS NOT NULL
            ),
            'sync_status', (
              SELECT szl.zoho_sync_status
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.zoho_id IS NOT NULL
              LIMIT 1
            ),
            'movimientos', (
              SELECT COALESCE(json_agg(
                json_build_object(
                  'mov_id', m.id,
                  'sku', m.sku,
                  'producto', med.producto,
                  'tipo', m.tipo::text,
                  'cantidad', m.cantidad,
                  'fecha', TO_CHAR(m.fecha_movimiento, 'YYYY-MM-DD'),
                  'zoho_link_id', m.id_saga_zoho_link,
                  'odv', (SELECT szl2.zoho_id FROM saga_zoho_links szl2 WHERE szl2.id = m.id_saga_zoho_link)
                ) ORDER BY m.sku, m.tipo
              ), '[]'::json)
              FROM movimientos_inventario m
              JOIN medicamentos med ON m.sku = med.sku
              WHERE m.id_saga_transaction = st.id
                AND m.id_cliente = p_cliente
            ),
            'anomalias', (
              SELECT COALESCE(json_agg(d.msg), '[]'::json)
              FROM (
                SELECT 'MOVIMIENTO_DUPLICADO: ' || m.sku || ' ' || m.tipo::text
                       || ' aparece ' || COUNT(*) || ' veces' as msg
                FROM movimientos_inventario m
                WHERE m.id_saga_transaction = st.id
                  AND m.id_cliente = p_cliente
                GROUP BY m.sku, m.tipo
                HAVING COUNT(*) > 1
              ) d
            )
          ) as sr
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )
        ) saga_sub
      ),
      'inventario_piezas', (
        SELECT COALESCE(SUM(
          CASE m.tipo
            WHEN 'CREACION' THEN m.cantidad
            WHEN 'VENTA' THEN -m.cantidad
            WHEN 'RECOLECCION' THEN -m.cantidad
            ELSE 0
          END
        ), 0)
        FROM movimientos_inventario m
        JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m.id_cliente = p_cliente
          AND av2.fecha_visita <= av.fecha_visita
          AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
      ),
      'inventario_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM movimientos_inventario m
          JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m.id_cliente = p_cliente
            AND av2.fecha_visita <= av.fecha_visita
            AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
          GROUP BY m.sku
          HAVING SUM(
            CASE m.tipo
              WHEN 'CREACION' THEN m.cantidad
              WHEN 'VENTA' THEN -m.cantidad
              WHEN 'RECOLECCION' THEN -m.cantidad
              ELSE 0
            END
          ) > 0
        ) sc
      ),
      'anomalias', (
        SELECT COALESCE(json_agg(va.msg), '[]'::json)
        FROM (
          SELECT 'PERMANENCIA_DUPLICADA: ' || COUNT(*)
                 || ' sagas PERMANENCIA' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.tipo = 'PERMANENCIA'
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )
          HAVING COUNT(*) > 1

          UNION ALL

          SELECT 'ODV_MISSING: saga ' || st.tipo::text || ' sin ODV BOTIQUIN' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.tipo IN ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION', 'LEV_POST_CORTE')
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id
                AND m.id_cliente = p_cliente
                AND m.tipo = 'CREACION'
            )
            AND NOT EXISTS (
              SELECT 1 FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.tipo = 'BOTIQUIN'
                AND szl.zoho_id IS NOT NULL
            )

          UNION ALL

          SELECT 'SYNC_PENDING: ODV ' || szl.zoho_id
                 || ' status=' || COALESCE(szl.zoho_sync_status, 'null') as msg
          FROM saga_zoho_links szl
          JOIN saga_transactions st ON szl.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND szl.zoho_id IS NOT NULL
            AND COALESCE(szl.zoho_sync_status, '') != 'synced'
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )

          UNION ALL

          SELECT 'VTA_SIN_CREACION: ' || m.sku || ' ' || m.tipo::text
                 || ' sin CREACION previa en botiquin' as msg
          FROM movimientos_inventario m
          JOIN saga_transactions st ON m.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND m.id_cliente = p_cliente
            AND m.tipo IN ('VENTA', 'RECOLECCION')
            AND NOT EXISTS (
              SELECT 1
              FROM movimientos_inventario m2
              JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
              JOIN _av av2 ON st2.visit_id = av2.visit_id
              WHERE m2.id_cliente = p_cliente
                AND m2.sku = m.sku
                AND m2.tipo = 'CREACION'
                AND av2.visit_num <= av.visit_num
            )
        ) va
      )
    ) as vr
    FROM _av av
  ) visit_rows;

  -- 3. SKU Lifecycle — use direct id_saga_zoho_link for ODV
  SELECT COALESCE(json_agg(sr ORDER BY sr->>'sku'), '[]'::json)
  INTO v_ciclo
  FROM (
    SELECT json_build_object(
      'sku', sub.sku,
      'producto', sub.producto,
      'eventos', sub.eventos,
      'estado_actual', CASE
        WHEN sub.last_tipo = 'RECOLECCION' THEN 'RECOLECTADO'
        WHEN sub.last_tipo = 'VENTA' THEN 'VENDIDO'
        ELSE 'ACTIVO'
      END
    ) as sr
    FROM (
      SELECT
        m.sku,
        MAX(med.producto) as producto,
        json_agg(
          json_build_object(
            'visit_num', av.visit_num,
            'fecha', TO_CHAR(m.fecha_movimiento, 'YYYY-MM-DD'),
            'tipo', m.tipo::text,
            'cantidad', m.cantidad,
            'odv', (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.id = m.id_saga_zoho_link)
          ) ORDER BY av.visit_num, m.tipo
        ) as eventos,
        (
          SELECT m2.tipo::text
          FROM movimientos_inventario m2
          JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m2.id_cliente = p_cliente AND m2.sku = m.sku
            AND m2.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
          ORDER BY av2.visit_num DESC, m2.fecha_movimiento DESC
          LIMIT 1
        ) as last_tipo
      FROM movimientos_inventario m
      JOIN medicamentos med ON m.sku = med.sku
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
      GROUP BY m.sku
    ) sub
  ) ciclo_rows;

  -- 4. Graph Nodes
  SELECT COALESCE(json_agg(n ORDER BY n->>'id'), '[]'::json)
  INTO v_grafo_nodos
  FROM (
    -- Visit nodes
    SELECT json_build_object(
      'id', 'v' || av.visit_num,
      'tipo', 'visita',
      'visit_num', av.visit_num,
      'fecha', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'label', 'V' || av.visit_num || ' ' || TO_CHAR(av.fecha_visita, 'Mon DD'),
      'visita_tipo', av.tipo_visita
    ) as n
    FROM _av av

    UNION ALL

    -- ODV nodes — now directly from movimientos via id_saga_zoho_link
    SELECT json_build_object(
      'id', 'odv-' || szl.zoho_id,
      'tipo', 'odv',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'piezas', SUM(m.cantidad),
      'skus_count', COUNT(DISTINCT m.sku)
    ) as n
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND szl.tipo = 'BOTIQUIN'
      AND szl.zoho_id IS NOT NULL
      AND m.tipo = 'CREACION'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- SKU nodes (deduplicated)
    SELECT DISTINCT ON (m.sku)
      json_build_object(
        'id', 'sku-' || m.sku,
        'tipo', 'sku',
        'label', m.sku,
        'producto', med.producto
      ) as n
    FROM movimientos_inventario m
    JOIN medicamentos med ON m.sku = med.sku
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
  ) all_nodes;

  -- 5. Graph Edges — CREACION uses direct id_saga_zoho_link
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- CREACION: ODV → Visit (aggregate per ODV using direct FK)
    SELECT json_build_object(
      'source', 'odv-' || szl.zoho_id,
      'target', 'v' || av.visit_num,
      'tipo', 'CREACION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND m.tipo = 'CREACION'
      AND szl.tipo = 'BOTIQUIN'
      AND szl.zoho_id IS NOT NULL
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- PERMANENCIA: Visit → Next Visit (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'v' || (av.visit_num + 1),
      'tipo', 'PERMANENCIA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'PERMANENCIA'
      AND EXISTS (SELECT 1 FROM _av av2 WHERE av2.visit_num = av.visit_num + 1)
    GROUP BY av.visit_num

    UNION ALL

    -- RECOLECCION: Visit → sink (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'rec-v' || av.visit_num,
      'tipo', 'RECOLECCION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'RECOLECCION'
    GROUP BY av.visit_num

    UNION ALL

    -- VENTA: Visit → sink (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'vta-v' || av.visit_num,
      'tipo', 'VENTA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'VENTA'
    GROUP BY av.visit_num

    UNION ALL

    -- SKU-level: each SKU's movement at each visit
    SELECT json_build_object(
      'source', CASE
        WHEN m.tipo = 'CREACION' THEN 'sku-' || m.sku
        ELSE 'v' || av.visit_num
      END,
      'target', CASE
        WHEN m.tipo = 'CREACION' THEN 'v' || av.visit_num
        ELSE 'sku-' || m.sku
      END,
      'tipo', 'sku_' || LOWER(m.tipo::text),
      'label', SUBSTR(m.tipo::text, 1, 3) || '(' || SUM(m.cantidad) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'cantidad', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
    GROUP BY m.sku, m.tipo, av.visit_num
  ) all_edges;

  -- 6. Count anomalies
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.tipo = 'PERMANENCIA'
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )
    GROUP BY av.visit_id
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
    GROUP BY st.id, m.sku, m.tipo
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.tipo IN ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION', 'LEV_POST_CORTE')
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id
          AND m.id_cliente = p_cliente
          AND m.tipo = 'CREACION'
      )
      AND NOT EXISTS (
        SELECT 1 FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = st.id
          AND szl.tipo = 'BOTIQUIN'
          AND szl.zoho_id IS NOT NULL
      )

    UNION ALL

    SELECT 1
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND m.tipo IN ('VENTA', 'RECOLECCION')
      AND NOT EXISTS (
        SELECT 1
        FROM movimientos_inventario m2
        JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m2.id_cliente = p_cliente
          AND m2.sku = m.sku
          AND m2.tipo = 'CREACION'
          AND av2.visit_num <= av.visit_num
      )
    GROUP BY m.sku

    UNION ALL

    SELECT 1
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE szl.zoho_id IS NOT NULL
      AND COALESCE(szl.zoho_sync_status, '') != 'synced'
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )
  ) anomalies;

  -- 7. Summary
  SELECT json_build_object(
    'total_visitas', (SELECT COUNT(*) FROM _av),
    'total_skus_historico', (
      SELECT COUNT(DISTINCT m.sku)
      FROM movimientos_inventario m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
    ),
    'inventario_actual_piezas', (
      SELECT COALESCE(SUM(
        CASE m.tipo
          WHEN 'CREACION' THEN m.cantidad
          WHEN 'VENTA' THEN -m.cantidad
          WHEN 'RECOLECCION' THEN -m.cantidad
          ELSE 0
        END
      ), 0)
      FROM movimientos_inventario m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
        AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
    ),
    'inventario_actual_skus', (
      SELECT COUNT(*) FROM (
        SELECT m.sku
        FROM movimientos_inventario m
        JOIN saga_transactions st ON m.id_saga_transaction = st.id
        JOIN _av av ON st.visit_id = av.visit_id
        WHERE m.id_cliente = p_cliente
          AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
        GROUP BY m.sku
        HAVING SUM(
          CASE m.tipo
            WHEN 'CREACION' THEN m.cantidad
            WHEN 'VENTA' THEN -m.cantidad
            WHEN 'RECOLECCION' THEN -m.cantidad
            ELSE 0
          END
        ) > 0
      ) active
    ),
    'total_anomalias', v_anomalias_count,
    'todas_odv_botiquin', (
      SELECT COALESCE(json_agg(DISTINCT szl.zoho_id ORDER BY szl.zoho_id), '[]'::json)
      FROM saga_zoho_links szl
      JOIN saga_transactions st ON szl.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE szl.zoho_id IS NOT NULL AND szl.tipo = 'BOTIQUIN'
        AND EXISTS (
          SELECT 1 FROM movimientos_inventario m
          WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
        )
    )
  ) INTO v_resumen;

  -- 8. Return combined result
  RETURN json_build_object(
    'cliente', v_cliente,
    'visitas', COALESCE(v_visitas, '[]'::json),
    'ciclo_vida_skus', COALESCE(v_ciclo, '[]'::json),
    'grafo', json_build_object(
      'nodos', COALESCE(v_grafo_nodos, '[]'::json),
      'aristas', COALESCE(v_grafo_aristas, '[]'::json)
    ),
    'resumen', v_resumen
  );
END;
$$;

-- Public wrapper (VOLATILE because inner fn uses temp tables)
CREATE OR REPLACE FUNCTION public.get_auditoria_cliente(p_cliente varchar)
RETURNS json
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_auditoria_cliente(p_cliente);
$$;

-- 8.2 get_corte_logistica_data — simplified ODV tracing via id_saga_zoho_link
CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text,
  nombre_cliente character varying,
  id_cliente character varying,
  fecha_visita text,
  sku character varying,
  producto character varying,
  cantidad_colocada integer,
  qty_venta integer,
  qty_recoleccion integer,
  total_corte integer,
  destino text,
  saga_estado text,
  odv_botiquin text,
  odv_venta text,
  recoleccion_id uuid,
  recoleccion_estado text,
  evidencia_paths text[],
  firma_path text,
  observaciones text,
  quien_recibio text
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
#variable_conflict use_column
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  IF p_fecha_inicio IS NOT NULL AND p_fecha_fin IS NOT NULL THEN
    v_fecha_inicio := p_fecha_inicio;
    v_fecha_fin := p_fecha_fin;
  ELSE
    SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
    FROM get_corte_actual_rango() r;
  END IF;

  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT
    u.nombre::text                                                          AS nombre_asesor,
    c.nombre_cliente,
    mov.id_cliente,
    TO_CHAR(mov.fecha_movimiento, 'YYYY-MM-DD')                            AS fecha_visita,
    mov.sku,
    med.producto,
    -- Cantidad colocada: CREACION de ese SKU en la misma visita
    (SELECT COALESCE(SUM(m_cre.cantidad), 0)
     FROM movimientos_inventario m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = st.visit_id
       AND m_cre.id_cliente = mov.id_cliente
       AND m_cre.sku = mov.sku
       AND m_cre.tipo = 'CREACION')::int                                    AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquin: trace per-SKU to most recent CREACION with a BOTIQUIN ODV (cross-visit)
    -- Uses direct id_saga_zoho_link where available, falls back to indirect tracing
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_zoho_link
       FROM movimientos_inventario m_cre
       WHERE m_cre.id_cliente = mov.id_cliente
         AND m_cre.sku = mov.sku
         AND m_cre.tipo = 'CREACION'
         AND m_cre.id_saga_zoho_link IS NOT NULL
         AND m_cre.fecha_movimiento <=
           CASE WHEN mov.tipo = 'RECOLECCION'
             THEN mov.fecha_movimiento - interval '1 second'
             ELSE mov.fecha_movimiento
           END
       ORDER BY m_cre.fecha_movimiento DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id = latest_cre.id_saga_zoho_link
       AND szl.tipo = 'BOTIQUIN'
       AND szl.zoho_id IS NOT NULL)                                         AS odv_botiquin,
    -- ODV Venta: direct via id_saga_zoho_link
    (SELECT szl.zoho_id
     FROM saga_zoho_links szl
     WHERE szl.id = mov.id_saga_zoho_link
       AND szl.tipo = 'VENTA')                                             AS odv_venta,
    rcl.recoleccion_id,
    rcl.estado::text                                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM recolecciones_evidencias re
     WHERE re.recoleccion_id = rcl.recoleccion_id)                         AS evidencia_paths,
    (SELECT rf.storage_path
     FROM recolecciones_firmas rf
     WHERE rf.recoleccion_id = rcl.recoleccion_id
     LIMIT 1)                                                              AS firma_path,
    rcl.cedis_observaciones                                                AS observaciones,
    rcl.cedis_responsable_nombre                                           AS quien_recibio
  FROM movimientos_inventario mov
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN saga_transactions st ON mov.id_saga_transaction = st.id
  LEFT JOIN visitas v            ON st.visit_id = v.visit_id
  LEFT JOIN usuarios u           ON v.id_usuario = u.id_usuario
  LEFT JOIN recolecciones rcl    ON v.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo IN ('VENTA', 'RECOLECCION')
    AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY mov.fecha_movimiento DESC, c.nombre_cliente, mov.sku;
END;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text,
  nombre_cliente character varying,
  id_cliente character varying,
  fecha_visita text,
  sku character varying,
  producto character varying,
  cantidad_colocada integer,
  qty_venta integer,
  qty_recoleccion integer,
  total_corte integer,
  destino text,
  saga_estado text,
  odv_botiquin text,
  odv_venta text,
  recoleccion_id uuid,
  recoleccion_estado text,
  evidencia_paths text[],
  firma_path text,
  observaciones text,
  quien_recibio text
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- 8.3 get_corte_historico_data — no structural changes needed, ODV lookup is optional
-- The join by id_saga_transaction is maintained for visitas_base CTE.
-- No changes to this function as it doesn't use saga_zoho_links directly.

-- ============================================================
-- PART 9: Clean up ghost PERMANENCIA records
-- ============================================================
-- 38 PERMANENCIA movimientos with cantidad=0 in VENTA/RECOLECCION sagas.
-- These were created by a previous code path that snapshotted existing inventory
-- during confirmations, but they carry zero information (cantidad_antes = cantidad_despues).
-- They don't exist in the saga items and clutter the auditoria view.
DELETE FROM movimientos_inventario
WHERE tipo = 'PERMANENCIA' AND cantidad = 0;

-- ============================================================
-- PART 10: Fix SYNC_PENDING — 70 links with DCOdV zoho_ids
-- ============================================================
-- These saga_zoho_links have real zoho_ids (DCOdV-*) assigned by Zoho,
-- meaning they were successfully synced, but zoho_sync_status was never
-- updated from 'pending' to 'synced'. Pure data fix, no schema changes.
UPDATE saga_zoho_links
SET zoho_sync_status = 'synced', updated_at = NOW()
WHERE zoho_id LIKE 'DCOdV-%'
  AND zoho_sync_status = 'pending';

-- ============================================================
-- PART 11: Fix LEV_POST_CORTE saga items for MEXPF13496 (10/15)
-- ============================================================
-- Saga 0d48541a had 12 items (copy of original botiquín) but legacy data
-- (development.lotes_botiquin group 136) confirms the 10/15 renovation was
-- only S402:3 + P040:5. The saga has 0 movimientos so this is safe.
UPDATE saga_transactions
SET items = '[{"sku":"S402","cantidad":3,"producto":"Solución Capilar Minoxidil con Finasterida"},{"sku":"P040","cantidad":5,"producto":"Astrinderm A"}]'::jsonb,
    updated_at = NOW()
WHERE id = '0d48541a-b0ba-468a-aa51-a6c408072cf4';

-- ============================================================
-- PART 12: Fix MEXPF13496 visit assignment — move new-botiquín
--          RECOLECCION + VENTA from visit 3 (10/30) to visit 4 (11/15)
-- ============================================================
-- Legacy data confirms the recolección of the new botiquín (DCOdV-33446)
-- happened on 11/15, not 10/30. The saga was mis-assigned to visit 3.
-- Same for VENTA of P077:2 + P165:1 (DCOdV-34390) dated 11/15.

-- 12a: Move RECOLECCION saga to visit 4
UPDATE saga_transactions
SET visit_id = '3c5c0567-e12f-43a6-b351-47da15b35c4b',
    updated_at = NOW()
WHERE id = '96ee9b36-f368-416c-8da3-8892bd9cdb9d';

-- 12b: Update fecha_movimiento on its movimientos
UPDATE movimientos_inventario
SET fecha_movimiento = '2025-11-15'
WHERE id_saga_transaction = '96ee9b36-f368-416c-8da3-8892bd9cdb9d';

-- 12c: Move P077/P165 VENTA movimientos to VENTA saga on visit 4
UPDATE movimientos_inventario
SET id_saga_transaction = 'a527da97-93f6-4d6a-9a71-ed4d8be1d68c'
WHERE id IN (2642, 2643);

-- 12d: Move zoho_link 119 (DCOdV-34390) to VENTA saga on visit 4
UPDATE saga_zoho_links
SET id_saga_transaction = 'a527da97-93f6-4d6a-9a71-ed4d8be1d68c',
    updated_at = NOW()
WHERE id = 119;

-- 12e: Fix items on both VENTA sagas
UPDATE saga_transactions
SET items = '[{"sku":"R846","cantidad":1},{"sku":"P032","cantidad":1}]'::jsonb,
    updated_at = NOW()
WHERE id = 'b6642345-8291-4794-8cbc-4e34bd1846a2';

UPDATE saga_transactions
SET items = '[{"sku":"P077","cantidad":2},{"sku":"P165","cantidad":1}]'::jsonb,
    updated_at = NOW()
WHERE id = 'a527da97-93f6-4d6a-9a71-ed4d8be1d68c';

-- ============================================================
-- PART 13: Rebuild inventario_botiquin from movimientos
-- ============================================================
-- Migration-inserted movimientos bypassed triggers, causing inventario_botiquin
-- to have phantom stock (old botiquín not zeroed) and missing stock (new botiquín
-- not added). Recalculate from CREACION - VENTA - RECOLECCION for all clients.

-- 13a: Fix existing rows
UPDATE inventario_botiquin ib
SET cantidad_disponible = COALESCE(c.qty, 0),
    ultima_actualizacion = NOW()
FROM (
  SELECT id_cliente, sku,
    GREATEST(0,
      SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo IN ('VENTA','RECOLECCION') THEN cantidad ELSE 0 END)
    ) as qty
  FROM movimientos_inventario
  WHERE tipo IN ('CREACION','VENTA','RECOLECCION')
  GROUP BY id_cliente, sku
) c
WHERE ib.id_cliente = c.id_cliente
  AND ib.sku = c.sku
  AND ib.cantidad_disponible != c.qty;

-- 13b: Zero out rows with no CREACION movimientos
UPDATE inventario_botiquin ib
SET cantidad_disponible = 0, ultima_actualizacion = NOW()
WHERE cantidad_disponible > 0
  AND NOT EXISTS (
    SELECT 1 FROM movimientos_inventario m
    WHERE m.id_cliente = ib.id_cliente AND m.sku = ib.sku AND m.tipo = 'CREACION'
  );

-- 13c: Insert missing rows
INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
SELECT c.id_cliente, c.sku, c.qty, NOW()
FROM (
  SELECT id_cliente, sku,
    GREATEST(0,
      SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo IN ('VENTA','RECOLECCION') THEN cantidad ELSE 0 END)
    ) as qty
  FROM movimientos_inventario
  WHERE tipo IN ('CREACION','VENTA','RECOLECCION')
  GROUP BY id_cliente, sku
  HAVING SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo IN ('VENTA','RECOLECCION') THEN cantidad ELSE 0 END) > 0
) c
WHERE NOT EXISTS (
  SELECT 1 FROM inventario_botiquin ib
  WHERE ib.id_cliente = c.id_cliente AND ib.sku = c.sku
);

-- ============================================================
-- PART 14: Populate botiquin_odv and ventas_odv from saga_zoho_links
-- ============================================================
-- ODVs created through the saga system were never inserted into botiquin_odv
-- or ventas_odv (those tables are populated from CSV/Zoho imports). Insert
-- the missing records so every CREACION and VENTA movimiento with a DCOdV
-- zoho_link has a corresponding row in the ODV tables.

-- 14a: Insert missing CREACION → botiquin_odv
INSERT INTO botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
SELECT DISTINCT
  m.id_cliente, m.sku, szl.zoho_id,
  m.fecha_movimiento::date, m.cantidad, 'Facturado'
FROM movimientos_inventario m
JOIN saga_zoho_links szl ON m.id_saga_zoho_link = szl.id
WHERE m.tipo = 'CREACION'
  AND szl.zoho_id LIKE 'DCOdV-%'
  AND NOT EXISTS (
    SELECT 1 FROM botiquin_odv bo
    WHERE bo.odv_id = szl.zoho_id AND bo.sku = m.sku AND bo.id_cliente = m.id_cliente
  );

-- 14b: Insert missing VENTA → ventas_odv (with precio from medicamentos)
INSERT INTO ventas_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura, precio)
SELECT DISTINCT
  m.id_cliente, m.sku, szl.zoho_id,
  m.fecha_movimiento::date, m.cantidad, 'Facturado', med.precio
FROM movimientos_inventario m
JOIN saga_zoho_links szl ON m.id_saga_zoho_link = szl.id
LEFT JOIN medicamentos med ON m.sku = med.sku
WHERE m.tipo = 'VENTA'
  AND szl.zoho_id LIKE 'DCOdV-%'
  AND NOT EXISTS (
    SELECT 1 FROM ventas_odv vo
    WHERE vo.odv_id = szl.zoho_id AND vo.sku = m.sku AND vo.id_cliente = m.id_cliente
  );

-- ============================================================
-- Reload PostgREST schema cache
-- ============================================================
NOTIFY pgrst, 'reload schema';
