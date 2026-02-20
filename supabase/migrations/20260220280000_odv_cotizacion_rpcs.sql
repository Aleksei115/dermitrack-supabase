-- Phase 2b: RPCs for 2-step ODV confirmation via PDF cotizacion
--
-- 1. rpc_save_borrador_step — persists step 1 (borrador validated) to server
-- 2. rpc_confirm_odv_with_cotizacion — step 2: inserts ODV data + pivots saga
-- 3. Updated rpc_confirm_saga_pivot — ON CONFLICT DO UPDATE for borrador→synced

BEGIN;

-- ============================================================================
-- 1. rpc_save_borrador_step
-- Called after client-side PDF validation (step 1).
-- Pre-creates saga_zoho_links with 'borrador_validado' status.
-- saga_transactions stays BORRADOR (no PIVOT yet).
-- ============================================================================
CREATE OR REPLACE FUNCTION public.rpc_save_borrador_step(
  p_visit_id uuid,
  p_odv_id text,
  p_saga_tipo text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_zoho_link_tipo tipo_zoho_link;
  v_task_tipo text;
BEGIN
  RAISE NOTICE '[cotizacion:borrador] visit_id=%, odv_id=%, saga_tipo=%', p_visit_id, p_odv_id, p_saga_tipo;

  -- 1. Find saga_transaction for this visit + tipo (latest non-cancelled)
  SELECT * INTO v_saga
  FROM saga_transactions
  WHERE id_visita = p_visit_id
    AND tipo::text = p_saga_tipo
    AND estado != 'CANCELADA'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'No se encontró saga % para visita %', p_saga_tipo, p_visit_id;
  END IF;

  RAISE NOTICE '[cotizacion:borrador] found saga_id=%, estado=%', v_saga.id, v_saga.estado;

  -- 2. Determine tipo_zoho_link and task_tipo
  CASE p_saga_tipo
    WHEN 'VENTA' THEN
      v_zoho_link_tipo := 'VENTA';
      v_task_tipo := 'VENTA_ODV';
    WHEN 'LEVANTAMIENTO_INICIAL' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
    WHEN 'LEV_POST_CORTE' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado para borrador: %', p_saga_tipo;
  END CASE;

  -- 3. Pre-create saga_zoho_links with borrador_validado status
  INSERT INTO saga_zoho_links (
    id_saga_transaction,
    zoho_id,
    tipo,
    items,
    zoho_sync_status,
    created_at,
    updated_at
  )
  VALUES (
    v_saga.id,
    p_odv_id,
    v_zoho_link_tipo,
    v_saga.items,
    'borrador_validado',
    now(),
    now()
  )
  ON CONFLICT (id_saga_transaction, zoho_id)
  DO UPDATE SET
    zoho_sync_status = 'borrador_validado',
    updated_at = now();

  RAISE NOTICE '[cotizacion:borrador] saga_zoho_links upserted, zoho_sync_status=borrador_validado';

  -- 4. Update visit_tasks.metadata with step progress
  UPDATE visit_tasks
  SET
    metadata = metadata || jsonb_build_object(
      'cotizacion_step', 'borrador_validado',
      'odv_id', p_odv_id
    ),
    last_activity_at = now()
  WHERE visit_id = p_visit_id
    AND task_tipo = v_task_tipo::visit_task_tipo;

  RAISE NOTICE '[cotizacion:borrador] visit_tasks.metadata updated';
END;
$function$;

-- ============================================================================
-- 2. rpc_confirm_odv_with_cotizacion
-- Called after step 2 PDF validation (Aprobado).
-- Inserts into ventas_odv or botiquin_odv, then calls rpc_confirm_saga_pivot.
-- ============================================================================
CREATE OR REPLACE FUNCTION public.rpc_confirm_odv_with_cotizacion(
  p_visit_id uuid,
  p_odv_id text,
  p_saga_tipo text,
  p_items jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_id_cliente varchar;
  v_target_table text;
  v_inserted_count int := 0;
  v_verify_count int;
  v_pivot_result jsonb;
  v_zoho_link_id int;
  v_item record;
BEGIN
  RAISE NOTICE '[cotizacion:confirm] visit_id=%, odv_id=%, saga_tipo=%, item_count=%',
    p_visit_id, p_odv_id, p_saga_tipo, jsonb_array_length(p_items);

  -- 1. Find saga_transaction
  SELECT * INTO v_saga
  FROM saga_transactions
  WHERE id_visita = p_visit_id
    AND tipo::text = p_saga_tipo
    AND estado != 'CANCELADA'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'No se encontró saga % para visita %', p_saga_tipo, p_visit_id;
  END IF;

  v_id_cliente := v_saga.id_cliente;

  -- 2. Determine target table
  IF p_saga_tipo = 'VENTA' THEN
    v_target_table := 'ventas_odv';
  ELSIF p_saga_tipo IN ('LEVANTAMIENTO_INICIAL', 'LEV_POST_CORTE') THEN
    v_target_table := 'botiquin_odv';
  ELSE
    RAISE EXCEPTION 'Tipo de saga no soportado: %', p_saga_tipo;
  END IF;

  RAISE NOTICE '[cotizacion:confirm] target_table=%, id_cliente=%', v_target_table, v_id_cliente;

  -- 3. INSERT into target table (one row per item)
  IF v_target_table = 'ventas_odv' THEN
    FOR v_item IN
      SELECT
        (elem->>'sku')::varchar(50) AS sku,
        (elem->>'cantidad')::int AS cantidad,
        (elem->>'precio')::numeric(10,2) AS precio
      FROM jsonb_array_elements(p_items) AS elem
    LOOP
      INSERT INTO ventas_odv (id_cliente, sku, odv_id, fecha, cantidad, precio, estado_factura)
      VALUES (v_id_cliente, v_item.sku, p_odv_id, CURRENT_DATE, v_item.cantidad, v_item.precio, 'cotizacion_aprobada')
      ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

      IF FOUND THEN
        v_inserted_count := v_inserted_count + 1;
      END IF;
    END LOOP;
  ELSE
    FOR v_item IN
      SELECT
        (elem->>'sku')::varchar(50) AS sku,
        (elem->>'cantidad')::int AS cantidad
      FROM jsonb_array_elements(p_items) AS elem
    LOOP
      INSERT INTO botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
      VALUES (v_id_cliente, v_item.sku, p_odv_id, CURRENT_DATE, v_item.cantidad, 'cotizacion_aprobada')
      ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

      IF FOUND THEN
        v_inserted_count := v_inserted_count + 1;
      END IF;
    END LOOP;
  END IF;

  RAISE NOTICE '[cotizacion:confirm] inserted % rows into %', v_inserted_count, v_target_table;

  -- 4. Verify data exists in target table
  IF v_target_table = 'ventas_odv' THEN
    SELECT COUNT(*) INTO v_verify_count
    FROM ventas_odv
    WHERE odv_id = p_odv_id AND id_cliente = v_id_cliente;
  ELSE
    SELECT COUNT(*) INTO v_verify_count
    FROM botiquin_odv
    WHERE odv_id = p_odv_id AND id_cliente = v_id_cliente;
  END IF;

  RAISE NOTICE '[cotizacion:confirm] verified % rows in % for odv_id=%', v_verify_count, v_target_table, p_odv_id;

  IF v_verify_count = 0 THEN
    RAISE EXCEPTION 'No se encontraron registros en % para odv_id=%', v_target_table, p_odv_id;
  END IF;

  -- 5. Call rpc_confirm_saga_pivot (creates movements, updates inventory, completes task)
  RAISE NOTICE '[cotizacion:confirm] calling rpc_confirm_saga_pivot(saga_id=%, odv_id=%)', v_saga.id, p_odv_id;

  v_pivot_result := rpc_confirm_saga_pivot(
    p_saga_id := v_saga.id,
    p_zoho_id := p_odv_id,
    p_zoho_items := p_items
  );

  RAISE NOTICE '[cotizacion:confirm] pivot result: %', v_pivot_result;

  -- 6. Get zoho_link_id and ensure status is 'synced'
  SELECT id INTO v_zoho_link_id
  FROM saga_zoho_links
  WHERE id_saga_transaction = v_saga.id AND zoho_id = p_odv_id;

  IF v_zoho_link_id IS NOT NULL THEN
    UPDATE saga_zoho_links
    SET
      zoho_sync_status = 'synced',
      zoho_synced_at = now(),
      updated_at = now()
    WHERE id = v_zoho_link_id;
  END IF;

  RAISE NOTICE '[cotizacion:confirm] SUCCESS: saga_id=%, zoho_link_id=%, items_inserted=%',
    v_saga.id, v_zoho_link_id, v_inserted_count;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', v_saga.id,
    'zoho_link_id', v_zoho_link_id,
    'items_inserted', v_inserted_count,
    'pivot_result', v_pivot_result
  );
END;
$function$;

-- ============================================================================
-- 3. Updated rpc_confirm_saga_pivot
-- Change: ON CONFLICT DO NOTHING → ON CONFLICT DO UPDATE SET
-- This handles the borrador→synced transition when a pre-existing link exists
-- from rpc_save_borrador_step.
-- ============================================================================
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

  -- 4. Create/update saga_zoho_link (handles borrador→synced transition)
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
      'synced',
      now(),
      now()
    )
    ON CONFLICT (id_saga_transaction, zoho_id)
    DO UPDATE SET
      items = COALESCE(EXCLUDED.items, saga_zoho_links.items),
      zoho_sync_status = 'synced',
      updated_at = now()
    RETURNING id INTO v_zoho_link_id;
  END IF;

  -- 5. Create movements OR assign FK (anti-duplication)
  IF v_already_confirmed THEN
    IF v_zoho_link_id IS NOT NULL THEN
      IF p_zoho_items IS NOT NULL THEN
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
    FOR v_item IN
      SELECT
        (item->>'sku')::varchar as sku,
        (item->>'cantidad')::int as cantidad
      FROM jsonb_array_elements(v_saga.items) as item
    LOOP
      SELECT COALESCE(cantidad_disponible, 0)
      INTO v_cantidad_antes
      FROM public.inventario_botiquin
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;

      IF v_cantidad_antes IS NULL THEN
        v_cantidad_antes := 0;
      END IF;

      IF v_tipo_movimiento = 'CREACION' THEN
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      ELSE
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      END IF;

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

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION public.rpc_save_borrador_step(uuid, text, text) TO authenticated;
GRANT EXECUTE ON FUNCTION public.rpc_confirm_odv_with_cotizacion(uuid, text, text, jsonb) TO authenticated;

NOTIFY pgrst, 'reload schema';

COMMIT;
