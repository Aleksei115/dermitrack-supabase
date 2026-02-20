-- Fix: rpc_save_borrador_step and rpc_confirm_odv_with_cotizacion used
-- "id_visita" but the actual column in saga_transactions is "visit_id".

BEGIN;

-- ============================================================================
-- 1. rpc_save_borrador_step — fix WHERE id_visita → WHERE visit_id
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
  WHERE visit_id = p_visit_id
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
-- 2. rpc_confirm_odv_with_cotizacion — fix WHERE id_visita → WHERE visit_id
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
  WHERE visit_id = p_visit_id
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

NOTIFY pgrst, 'reload schema';

COMMIT;
