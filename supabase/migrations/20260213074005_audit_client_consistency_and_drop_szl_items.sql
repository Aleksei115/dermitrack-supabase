-- Part 1: Diagnostic RPC to audit client data consistency across saga pipeline tables
-- Part 2: Update rpc_confirm_saga_pivot to stop writing saga_zoho_links.items
-- Part 3: Drop saga_zoho_links.items column (unused duplicate of saga_transactions.items)

--------------------------------------------------------------------------------
-- PART 1: rpc_audit_client_consistency
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.rpc_audit_client_consistency(p_id_cliente text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_user_rol text;
  v_saga record;
  v_sagas_arr jsonb := '[]'::jsonb;
  v_saga_obj jsonb;
  v_checks jsonb;
  v_zoho_links jsonb;
  v_items_format text;
  v_saga_items jsonb; -- normalized {sku → qty} map
  v_item record;
  v_total_qty int;
  v_items_count int;
  v_check_result jsonb;
  v_odv_table text;
  v_total_ok int := 0;
  v_total_failed int := 0;
BEGIN
  -- Auth check: ADMINISTRADOR/OWNER only
  SELECT u.rol::text INTO v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_rol IS NULL OR v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  -- Validate client exists
  IF NOT EXISTS (SELECT 1 FROM public.clientes WHERE id_cliente = p_id_cliente) THEN
    RAISE EXCEPTION 'Cliente no encontrado: %', p_id_cliente;
  END IF;

  -- Loop through all CONFIRMADO sagas for this client
  FOR v_saga IN
    SELECT id, tipo::text as tipo, items, visit_id, created_at
    FROM public.saga_transactions
    WHERE id_cliente = p_id_cliente AND estado = 'CONFIRMADO'
    ORDER BY created_at
  LOOP
    v_checks := '[]'::jsonb;

    -- Determine items format and build normalized SKU->qty map
    IF v_saga.items IS NULL OR v_saga.items = '[]'::jsonb THEN
      v_items_format := 'EMPTY';
      v_saga_items := '{}'::jsonb;
      v_items_count := 0;
      v_total_qty := 0;
    ELSIF v_saga.items->0 ? 'sku' AND v_saga.items->0 ? 'cantidad' THEN
      v_items_format := 'NEW';
      -- Aggregate duplicates with SUM
      SELECT jsonb_object_agg(sku, qty), COALESCE(SUM(qty), 0)::int, COUNT(*)::int
      INTO v_saga_items, v_total_qty, v_items_count
      FROM (
        SELECT item->>'sku' as sku, SUM((item->>'cantidad')::int) as qty
        FROM jsonb_array_elements(v_saga.items) as item
        GROUP BY item->>'sku'
      ) agg;
    ELSE
      v_items_format := 'LEGACY';
      v_saga_items := '{}'::jsonb;
      v_items_count := jsonb_array_length(v_saga.items);
      v_total_qty := 0;
    END IF;

    -- Fetch zoho_links for this saga
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'link_id', szl.id,
      'zoho_id', szl.zoho_id,
      'tipo', szl.tipo::text
    )), '[]'::jsonb)
    INTO v_zoho_links
    FROM public.saga_zoho_links szl
    WHERE szl.id_saga_transaction = v_saga.id;

    -------------------------------------------------------------------
    -- CHECK 1: saga_vs_odv
    -------------------------------------------------------------------
    IF v_items_format = 'LEGACY' OR v_items_format = 'EMPTY' THEN
      v_check_result := jsonb_build_object(
        'check_name', 'saga_vs_odv',
        'status', 'N_A',
        'skip_reason', v_items_format || ' items format - no SKU-level comparison possible'
      );
    ELSIF v_saga.tipo = 'RECOLECCION' THEN
      v_check_result := jsonb_build_object(
        'check_name', 'saga_vs_odv',
        'status', 'N_A',
        'skip_reason', 'RECOLECCION has no ODV table'
      );
    ELSIF v_zoho_links = '[]'::jsonb THEN
      v_check_result := jsonb_build_object(
        'check_name', 'saga_vs_odv',
        'status', 'MISSING',
        'details', jsonb_build_object('reason', 'No zoho_links found for this saga')
      );
      v_total_failed := v_total_failed + 1;
    ELSE
      -- Determine which ODV table to check
      IF v_saga.tipo = 'VENTA' THEN
        v_odv_table := 'ventas_odv';
      ELSE
        v_odv_table := 'botiquin_odv';
      END IF;

      -- Build ODV items map from the appropriate table
      DECLARE
        v_odv_items jsonb := '{}'::jsonb;
        v_missing_in_odv jsonb := '[]'::jsonb;
        v_extra_in_odv jsonb := '[]'::jsonb;
        v_qty_mismatch jsonb := '[]'::jsonb;
        v_zoho_ids text[];
        v_odv_row record;
        v_key text;
        v_val numeric;
      BEGIN
        -- Collect zoho_ids
        SELECT array_agg(link_elem->>'zoho_id')
        INTO v_zoho_ids
        FROM jsonb_array_elements(v_zoho_links) as link_elem;

        -- Build ODV map
        IF v_odv_table = 'ventas_odv' THEN
          FOR v_odv_row IN
            SELECT vo.sku, SUM(vo.cantidad) as qty
            FROM public.ventas_odv vo
            WHERE vo.odv_id = ANY(v_zoho_ids)
            GROUP BY vo.sku
          LOOP
            v_odv_items := v_odv_items || jsonb_build_object(v_odv_row.sku, v_odv_row.qty);
          END LOOP;
        ELSE
          FOR v_odv_row IN
            SELECT bo.sku, SUM(bo.cantidad) as qty
            FROM public.botiquin_odv bo
            WHERE bo.odv_id = ANY(v_zoho_ids)
            GROUP BY bo.sku
          LOOP
            v_odv_items := v_odv_items || jsonb_build_object(v_odv_row.sku, v_odv_row.qty);
          END LOOP;
        END IF;

        -- Compare saga_items vs odv_items
        FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_saga_items)
        LOOP
          IF NOT v_odv_items ? v_key THEN
            v_missing_in_odv := v_missing_in_odv || jsonb_build_object('sku', v_key, 'saga_qty', v_val::int);
          ELSIF (v_odv_items->>v_key)::int != v_val::int THEN
            v_qty_mismatch := v_qty_mismatch || jsonb_build_object(
              'sku', v_key, 'saga_qty', v_val::int, 'odv_qty', (v_odv_items->>v_key)::int
            );
          END IF;
        END LOOP;

        FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_odv_items)
        LOOP
          IF NOT v_saga_items ? v_key THEN
            v_extra_in_odv := v_extra_in_odv || jsonb_build_object('sku', v_key, 'odv_qty', v_val::int);
          END IF;
        END LOOP;

        IF jsonb_array_length(v_missing_in_odv) = 0
           AND jsonb_array_length(v_extra_in_odv) = 0
           AND jsonb_array_length(v_qty_mismatch) = 0 THEN
          IF v_odv_items = '{}'::jsonb THEN
            v_check_result := jsonb_build_object(
              'check_name', 'saga_vs_odv',
              'status', 'MISSING',
              'details', jsonb_build_object('reason', 'zoho_ids exist but no ODV rows found')
            );
            v_total_failed := v_total_failed + 1;
          ELSE
            v_check_result := jsonb_build_object('check_name', 'saga_vs_odv', 'status', 'OK');
            v_total_ok := v_total_ok + 1;
          END IF;
        ELSE
          v_check_result := jsonb_build_object(
            'check_name', 'saga_vs_odv',
            'status', 'MISMATCH',
            'details', jsonb_build_object(
              'missing_in_odv', v_missing_in_odv,
              'extra_in_odv', v_extra_in_odv,
              'qty_mismatch', v_qty_mismatch
            )
          );
          v_total_failed := v_total_failed + 1;
        END IF;
      END;
    END IF;

    v_checks := v_checks || v_check_result;

    -------------------------------------------------------------------
    -- CHECK 2: saga_vs_movimientos
    -------------------------------------------------------------------
    IF v_items_format = 'LEGACY' OR v_items_format = 'EMPTY' THEN
      v_check_result := jsonb_build_object(
        'check_name', 'saga_vs_movimientos',
        'status', 'N_A',
        'skip_reason', v_items_format || ' items format - no SKU-level comparison possible'
      );
    ELSE
      DECLARE
        v_mov_items jsonb := '{}'::jsonb;
        v_missing_in_mov jsonb := '[]'::jsonb;
        v_extra_in_mov jsonb := '[]'::jsonb;
        v_qty_mismatch_mov jsonb := '[]'::jsonb;
        v_mov_row record;
        v_key text;
        v_val numeric;
      BEGIN
        FOR v_mov_row IN
          SELECT mi.sku, SUM(mi.cantidad) as qty
          FROM public.movimientos_inventario mi
          WHERE mi.id_saga_transaction = v_saga.id
          GROUP BY mi.sku
        LOOP
          v_mov_items := v_mov_items || jsonb_build_object(v_mov_row.sku, v_mov_row.qty);
        END LOOP;

        FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_saga_items)
        LOOP
          IF NOT v_mov_items ? v_key THEN
            v_missing_in_mov := v_missing_in_mov || jsonb_build_object('sku', v_key, 'saga_qty', v_val::int);
          ELSIF (v_mov_items->>v_key)::int != v_val::int THEN
            v_qty_mismatch_mov := v_qty_mismatch_mov || jsonb_build_object(
              'sku', v_key, 'saga_qty', v_val::int, 'mov_qty', (v_mov_items->>v_key)::int
            );
          END IF;
        END LOOP;

        FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_mov_items)
        LOOP
          IF NOT v_saga_items ? v_key THEN
            v_extra_in_mov := v_extra_in_mov || jsonb_build_object('sku', v_key, 'mov_qty', v_val::int);
          END IF;
        END LOOP;

        IF jsonb_array_length(v_missing_in_mov) = 0
           AND jsonb_array_length(v_extra_in_mov) = 0
           AND jsonb_array_length(v_qty_mismatch_mov) = 0 THEN
          IF v_mov_items = '{}'::jsonb THEN
            v_check_result := jsonb_build_object(
              'check_name', 'saga_vs_movimientos',
              'status', 'MISSING',
              'details', jsonb_build_object('reason', 'No movimientos found for this saga')
            );
            v_total_failed := v_total_failed + 1;
          ELSE
            v_check_result := jsonb_build_object('check_name', 'saga_vs_movimientos', 'status', 'OK');
            v_total_ok := v_total_ok + 1;
          END IF;
        ELSE
          v_check_result := jsonb_build_object(
            'check_name', 'saga_vs_movimientos',
            'status', 'MISMATCH',
            'details', jsonb_build_object(
              'missing_in_movimientos', v_missing_in_mov,
              'extra_in_movimientos', v_extra_in_mov,
              'qty_mismatch', v_qty_mismatch_mov
            )
          );
          v_total_failed := v_total_failed + 1;
        END IF;
      END;
    END IF;

    v_checks := v_checks || v_check_result;

    -------------------------------------------------------------------
    -- CHECK 3: odv_vs_movimientos (only if both exist)
    -------------------------------------------------------------------
    IF v_items_format IN ('LEGACY', 'EMPTY') THEN
      v_check_result := jsonb_build_object(
        'check_name', 'odv_vs_movimientos',
        'status', 'N_A',
        'skip_reason', v_items_format || ' items format'
      );
    ELSIF v_saga.tipo = 'RECOLECCION' THEN
      v_check_result := jsonb_build_object(
        'check_name', 'odv_vs_movimientos',
        'status', 'N_A',
        'skip_reason', 'RECOLECCION has no ODV table'
      );
    ELSIF v_zoho_links = '[]'::jsonb THEN
      v_check_result := jsonb_build_object(
        'check_name', 'odv_vs_movimientos',
        'status', 'N_A',
        'skip_reason', 'No zoho_links to compare'
      );
    ELSE
      DECLARE
        v_odv_map jsonb := '{}'::jsonb;
        v_mov_map jsonb := '{}'::jsonb;
        v_missing jsonb := '[]'::jsonb;
        v_extra jsonb := '[]'::jsonb;
        v_mismatch jsonb := '[]'::jsonb;
        v_zoho_ids text[];
        v_row record;
        v_key text;
        v_val numeric;
      BEGIN
        SELECT array_agg(link_elem->>'zoho_id')
        INTO v_zoho_ids
        FROM jsonb_array_elements(v_zoho_links) as link_elem;

        -- Build ODV map
        IF v_saga.tipo = 'VENTA' THEN
          FOR v_row IN
            SELECT vo.sku, SUM(vo.cantidad) as qty
            FROM public.ventas_odv vo WHERE vo.odv_id = ANY(v_zoho_ids) GROUP BY vo.sku
          LOOP
            v_odv_map := v_odv_map || jsonb_build_object(v_row.sku, v_row.qty);
          END LOOP;
        ELSE
          FOR v_row IN
            SELECT bo.sku, SUM(bo.cantidad) as qty
            FROM public.botiquin_odv bo WHERE bo.odv_id = ANY(v_zoho_ids) GROUP BY bo.sku
          LOOP
            v_odv_map := v_odv_map || jsonb_build_object(v_row.sku, v_row.qty);
          END LOOP;
        END IF;

        -- Build movimientos map
        FOR v_row IN
          SELECT mi.sku, SUM(mi.cantidad) as qty
          FROM public.movimientos_inventario mi
          WHERE mi.id_saga_transaction = v_saga.id GROUP BY mi.sku
        LOOP
          v_mov_map := v_mov_map || jsonb_build_object(v_row.sku, v_row.qty);
        END LOOP;

        -- If either side is empty, skip
        IF v_odv_map = '{}'::jsonb OR v_mov_map = '{}'::jsonb THEN
          v_check_result := jsonb_build_object(
            'check_name', 'odv_vs_movimientos',
            'status', 'N_A',
            'skip_reason', 'One or both sides have no data'
          );
        ELSE
          FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_odv_map)
          LOOP
            IF NOT v_mov_map ? v_key THEN
              v_missing := v_missing || jsonb_build_object('sku', v_key, 'odv_qty', v_val::int);
            ELSIF (v_mov_map->>v_key)::int != v_val::int THEN
              v_mismatch := v_mismatch || jsonb_build_object(
                'sku', v_key, 'odv_qty', v_val::int, 'mov_qty', (v_mov_map->>v_key)::int
              );
            END IF;
          END LOOP;

          FOR v_key, v_val IN SELECT * FROM jsonb_each_text(v_mov_map)
          LOOP
            IF NOT v_odv_map ? v_key THEN
              v_extra := v_extra || jsonb_build_object('sku', v_key, 'mov_qty', v_val::int);
            END IF;
          END LOOP;

          IF jsonb_array_length(v_missing) = 0
             AND jsonb_array_length(v_extra) = 0
             AND jsonb_array_length(v_mismatch) = 0 THEN
            v_check_result := jsonb_build_object('check_name', 'odv_vs_movimientos', 'status', 'OK');
            v_total_ok := v_total_ok + 1;
          ELSE
            v_check_result := jsonb_build_object(
              'check_name', 'odv_vs_movimientos',
              'status', 'MISMATCH',
              'details', jsonb_build_object(
                'missing_in_movimientos', v_missing,
                'extra_in_movimientos', v_extra,
                'qty_mismatch', v_mismatch
              )
            );
            v_total_failed := v_total_failed + 1;
          END IF;
        END IF;
      END;
    END IF;

    v_checks := v_checks || v_check_result;

    -- Build saga result object
    v_saga_obj := jsonb_build_object(
      'saga_id', v_saga.id,
      'tipo', v_saga.tipo,
      'visit_id', v_saga.visit_id,
      'created_at', v_saga.created_at,
      'items_format', v_items_format,
      'items_count', v_items_count,
      'total_qty', v_total_qty,
      'zoho_links', v_zoho_links,
      'checks', v_checks
    );

    v_sagas_arr := v_sagas_arr || v_saga_obj;
  END LOOP;

  RETURN jsonb_build_object(
    'client', jsonb_build_object(
      'id_cliente', p_id_cliente,
      'total_sagas_analyzed', jsonb_array_length(v_sagas_arr),
      'total_checks_ok', v_total_ok,
      'total_checks_failed', v_total_failed
    ),
    'sagas', v_sagas_arr
  );
END;
$function$;


--------------------------------------------------------------------------------
-- PART 2: Update rpc_confirm_saga_pivot — remove items from INSERT
--------------------------------------------------------------------------------

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
  v_fallback_items jsonb;
BEGIN
  -- 1. Obtener y validar saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  -- Fix Bug 1: Only early-return if CONFIRMADO *and* movements already exist
  IF v_saga.estado = 'CONFIRMADO' THEN
    IF EXISTS (SELECT 1 FROM public.movimientos_inventario WHERE id_saga_transaction = p_saga_id) THEN
      -- Already confirmed WITH movements -> return existing info
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
    -- CONFIRMADO but no movements -> continue to create them
    v_already_confirmed := true;
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determinar tipo de zoho_link, task_tipo y tipo_movimiento segun saga.tipo
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

  -- 3. Cambiar estado de saga a CONFIRMADO (skip if already confirmed)
  IF NOT v_already_confirmed THEN
    UPDATE public.saga_transactions
    SET
      estado = 'CONFIRMADO'::estado_saga_transaction,
      updated_at = now()
    WHERE id = p_saga_id;
  END IF;

  -- 4. Crear saga_zoho_links (items column dropped)
  IF p_zoho_id IS NOT NULL THEN
    -- Check if zoho_link already exists (for re-run on already confirmed sagas)
    SELECT id INTO v_zoho_link_id
    FROM public.saga_zoho_links
    WHERE id_saga_transaction = p_saga_id AND zoho_id = p_zoho_id
    LIMIT 1;

    IF v_zoho_link_id IS NULL THEN
      INSERT INTO public.saga_zoho_links (
        id_saga_transaction,
        zoho_id,
        tipo,
        zoho_sync_status,
        created_at,
        updated_at
      )
      VALUES (
        p_saga_id,
        p_zoho_id,
        v_zoho_link_tipo,
        'pending',
        now(),
        now()
      )
      RETURNING id INTO v_zoho_link_id;
    END IF;
  END IF;

  -- Fix Bug 2: Fallback to ventas_odv when saga.items is empty
  IF jsonb_array_length(COALESCE(v_saga.items, '[]'::jsonb)) = 0 AND p_zoho_id IS NOT NULL THEN
    SELECT jsonb_agg(jsonb_build_object('sku', vo.sku, 'cantidad', vo.cantidad))
    INTO v_fallback_items
    FROM public.ventas_odv vo
    WHERE vo.odv_id = p_zoho_id;

    IF v_fallback_items IS NOT NULL AND jsonb_array_length(v_fallback_items) > 0 THEN
      UPDATE public.saga_transactions SET items = v_fallback_items WHERE id = p_saga_id;
      v_saga.items := v_fallback_items;
    END IF;
  END IF;

  -- 5. Generar movimientos_inventario desde saga.items
  FOR v_item IN
    SELECT
      (item->>'sku')::varchar as sku,
      (item->>'cantidad')::int as cantidad
    FROM jsonb_array_elements(v_saga.items) as item
  LOOP
    -- Obtener cantidad_antes
    SELECT COALESCE(cantidad_disponible, 0)
    INTO v_cantidad_antes
    FROM public.inventario_botiquin
    WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;

    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    -- Calcular cantidad_despues segun tipo de movimiento semantico
    IF v_tipo_movimiento = 'CREACION' THEN
      v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
    ELSE
      -- VENTA y RECOLECCION son salidas
      v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
    END IF;

    -- Insertar movimiento con tipo semantico
    INSERT INTO public.movimientos_inventario (
      id_saga_transaction,
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
      v_saga.id_cliente,
      v_item.sku,
      v_tipo_movimiento,
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      now()
    );

    -- Actualizar inventario_botiquin
    IF v_cantidad_despues > 0 THEN
      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues, now())
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET
        cantidad_disponible = v_cantidad_despues,
        ultima_actualizacion = now();
    ELSE
      -- Si cantidad es 0, eliminar del inventario
      DELETE FROM public.inventario_botiquin
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    END IF;

    -- Si es VENTA, eliminar de botiquin_clientes_sku_disponibles
    IF v_saga.tipo::text = 'VENTA' THEN
      DELETE FROM public.botiquin_clientes_sku_disponibles
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    END IF;
  END LOOP;

  -- 6. Actualizar visit_tasks.reference_id -> saga_zoho_links.id
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
    -- Sin zoho_id, solo marcar como completado
    UPDATE public.visit_tasks
    SET
      estado = 'COMPLETADO',
      completed_at = COALESCE(completed_at, now()),
      last_activity_at = now()
    WHERE visit_id = v_saga.visit_id
    AND task_tipo = v_task_tipo::visit_task_tipo;
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


--------------------------------------------------------------------------------
-- PART 3: Drop saga_zoho_links.items column
--------------------------------------------------------------------------------

DROP INDEX IF EXISTS idx_saga_zoho_links_items_not_null;
ALTER TABLE public.saga_zoho_links DROP COLUMN IF EXISTS items;
