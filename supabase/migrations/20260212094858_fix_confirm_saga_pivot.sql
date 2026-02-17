-- Fix rpc_confirm_saga_pivot:
--   1) Don't early-return for CONFIRMADO sagas that have no movements
--   2) Fallback to ventas_odv when saga.items is empty
-- Fix rpc_set_manual_odv_id:
--   3) Fallback to ventas_odv when items are empty before creating saga

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
      -- Already confirmed WITH movements → return existing info
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
    -- CONFIRMADO but no movements → continue to create them
    v_already_confirmed := true;
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determinar tipo de zoho_link, task_tipo y tipo_movimiento según saga.tipo
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

  -- 4. Crear saga_zoho_links
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

    -- Calcular cantidad_despues según tipo de movimiento semántico
    IF v_tipo_movimiento = 'CREACION' THEN
      v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
    ELSE
      -- VENTA y RECOLECCION son salidas
      v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
    END IF;

    -- Insertar movimiento con tipo semántico
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

  -- 6. Actualizar visit_tasks.reference_id → saga_zoho_links.id
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


CREATE OR REPLACE FUNCTION public.rpc_set_manual_odv_id(p_visit_id uuid, p_zoho_odv_id text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga_id uuid;
  v_id_cliente varchar;
  v_id_usuario varchar;
  v_items jsonb;
  v_odv_id text;
  v_result jsonb;
BEGIN
  IF p_zoho_odv_id IS NULL THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Normalizar el formato del ODV ID
  IF p_zoho_odv_id ~ '^[0-9]{1,5}$' THEN
    v_odv_id := 'DCOdV-' || p_zoho_odv_id;
  ELSE
    v_odv_id := p_zoho_odv_id;
  END IF;

  IF v_odv_id !~ '^DCOdV-[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Buscar saga existente de tipo VENTA
  SELECT st.id INTO v_saga_id
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id AND st.tipo::text = 'VENTA'
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_saga_id IS NULL THEN
    -- Obtener datos de la visita
    SELECT v.id_cliente, v.id_usuario
    INTO v_id_cliente, v_id_usuario
    FROM public.visitas v
    WHERE v.visit_id = p_visit_id;

    IF v_id_cliente IS NULL THEN
      RAISE EXCEPTION 'Visita no encontrada';
    END IF;

    -- Obtener items del corte (saga VENTA que se creó en submit_corte)
    -- o de la metadata de la tarea CORTE
    SELECT COALESCE(
      (SELECT st.items FROM saga_transactions st
       WHERE st.visit_id = p_visit_id AND st.tipo::text = 'VENTA'
       ORDER BY created_at DESC LIMIT 1),
      '[]'::jsonb
    ) INTO v_items;

    -- Si no hay items, intentar extraer del corte
    IF v_items = '[]'::jsonb THEN
      SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'cantidad', (item->>'vendido')::int
          )
        ),
        '[]'::jsonb
      )
      INTO v_items
      FROM (
        SELECT item
        FROM saga_transactions st,
        LATERAL jsonb_array_elements(st.items) AS item
        WHERE st.visit_id = p_visit_id
        AND st.tipo::text = 'VENTA'
        AND COALESCE((item->>'vendido')::int, (item->>'cantidad')::int, 0) > 0
        ORDER BY st.created_at DESC
        LIMIT 100
      ) sub;
    END IF;

    -- Fix: Fallback to ventas_odv when items are still empty
    IF v_items = '[]'::jsonb OR v_items IS NULL THEN
      SELECT jsonb_agg(jsonb_build_object('sku', vo.sku, 'cantidad', vo.cantidad))
      INTO v_items
      FROM public.ventas_odv vo
      WHERE vo.odv_id = v_odv_id;
      v_items := COALESCE(v_items, '[]'::jsonb);
    END IF;

    -- Crear nueva saga de tipo VENTA
    INSERT INTO public.saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'VENTA'::tipo_saga_transaction,
      'BORRADOR'::estado_saga_transaction,
      v_id_cliente,
      v_id_usuario,
      v_items,
      jsonb_build_object(
        'zoho_required', true,
        'zoho_manual', true,
        'zoho_odv_id', v_odv_id
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  SELECT rpc_confirm_saga_pivot(v_saga_id, v_odv_id, NULL) INTO v_result;

  -- Actualizar tarea VENTA_ODV
  UPDATE public.visit_tasks
  SET
    estado = 'COMPLETADO',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'saga_id', v_saga_id,
      'zoho_odv_id', v_odv_id,
      'manual_odv', true
    )
  WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV';
END;
$function$;
