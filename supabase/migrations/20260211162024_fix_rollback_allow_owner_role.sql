-- Allow OWNER role to execute visit rollback (previously only ADMINISTRADOR)
-- Only change: line "IF v_user_rol != 'ADMINISTRADOR'" → "IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER')"
CREATE OR REPLACE FUNCTION public.rpc_admin_rollback_visit(p_visit_id uuid, p_razon text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_id_ciclo integer;
  v_saga_ids uuid[];
  v_recoleccion_ids uuid[];
  v_deleted_event_outbox int := 0;
  v_deleted_movimientos int := 0;
  v_deleted_saga int := 0;
  v_deleted_tasks int := 0;
  v_deleted_odvs int := 0;
  v_deleted_recolecciones int := 0;
  v_deleted_rec_items int := 0;
  v_deleted_rec_firmas int := 0;
  v_deleted_rec_evidencias int := 0;
  v_deleted_informes int := 0;
  v_visit_data jsonb;
  -- Variables para restauración de inventario
  v_current_visit_had_lev_post_corte boolean := false;
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_restore_source text := NULL;
  v_count_inventario_restored int := 0;
  v_inventory_reverted boolean := false;
BEGIN
  -- Get current user and verify admin/owner role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden ejecutar rollback de visitas';
  END IF;

  -- Get visit info and snapshot before deletion
  SELECT
    v.id_cliente,
    v.id_ciclo,
    jsonb_build_object(
      'visit_id', v.visit_id,
      'id_cliente', v.id_cliente,
      'id_usuario', v.id_usuario,
      'id_ciclo', v.id_ciclo,
      'tipo', v.tipo::text,
      'estado', v.estado::text,
      'created_at', v.created_at,
      'etiqueta', v.etiqueta,
      'metadata', v.metadata
    )
  INTO v_id_cliente, v_id_ciclo, v_visit_data
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- ============ RESTAURAR INVENTARIO ANTES DE BORRAR ============
  -- Verificar si esta visita tiene un LEV_POST_CORTE confirmado
  SELECT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.tipo = 'LEV_POST_CORTE'
      AND st.estado = 'CONFIRMADO'
  ) INTO v_current_visit_had_lev_post_corte;

  IF v_current_visit_had_lev_post_corte THEN
    -- Buscar la última visita COMPLETADA del mismo cliente (excluyendo la actual)
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.estado = 'COMPLETADO'
      AND v.tipo IN ('VISITA_CORTE', 'VISITA_LEVANTAMIENTO_INICIAL')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      -- Intentar LEV_POST_CORTE primero
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.tipo = 'LEV_POST_CORTE'
        AND st.estado = 'CONFIRMADO'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        -- Fallback a LEVANTAMIENTO_INICIAL
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.tipo = 'LEVANTAMIENTO_INICIAL'
          AND st.estado = 'CONFIRMADO'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- Restaurar inventario si encontramos items
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      -- Limpiar inventario actual del cliente
      DELETE FROM public.inventario_botiquin WHERE id_cliente = v_id_cliente;

      -- Restaurar desde la última visita COMPLETADA
      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'cantidad')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      -- También restaurar botiquin_clientes_sku_disponibles
      INSERT INTO public.botiquin_clientes_sku_disponibles (id_cliente, sku, fecha_ingreso)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0
      ON CONFLICT (id_cliente, sku) DO NOTHING;

      v_inventory_reverted := true;
    ELSE
      v_restore_source := 'Sin visita completada anterior - inventario no modificado';
    END IF;
  END IF;
  -- ============ FIN RESTAURACIÓN DE INVENTARIO ============

  -- Get saga transaction IDs for this visit
  SELECT ARRAY_AGG(st.id) INTO v_saga_ids
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id;

  -- Get recoleccion IDs for this visit
  SELECT ARRAY_AGG(r.recoleccion_id) INTO v_recoleccion_ids
  FROM public.recolecciones r
  WHERE r.visit_id = p_visit_id;

  -- DELETE IN ORDER (child tables first)

  -- 1. Delete event_outbox (references saga_transactions)
  IF v_saga_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.event_outbox
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_event_outbox FROM deleted;

    -- 2. Delete movimientos_inventario (references saga_transactions)
    WITH deleted AS (
      DELETE FROM public.movimientos_inventario
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_movimientos FROM deleted;

    -- 3. Delete saga_transactions
    WITH deleted AS (
      DELETE FROM public.saga_transactions
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_saga FROM deleted;
  END IF;

  -- 4. Delete visit_tasks
  WITH deleted AS (
    DELETE FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_tasks FROM deleted;

  -- 5. Delete visita_odvs
  WITH deleted AS (
    DELETE FROM public.visita_odvs
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_odvs FROM deleted;

  -- 6. Delete recolecciones and related tables
  IF v_recoleccion_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.recolecciones_evidencias
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_evidencias FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_firmas
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_firmas FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_items
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_items FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_recolecciones FROM deleted;
  END IF;

  -- 7. Delete visita_informes
  WITH deleted AS (
    DELETE FROM public.visita_informes
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_informes FROM deleted;

  -- 8. Update visit status to CANCELADO
  UPDATE public.visitas
  SET
    estado = 'CANCELADO',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'rollback_at', NOW(),
      'rollback_by', v_user_id,
      'rollback_razon', p_razon,
      'rollback_deleted', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'visita_odvs', v_deleted_odvs,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored
    )
  WHERE visit_id = p_visit_id;

  -- LOG TO AUDIT_LOG
  INSERT INTO public.audit_log (
    tabla,
    registro_id,
    accion,
    usuario_id,
    valores_antes,
    valores_despues
  )
  VALUES (
    'visitas',
    p_visit_id::text,
    'DELETE',
    v_user_id,
    v_visit_data || jsonb_build_object(
      'accion_tipo', 'ADMIN_ROLLBACK',
      'razon', p_razon
    ),
    jsonb_build_object(
      'deleted_counts', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'visita_odvs', v_deleted_odvs,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored,
      'executed_at', NOW(),
      'executed_by', v_user_id
    )
  );

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'id_cliente', v_id_cliente,
    'id_ciclo', v_id_ciclo,
    'executed_by', v_user_id,
    'razon', p_razon,
    'deleted', jsonb_build_object(
      'event_outbox', v_deleted_event_outbox,
      'movimientos_inventario', v_deleted_movimientos,
      'saga_transactions', v_deleted_saga,
      'visit_tasks', v_deleted_tasks,
      'visita_odvs', v_deleted_odvs,
      'recolecciones', v_deleted_recolecciones,
      'recolecciones_items', v_deleted_rec_items,
      'recolecciones_firmas', v_deleted_rec_firmas,
      'recolecciones_evidencias', v_deleted_rec_evidencias,
      'visita_informes', v_deleted_informes
    ),
    'inventory_reverted', v_inventory_reverted,
    'inventory_restore_source', v_restore_source,
    'inventory_items_restored', v_count_inventario_restored,
    'last_completed_visit_id', v_last_completed_visit_id,
    'message', 'Rollback completado exitosamente'
  );
END;
$function$;
