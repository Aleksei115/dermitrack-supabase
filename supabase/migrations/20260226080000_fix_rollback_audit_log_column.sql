-- Fix: rpc_admin_rollback_visit references audit_log.user_id which was renamed
-- to audit_user_id during the English schema rename. This causes a 400 error.

CREATE OR REPLACE FUNCTION public.rpc_admin_rollback_visit(p_visit_id uuid, p_reason text DEFAULT NULL::text)
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
  v_collection_ids uuid[];
  v_deleted_event_outbox int := 0;
  v_deleted_movimientos int := 0;
  v_deleted_zoho_links int := 0;
  v_deleted_compensations int := 0;
  v_deleted_adjustments int := 0;
  v_deleted_saga int := 0;
  v_deleted_tasks int := 0;
  v_deleted_recolecciones int := 0;
  v_deleted_rec_items int := 0;
  v_deleted_rec_firmas int := 0;
  v_deleted_rec_evidencias int := 0;
  v_deleted_informes int := 0;
  v_visit_data jsonb;
  v_current_visit_had_lev_post_corte boolean := false;
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_restore_source text := NULL;
  v_count_inventario_restored int := 0;
  v_inventory_reverted boolean := false;
BEGIN
  -- Get current user and verify admin role
  SELECT u.user_id, u.role::text
  INTO v_user_id, v_user_rol
  FROM public.users u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden ejecutar rollback de visits';
  END IF;

  -- Get visit info and snapshot before deletion
  SELECT
    v.client_id,
    v.cycle_id,
    jsonb_build_object(
      'visit_id', v.visit_id,
      'client_id', v.client_id,
      'user_id', v.user_id,
      'cycle_id', v.cycle_id,
      'type', v.type::text,
      'status', v.status::text,
      'created_at', v.created_at,
      'label', v.label,
      'metadata', v.metadata
    )
  INTO v_id_cliente, v_id_ciclo, v_visit_data
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- ============ RESTAURAR INVENTARIO ANTES DE BORRAR ============
  SELECT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.type = 'POST_CUTOFF_PLACEMENT'
      AND st.status = 'CONFIRMED'
  ) INTO v_current_visit_had_lev_post_corte;

  IF v_current_visit_had_lev_post_corte THEN
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visits v
    WHERE v.client_id = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.status = 'COMPLETED'
      AND v.type IN ('VISIT_CUTOFF', 'VISIT_INITIAL_PLACEMENT')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.type = 'POST_CUTOFF_PLACEMENT'
        AND st.status = 'CONFIRMED'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.type = 'INITIAL_PLACEMENT'
          AND st.status = 'CONFIRMED'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      DELETE FROM public.cabinet_inventory WHERE client_id = v_id_cliente;

      INSERT INTO public.cabinet_inventory (client_id, sku, available_quantity, last_updated)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'quantity')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      INSERT INTO public.cabinet_client_available_skus (client_id, sku, intake_date)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0
      ON CONFLICT (client_id, sku) DO NOTHING;

      v_inventory_reverted := true;
    ELSE
      v_restore_source := 'Sin visita completed anterior - inventario no modificado';
    END IF;
  END IF;

  -- Get saga transaction IDs for this visit
  SELECT ARRAY_AGG(st.id) INTO v_saga_ids
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id;

  -- Get recoleccion IDs for this visit
  SELECT ARRAY_AGG(r.collection_id) INTO v_collection_ids
  FROM public.collections r
  WHERE r.visit_id = p_visit_id;

  -- DELETE IN ORDER (child tables first)
  IF v_saga_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.event_outbox
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_event_outbox FROM deleted;

    WITH deleted AS (
      DELETE FROM public.inventory_movements
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_movimientos FROM deleted;

    WITH deleted AS (
      DELETE FROM public.saga_zoho_links
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_zoho_links FROM deleted;

    WITH deleted AS (
      DELETE FROM public.saga_compensations
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_compensations FROM deleted;

    WITH deleted AS (
      DELETE FROM public.saga_adjustments
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_adjustments FROM deleted;

    WITH deleted AS (
      DELETE FROM public.saga_transactions
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_saga FROM deleted;
  END IF;

  WITH deleted AS (
    DELETE FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_tasks FROM deleted;

  IF v_collection_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.collection_evidence
      WHERE collection_id = ANY(v_collection_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_evidencias FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collection_signatures
      WHERE collection_id = ANY(v_collection_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_firmas FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collection_items
      WHERE collection_id = ANY(v_collection_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_items FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collections
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_recolecciones FROM deleted;
  END IF;

  WITH deleted AS (
    DELETE FROM public.visit_reports
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_informes FROM deleted;

  -- Update visit status to CANCELLED
  UPDATE public.visits
  SET
    status = 'CANCELLED',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'rollback_at', NOW(),
      'rollback_by', v_user_id,
      'rollback_razon', p_reason,
      'rollback_deleted', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'inventory_movements', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'collections', v_deleted_recolecciones,
        'collection_items', v_deleted_rec_items,
        'collection_signatures', v_deleted_rec_firmas,
        'collection_evidence', v_deleted_rec_evidencias,
        'visit_reports', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored
    )
  WHERE visit_id = p_visit_id;

  -- LOG TO AUDIT_LOG (fixed: user_id -> audit_user_id)
  INSERT INTO public.audit_log (
    table_name,
    record_id,
    action,
    audit_user_id,
    values_before,
    values_after
  )
  VALUES (
    'visits',
    p_visit_id::text,
    'DELETE',
    v_user_id,
    v_visit_data || jsonb_build_object(
      'accion_tipo', 'ADMIN_ROLLBACK',
      'reason', p_reason
    ),
    jsonb_build_object(
      'deleted_counts', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'inventory_movements', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'collections', v_deleted_recolecciones,
        'collection_items', v_deleted_rec_items,
        'collection_signatures', v_deleted_rec_firmas,
        'collection_evidence', v_deleted_rec_evidencias,
        'visit_reports', v_deleted_informes
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
    'client_id', v_id_cliente,
    'cycle_id', v_id_ciclo,
    'executed_by', v_user_id,
    'reason', p_reason,
    'deleted', jsonb_build_object(
      'event_outbox', v_deleted_event_outbox,
      'inventory_movements', v_deleted_movimientos,
      'saga_zoho_links', v_deleted_zoho_links,
      'saga_compensations', v_deleted_compensations,
      'saga_adjustments', v_deleted_adjustments,
      'saga_transactions', v_deleted_saga,
      'visit_tasks', v_deleted_tasks,
      'collections', v_deleted_recolecciones,
      'collection_items', v_deleted_rec_items,
      'collection_signatures', v_deleted_rec_firmas,
      'collection_evidence', v_deleted_rec_evidencias,
      'visit_reports', v_deleted_informes
    ),
    'inventory_reverted', v_inventory_reverted,
    'inventory_restore_source', v_restore_source,
    'inventory_items_restored', v_count_inventario_restored,
    'last_completed_visit_id', v_last_completed_visit_id,
    'message', 'Rollback completado exitosamente'
  );
END;
$function$;
