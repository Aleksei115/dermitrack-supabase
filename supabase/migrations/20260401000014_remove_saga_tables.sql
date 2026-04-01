-- Migration: Remove saga tables and all saga references
-- Context: Phases 5-7 migrated from saga pattern to direct RPCs.
--   All 155 saga_transactions are in final state (146 CONFIRMED, 9 CANCELLED).
--   All 1069 inventory_movements have visit_id (100% backfilled).
--   saga_zoho_links data migrated to cabinet_sale_odv_ids.

BEGIN;

-- ============================================================
-- STEP 1: Rename visits.saga_status → visits.workflow_status
-- ============================================================
ALTER TABLE visits RENAME COLUMN saga_status TO workflow_status;

-- ============================================================
-- STEP 2: Drop triggers on saga_transactions and visits
-- ============================================================
DROP TRIGGER IF EXISTS audit_saga_transactions ON saga_transactions;
DROP TRIGGER IF EXISTS saga_transactions_audit ON saga_transactions;
DROP TRIGGER IF EXISTS trg_sync_saga_status ON visits;
DROP TRIGGER IF EXISTS update_saga_transactions_updated_at ON saga_transactions;
DROP TRIGGER IF EXISTS validate_saga_items_unique ON saga_transactions;

-- ============================================================
-- STEP 3: Drop FK columns on inventory_movements
-- ============================================================
ALTER TABLE inventory_movements DROP CONSTRAINT IF EXISTS inventory_movements_id_saga_transaction_fkey;
ALTER TABLE inventory_movements DROP CONSTRAINT IF EXISTS inventory_movements_id_saga_zoho_link_fkey;
ALTER TABLE inventory_movements DROP COLUMN IF EXISTS id_saga_transaction;
ALTER TABLE inventory_movements DROP COLUMN IF EXISTS id_saga_zoho_link;

-- ============================================================
-- STEP 4: Drop event_outbox.saga_transaction_id
-- ============================================================
ALTER TABLE event_outbox DROP CONSTRAINT IF EXISTS event_outbox_saga_transaction_id_fkey;
ALTER TABLE event_outbox DROP COLUMN IF EXISTS saga_transaction_id;

-- ============================================================
-- STEP 4b: Drop views that depend on saga tables
-- ============================================================
DROP VIEW IF EXISTS v_visit_tasks_operational CASCADE;

-- ============================================================
-- STEP 5: Drop saga tables (CASCADE handles FK dependencies)
-- ============================================================
DROP TABLE IF EXISTS saga_adjustments CASCADE;
DROP TABLE IF EXISTS saga_compensations CASCADE;
DROP TABLE IF EXISTS saga_zoho_links CASCADE;
DROP TABLE IF EXISTS saga_transactions CASCADE;

-- ============================================================
-- STEP 6: Drop saga enums
-- ============================================================
DROP TYPE IF EXISTS saga_transaction_status;
DROP TYPE IF EXISTS saga_transaction_type;
DROP TYPE IF EXISTS zoho_link_type;

-- ============================================================
-- STEP 7: Drop legacy saga-named functions (11)
-- ============================================================
DROP FUNCTION IF EXISTS public.audit_saga_transactions();
DROP FUNCTION IF EXISTS public.deduplicate_saga_items();
DROP FUNCTION IF EXISTS public.fn_sync_saga_status();
DROP FUNCTION IF EXISTS public.publish_saga_event();
DROP FUNCTION IF EXISTS public.regenerate_movements_from_saga();
DROP FUNCTION IF EXISTS public.rpc_compensate_saga(uuid, text);
DROP FUNCTION IF EXISTS public.rpc_confirm_saga_pivot(uuid, text, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_get_visit_saga_summary(uuid);
DROP FUNCTION IF EXISTS public.rpc_migrate_legacy_sagas();
DROP FUNCTION IF EXISTS public.saga_outbox_trigger();
DROP FUNCTION IF EXISTS public.trigger_generate_movements_from_saga();

-- ============================================================
-- STEP 8: Drop legacy RPCs replaced by Phase 5-7
-- ============================================================
DROP FUNCTION IF EXISTS public.rpc_submit_initial_placement(uuid, jsonb);
DROP FUNCTION IF EXISTS public.rpc_submit_cutoff(uuid, jsonb);
DROP FUNCTION IF EXISTS public.rpc_submit_post_cutoff_placement(uuid, jsonb);
DROP FUNCTION IF EXISTS public.rpc_confirm_odv(uuid, text);
DROP FUNCTION IF EXISTS public.rpc_confirm_odv_with_cotizacion(uuid, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_set_manual_odv_id(uuid, text);
DROP FUNCTION IF EXISTS public.rpc_set_manual_botiquin_odv_id(uuid, text);
DROP FUNCTION IF EXISTS public.rpc_complete_collection(uuid, jsonb);
DROP FUNCTION IF EXISTS public.rpc_cancel_visit(uuid, text);
DROP FUNCTION IF EXISTS public.rpc_get_visit_odvs(uuid);
DROP FUNCTION IF EXISTS public.rpc_admin_compensate_task(uuid, text, uuid, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_admin_retry_pivot(uuid, uuid);
DROP FUNCTION IF EXISTS public.rpc_save_draft_step(uuid, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_admin_rollback_visit(uuid, text);

-- ============================================================
-- STEP 9: Drop legacy RPCs that read from saga_transactions
-- ============================================================
DROP FUNCTION IF EXISTS public.rpc_get_odv_items(uuid);
DROP FUNCTION IF EXISTS public.rpc_get_placement_items(uuid);
DROP FUNCTION IF EXISTS public.rpc_get_post_cutoff_placement_items(uuid);

-- ============================================================
-- STEP 10: Drop migration/utility functions (one-time use)
-- ============================================================
DROP FUNCTION IF EXISTS public.rpc_migrate_dev_legacy();
DROP FUNCTION IF EXISTS public.rpc_migrate_full_history();
DROP FUNCTION IF EXISTS public.rpc_verify_consolidation();
DROP FUNCTION IF EXISTS public.rpc_verify_dev_migration();
DROP FUNCTION IF EXISTS public.rpc_verify_migration_consistency();
DROP FUNCTION IF EXISTS public.rpc_consolidate_visits();
DROP FUNCTION IF EXISTS public.rebuild_inventory_movements();

-- ============================================================
-- STEP 11: Drop unused analytics functions that reference saga
-- ============================================================
DROP FUNCTION IF EXISTS analytics.get_cutoff_logistics_detail();
DROP FUNCTION IF EXISTS analytics.get_billing_composition_legacy();
DROP FUNCTION IF EXISTS public.audit_visit_graph(text);

-- ============================================================
-- STEP 12: Rewrite rpc_compensate_visit_v2 (saga_status → workflow_status)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_compensate_visit_v2(p_visit_id uuid, p_reason text DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_visit RECORD;
  v_movement RECORD;
  v_reverse_qty_before integer;
  v_reverse_qty_after integer;
  v_movements_reversed integer := 0;
  v_lots_restored integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.workflow_status
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Cannot compensate if VALIDATION tasks are already completed
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id
      AND task_type IN ('SALE_ODV', 'ODV_CABINET')
      AND status = 'COMPLETED'
  ) THEN
    RAISE EXCEPTION 'Cannot compensate: validation tasks already completed. ODV has been linked.';
  END IF;

  -- Cannot compensate if already compensated
  IF v_visit.workflow_status = 'COMPENSATED' THEN
    RETURN jsonb_build_object('success', true, 'already_compensated', true);
  END IF;

  -- Reverse all movements for this visit
  FOR v_movement IN
    SELECT id, client_id, sku, quantity, type, unit_price, task_id
    FROM inventory_movements
    WHERE visit_id = p_visit_id
    ORDER BY id DESC
  LOOP
    SELECT COALESCE(ci.available_quantity, 0) INTO v_reverse_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_movement.client_id AND ci.sku = v_movement.sku;
    IF NOT FOUND THEN v_reverse_qty_before := 0; END IF;

    CASE v_movement.type
      WHEN 'PLACEMENT' THEN
        v_reverse_qty_after := GREATEST(0, v_reverse_qty_before - v_movement.quantity);
      WHEN 'SALE', 'COLLECTION' THEN
        v_reverse_qty_after := v_reverse_qty_before + v_movement.quantity;
    END CASE;

    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_movement.client_id, v_movement.sku, v_movement.quantity,
      v_reverse_qty_before, v_reverse_qty_after,
      now(),
      CASE v_movement.type
        WHEN 'PLACEMENT' THEN 'COLLECTION'::cabinet_movement_type
        ELSE 'PLACEMENT'::cabinet_movement_type
      END,
      v_movement.unit_price, v_movement.task_id, p_visit_id, true
    );

    v_movements_reversed := v_movements_reversed + 1;
  END LOOP;

  -- Restore consumed/collected lots back to active
  UPDATE cabinet_inventory_lots
  SET status = 'active',
      remaining_quantity = quantity,
      consumed_by_movement_id = NULL
  WHERE visit_id = p_visit_id
    AND status IN ('consumed', 'collected');

  GET DIAGNOSTICS v_lots_restored = ROW_COUNT;

  -- Delete lots created by this visit's placements
  DELETE FROM cabinet_inventory_lots
  WHERE visit_id = p_visit_id AND status = 'active';

  -- Mark all tasks as COMPENSATED
  UPDATE visit_tasks
  SET status = 'COMPENSATED',
      metadata = metadata || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason
      )
  WHERE visit_id = p_visit_id
    AND status NOT IN ('COMPENSATED', 'SKIPPED', 'SKIPPED_M');

  -- Mark visit as compensated
  UPDATE visits
  SET workflow_status = 'COMPENSATED',
      updated_at = now(),
      metadata = metadata || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason,
        'movements_reversed', v_movements_reversed,
        'lots_restored', v_lots_restored
      )
  WHERE visit_id = p_visit_id;

  -- Delete associated collections
  DELETE FROM collection_evidence WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collection_signatures WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collection_items WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collections WHERE visit_id = p_visit_id;

  -- Log compensation in audit_log
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before, values_after)
  VALUES ('visits', p_visit_id::text, 'COMPENSATE',
          (SELECT current_user_id()),
          jsonb_build_object('workflow_status', v_visit.workflow_status),
          jsonb_build_object('workflow_status', 'COMPENSATED', 'reason', p_reason,
                             'movements_reversed', v_movements_reversed, 'lots_restored', v_lots_restored));

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'movements_reversed', v_movements_reversed,
    'lots_restored', v_lots_restored
  );
END;
$function$;

-- ============================================================
-- STEP 13: Rewrite rpc_get_visit_summary_v2 (saga_status → workflow_status)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_get_visit_summary_v2(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_visit RECORD;
  v_tasks jsonb;
  v_movements jsonb;
  v_collection jsonb;
BEGIN
  SELECT v.* INTO v_visit FROM visits v WHERE v.visit_id = p_visit_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Tasks
  SELECT jsonb_agg(jsonb_build_object(
    'task_type', vt.task_type,
    'status', vt.status,
    'transaction_type', vt.transaction_type,
    'step_order', vt.step_order,
    'completed_at', vt.completed_at,
    'metadata', vt.metadata
  ) ORDER BY vt.step_order)
  INTO v_tasks
  FROM visit_tasks vt WHERE vt.visit_id = p_visit_id;

  -- Movements
  SELECT jsonb_agg(jsonb_build_object(
    'id', im.id,
    'sku', im.sku,
    'product', m.product,
    'type', im.type,
    'quantity', im.quantity,
    'unit_price', im.unit_price,
    'validated', im.validated,
    'movement_date', im.movement_date
  ) ORDER BY im.movement_date)
  INTO v_movements
  FROM inventory_movements im
  JOIN medications m ON im.sku = m.sku
  WHERE im.visit_id = p_visit_id;

  -- Collection
  SELECT jsonb_build_object(
    'collection_id', c.collection_id,
    'status', c.status,
    'transit_started_at', c.transit_started_at,
    'delivered_at', c.delivered_at,
    'items', (
      SELECT jsonb_agg(jsonb_build_object('sku', ci.sku, 'quantity', ci.quantity))
      FROM collection_items ci WHERE ci.collection_id = c.collection_id
    )
  )
  INTO v_collection
  FROM collections c WHERE c.visit_id = p_visit_id;

  RETURN jsonb_build_object(
    'visit', jsonb_build_object(
      'visit_id', v_visit.visit_id,
      'client_id', v_visit.client_id,
      'type', v_visit.type,
      'status', v_visit.status,
      'workflow_status', v_visit.workflow_status,
      'created_at', v_visit.created_at
    ),
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'movements', COALESCE(v_movements, '[]'::jsonb),
    'collection', v_collection
  );
END;
$function$;

-- ============================================================
-- STEP 14: Rewrite rpc_admin_get_all_visits (remove saga JOINs)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_admin_get_all_visits(
  p_limit integer DEFAULT 50,
  p_offset integer DEFAULT 0,
  p_status text DEFAULT NULL,
  p_search text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_visits jsonb;
  v_total int;
BEGIN
  SELECT u.user_id, u.role::text
  INTO v_user_id, v_user_rol
  FROM public.users u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  SELECT COUNT(*)
  INTO v_total
  FROM public.visits v
  JOIN public.clients c ON c.client_id = v.client_id
  WHERE (p_status IS NULL OR v.status::text = p_status)
    AND (p_search IS NULL OR c.client_name ILIKE '%' || p_search || '%')
    AND (p_date_from IS NULL OR v.created_at::date >= p_date_from)
    AND (p_date_to IS NULL OR v.created_at::date <= p_date_to);

  SELECT jsonb_agg(row_data)
  INTO v_visits
  FROM (
    SELECT jsonb_build_object(
      'visit_id', v.visit_id,
      'client_id', v.client_id,
      'client_name', c.client_name,
      'user_id', v.user_id,
      'user_name', u.name,
      'type', v.type::text,
      'status', v.status::text,
      'workflow_status', COALESCE(v.workflow_status::text,
        CASE WHEN v.status = 'COMPLETED' THEN 'COMPLETED'
             WHEN v.status = 'CANCELLED' THEN 'COMPENSATED'
             ELSE 'RUNNING' END
      ),
      'label', v.label,
      'created_at', v.created_at,
      'started_at', v.started_at,
      'completed_at', v.completed_at,
      'metadata', v.metadata,
      'tasks_count', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id),
      'tasks_completed', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND vt.status = 'COMPLETED'),
      'odvs_count', (
        SELECT COUNT(DISTINCT odv_id) FROM cabinet_sale_odv_ids
        WHERE visit_id = v.visit_id
      ),
      'collections_count', (SELECT COUNT(*) FROM collections col WHERE col.visit_id = v.visit_id)
    ) as row_data
    FROM public.visits v
    JOIN public.clients c ON c.client_id = v.client_id
    LEFT JOIN public.users u ON u.user_id = v.user_id
    WHERE (p_status IS NULL OR v.status::text = p_status)
      AND (p_search IS NULL OR c.client_name ILIKE '%' || p_search || '%')
      AND (p_date_from IS NULL OR v.created_at::date >= p_date_from)
      AND (p_date_to IS NULL OR v.created_at::date <= p_date_to)
    ORDER BY v.created_at DESC
    LIMIT p_limit
    OFFSET p_offset
  ) sub;

  RETURN jsonb_build_object(
    'visits', COALESCE(v_visits, '[]'::jsonb),
    'total', v_total,
    'limit', p_limit,
    'offset', p_offset
  );
END;
$function$;

-- ============================================================
-- STEP 15: Rewrite rpc_admin_get_visit_detail (remove saga JOINs)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_admin_get_visit_detail(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_visit jsonb;
  v_tasks jsonb;
  v_odvs jsonb;
  v_movements jsonb;
  v_report jsonb;
  v_collections jsonb;
BEGIN
  SELECT u.user_id, u.role::text
  INTO v_user_id, v_user_rol
  FROM public.users u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  SELECT jsonb_build_object(
    'visit_id', v.visit_id,
    'client_id', v.client_id,
    'client_name', c.client_name,
    'user_id', v.user_id,
    'user_name', u.name,
    'type', v.type::text,
    'status', v.status::text,
    'workflow_status', COALESCE(v.workflow_status::text,
      CASE WHEN v.status = 'COMPLETED' THEN 'COMPLETED'
           WHEN v.status = 'CANCELLED' THEN 'COMPENSATED'
           ELSE 'RUNNING' END
    ),
    'label', v.label,
    'created_at', v.created_at,
    'started_at', v.started_at,
    'completed_at', v.completed_at,
    'metadata', v.metadata
  ), v.client_id
  INTO v_visit, v_id_cliente
  FROM public.visits v
  JOIN public.clients c ON c.client_id = v.client_id
  LEFT JOIN public.users u ON u.user_id = v.user_id
  WHERE v.visit_id = p_visit_id;

  IF v_visit IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Get visit_tasks
  SELECT jsonb_agg(row_data)
  INTO v_tasks
  FROM (
    SELECT jsonb_build_object(
      'task_id', COALESCE(vt.task_id::text, vt.task_type::text || '-' || p_visit_id::text),
      'task_type', vt.task_type::text,
      'status', vt.status::text,
      'required', vt.required,
      'created_at', vt.created_at,
      'started_at', vt.started_at,
      'completed_at', vt.completed_at,
      'due_at', vt.due_at,
      'metadata', vt.metadata,
      'transaction_type', CASE vt.task_type::text
        WHEN 'INITIAL_PLACEMENT' THEN 'COMPENSABLE'
        WHEN 'CUTOFF' THEN 'COMPENSABLE'
        WHEN 'POST_CUTOFF_PLACEMENT' THEN 'COMPENSABLE'
        WHEN 'ODV_CABINET' THEN 'PIVOT'
        WHEN 'SALE_ODV' THEN 'PIVOT'
        ELSE 'RETRYABLE'
      END,
      'step_order', CASE vt.task_type::text
        WHEN 'INITIAL_PLACEMENT' THEN 1
        WHEN 'CUTOFF' THEN 1
        WHEN 'SALE_ODV' THEN 2
        WHEN 'COLLECTION' THEN 3
        WHEN 'POST_CUTOFF_PLACEMENT' THEN 4
        WHEN 'ODV_CABINET' THEN 5
        WHEN 'VISIT_REPORT' THEN 6
        ELSE 99
      END,
      'compensation_status', 'NOT_NEEDED'
    ) as row_data
    FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
    ORDER BY CASE vt.task_type::text
      WHEN 'INITIAL_PLACEMENT' THEN 1
      WHEN 'CUTOFF' THEN 1
      WHEN 'SALE_ODV' THEN 2
      WHEN 'COLLECTION' THEN 3
      WHEN 'POST_CUTOFF_PLACEMENT' THEN 4
      WHEN 'ODV_CABINET' THEN 5
      WHEN 'VISIT_REPORT' THEN 6
      ELSE 99
    END
  ) sub;

  -- Get ODVs from cabinet_sale_odv_ids
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', cso.odv_id,
      'odv_numero', cso.odv_id,
      'type', cso.odv_type::text,
      'odv_date', cso.created_at,
      'status', 'linked',
      'total_piezas', COALESCE(
        (SELECT SUM(im.quantity)::int
         FROM inventory_movements im
         WHERE im.visit_id = p_visit_id
           AND im.type = CASE cso.odv_type
             WHEN 'SALE' THEN 'SALE'::cabinet_movement_type
             ELSE 'PLACEMENT'::cabinet_movement_type
           END
        ), 0
      ),
      'items', COALESCE(
        (SELECT jsonb_agg(jsonb_build_object(
          'sku', im.sku,
          'product', COALESCE(m.product, im.sku),
          'quantity', im.quantity
        ))
        FROM inventory_movements im
        LEFT JOIN medications m ON m.sku = im.sku
        WHERE im.visit_id = p_visit_id
          AND im.type = CASE cso.odv_type
            WHEN 'SALE' THEN 'SALE'::cabinet_movement_type
            ELSE 'PLACEMENT'::cabinet_movement_type
          END
        ), '[]'::jsonb
      )
    ) as odv_data
    FROM cabinet_sale_odv_ids cso
    WHERE cso.visit_id = p_visit_id
    ORDER BY cso.created_at
  ) sub;

  -- Get movements (directly via visit_id)
  SELECT jsonb_build_object(
    'total', COALESCE(mov_stats.cnt, 0),
    'total_cantidad', COALESCE(mov_stats.suma_cantidad, 0),
    'unique_skus', COALESCE(mov_stats.skus_unicos, 0),
    'by_tipo', COALESCE(mov_tipos.tipos, '{}'::jsonb),
    'items', COALESCE(mov_items.items, '[]'::jsonb)
  )
  INTO v_movements
  FROM (
    SELECT
      COUNT(*)::int as cnt,
      COALESCE(SUM(mi.quantity), 0)::int as suma_cantidad,
      COUNT(DISTINCT mi.sku)::int as skus_unicos
    FROM public.inventory_movements mi
    WHERE mi.visit_id = p_visit_id
  ) mov_stats,
  (
    SELECT jsonb_object_agg(type::text, suma_cantidad) as tipos
    FROM (
      SELECT mi.type, COALESCE(SUM(mi.quantity), 0)::int as suma_cantidad
      FROM public.inventory_movements mi
      WHERE mi.visit_id = p_visit_id
      GROUP BY mi.type
    ) sub
  ) mov_tipos,
  (
    SELECT jsonb_agg(row_data) as items
    FROM (
      SELECT jsonb_build_object(
        'sku', mi.sku,
        'type', mi.type::text,
        'quantity', mi.quantity,
        'quantity_before', mi.quantity_before,
        'quantity_after', mi.quantity_after,
        'created_at', mi.movement_date
      ) as row_data
      FROM public.inventory_movements mi
      WHERE mi.visit_id = p_visit_id
      ORDER BY mi.movement_date
      LIMIT 100
    ) sub
  ) mov_items;

  -- Get visit report
  SELECT jsonb_build_object(
    'report_id', vi.report_id,
    'completed', vi.completed,
    'compliance_score', vi.compliance_score,
    'label', vi.label,
    'responses', vi.responses,
    'completed_date', vi.completed_date,
    'created_at', vi.created_at
  )
  INTO v_report
  FROM public.visit_reports vi
  WHERE vi.visit_id = p_visit_id;

  -- Get collections
  SELECT jsonb_agg(row_data)
  INTO v_collections
  FROM (
    SELECT jsonb_build_object(
      'collection_id', r.collection_id,
      'status', r.status,
      'latitud', r.latitude,
      'longitud', r.longitude,
      'cedis_observations', r.cedis_observations,
      'cedis_responsible_name', r.cedis_responsible_name,
      'delivered_at', r.delivered_at,
      'created_at', r.created_at,
      'metadata', r.metadata
    ) as row_data
    FROM public.collections r
    WHERE r.visit_id = p_visit_id
    ORDER BY r.created_at
  ) sub;

  RETURN jsonb_build_object(
    'visit', v_visit,
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'odvs', COALESCE(v_odvs, '[]'::jsonb),
    'movements', COALESCE(v_movements, '{"total": 0, "total_cantidad": 0, "unique_skus": 0, "by_tipo": {}, "items": []}'::jsonb),
    'report', v_report,
    'collections', COALESCE(v_collections, '[]'::jsonb)
  );
END;
$function$;

-- ============================================================
-- STEP 16: Rewrite rpc_get_cutoff_items (remove saga fallback)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_items(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_result jsonb;
BEGIN
  SELECT jsonb_agg(jsonb_build_object(
    'sku', im.sku,
    'product', m.product,
    'brand', m.brand,
    'type', im.type,
    'quantity', im.quantity,
    'unit_price', im.unit_price,
    'validated', im.validated,
    'movement_date', im.movement_date
  ) ORDER BY im.movement_date)
  INTO v_result
  FROM inventory_movements im
  JOIN medications m ON im.sku = m.sku
  WHERE im.visit_id = p_visit_id;

  RETURN COALESCE(v_result, '[]'::jsonb);
END;
$function$;

-- ============================================================
-- STEP 17: Rewrite rpc_owner_delete_visit (remove saga fallback)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_owner_delete_visit(p_visit_id uuid, p_user_id text, p_reason text DEFAULT NULL)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_visit RECORD;
BEGIN
  SELECT v.* INTO v_visit FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM users u
    WHERE u.user_id = p_user_id
      AND u.role = 'OWNER'
  ) THEN
    RAISE EXCEPTION 'Only OWNER role can delete visits';
  END IF;

  -- Compensate if there are movements
  IF EXISTS (SELECT 1 FROM inventory_movements WHERE visit_id = p_visit_id) THEN
    PERFORM rpc_compensate_visit_v2(p_visit_id, COALESCE(p_reason, 'Owner delete'));
  END IF;

  -- Delete cabinet_sale_odv_ids for this visit
  DELETE FROM cabinet_sale_odv_ids WHERE visit_id = p_visit_id;

  -- Audit log
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before)
  VALUES ('visits', p_visit_id::text, 'DELETE_VISIT', p_user_id,
          jsonb_build_object('reason', p_reason, 'client_id', v_visit.client_id, 'status', v_visit.status));

  -- Delete visit (cascades via FK)
  DELETE FROM visit_reports WHERE visit_id = p_visit_id;
  DELETE FROM visit_tasks WHERE visit_id = p_visit_id;
  DELETE FROM visits WHERE visit_id = p_visit_id;

  RETURN jsonb_build_object('success', true, 'visit_id', p_visit_id);
END;
$function$;

-- ============================================================
-- STEP 18: Rewrite rpc_admin_force_task_status (remove saga refs)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_admin_force_task_status(
  p_visit_task_id uuid,
  p_admin_id varchar,
  p_new_status text,
  p_reason text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_task RECORD;
BEGIN
  -- Only OWNER can force states
  IF NOT EXISTS (
    SELECT 1 FROM users
    WHERE user_id = p_admin_id AND role = 'OWNER'
  ) THEN
    RAISE EXCEPTION 'Solo OWNER puede forzar estados de tareas';
  END IF;

  IF p_new_status NOT IN ('PENDING', 'COMPLETED', 'ERROR', 'SKIPPED_M', 'SKIPPED') THEN
    RAISE EXCEPTION 'Estado inválido: %', p_new_status;
  END IF;

  SELECT * INTO v_task FROM visit_tasks WHERE task_id = p_visit_task_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Tarea no encontrada';
  END IF;

  -- Record in audit_log
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before, values_after)
  VALUES ('visit_tasks', p_visit_task_id::text, 'FORCE_STATUS', p_admin_id,
          jsonb_build_object('task_status', v_task.status::text),
          jsonb_build_object('task_status', p_new_status, 'reason', p_reason));

  -- Force status change
  UPDATE visit_tasks
  SET
    status = p_new_status::visit_task_status,
    completed_at = CASE WHEN p_new_status = 'COMPLETED' THEN NOW() ELSE completed_at END,
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'forced_at', NOW(),
      'forced_by', p_admin_id,
      'forced_reason', p_reason
    )
  WHERE task_id = p_visit_task_id;

  RETURN jsonb_build_object(
    'success', true,
    'message', format('Estado forzado a %s', p_new_status)
  );
END;
$function$;

-- ============================================================
-- STEP 19: Rewrite rpc_get_cutoff_holding_items (remove saga refs)
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_holding_items(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
BEGIN
  -- Get holding items from visit_tasks metadata or inventory_movements
  -- In the new model, holding items are tracked via visit_tasks CUTOFF metadata
  SELECT jsonb_agg(jsonb_build_object(
    'sku', im.sku,
    'product', COALESCE(m.product, im.sku),
    'quantity', im.quantity
  ))
  INTO v_items
  FROM inventory_movements im
  LEFT JOIN medications m ON m.sku = im.sku
  WHERE im.visit_id = p_visit_id
    AND im.type = 'HOLDING';

  -- Fallback: get holding info from task metadata
  IF v_items IS NULL THEN
    SELECT jsonb_agg(jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', GREATEST(0,
        (COALESCE(item->>'cantidad_actual', item->>'quantity', '0'))::int
        - (COALESCE(item->>'vendido', '0'))::int
        - (COALESCE(item->>'recolectado', '0'))::int
      )
    )) FILTER (WHERE
      GREATEST(0,
        (COALESCE(item->>'cantidad_actual', item->>'quantity', '0'))::int
        - (COALESCE(item->>'vendido', '0'))::int
        - (COALESCE(item->>'recolectado', '0'))::int
      ) > 0
    )
    INTO v_items
    FROM visit_tasks vt
    CROSS JOIN LATERAL jsonb_array_elements(vt.metadata->'items') AS item
    LEFT JOIN medications m ON m.sku = item->>'sku'
    WHERE vt.visit_id = p_visit_id
      AND vt.task_type = 'CUTOFF'
      AND (item->>'permanencia')::boolean = true;
  END IF;

  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$function$;

-- ============================================================
-- STEP 20: Rewrite analytics.get_dashboard_data
--   Replace saga_odv_ids CTE and m1_odv_ids CTE
-- ============================================================
-- The saga_odv_ids CTE needs to use cabinet_sale_odv_ids instead of saga_zoho_links
-- The m1_odv_ids CTE needs to use cabinet_sale_odv_ids instead of saga_zoho_links + saga_transactions

-- First get the full function and replace saga references
-- saga_odv_ids: SELECT DISTINCT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE'
--   → SELECT DISTINCT odv_id FROM cabinet_sale_odv_ids WHERE odv_type = 'SALE'
-- m1_odv_ids: SELECT DISTINCT szl.zoho_id, st.client_id FROM saga_zoho_links szl JOIN saga_transactions st...
--   → SELECT DISTINCT odv_id, client_id FROM cabinet_sale_odv_ids WHERE odv_type = 'SALE'
-- The NOT IN (SELECT zoho_id FROM saga_odv_ids) filters need the same treatment

-- We need to update the function in place. The function is a single SQL statement.
-- Use a targeted approach: CREATE OR REPLACE with the corrected CTEs.

-- Due to the massive size of get_dashboard_data, we update it via a DO block
-- that reads the full prosrc and replaces saga references
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_dashboard_data';

  -- 1. Rename CTE: saga_odv_ids → linked_odv_ids (definition + all downstream refs)
  v_def := REPLACE(v_def, 'saga_odv_ids', 'linked_odv_ids');

  -- 2. Replace table: saga_zoho_links → cabinet_sale_odv_ids
  v_def := REPLACE(v_def, 'saga_zoho_links', 'cabinet_sale_odv_ids');

  -- 3. Replace column refs: szl.zoho_id → szl.odv_id
  v_def := REPLACE(v_def, 'szl.zoho_id', 'szl.odv_id');

  -- 4. Replace column refs: szl.type → szl.odv_type
  v_def := REPLACE(v_def, 'szl.type', 'szl.odv_type');

  -- 5. Replace column: st.client_id → szl.client_id (m1_odv_ids CTE uses saga_transactions alias)
  v_def := REPLACE(v_def, 'st.client_id', 'szl.client_id');

  -- 6. Remove saga_transactions JOIN in m1_odv_ids CTE (handles any whitespace)
  v_def := regexp_replace(v_def,
    E'\\s+JOIN saga_transactions st ON szl\\.id_saga_transaction = st\\.id',
    '', 'g');

  -- 7. Fix downstream CTE column refs: zoho_id → odv_id
  v_def := REPLACE(v_def, 'zoho_id', 'odv_id');

  -- Verify no saga references remain
  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_dashboard_data still contains saga references: %',
      substring(v_def from '([^\n]*saga[^\n]*)');
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 21: Rewrite analytics.get_cutoff_logistics_data
--   Replace all saga_transactions/saga_zoho_links JOINs with
--   inventory_movements.visit_id and cabinet_sale_odv_ids
--   NOTE: Must DROP first because return type changed (saga_status → workflow_status)
-- ============================================================
DROP FUNCTION IF EXISTS analytics.get_cutoff_logistics_data(varchar[], varchar[], varchar[]);
DROP FUNCTION IF EXISTS public.get_cutoff_logistics_data(varchar[], varchar[], varchar[]);
CREATE OR REPLACE FUNCTION analytics.get_cutoff_logistics_data(
  p_doctors varchar[] DEFAULT NULL,
  p_brands varchar[] DEFAULT NULL,
  p_conditions varchar[] DEFAULT NULL
)
RETURNS TABLE(
  advisor_name text,
  client_name varchar,
  client_id varchar,
  visit_date text,
  sku varchar,
  product varchar,
  placed_quantity integer,
  sale_qty integer,
  collection_qty integer,
  total_cutoff integer,
  destino text,
  workflow_status text,
  odv_cabinet text,
  odv_sale text,
  collection_id uuid,
  collection_status text,
  evidence_paths text[],
  signature_path text,
  observations text,
  received_by text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.client_id,
      v.user_id,
      v.completed_at::date AS fecha_visita,
      v.workflow_status,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.type = 'VISIT_CUTOFF'
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_placement_visits AS (
    SELECT DISTINCT ON (cv.client_id)
      cv.client_id,
      v.visit_id
    FROM current_visits cv
    JOIN visits v_cur ON v_cur.visit_id = cv.visit_id
    JOIN visits v ON v.client_id = cv.client_id
      AND v.visit_id != cv.visit_id
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at < v_cur.completed_at
    WHERE EXISTS (
      SELECT 1 FROM inventory_movements mi
      WHERE mi.visit_id = v.visit_id
        AND mi.type = 'PLACEMENT'
        AND mi.client_id = cv.client_id
    )
    ORDER BY cv.client_id, v.completed_at DESC
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  )
  SELECT
    u.name::text                                          AS nombre_asesor,
    c.client_name,
    mov.client_id,
    TO_CHAR(cv.fecha_visita, 'YYYY-MM-DD')                AS fecha_visita,
    mov.sku,
    med.product,
    (SELECT COALESCE(SUM(m_cre.quantity), 0)
     FROM inventory_movements m_cre
     WHERE m_cre.visit_id = ppv.visit_id
       AND m_cre.client_id = mov.client_id
       AND m_cre.sku = mov.sku
       AND m_cre.type = 'PLACEMENT')::int                  AS cantidad_colocada,
    CASE WHEN mov.type = 'SALE'       THEN mov.quantity ELSE 0 END AS qty_venta,
    CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END AS qty_recoleccion,
    mov.quantity                                           AS total_corte,
    mov.type::text                                         AS destino,
    cv.workflow_status::text                                AS workflow_status,
    (SELECT STRING_AGG(DISTINCT cso.odv_id, ', ' ORDER BY cso.odv_id)
     FROM cabinet_sale_odv_ids cso
     WHERE cso.visit_id = ppv.visit_id
       AND cso.odv_type = 'CABINET'
    )                                                      AS odv_botiquin,
    (SELECT STRING_AGG(DISTINCT cso.odv_id, ', ' ORDER BY cso.odv_id)
     FROM cabinet_sale_odv_ids cso
     WHERE cso.visit_id = cv.visit_id
       AND cso.odv_type = 'SALE'
    )                                                      AS odv_venta,
    rcl.collection_id,
    rcl.status::text                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM collection_evidence re
     WHERE re.collection_id = rcl.collection_id)           AS evidencia_paths,
    (SELECT rf.storage_path
     FROM collection_signatures rf
     WHERE rf.collection_id = rcl.collection_id
     LIMIT 1)                                              AS firma_path,
    rcl.cedis_observations                                 AS observaciones,
    rcl.cedis_responsible_name                             AS quien_recibio
  FROM current_visits cv
  LEFT JOIN prev_placement_visits ppv ON ppv.client_id = cv.client_id
  JOIN inventory_movements mov ON mov.visit_id = cv.visit_id
  JOIN clients c        ON mov.client_id = c.client_id
  JOIN medications med  ON mov.sku = med.sku
  LEFT JOIN users u     ON cv.user_id = u.user_id
  LEFT JOIN collections rcl ON cv.visit_id = rcl.visit_id AND mov.client_id = rcl.client_id
  WHERE mov.type IN ('SALE', 'COLLECTION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY c.client_name, mov.sku;
END;
$function$;

-- Public wrapper for PostgREST
CREATE OR REPLACE FUNCTION public.get_cutoff_logistics_data(
  p_doctors varchar[] DEFAULT NULL,
  p_brands varchar[] DEFAULT NULL,
  p_conditions varchar[] DEFAULT NULL
)
RETURNS TABLE(
  advisor_name text,
  client_name varchar,
  client_id varchar,
  visit_date text,
  sku varchar,
  product varchar,
  placed_quantity integer,
  sale_qty integer,
  collection_qty integer,
  total_cutoff integer,
  destino text,
  workflow_status text,
  odv_cabinet text,
  odv_sale text,
  collection_id uuid,
  collection_status text,
  evidence_paths text[],
  signature_path text,
  observations text,
  received_by text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_cutoff_logistics_data(p_doctors, p_brands, p_conditions);
$$;

-- ============================================================
-- STEP 22: Rewrite analytics.get_current_cutoff_data
--   Replace saga_transactions JOIN with inventory_movements.visit_id
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_current_cutoff_data';

  -- Replace both saga_transactions + inventory_movements JOIN pairs using regex
  -- Handles any whitespace (trailing spaces from pg_get_functiondef)
  v_def := regexp_replace(v_def,
    E'JOIN saga_transactions st ON st\\.visit_id = (\\w+)\\.visit_id\\s+JOIN inventory_movements mov ON mov\\.id_saga_transaction = st\\.id',
    E'JOIN inventory_movements mov ON mov.visit_id = \\1.visit_id',
    'g');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_current_cutoff_data still contains saga references: %',
      substring(v_def from '([^\n]*saga[^\n]*)');
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 23: Rewrite analytics.get_cutoff_stats_by_doctor
--   Replace id_saga_transaction grouping with visit_id
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_cutoff_stats_by_doctor()
RETURNS TABLE(
  client_id varchar,
  client_name varchar,
  visit_date date,
  sale_pieces integer,
  placement_pieces integer,
  collection_pieces integer,
  sale_value numeric,
  placement_value numeric,
  collection_value numeric,
  sold_skus text,
  placed_skus text,
  collected_skus text,
  has_sale boolean
)
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH visitas_en_corte AS (
    SELECT DISTINCT
      mov.client_id,
      mov.visit_id,
      MIN(mov.movement_date::date) as fecha_visita
    FROM inventory_movements mov
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.visit_id IS NOT NULL
    GROUP BY mov.client_id, mov.visit_id
  )
  SELECT
    c.client_id,
    c.client_name,
    MAX(v.fecha_visita) as fecha_visita,
    SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END)::int,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'PLACEMENT' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'COLLECTION' THEN mov.sku END, ', '),
    SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END) > 0
  FROM visitas_en_corte v
  JOIN inventory_movements mov ON v.visit_id = mov.visit_id
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON v.client_id = c.client_id
  GROUP BY c.client_id, c.client_name
  ORDER BY SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) DESC,
           c.client_name;
END;
$function$;

-- ============================================================
-- STEP 24: Rewrite analytics.get_historical_cutoff_data
--   Replace id_saga_transaction grouping with visit_id
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_historical_cutoff_data';

  -- Replace all id_saga_transaction references with visit_id
  v_def := REPLACE(v_def, 'id_saga_transaction', 'visit_id');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_historical_cutoff_data still contains saga references: %',
      substring(v_def from '([^\n]*saga[^\n]*)');
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 25: Rewrite analytics.get_historical_skus_value_per_visit
--   Replace id_saga_transaction grouping with visit_id
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_historical_skus_value_per_visit';

  -- Replace all id_saga_transaction references with visit_id
  v_def := REPLACE(v_def, 'id_saga_transaction', 'visit_id');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_historical_skus_value_per_visit still contains saga references: %',
      substring(v_def from '([^\n]*saga[^\n]*)');
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 26: Rewrite analytics.get_conversion_details
--   Replace saga_zoho_links filter with cabinet_sale_odv_ids
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_conversion_details';

  v_def := REPLACE(v_def,
    E'AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = \'SALE\' AND szl.zoho_id IS NOT NULL)',
    E'AND v.odv_id NOT IN (SELECT cso.odv_id FROM cabinet_sale_odv_ids cso WHERE cso.odv_type = \'SALE\' AND cso.odv_id IS NOT NULL)');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_conversion_details still contains saga references';
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 27: Rewrite analytics.get_crosssell_significance
--   Replace saga_zoho_links filter
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_crosssell_significance';

  -- Use word-level replacements to avoid whitespace mismatch
  v_def := REPLACE(v_def, 'saga_zoho_links szl', 'cabinet_sale_odv_ids cso');
  v_def := REPLACE(v_def, 'szl.zoho_id', 'cso.odv_id');
  v_def := REPLACE(v_def, 'szl.type', 'cso.odv_type');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_crosssell_significance still contains saga references: %',
      substring(v_def from '([^\n]*saga[^\n]*)');
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 28: Rewrite analytics.get_impact_detail
--   Replace saga_zoho_links filter
-- ============================================================
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_impact_detail';

  v_def := REPLACE(v_def,
    E'AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = \'SALE\' AND szl.zoho_id IS NOT NULL)',
    E'AND v.odv_id NOT IN (SELECT cso.odv_id FROM cabinet_sale_odv_ids cso WHERE cso.odv_type = \'SALE\' AND cso.odv_id IS NOT NULL)');

  IF v_def LIKE '%saga%' THEN
    RAISE EXCEPTION 'get_impact_detail still contains saga references';
  END IF;

  EXECUTE v_def;
END;
$$;

-- ============================================================
-- STEP 29: Rewrite analytics.get_client_audit
--   Complete rewrite using visit_id + cabinet_sale_odv_ids
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_client_audit(p_client varchar)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
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
  SELECT json_build_object('id', c.client_id, 'name', c.client_name)
  INTO v_cliente
  FROM clients c
  WHERE c.client_id = p_client;

  IF v_cliente IS NULL THEN
    RETURN json_build_object('error', 'Client not found');
  END IF;

  -- _av: All completed visits with movements
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    COALESCE(v.completed_at, v.created_at)::date as visit_date,
    COALESCE(v.type::text, 'UNKNOWN') as visit_type,
    ROW_NUMBER() OVER (ORDER BY v.corte_number, v.created_at, v.visit_id) as visit_num
  FROM visits v
  WHERE v.client_id = p_client
    AND v.status = 'COMPLETED'
    AND EXISTS (SELECT 1 FROM inventory_movements im WHERE im.visit_id = v.visit_id AND im.client_id = p_client);

  -- 2. Visits with tasks, movements, anomalies
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.visit_date, 'YYYY-MM-DD'),
      'visit_type', av.visit_type,
      'tasks', (
        SELECT COALESCE(json_agg(json_build_object(
          'task_type', vt.task_type::text,
          'status', vt.status::text,
          'transaction_type', vt.transaction_type::text,
          'completed_at', vt.completed_at
        ) ORDER BY vt.step_order), '[]'::json)
        FROM visit_tasks vt
        WHERE vt.visit_id = av.visit_id
      ),
      'odvs', (
        SELECT COALESCE(json_agg(json_build_object(
          'odv_id', cso.odv_id,
          'type', cso.odv_type::text
        )), '[]'::json)
        FROM cabinet_sale_odv_ids cso
        WHERE cso.visit_id = av.visit_id
      ),
      'movements', (
        SELECT COALESCE(json_agg(
          json_build_object(
            'mov_id', m.id,
            'sku', m.sku,
            'product', med.product,
            'type', m.type::text,
            'quantity', m.quantity,
            'date', TO_CHAR(m.movement_date, 'YYYY-MM-DD')
          ) ORDER BY m.sku, m.type
        ), '[]'::json)
        FROM inventory_movements m
        JOIN medications med ON m.sku = med.sku
        WHERE m.visit_id = av.visit_id
          AND m.client_id = p_client
      ),
      'inventory_pieces', (
        SELECT COALESCE(SUM(
          CASE m.type
            WHEN 'PLACEMENT' THEN m.quantity
            WHEN 'SALE' THEN -m.quantity
            WHEN 'COLLECTION' THEN -m.quantity
            ELSE 0
          END
        ), 0)
        FROM inventory_movements m
        JOIN _av av2 ON m.visit_id = av2.visit_id
        WHERE m.client_id = p_client
          AND av2.visit_date <= av.visit_date
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
      ),
      'inventory_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM inventory_movements m
          JOIN _av av2 ON m.visit_id = av2.visit_id
          WHERE m.client_id = p_client
            AND av2.visit_date <= av.visit_date
            AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
          GROUP BY m.sku
          HAVING SUM(
            CASE m.type
              WHEN 'PLACEMENT' THEN m.quantity
              WHEN 'SALE' THEN -m.quantity
              WHEN 'COLLECTION' THEN -m.quantity
              ELSE 0
            END
          ) > 0
        ) sc
      ),
      'anomalies', (
        SELECT COALESCE(json_agg(va.msg), '[]'::json)
        FROM (
          SELECT 'DUPLICATE_MOVEMENT: ' || m.sku || ' ' || m.type::text
                 || ' appears ' || COUNT(*) || ' times' as msg
          FROM inventory_movements m
          WHERE m.visit_id = av.visit_id
            AND m.client_id = p_client
          GROUP BY m.sku, m.type
          HAVING COUNT(*) > 1

          UNION ALL

          SELECT 'SALE_WITHOUT_PLACEMENT: ' || m.sku || ' ' || m.type::text
                 || ' without prior PLACEMENT in cabinet' as msg
          FROM inventory_movements m
          WHERE m.visit_id = av.visit_id
            AND m.client_id = p_client
            AND m.type IN ('SALE', 'COLLECTION')
            AND NOT EXISTS (
              SELECT 1
              FROM inventory_movements m2
              JOIN _av av2 ON m2.visit_id = av2.visit_id
              WHERE m2.client_id = p_client
                AND m2.sku = m.sku
                AND m2.type = 'PLACEMENT'
                AND av2.visit_num <= av.visit_num
            )
        ) va
      )
    ) as vr
    FROM _av av
  ) visit_rows;

  -- 3. SKU Lifecycle
  SELECT COALESCE(json_agg(sr ORDER BY sr->>'sku'), '[]'::json)
  INTO v_ciclo
  FROM (
    SELECT json_build_object(
      'sku', sub.sku,
      'product', sub.product,
      'events', sub.eventos,
      'current_status', CASE
        WHEN sub.last_tipo = 'COLLECTION' THEN 'COLLECTED'
        WHEN sub.last_tipo = 'SALE' THEN 'SOLD'
        ELSE 'ACTIVE'
      END
    ) as sr
    FROM (
      SELECT
        m.sku,
        MAX(med.product) as product,
        json_agg(
          json_build_object(
            'visit_num', av.visit_num,
            'date', TO_CHAR(m.movement_date, 'YYYY-MM-DD'),
            'type', m.type::text,
            'quantity', m.quantity
          ) ORDER BY av.visit_num, m.type
        ) as eventos,
        (
          SELECT m2.type::text
          FROM inventory_movements m2
          JOIN _av av2 ON m2.visit_id = av2.visit_id
          WHERE m2.client_id = p_client AND m2.sku = m.sku
            AND m2.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
          ORDER BY av2.visit_num DESC, m2.movement_date DESC
          LIMIT 1
        ) as last_tipo
      FROM inventory_movements m
      JOIN medications med ON m.sku = med.sku
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
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
      'type', 'visit',
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.visit_date, 'YYYY-MM-DD'),
      'label', 'V' || av.visit_num || ' ' || TO_CHAR(av.visit_date, 'Mon DD'),
      'visit_type', av.visit_type
    ) as n
    FROM _av av

    UNION ALL

    -- ODV nodes (from cabinet_sale_odv_ids)
    SELECT json_build_object(
      'id', 'odv-' || cso.odv_id || '-v' || av.visit_num,
      'type', CASE cso.odv_type WHEN 'SALE' THEN 'odv_sale' ELSE 'odv' END,
      'label', cso.odv_id,
      'visit_num', av.visit_num,
      'pieces', (SELECT SUM(im.quantity) FROM inventory_movements im
                 WHERE im.visit_id = av.visit_id AND im.client_id = p_client),
      'skus_count', (SELECT COUNT(DISTINCT im.sku) FROM inventory_movements im
                     WHERE im.visit_id = av.visit_id AND im.client_id = p_client)
    ) as n
    FROM cabinet_sale_odv_ids cso
    JOIN _av av ON cso.visit_id = av.visit_id
    WHERE cso.odv_id IS NOT NULL

    UNION ALL

    -- SKU nodes
    SELECT DISTINCT ON (m.sku)
      json_build_object(
        'id', 'sku-' || m.sku,
        'type', 'sku',
        'label', m.sku,
        'product', med.product
      ) as n
    FROM inventory_movements m
    JOIN medications med ON m.sku = med.sku
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
  ) all_nodes;

  -- 5. Graph Edges
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- PLACEMENT edges
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'sku-' || m.sku,
      'type', 'PLACEMENT',
      'label', 'PLA(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'PLACEMENT'
    GROUP BY m.sku, av.visit_num

    UNION ALL

    -- SALE edges
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'sku-' || m.sku,
      'type', 'SALE',
      'label', 'SAL(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'SALE'
    GROUP BY m.sku, av.visit_num

    UNION ALL

    -- COLLECTION edges
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'sku-' || m.sku,
      'type', 'COLLECTION',
      'label', 'COL(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'COLLECTION'
    GROUP BY m.sku, av.visit_num
  ) all_edges;

  -- 6. Count anomalies
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
    SELECT 1
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
    GROUP BY av.visit_id, m.sku, m.type
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND m.type IN ('SALE', 'COLLECTION')
      AND NOT EXISTS (
        SELECT 1
        FROM inventory_movements m2
        JOIN _av av2 ON m2.visit_id = av2.visit_id
        WHERE m2.client_id = p_client
          AND m2.sku = m.sku
          AND m2.type = 'PLACEMENT'
          AND av2.visit_num <= av.visit_num
      )
    GROUP BY m.sku
  ) anomalies;

  -- 7. Summary
  SELECT json_build_object(
    'total_visits', (SELECT COUNT(*) FROM _av),
    'total_historical_skus', (
      SELECT COUNT(DISTINCT m.sku)
      FROM inventory_movements m
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
    ),
    'current_inventory_pieces', (
      SELECT COALESCE(SUM(
        CASE m.type
          WHEN 'PLACEMENT' THEN m.quantity
          WHEN 'SALE' THEN -m.quantity
          WHEN 'COLLECTION' THEN -m.quantity
          ELSE 0
        END
      ), 0)
      FROM inventory_movements m
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
        AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
    ),
    'current_inventory_skus', (
      SELECT COUNT(*) FROM (
        SELECT m.sku
        FROM inventory_movements m
        JOIN _av av ON m.visit_id = av.visit_id
        WHERE m.client_id = p_client
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
        GROUP BY m.sku
        HAVING SUM(
          CASE m.type
            WHEN 'PLACEMENT' THEN m.quantity
            WHEN 'SALE' THEN -m.quantity
            WHEN 'COLLECTION' THEN -m.quantity
            ELSE 0
          END
        ) > 0
      ) active
    ),
    'total_anomalies', v_anomalias_count,
    'all_cabinet_odvs', (
      SELECT COALESCE(json_agg(DISTINCT cso.odv_id ORDER BY cso.odv_id), '[]'::json)
      FROM cabinet_sale_odv_ids cso
      JOIN _av av ON cso.visit_id = av.visit_id
      WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'CABINET'
    ),
    'all_sale_odvs', (
      SELECT COALESCE(json_agg(DISTINCT cso.odv_id ORDER BY cso.odv_id), '[]'::json)
      FROM cabinet_sale_odv_ids cso
      JOIN _av av ON cso.visit_id = av.visit_id
      WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'SALE'
    )
  ) INTO v_resumen;

  -- 8. Return combined result
  RETURN json_build_object(
    'client', v_cliente,
    'visits', COALESCE(v_visitas, '[]'::json),
    'sku_lifecycle', COALESCE(v_ciclo, '[]'::json),
    'graph', json_build_object(
      'nodes', COALESCE(v_grafo_nodos, '[]'::json),
      'edges', COALESCE(v_grafo_aristas, '[]'::json)
    ),
    'summary', v_resumen
  );
END;
$function$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_client_audit(p_client varchar)
RETURNS json
LANGUAGE sql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT analytics.get_client_audit(p_client);
$$;

-- ============================================================
-- STEP 30: Recreate v_visit_tasks_operational view without saga refs
-- ============================================================
CREATE OR REPLACE VIEW v_visit_tasks_operational AS
SELECT
  task_id::text AS task_id,
  visit_id,
  task_type,
  status,
  required,
  created_at,
  started_at,
  completed_at,
  due_at,
  last_activity_at,
  reference_table,
  reference_id,
  metadata,
  transaction_type::text AS transaction_type,
  step_order,
  'NOT_NEEDED'::text AS compensation_status,
  '{}'::jsonb AS input_payload,
  '{}'::jsonb AS output_result,
  NULL::jsonb AS compensation_payload,
  gen_random_uuid()::text AS idempotency_key,
  0 AS retry_count,
  3 AS max_retries,
  NULL::text AS last_error,
  NULL::timestamptz AS compensation_executed_at,
  CASE
    WHEN status = 'COMPLETED' THEN 'COMPLETED'::visit_task_status
    WHEN status = 'SKIPPED_M' THEN status
    WHEN status = 'SKIPPED' THEN status
    WHEN status = 'ERROR' THEN status
    WHEN due_at IS NOT NULL AND (due_at + interval '1 day') < now()
      AND status NOT IN ('COMPLETED', 'SKIPPED_M', 'SKIPPED')
      THEN 'DELAYED'::visit_task_status
    ELSE status
  END AS operational_status,
  -- ODV IDs from cabinet_sale_odv_ids instead of saga_zoho_links
  CASE
    WHEN task_type = 'SALE_ODV' THEN (
      SELECT string_agg(cso.odv_id, ', ' ORDER BY cso.created_at)
      FROM cabinet_sale_odv_ids cso
      WHERE cso.visit_id = vt.visit_id AND cso.odv_type = 'SALE'
    )
    WHEN task_type = 'ODV_CABINET' THEN (
      SELECT string_agg(cso.odv_id, ', ' ORDER BY cso.created_at)
      FROM cabinet_sale_odv_ids cso
      WHERE cso.visit_id = vt.visit_id AND cso.odv_type = 'CABINET'
    )
    ELSE NULL
  END AS odv_id,
  -- Total pieces from inventory_movements
  CASE
    WHEN task_type = 'SALE_ODV' THEN (
      SELECT COALESCE(SUM(im.quantity), 0)::int
      FROM inventory_movements im
      WHERE im.visit_id = vt.visit_id AND im.type = 'SALE'
    )
    WHEN task_type = 'ODV_CABINET' THEN (
      SELECT COALESCE(SUM(im.quantity), 0)::int
      FROM inventory_movements im
      WHERE im.visit_id = vt.visit_id AND im.type = 'PLACEMENT'
    )
    ELSE NULL
  END AS odv_total_pieces
FROM visit_tasks vt;

-- ============================================================
-- STEP 31: PostgREST cache reload
-- ============================================================
NOTIFY pgrst, 'reload schema';

COMMIT;
