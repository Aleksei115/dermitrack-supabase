-- ============================================================
-- Fix: rpc_owner_delete_visit returns deleted_counts
--
-- Bug: the UI (useDeleteVisit.ts) expects a `deleted_counts`
-- JSONB with {visits, visit_tasks, task_odvs, inventory_movements,
-- collections}, but the previous version of the RPC only returned
-- {success, visit_id}. Result: Alert shows "0 registros eliminados"
-- for every field.
--
-- This migration:
--   1. Counts collections BEFORE calling compensate (compensate
--      deletes them, so capturing ROW_COUNT after is too late).
--   2. Captures compensate's return JSONB with SELECT INTO instead
--      of PERFORM, to rescue movements_reversed and lots_restored.
--   3. Uses GET DIAGNOSTICS ROW_COUNT after each DELETE.
--   4. Returns a structured deleted_counts + compensate summary.
--
-- Note: the UI's "inventory_movements" counter reflects the total
-- number of rows that disappear from inventory_movements (original
-- + reverses created by compensate). That matches user intuition:
-- "how many rows vanished from the table".
-- ============================================================

CREATE OR REPLACE FUNCTION rpc_owner_delete_visit(
  p_visit_id uuid,
  p_user_id text,
  p_reason text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $function$
DECLARE
  v_visit RECORD;
  v_compensate_result jsonb;
  v_already_compensated boolean := false;
  v_movements_reversed integer := 0;
  v_lots_restored integer := 0;
  v_collections_before integer := 0;
  v_odvs_deleted integer := 0;
  v_movements_cleanup integer := 0;
  v_reports_deleted integer := 0;
  v_tasks_deleted integer := 0;
  v_visits_deleted integer := 0;
  v_total_movements integer := 0;
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

  -- Count collections BEFORE compensate (compensate will delete them)
  SELECT COUNT(*) INTO v_collections_before
  FROM collections
  WHERE visit_id = p_visit_id;

  -- Compensate if there are movements, capturing the JSONB response
  IF EXISTS (SELECT 1 FROM inventory_movements WHERE visit_id = p_visit_id) THEN
    SELECT rpc_compensate_visit_v2(p_visit_id, COALESCE(p_reason, 'Owner delete'))
      INTO v_compensate_result;

    v_already_compensated := COALESCE(
      (v_compensate_result ->> 'already_compensated')::boolean, false
    );
    v_movements_reversed := COALESCE(
      (v_compensate_result ->> 'movements_reversed')::integer, 0
    );
    v_lots_restored := COALESCE(
      (v_compensate_result ->> 'lots_restored')::integer, 0
    );
  END IF;

  -- Delete cabinet_sale_odv_ids for this visit
  DELETE FROM cabinet_sale_odv_ids WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_odvs_deleted = ROW_COUNT;

  -- Cleanup any orphan movements still attached to the visit
  -- (compensate deletes them already; this covers edge cases where
  -- compensate was skipped or bypassed the IF check above).
  DELETE FROM inventory_movements WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_movements_cleanup = ROW_COUNT;

  -- Total movements that disappeared from the table:
  -- original rows (= movements_reversed, since compensate deletes both
  -- originals + reverses) + any leftover cleanup.
  -- Each original movement generates one reverse, so the table lost
  -- 2 × movements_reversed rows via compensate, plus the cleanup pass.
  v_total_movements := (v_movements_reversed * 2) + v_movements_cleanup;

  -- Audit log (keep existing shape, extend values_before with counts)
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before)
  VALUES (
    'visits', p_visit_id::text, 'DELETE_VISIT', p_user_id,
    jsonb_build_object(
      'reason', p_reason,
      'client_id', v_visit.client_id,
      'status', v_visit.status,
      'already_compensated', v_already_compensated,
      'movements_reversed', v_movements_reversed,
      'lots_restored', v_lots_restored,
      'collections_deleted', v_collections_before,
      'odvs_deleted', v_odvs_deleted
    )
  );

  -- Delete visit + children
  DELETE FROM visit_reports WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_reports_deleted = ROW_COUNT;

  DELETE FROM visit_tasks WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_tasks_deleted = ROW_COUNT;

  DELETE FROM visits WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_visits_deleted = ROW_COUNT;

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'deleted_counts', jsonb_build_object(
      'visits', v_visits_deleted,
      'visit_tasks', v_tasks_deleted,
      'task_odvs', v_odvs_deleted,
      'inventory_movements', v_total_movements,
      'collections', v_collections_before,
      'visit_reports', v_reports_deleted
    ),
    'compensate', jsonb_build_object(
      'already_compensated', v_already_compensated,
      'movements_reversed', v_movements_reversed,
      'lots_restored', v_lots_restored
    )
  );
END;
$function$;

GRANT EXECUTE ON FUNCTION rpc_owner_delete_visit(uuid, text, text)
  TO authenticated, service_role;

NOTIFY pgrst, 'reload schema';
