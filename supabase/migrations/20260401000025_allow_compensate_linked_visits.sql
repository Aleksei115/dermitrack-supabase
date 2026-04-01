-- ============================================================================
-- Migration 25: Allow compensation of visits with linked ODVs
--
-- Previously, rpc_compensate_visit_v2 blocked compensation when SALE_ODV or
-- ODV_CABINET tasks were COMPLETED (i.e. ODV had been linked). This prevented
-- OWNER from deleting visits that had progressed past the pivot point.
--
-- This migration removes that hard block. Instead, the function records
-- which ODV-linked tasks existed (for audit trail) and proceeds with full
-- compensation: reversing movements, restoring lots, cleaning up ODV links,
-- and marking everything as COMPENSATED.
-- ============================================================================

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
  v_odv_tasks_compensated integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.workflow_status
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Cannot compensate if already compensated
  IF v_visit.workflow_status = 'COMPENSATED' THEN
    RETURN jsonb_build_object('success', true, 'already_compensated', true);
  END IF;

  -- Count ODV-linked tasks (for audit trail, no longer blocks)
  SELECT COUNT(*) INTO v_odv_tasks_compensated
  FROM visit_tasks
  WHERE visit_id = p_visit_id
    AND task_type IN ('SALE_ODV', 'ODV_CABINET')
    AND status = 'COMPLETED';

  -- Reverse all movements for this visit (in reverse order)
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

  -- Clean up ODV links for this visit
  DELETE FROM cabinet_sale_odv_ids WHERE visit_id = p_visit_id;

  -- Mark all tasks as COMPENSATED
  UPDATE visit_tasks
  SET status = 'COMPENSATED',
      metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason
      )
  WHERE visit_id = p_visit_id
    AND status NOT IN ('COMPENSATED', 'SKIPPED', 'SKIPPED_M');

  -- Mark visit as compensated
  UPDATE visits
  SET workflow_status = 'COMPENSATED',
      updated_at = now(),
      metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason,
        'movements_reversed', v_movements_reversed,
        'lots_restored', v_lots_restored,
        'odv_tasks_compensated', v_odv_tasks_compensated
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
                             'movements_reversed', v_movements_reversed, 'lots_restored', v_lots_restored,
                             'odv_tasks_compensated', v_odv_tasks_compensated));

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'movements_reversed', v_movements_reversed,
    'lots_restored', v_lots_restored,
    'odv_tasks_compensated', v_odv_tasks_compensated
  );
END;
$function$;

COMMENT ON FUNCTION rpc_compensate_visit_v2(uuid, text) IS
'Compensate a visit: reverse all movements, restore lots, clean up ODV links, mark as COMPENSATED. Works even when ODV tasks are completed. Logs to audit_log.';

NOTIFY pgrst, 'reload schema';
