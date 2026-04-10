-- ============================================================
-- Fix: rpc_compensate_visit_v2 leaves double movements
-- Fix: rpc_owner_delete_visit leaves orphan movements
--
-- Root cause: compensate creates reverse movements but never
-- deletes the originals. The trigger updates cabinet_inventory
-- correctly on each INSERT, but the 2N movements persist and
-- pollute analytics RPCs that COUNT/SUM over inventory_movements.
--
-- Solution: after the reverse loop (cabinet already updated by
-- trigger), DELETE all movements for the visit. The audit_log
-- already captured the counts before deletion.
-- ============================================================

-- ============================================================
-- 1. rpc_compensate_visit_v2 — add DELETE at end
-- ============================================================
CREATE OR REPLACE FUNCTION rpc_compensate_visit_v2(
  p_visit_id uuid,
  p_reason text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_movement RECORD;
  v_reverse_qty_before integer;
  v_reverse_qty_after integer;
  v_movements_reversed integer := 0;
  v_lots_restored integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.saga_status
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
  IF v_visit.saga_status = 'COMPENSATED' THEN
    RETURN jsonb_build_object('success', true, 'already_compensated', true);
  END IF;

  -- Reverse all movements for this visit
  FOR v_movement IN
    SELECT id, client_id, sku, quantity, type, unit_price, task_id
    FROM inventory_movements
    WHERE visit_id = p_visit_id
    ORDER BY id DESC  -- Reverse in opposite order
  LOOP
    -- Get current stock for reverse
    SELECT COALESCE(ci.available_quantity, 0) INTO v_reverse_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_movement.client_id AND ci.sku = v_movement.sku;
    IF NOT FOUND THEN v_reverse_qty_before := 0; END IF;

    -- Reverse: PLACEMENT becomes negative, SALE/COLLECTION become positive
    CASE v_movement.type
      WHEN 'PLACEMENT' THEN
        v_reverse_qty_after := GREATEST(0, v_reverse_qty_before - v_movement.quantity);
      WHEN 'SALE', 'COLLECTION' THEN
        v_reverse_qty_after := v_reverse_qty_before + v_movement.quantity;
    END CASE;

    -- Create reverse movement (always validated=true)
    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_movement.client_id, v_movement.sku, v_movement.quantity,
      v_reverse_qty_before, v_reverse_qty_after,
      now(),
      -- Reverse type: PLACEMENT reversal uses COLLECTION, SALE/COLLECTION reversal uses PLACEMENT
      CASE v_movement.type
        WHEN 'PLACEMENT' THEN 'COLLECTION'::cabinet_movement_type
        ELSE 'PLACEMENT'::cabinet_movement_type
      END,
      v_movement.unit_price, v_movement.task_id, p_visit_id, true
    );

    -- cabinet_inventory updated by trigger

    v_movements_reversed := v_movements_reversed + 1;
  END LOOP;

  -- *** FIX: Delete ALL movements for this visit (originals + reverses) ***
  -- cabinet_inventory is already correct (trigger updated it on each reverse INSERT).
  -- Leaving them causes analytics RPCs to double-count.
  DELETE FROM inventory_movements WHERE visit_id = p_visit_id;

  -- Restore consumed/collected lots back to active
  UPDATE cabinet_inventory_lots
  SET status = 'active',
      remaining_quantity = quantity,  -- Restore to original
      consumed_by_movement_id = NULL
  WHERE visit_id = p_visit_id
    AND status IN ('consumed', 'collected');

  GET DIAGNOSTICS v_lots_restored = ROW_COUNT;

  -- Delete lots created by this visit's placements (they were reversed)
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
  SET saga_status = 'COMPENSATED',
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
          jsonb_build_object('saga_status', v_visit.saga_status),
          jsonb_build_object('saga_status', 'COMPENSATED', 'reason', p_reason,
                             'movements_reversed', v_movements_reversed, 'lots_restored', v_lots_restored));

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'movements_reversed', v_movements_reversed,
    'lots_restored', v_lots_restored
  );
END;
$$;

COMMENT ON FUNCTION rpc_compensate_visit_v2(uuid, text) IS
'Compensate (rollback) a visit. Creates reverse movements to fix cabinet_inventory via trigger, then deletes ALL movements for the visit to prevent analytics double-counting. Only allowed before VALIDATION tasks complete.';

-- ============================================================
-- 2. rpc_owner_delete_visit — add DELETE movements before visit
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

  -- *** FIX: Delete any remaining orphan movements ***
  -- rpc_compensate_visit_v2 already deletes them, but this covers
  -- edge cases where compensate was skipped (no movements existed
  -- at IF check but were created concurrently, or future code paths).
  DELETE FROM inventory_movements WHERE visit_id = p_visit_id;

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

-- Notify PostgREST to reload schema cache
NOTIFY pgrst, 'reload schema';
