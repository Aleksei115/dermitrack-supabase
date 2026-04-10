-- ============================================================
-- Fix rpc_compensate_visit_v2: DELETE order + broaden lot cleanup
-- ============================================================
-- Second bug found while testing the previous fix (20260406000001):
--
--   ERROR: update or delete on table "inventory_movements" violates
--   foreign key constraint "cabinet_inventory_lots_movement_id_fkey"
--
-- ROOT CAUSE:
--   cabinet_inventory_lots.movement_id is NOT NULL and references the
--   PLACEMENT movement that created the lot. The previous function ordered:
--     1) DELETE inventory_movements       ← fails: own placement lots still reference
--     2) DELETE lots WHERE status='active'
--
-- FIX:
--   - Move lot deletion BEFORE movement deletion.
--   - Widen the lot-delete filter: any lot with visit_id = p_visit_id,
--     regardless of status. This also covers the edge case of a lot
--     produced by this visit that was later consumed by a different
--     visit (status='consumed'/'collected'), whose movement_id would
--     otherwise still point to this visit's placement movement being
--     deleted.
--
-- Final order inside the compensate RPC:
--   1) Generate reverse movements
--   2) Restore external lots (consumed_by_movement_id pointing at this visit → NULL)
--   3) DELETE lots produced by this visit (any status)
--   4) DELETE movements of this visit (originals + reverses)
--   5) Mark tasks + visit COMPENSATED, delete collection, audit log
-- ============================================================

CREATE OR REPLACE FUNCTION public.rpc_compensate_visit_v2(p_visit_id uuid, p_reason text DEFAULT NULL::text)
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
  SELECT v.visit_id, v.client_id, v.user_id, v.workflow_status
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id
      AND task_type IN ('SALE_ODV', 'ODV_CABINET')
      AND status = 'COMPLETED'
  ) THEN
    RAISE EXCEPTION 'Cannot compensate: validation tasks already completed. ODV has been linked.';
  END IF;

  IF v_visit.workflow_status = 'COMPENSATED' THEN
    RETURN jsonb_build_object('success', true, 'already_compensated', true);
  END IF;

  -- ── 1) Reverse all movements ──
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

  -- ── 2) Restore lots CONSUMED BY this visit (from other visits) ──
  UPDATE cabinet_inventory_lots
  SET status = 'active',
      remaining_quantity = quantity,
      consumed_by_movement_id = NULL
  WHERE consumed_by_movement_id IN (
    SELECT id FROM inventory_movements WHERE visit_id = p_visit_id
  );

  GET DIAGNOSTICS v_lots_restored = ROW_COUNT;

  -- ── 3) Delete lots PRODUCED by this visit (any status) ────────────────
  --    Must run BEFORE deleting movements because movement_id is NOT NULL
  --    and references this visit's PLACEMENT movements.
  DELETE FROM cabinet_inventory_lots
  WHERE visit_id = p_visit_id;

  -- ── 4) Delete ALL movements for this visit (originals + reverses) ──
  DELETE FROM inventory_movements WHERE visit_id = p_visit_id;

  -- ── 5) Mark all tasks as COMPENSATED ──
  UPDATE visit_tasks
  SET status = 'COMPENSATED',
      metadata = metadata || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason
      )
  WHERE visit_id = p_visit_id
    AND status NOT IN ('COMPENSATED', 'SKIPPED', 'SKIPPED_M');

  -- ── 6) Mark visit as compensated ──
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

  -- ── 7) Delete associated collections ──
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

  -- ── 8) Audit log ──
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
