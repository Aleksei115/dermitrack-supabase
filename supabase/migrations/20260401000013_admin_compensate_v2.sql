-- ============================================================================
-- Migration 14: Admin compensation v2 — visit-based admin tools
-- Replaces: rpc_admin_compensate_task (saga-based)
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_admin_compensate_task_v2: Adjust movements by visit/task
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_admin_compensate_task_v2(
  p_visit_id uuid,
  p_task_type text,
  p_admin_id text,
  p_reason text,
  p_new_items jsonb DEFAULT '[]'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_task RECORD;
  v_item RECORD;
  v_old_movement RECORD;
  v_qty_diff integer;
  v_qty_before integer;
  v_qty_after integer;
  v_unit_price numeric;
  v_adjustments_made integer := 0;
  v_old_items jsonb;
BEGIN
  -- Validate admin role
  IF NOT EXISTS (
    SELECT 1 FROM users u
    WHERE u.user_id = p_admin_id
      AND u.role IN ('ADMIN', 'OWNER')
  ) THEN
    RAISE EXCEPTION 'Only ADMIN or OWNER can compensate tasks';
  END IF;

  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Find the task
  SELECT vt.* INTO v_task
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = p_task_type::visit_task_type;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Task % not found for visit %', p_task_type, p_visit_id;
  END IF;

  -- Capture old state for audit
  SELECT jsonb_agg(jsonb_build_object(
    'sku', im.sku, 'quantity', im.quantity, 'type', im.type, 'validated', im.validated
  ))
  INTO v_old_items
  FROM inventory_movements im
  WHERE im.visit_id = p_visit_id AND im.task_id = v_task.task_id;

  -- Determine movement type based on task
  -- CUTOFF/SALE_ODV → SALE movements; INITIAL_PLACEMENT/ODV_CABINET/POST_CUTOFF_PLACEMENT → PLACEMENT
  DECLARE
    v_movement_type cabinet_movement_type;
  BEGIN
    v_movement_type := CASE
      WHEN p_task_type IN ('CUTOFF', 'SALE_ODV') THEN 'SALE'::cabinet_movement_type
      ELSE 'PLACEMENT'::cabinet_movement_type
    END;

    -- Process each new item adjustment
    FOR v_item IN SELECT * FROM jsonb_array_elements(p_new_items)
    LOOP
      -- Find existing movement for this SKU
      SELECT im.* INTO v_old_movement
      FROM inventory_movements im
      WHERE im.visit_id = p_visit_id
        AND im.task_id = v_task.task_id
        AND im.sku = v_item.value->>'sku'
        AND im.type = v_movement_type
      LIMIT 1;

      v_qty_diff := (v_item.value->>'quantity')::integer - COALESCE(v_old_movement.quantity, 0);

      IF v_qty_diff = 0 THEN CONTINUE; END IF;

      -- Get current stock
      SELECT COALESCE(ci.available_quantity, 0) INTO v_qty_before
      FROM cabinet_inventory ci
      WHERE ci.client_id = v_visit.client_id AND ci.sku = v_item.value->>'sku';
      IF NOT FOUND THEN v_qty_before := 0; END IF;

      -- Get unit price
      SELECT m.price INTO v_unit_price
      FROM medications m WHERE m.sku = v_item.value->>'sku';

      -- Create adjustment movement
      IF v_movement_type = 'SALE' THEN
        v_qty_after := GREATEST(0, v_qty_before - v_qty_diff);
      ELSE
        v_qty_after := v_qty_before + v_qty_diff;
      END IF;

      INSERT INTO inventory_movements (
        client_id, sku, quantity, quantity_before, quantity_after,
        movement_date, type, unit_price, task_id, visit_id, validated
      ) VALUES (
        v_visit.client_id, v_item.value->>'sku', ABS(v_qty_diff),
        v_qty_before, v_qty_after,
        now(),
        -- If diff is negative (reducing), reverse the movement type
        CASE
          WHEN v_qty_diff > 0 THEN v_movement_type
          WHEN v_movement_type = 'SALE' THEN 'PLACEMENT'::cabinet_movement_type
          ELSE 'COLLECTION'::cabinet_movement_type
        END,
        v_unit_price, v_task.task_id, p_visit_id, true
      );

      -- Update FEFO lots if applicable
      IF v_qty_diff > 0 AND v_movement_type = 'SALE' THEN
        PERFORM _consume_lots_fefo(v_visit.client_id, v_item.value->>'sku', v_qty_diff, 'sale');
      END IF;

      v_adjustments_made := v_adjustments_made + 1;
    END LOOP;
  END;

  -- Log to audit_log
  INSERT INTO audit_log (
    table_name, record_id, action, audit_user_id,
    values_before, values_after
  ) VALUES (
    'visits', p_visit_id::text, 'COMPENSATE_TASK', p_admin_id,
    jsonb_build_object('task_type', p_task_type, 'old_items', v_old_items),
    jsonb_build_object('reason', p_reason, 'new_items', p_new_items,
                       'adjustments_made', v_adjustments_made)
  );

  RETURN jsonb_build_object(
    'success', true,
    'message', format('Ajuste aplicado: %s movimientos modificados', v_adjustments_made),
    'adjustments_made', v_adjustments_made
  );
END;
$$;

COMMENT ON FUNCTION rpc_admin_compensate_task_v2(uuid, text, text, text, jsonb) IS
'Admin: adjust movements for a specific visit task. Creates corrective movements and logs to audit_log. Replaces saga-based rpc_admin_compensate_task.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_admin_retry_validation: Re-run validation for failed tasks
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_admin_retry_validation(
  p_visit_task_id uuid,
  p_admin_id text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_task RECORD;
  v_unvalidated_count integer;
BEGIN
  -- Validate admin role
  IF NOT EXISTS (
    SELECT 1 FROM users u
    WHERE u.user_id = p_admin_id
      AND u.role IN ('ADMIN', 'OWNER')
  ) THEN
    RAISE EXCEPTION 'Only ADMIN or OWNER can retry validation';
  END IF;

  -- Find the task
  SELECT vt.* INTO v_task
  FROM visit_tasks vt
  WHERE vt.task_id = p_visit_task_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Task not found: %', p_visit_task_id;
  END IF;

  -- Task must be in a retryable state
  IF v_task.status NOT IN ('PENDING', 'ERROR', 'FAILED') THEN
    RAISE EXCEPTION 'Task must be PENDING, ERROR, or FAILED to retry. Current: %', v_task.status;
  END IF;

  -- Count unvalidated movements for this visit
  SELECT COUNT(*) INTO v_unvalidated_count
  FROM inventory_movements im
  WHERE im.visit_id = v_task.visit_id
    AND im.validated = false;

  IF v_unvalidated_count = 0 THEN
    -- No unvalidated movements — mark task as completed
    UPDATE visit_tasks
    SET status = 'COMPLETED',
        completed_at = now(),
        last_activity_at = now(),
        metadata = metadata || jsonb_build_object(
          'retried_by', p_admin_id,
          'retried_at', now(),
          'auto_completed', true,
          'reason', 'No unvalidated movements found'
        )
    WHERE task_id = p_visit_task_id;

    -- Log
    INSERT INTO audit_log (
      table_name, record_id, action, audit_user_id,
      values_before, values_after
    ) VALUES (
      'visit_tasks', p_visit_task_id::text, 'RETRY_VALIDATION', p_admin_id,
      jsonb_build_object('status', v_task.status),
      jsonb_build_object('status', 'COMPLETED', 'auto_completed', true)
    );

    RETURN jsonb_build_object(
      'success', true,
      'message', 'Tarea completada automaticamente — no hay movimientos sin validar'
    );
  END IF;

  -- Reset task to PENDING for retry
  UPDATE visit_tasks
  SET status = 'PENDING',
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'retried_by', p_admin_id,
        'retried_at', now(),
        'unvalidated_movements', v_unvalidated_count
      )
  WHERE task_id = p_visit_task_id;

  -- Log
  INSERT INTO audit_log (
    table_name, record_id, action, audit_user_id,
    values_before, values_after
  ) VALUES (
    'visit_tasks', p_visit_task_id::text, 'RETRY_VALIDATION', p_admin_id,
    jsonb_build_object('status', v_task.status),
    jsonb_build_object('status', 'PENDING', 'unvalidated_movements', v_unvalidated_count)
  );

  RETURN jsonb_build_object(
    'success', true,
    'message', format('Tarea reiniciada a PENDING — %s movimientos sin validar', v_unvalidated_count)
  );
END;
$$;

COMMENT ON FUNCTION rpc_admin_retry_validation(uuid, text) IS
'Admin: retry a failed validation task. If no unvalidated movements remain, auto-completes. Otherwise resets to PENDING. Logs to audit_log.';

-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION rpc_admin_compensate_task_v2(uuid, text, text, text, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_admin_retry_validation(uuid, text) TO authenticated;

NOTIFY pgrst, 'reload schema';
