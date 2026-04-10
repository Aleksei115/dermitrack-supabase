-- ============================================================================
-- Migration 11: Update admin RPCs — remove saga dependencies
-- Fase 3: Admin functions use visit_id directly
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- Update rpc_get_cutoff_items to work without sagas
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_items(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_result jsonb;
BEGIN
  -- Try new path first: get items from inventory_movements directly
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

  IF v_result IS NOT NULL THEN
    RETURN v_result;
  END IF;

  -- Fallback: get items from saga_transactions (legacy visits)
  SELECT st.items
  INTO v_result
  FROM saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.type IN ('SALE', 'CUTOFF')
    AND st.status = 'CONFIRMED'
  ORDER BY st.created_at DESC
  LIMIT 1;

  RETURN COALESCE(v_result, '[]'::jsonb);
END;
$$;

COMMENT ON FUNCTION public.rpc_get_cutoff_items(uuid) IS
'Get cutoff items for a visit. Reads from inventory_movements (new path) or saga_transactions (legacy fallback).';

-- ═══════════════════════════════════════════════════════════════════════════
-- Update rpc_owner_delete_visit to work with direct movements
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.rpc_owner_delete_visit(
  p_visit_id uuid,
  p_user_id text,
  p_reason text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
BEGIN
  -- Validate ownership
  SELECT v.* INTO v_visit FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Check permission (OWNER only)
  IF NOT EXISTS (
    SELECT 1 FROM users u
    WHERE u.user_id = p_user_id
      AND u.role = 'OWNER'
  ) THEN
    RAISE EXCEPTION 'Only OWNER role can delete visits';
  END IF;

  -- Compensate if there are movements (new path)
  IF EXISTS (SELECT 1 FROM inventory_movements WHERE visit_id = p_visit_id) THEN
    PERFORM rpc_compensate_visit_v2(p_visit_id, COALESCE(p_reason, 'Owner delete'));
  END IF;

  -- Compensate any DRAFT sagas (legacy path)
  UPDATE saga_transactions
  SET status = 'CANCELLED_F',
      metadata = metadata || jsonb_build_object('cancel_reason', 'owner_delete')
  WHERE visit_id = p_visit_id AND status = 'DRAFT';

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
$$;

COMMENT ON FUNCTION public.rpc_owner_delete_visit(uuid, text, text) IS
'OWNER-only: delete a visit. Compensates movements (new and legacy paths), cleans up ODV links, and cascade deletes.';

NOTIFY pgrst, 'reload schema';
