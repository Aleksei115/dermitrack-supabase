-- =============================================================================
-- Reconcile cabinet_inventory.available_quantity with active lots
-- =============================================================================
-- The lot backfill migration (20260401000006) combined with the inventory sync
-- trigger caused cabinet_inventory to double-count some SKUs. The lots are the
-- source of truth. This migration aligns cabinet_inventory to match.
--
-- Pattern: for every (client_id, sku) where cabinet_inventory.available_quantity
-- differs from SUM(cabinet_inventory_lots.remaining_quantity WHERE active),
-- set cabinet_inventory to match lots.
--
-- This is idempotent — if already reconciled, it updates 0 rows.
-- =============================================================================

DO $$
DECLARE
  v_fixed int;
BEGIN
  WITH lot_sums AS (
    SELECT client_id, sku, COALESCE(SUM(remaining_quantity), 0) AS lot_total
    FROM cabinet_inventory_lots
    WHERE status = 'active'
    GROUP BY client_id, sku
  ),
  mismatches AS (
    SELECT ci.client_id, ci.sku, ci.available_quantity AS inv_qty, ls.lot_total
    FROM cabinet_inventory ci
    JOIN lot_sums ls ON ci.client_id = ls.client_id AND ci.sku = ls.sku
    WHERE ci.available_quantity <> ls.lot_total
  )
  UPDATE cabinet_inventory ci
  SET available_quantity = m.lot_total,
      last_updated = now()
  FROM mismatches m
  WHERE ci.client_id = m.client_id AND ci.sku = m.sku;

  GET DIAGNOSTICS v_fixed = ROW_COUNT;

  IF v_fixed > 0 THEN
    INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_after)
    VALUES (
      'cabinet_inventory',
      'lots-reconciliation-20260411',
      'UPDATE',
      NULL,
      jsonb_build_object(
        'reason', 'Reconcile cabinet_inventory with active lots (source of truth)',
        'rows_fixed', v_fixed
      )
    );
    RAISE NOTICE 'Reconciled % cabinet_inventory rows to match active lots', v_fixed;
  ELSE
    RAISE NOTICE 'Cabinet inventory already in sync with lots — no changes needed';
  END IF;
END $$;
