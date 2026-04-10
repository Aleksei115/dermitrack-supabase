-- ============================================================
-- Cleanup: MEXBR172 legacy bulk-import orphan movements
--
-- Context:
--   25 rows in inventory_movements (IDs 2751-2776, minus 2765)
--   for client MEXBR172 on 2026-02-04 have visit_id IS NULL and
--   task_id IS NULL. They come from a PROD bulk-import that
--   predated the Hibrido inventory migration and never passed
--   through rpc_register_* RPCs.
--
--   Composition: 12 SALE, 3 COLLECTION, 10 PLACEMENT
--
--   Side effect: cabinet_inventory.available_quantity for the
--   affected SKUs is inflated ~2× relative to the actual lots in
--   cabinet_inventory_lots (which never received production/
--   consumption entries from these orphans).
--
-- Action:
--   1. Delete the 25 orphan rows (with strict guards: client_id,
--      date, visit_id IS NULL, task_id IS NULL).
--   2. Reconcile cabinet_inventory for the 24 affected SKUs by
--      setting available_quantity = SUM(lots.remaining_quantity)
--      WHERE status = 'active'.
--   3. Audit log the cleanup.
-- ============================================================

DO $$
DECLARE
  v_deleted_ids integer[];
  v_affected_skus text[];
  v_deleted_count integer;
  v_reconciled_count integer := 0;
  v_sku text;
BEGIN
  -- Capture the orphan IDs and their SKUs before deletion
  SELECT array_agg(id ORDER BY id), array_agg(DISTINCT sku)
  INTO v_deleted_ids, v_affected_skus
  FROM inventory_movements
  WHERE client_id = 'MEXBR172'
    AND movement_date::date = '2026-02-04'
    AND visit_id IS NULL
    AND task_id IS NULL;

  IF v_deleted_ids IS NULL OR cardinality(v_deleted_ids) = 0 THEN
    RAISE NOTICE 'No MEXBR172 2026-02-04 orphan movements found. Skipping.';
    RETURN;
  END IF;

  RAISE NOTICE 'Found % orphan movements across % SKUs',
    cardinality(v_deleted_ids), cardinality(v_affected_skus);

  -- Delete the orphan movements (guarded)
  DELETE FROM inventory_movements
  WHERE client_id = 'MEXBR172'
    AND movement_date::date = '2026-02-04'
    AND visit_id IS NULL
    AND task_id IS NULL;

  GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

  IF v_deleted_count <> cardinality(v_deleted_ids) THEN
    RAISE EXCEPTION 'Delete count mismatch: expected %, got %',
      cardinality(v_deleted_ids), v_deleted_count;
  END IF;

  -- Reconcile cabinet_inventory for each affected SKU against lots
  FOREACH v_sku IN ARRAY v_affected_skus LOOP
    UPDATE cabinet_inventory ci
    SET available_quantity = COALESCE((
          SELECT SUM(remaining_quantity)
          FROM cabinet_inventory_lots
          WHERE client_id = 'MEXBR172'
            AND sku = v_sku
            AND status = 'active'
        ), 0),
        last_updated = now()
    WHERE ci.client_id = 'MEXBR172'
      AND ci.sku = v_sku;

    v_reconciled_count := v_reconciled_count + 1;
  END LOOP;

  -- Audit log entry
  INSERT INTO audit_log (
    table_name, record_id, action, audit_user_id, values_after
  ) VALUES (
    'inventory_movements',
    'MEXBR172-2026-02-04-orphans',
    'DELETE',
    'admin_aleksei',
    jsonb_build_object(
      'reason', 'Legacy bulk-import cleanup (pre-Hibrido orphans)',
      'deleted_ids', v_deleted_ids,
      'deleted_count', v_deleted_count,
      'affected_skus', v_affected_skus,
      'reconciled_skus', v_reconciled_count,
      'client_id', 'MEXBR172',
      'movement_date', '2026-02-04'
    )
  );

  RAISE NOTICE 'Cleanup complete: % movements deleted, % SKUs reconciled',
    v_deleted_count, v_reconciled_count;
END $$;
