-- ============================================================
-- Revert erroneous compensation of visit f220d247-cddc-4d1a-b2ec-301784b51fdd
--
-- Background:
--   Migration 20260402000001_compensate_visit_f220d247.sql improperly
--   compensated this real Nov 28 cutoff visit (MEXBR172 / BEATRIZ REYES
--   JUAREZ / Anabel Baltazar). The visit had 6 legitimate ODV linkages
--   (DCOdV-35155, 35158, 35423, 35428, 36315, 36318) and should not have
--   been compensated. The bad migration deliberately bypassed the ODV
--   safety check and wiped out all 31 inventory movements + 14 lots,
--   destroying all M1/M2/M3 revenue evidence for the visit.
--
-- This migration:
--   1. Re-inserts 31 inventory movements (11 SALE + 6 COLLECTION + 14 PLACEMENT)
--   2. Re-creates 14 cabinet_inventory_lots
--   3. Adjusts cabinet_inventory to the pre-compensation state
--   4. Restores visit workflow_status COMPENSATED -> COMPLETED
--   5. Restores 6 visit_tasks COMPENSATED -> COMPLETED
--   6. Removes 20260402000001 from supabase_migrations.schema_migrations
--      so it can never be re-applied
--
-- Source data:
--   - Movements: seed dump 20260211145225_remote_schema.sql + audit_log
--     of bad migration's reverse movements (records 3071-3101). Cross-
--     referenced shows Y601 q2 COLLECTION was removed and P867 q2 SALE
--     was added between Feb 11 and April 3.
--   - Cabinet target state: derived from quantity_before of the FIRST
--     reverse movement per SKU in the bad migration's audit log.
--
-- Decisions (per user):
--   - Disable trg_sync_inventory + trg_remove_available_sku_on_sale and
--     adjust cabinet_inventory manually (avoids re-applying synthetic
--     legacy quantity_after values).
--   - audit_inventory_movements stays ON so each restored row is logged.
--   - P867 q2 classified as SALE (saga_transactions table dropped during
--     Inventario Hibrido cleanup; classification inferred from outbound
--     direction at cutoff time).
--   - Lots recreated as 'active' with full quantity (matches the state
--     left by the bad migration's "restore" step right before deletion).
--     Any (lots vs cabinet_inventory) discrepancies will be reported by
--     the next backfill verification but do not block operations.
-- ============================================================

DO $REVERT$
DECLARE
  c_visit_id          CONSTANT uuid        := 'f220d247-cddc-4d1a-b2ec-301784b51fdd';
  c_client_id         CONSTANT varchar     := 'MEXBR172';
  c_movement_date     CONSTANT timestamptz := '2025-11-28 22:47:54+00';
  c_movement_date_alt CONSTANT timestamptz := '2025-11-28 00:00:00+00';
  c_cutoff_task       CONSTANT uuid        := 'b25c6007-94a3-44b0-8be9-a26c987b8c0f';
  c_collection_task   CONSTANT uuid        := 'e500079c-4c30-4aac-b3fa-ee742c57df2f';
  v_count             int;
  v_lot_count         int;
BEGIN
  -- ── Sanity checks ──
  PERFORM 1 FROM visits WHERE visit_id = c_visit_id AND workflow_status = 'COMPENSATED';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit % is not in COMPENSATED state - aborting revert', c_visit_id;
  END IF;

  IF EXISTS (SELECT 1 FROM inventory_movements WHERE visit_id = c_visit_id) THEN
    RAISE EXCEPTION 'Visit % already has movements - aborting to avoid duplicates', c_visit_id;
  END IF;

  IF EXISTS (SELECT 1 FROM cabinet_inventory_lots WHERE visit_id = c_visit_id) THEN
    RAISE EXCEPTION 'Visit % already has lots - aborting to avoid duplicates', c_visit_id;
  END IF;

  -- ── Disable triggers (per user decision: manual cabinet_inventory adjustment) ──
  ALTER TABLE inventory_movements DISABLE TRIGGER trg_sync_inventory;
  ALTER TABLE inventory_movements DISABLE TRIGGER trg_remove_available_sku_on_sale;
  -- audit_inventory_movements stays enabled so each restored row is logged

  -- ── Insert 17 outbound movements (11 SALE + 6 COLLECTION) ──
  INSERT INTO inventory_movements
    (client_id, sku, quantity, quantity_before, quantity_after, movement_date, type, unit_price, task_id, visit_id, validated)
  VALUES
    -- SALE (10 from seed) — task=collection per legacy mapping
    (c_client_id, 'P081', 5, 0, 5, c_movement_date,     'SALE',       331.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'Y587', 2, 0, 2, c_movement_date,     'SALE',       646.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'S531', 1, 0, 1, c_movement_date,     'SALE',       412.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P592', 2, 0, 2, c_movement_date,     'SALE',       258.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'X952', 1, 0, 1, c_movement_date,     'SALE',       780.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P299', 1, 0, 1, c_movement_date,     'SALE',       485.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P040', 3, 0, 3, c_movement_date,     'SALE',       284.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P072', 1, 0, 1, c_movement_date,     'SALE',       250.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'Y810', 2, 0, 2, c_movement_date,     'SALE',       400.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P632', 1, 0, 1, c_movement_date,     'SALE',       202.00, c_collection_task, c_visit_id, true),
    -- SALE P867 (added between Feb 11 and April 3, classification inferred)
    (c_client_id, 'P867', 2, 0, 2, c_movement_date,     'SALE',       300.00, c_collection_task, c_visit_id, true),
    -- COLLECTION (6 from seed; Y601 q2 excluded — removed from visit before April 3)
    (c_client_id, 'P299', 1, 0, 1, c_movement_date,     'COLLECTION', 485.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P072', 2, 0, 2, c_movement_date,     'COLLECTION', 250.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'S809', 3, 0, 3, c_movement_date,     'COLLECTION', 147.00, c_collection_task, c_visit_id, true),
    (c_client_id, 'P592', 1, 1, 1, c_movement_date_alt, 'COLLECTION', 258.00, NULL,              c_visit_id, true),
    (c_client_id, 'X952', 1, 1, 1, c_movement_date_alt, 'COLLECTION', 780.00, NULL,              c_visit_id, true),
    (c_client_id, 'Y810', 1, 1, 1, c_movement_date_alt, 'COLLECTION', 400.00, NULL,              c_visit_id, true);

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RAISE NOTICE 'Inserted % outbound movements (target 17)', v_count;

  -- ── Insert 14 PLACEMENT movements ──
  INSERT INTO inventory_movements
    (client_id, sku, quantity, quantity_before, quantity_after, movement_date, type, unit_price, task_id, visit_id, validated)
  VALUES
    (c_client_id, 'Y810', 1, 0, 1, c_movement_date, 'PLACEMENT', 400.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P299', 1, 0, 1, c_movement_date, 'PLACEMENT', 485.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P141', 3, 0, 3, c_movement_date, 'PLACEMENT', 372.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'S809', 3, 0, 3, c_movement_date, 'PLACEMENT', 147.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P072', 2, 0, 2, c_movement_date, 'PLACEMENT', 250.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P105', 2, 0, 2, c_movement_date, 'PLACEMENT', 292.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P371', 3, 0, 3, c_movement_date, 'PLACEMENT', 434.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'V160', 2, 0, 2, c_movement_date, 'PLACEMENT', 626.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P047', 2, 0, 2, c_movement_date, 'PLACEMENT', 229.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P085', 3, 0, 3, c_movement_date, 'PLACEMENT', 270.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P062', 3, 0, 3, c_movement_date, 'PLACEMENT', 267.00, c_cutoff_task, c_visit_id, true),
    (c_client_id, 'P630', 2, 0, 2, c_movement_date, 'PLACEMENT', 243.00, NULL,          c_visit_id, true),
    (c_client_id, 'P567', 2, 0, 2, c_movement_date, 'PLACEMENT', 342.00, NULL,          c_visit_id, true),
    (c_client_id, 'P146', 1, 0, 1, c_movement_date, 'PLACEMENT', 247.00, NULL,          c_visit_id, true);

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RAISE NOTICE 'Inserted % placement movements (target 14)', v_count;

  -- ── Re-enable triggers ──
  ALTER TABLE inventory_movements ENABLE TRIGGER trg_sync_inventory;
  ALTER TABLE inventory_movements ENABLE TRIGGER trg_remove_available_sku_on_sale;

  -- ── Create 14 lots from the placements (FEFO source-of-truth for cabinet stock) ──
  INSERT INTO cabinet_inventory_lots
    (client_id, sku, movement_id, visit_id, quantity, remaining_quantity, placement_date, expiry_date, status)
  SELECT
    im.client_id,
    im.sku,
    im.id,
    im.visit_id,
    im.quantity,
    im.quantity,
    im.movement_date::date,
    CASE
      WHEN m.shelf_life_months IS NOT NULL
        THEN (im.movement_date::date + (m.shelf_life_months || ' months')::interval)::date
      ELSE NULL
    END,
    'active'::lot_status
  FROM inventory_movements im
  JOIN medications m ON im.sku = m.sku
  WHERE im.visit_id = c_visit_id AND im.type = 'PLACEMENT';

  GET DIAGNOSTICS v_lot_count = ROW_COUNT;
  RAISE NOTICE 'Created % lots (target 14)', v_lot_count;

  -- ── Manually adjust cabinet_inventory to pre-compensation state ──
  -- Target values derived from quantity_before of first reverse movement
  -- per SKU in the bad migration's audit log (records 3071-3101).
  UPDATE cabinet_inventory SET available_quantity = 2, last_updated = now() WHERE client_id = c_client_id AND sku = 'Y810';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'P081';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'Y587';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'S531';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'P592';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'X952';
  UPDATE cabinet_inventory SET available_quantity = 2, last_updated = now() WHERE client_id = c_client_id AND sku = 'P299';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'P040';
  UPDATE cabinet_inventory SET available_quantity = 4, last_updated = now() WHERE client_id = c_client_id AND sku = 'P072';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'P632';
  UPDATE cabinet_inventory SET available_quantity = 0, last_updated = now() WHERE client_id = c_client_id AND sku = 'P867';
  UPDATE cabinet_inventory SET available_quantity = 3, last_updated = now() WHERE client_id = c_client_id AND sku = 'P141';
  -- S809 unchanged (target 6 == current 6)
  UPDATE cabinet_inventory SET available_quantity = 4, last_updated = now() WHERE client_id = c_client_id AND sku = 'P105';
  UPDATE cabinet_inventory SET available_quantity = 6, last_updated = now() WHERE client_id = c_client_id AND sku = 'P371';
  UPDATE cabinet_inventory SET available_quantity = 4, last_updated = now() WHERE client_id = c_client_id AND sku = 'V160';
  UPDATE cabinet_inventory SET available_quantity = 4, last_updated = now() WHERE client_id = c_client_id AND sku = 'P047';
  UPDATE cabinet_inventory SET available_quantity = 6, last_updated = now() WHERE client_id = c_client_id AND sku = 'P085';
  UPDATE cabinet_inventory SET available_quantity = 6, last_updated = now() WHERE client_id = c_client_id AND sku = 'P062';
  UPDATE cabinet_inventory SET available_quantity = 3, last_updated = now() WHERE client_id = c_client_id AND sku = 'P630';
  UPDATE cabinet_inventory SET available_quantity = 4, last_updated = now() WHERE client_id = c_client_id AND sku = 'P567';
  UPDATE cabinet_inventory SET available_quantity = 2, last_updated = now() WHERE client_id = c_client_id AND sku = 'P146';

  -- ── Restore visit workflow_status ──
  UPDATE visits
  SET workflow_status = 'COMPLETED',
      updated_at      = now(),
      metadata        = COALESCE(metadata, '{}'::jsonb)
                        - 'compensated_at'
                        - 'compensation_reason'
                        - 'movements_reversed'
                        - 'lots_restored'
                        || jsonb_build_object(
                             'reverted_compensation_at', now(),
                             'reverted_by_migration',    '20260406000000'
                           )
  WHERE visit_id = c_visit_id;

  -- ── Restore 6 visit_tasks (all currently COMPENSATED) ──
  UPDATE visit_tasks
  SET status   = 'COMPLETED',
      metadata = COALESCE(metadata, '{}'::jsonb)
                 - 'compensated_at'
                 - 'compensation_reason'
  WHERE visit_id = c_visit_id AND status = 'COMPENSATED';

  -- ── Audit log entry ──
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before, values_after)
  VALUES (
    'visits',
    c_visit_id::text,
    'REVERT',
    NULL,
    jsonb_build_object('workflow_status', 'COMPENSATED'),
    jsonb_build_object(
      'workflow_status',       'COMPLETED',
      'restored_movements',    31,
      'restored_lots',         v_lot_count,
      'reason',                'Erroneous compensation by migration 20260402000001 - visit had legitimate ODV linkages',
      'reverted_by_migration', '20260406000000'
    )
  );

  RAISE NOTICE 'Visit % restored: 31 movements, % lots, 6 tasks, cabinet_inventory adjusted',
    c_visit_id, v_lot_count;
END
$REVERT$;

-- ── Remove the offending migration from the registry ──
-- This prevents 20260402000001 from being re-applied if anyone runs
-- `supabase db reset` or pulls the migration history again.
DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260402000001';
