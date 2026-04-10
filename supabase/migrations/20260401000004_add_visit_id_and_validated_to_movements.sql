-- ============================================================================
-- Migration 5: Add visit_id and validated columns to inventory_movements
-- Fase 0: Prepare for saga removal — direct visit link + validation tracking
-- ============================================================================

-- ── Add visit_id column ─────────────────────────────────────────────────────
-- Direct link to visits, replacing saga_transactions.visit_id indirection

ALTER TABLE inventory_movements
  ADD COLUMN visit_id uuid REFERENCES visits(visit_id) ON DELETE SET NULL;

COMMENT ON COLUMN inventory_movements.visit_id IS
'FK to visits. Direct link replacing id_saga_transaction → saga_transactions.visit_id indirection. Extensibility: if location_id is added, visit_id remains the grouping key.';

-- ── Add validated column ────────────────────────────────────────────────────
-- Tracks whether movement has been confirmed against external Zoho ODV document

ALTER TABLE inventory_movements
  ADD COLUMN validated boolean NOT NULL DEFAULT true;

COMMENT ON COLUMN inventory_movements.validated IS
'True when confirmed against Zoho ODV PDF by SALE_ODV/ODV_CABINET tasks via rpc_link_odv(). Movements created by rpc_register_cutoff/placement start as false, set true by rpc_link_odv. COLLECTION and COMPENSATION movements are always true. Analytics can filter by validated=true for official M1 reports.';

-- ── Backfill visit_id from saga_transactions ────────────────────────────────
-- All existing movements go through saga_transactions.visit_id

UPDATE inventory_movements im
SET visit_id = st.visit_id
FROM saga_transactions st
WHERE im.id_saga_transaction = st.id
  AND im.visit_id IS NULL
  AND st.visit_id IS NOT NULL;

-- For movements without saga_transactions but with task_id, find visit via visit_tasks
UPDATE inventory_movements im
SET visit_id = vt.visit_id
FROM visit_tasks vt
WHERE im.task_id = vt.task_id
  AND im.visit_id IS NULL;

-- ── Backfill validated ──────────────────────────────────────────────────────
-- All existing movements already went through saga CONFIRMED → they're validated
-- Default is already true, so existing rows are correct. New rows from new RPCs
-- will explicitly set validated=false for SALE/PLACEMENT movements.

-- ── Indexes ─────────────────────────────────────────────────────────────────

CREATE INDEX idx_inventory_movements_visit_id ON inventory_movements (visit_id)
  WHERE visit_id IS NOT NULL;

-- Partial index for quick "pending validation" queries
CREATE INDEX idx_inventory_movements_unvalidated ON inventory_movements (visit_id, client_id)
  WHERE validated = false;

NOTIFY pgrst, 'reload schema';
