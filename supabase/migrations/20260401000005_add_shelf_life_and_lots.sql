-- ============================================================================
-- Migration 6: Add shelf_life_months to medications + cabinet_inventory_lots
-- Fase 0: Expiry tracking with FEFO (First Expired First Out)
-- ============================================================================

-- ── Shelf life on medications ───────────────────────────────────────────────

ALTER TABLE medications
  ADD COLUMN shelf_life_months integer DEFAULT 12;

COMMENT ON COLUMN medications.shelf_life_months IS
'Shelf life in months after placement. Used to compute cabinet_inventory_lots.expiry_date = placement_date + shelf_life_months. Default: 12 months. NULL means no expiry tracking.';

-- ── Lot status enum ─────────────────────────────────────────────────────────

CREATE TYPE lot_status AS ENUM ('active', 'consumed', 'collected', 'expired');

COMMENT ON TYPE lot_status IS
'active: in cabinet. consumed: sold via SALE movement (FEFO). collected: returned via COLLECTION. expired: past expiry_date (marked by daily cron).';

-- ── Cabinet inventory lots table ────────────────────────────────────────────
-- Per-batch expiry tracking. Each placement creates lots with computed expiry.
-- FEFO consumption: oldest expiry consumed first on sale/collection.

CREATE TABLE cabinet_inventory_lots (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  client_id character varying NOT NULL REFERENCES clients(client_id),
  sku character varying NOT NULL REFERENCES medications(sku),
  movement_id bigint NOT NULL REFERENCES inventory_movements(id),
  visit_id uuid REFERENCES visits(visit_id),
  quantity integer NOT NULL CHECK (quantity > 0),
  remaining_quantity integer NOT NULL CHECK (remaining_quantity >= 0),
  placement_date date NOT NULL DEFAULT CURRENT_DATE,
  expiry_date date,
  status lot_status NOT NULL DEFAULT 'active',
  consumed_by_movement_id bigint REFERENCES inventory_movements(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE cabinet_inventory_lots IS
'Per-batch expiry tracking for cabinet inventory. Each PLACEMENT creates lots with computed expiry_date = placement_date + medications.shelf_life_months. FEFO consumption: oldest expiry consumed first on SALE/COLLECTION. Extensibility: location_id can be added as nullable column for multi-location lot tracking.';

COMMENT ON COLUMN cabinet_inventory_lots.id IS 'PK. Auto-incrementing bigint.';
COMMENT ON COLUMN cabinet_inventory_lots.client_id IS 'FK to clients. The doctor''s cabinet containing this lot.';
COMMENT ON COLUMN cabinet_inventory_lots.sku IS 'FK to medications. The product in this lot.';
COMMENT ON COLUMN cabinet_inventory_lots.movement_id IS 'FK to inventory_movements. The PLACEMENT movement that created this lot.';
COMMENT ON COLUMN cabinet_inventory_lots.visit_id IS 'FK to visits. The visit during which this lot was placed.';
COMMENT ON COLUMN cabinet_inventory_lots.quantity IS 'Original quantity placed in this lot. Immutable after creation.';
COMMENT ON COLUMN cabinet_inventory_lots.remaining_quantity IS 'Current remaining quantity. Decremented by FEFO consumption (sales/collections). When 0, status changes to consumed/collected.';
COMMENT ON COLUMN cabinet_inventory_lots.placement_date IS 'Date when lot was placed in cabinet. Used with shelf_life_months to compute expiry_date.';
COMMENT ON COLUMN cabinet_inventory_lots.expiry_date IS 'placement_date + medications.shelf_life_months. Lots consumed FEFO (earliest expiry first). NULL if medication has no shelf_life_months.';
COMMENT ON COLUMN cabinet_inventory_lots.status IS 'active: in cabinet. consumed: fully sold via SALE. collected: returned via COLLECTION. expired: past expiry_date (marked by daily cron).';
COMMENT ON COLUMN cabinet_inventory_lots.consumed_by_movement_id IS 'FK to the SALE/COLLECTION movement that consumed this lot. Set when remaining_quantity reaches 0.';

-- ── Indexes ─────────────────────────────────────────────────────────────────

-- Primary query pattern: FEFO consumption (active lots for client+sku ordered by expiry)
CREATE INDEX idx_lots_fefo ON cabinet_inventory_lots (client_id, sku, expiry_date ASC)
  WHERE status = 'active';

-- Expiry cron: find lots that expired today
CREATE INDEX idx_lots_expiring ON cabinet_inventory_lots (expiry_date)
  WHERE status = 'active' AND expiry_date IS NOT NULL;

-- Visit-level lot queries
CREATE INDEX idx_lots_visit ON cabinet_inventory_lots (visit_id)
  WHERE visit_id IS NOT NULL;

-- Movement backref
CREATE INDEX idx_lots_movement ON cabinet_inventory_lots (movement_id);

-- ── RLS ─────────────────────────────────────────────────────────────────────

ALTER TABLE cabinet_inventory_lots ENABLE ROW LEVEL SECURITY;

-- Service role has full access (for RPCs running as SECURITY DEFINER)
CREATE POLICY "service_role_all" ON cabinet_inventory_lots
  FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Authenticated users can read lots for their clients
CREATE POLICY "users_lots_select" ON cabinet_inventory_lots
  FOR SELECT TO authenticated
  USING (
    is_admin()
    OR client_id::text IN (
      SELECT c.client_id FROM clients c
      JOIN users u ON c.user_id::text = u.user_id::text
      WHERE u.auth_user_id = (select auth.uid())
    )
  );

-- ── Updated_at trigger ──────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_lots_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_lots_updated_at
  BEFORE UPDATE ON cabinet_inventory_lots
  FOR EACH ROW EXECUTE FUNCTION fn_lots_updated_at();

NOTIFY pgrst, 'reload schema';
