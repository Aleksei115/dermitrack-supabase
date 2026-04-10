-- ============================================================================
-- Migration 7: Backfill cabinet_inventory_lots from historical PLACEMENT movements
-- Fase 0: Populate lot data from existing inventory_movements
-- ============================================================================

-- Strategy:
-- 1. Create lots from all PLACEMENT movements (each placement = one lot)
-- 2. Compute expiry from placement_date + shelf_life_months
-- 3. Mark consumed/collected lots based on subsequent SALE/COLLECTION movements
-- 4. Verify: SUM(active lot remaining) = cabinet_inventory.available_quantity

-- ── Step 1: Create lots from PLACEMENT movements ────────────────────────────

INSERT INTO cabinet_inventory_lots (
  client_id, sku, movement_id, visit_id, quantity, remaining_quantity,
  placement_date, expiry_date, status
)
SELECT
  im.client_id,
  im.sku,
  im.id AS movement_id,
  im.visit_id,
  im.quantity,
  im.quantity AS remaining_quantity,  -- Will be adjusted in step 3
  im.movement_date::date AS placement_date,
  CASE
    WHEN m.shelf_life_months IS NOT NULL
    THEN (im.movement_date::date + (m.shelf_life_months || ' months')::interval)::date
    ELSE NULL
  END AS expiry_date,
  'active'::lot_status
FROM inventory_movements im
JOIN medications m ON im.sku = m.sku
WHERE im.type = 'PLACEMENT'
ORDER BY im.movement_date ASC;

-- ── Step 2: Consume lots FEFO based on SALE/COLLECTION history ──────────────
-- For each (client, sku), calculate total sold+collected and consume lots

DO $$
DECLARE
  r RECORD;
  consumed_qty integer;
  lot RECORD;
  to_consume integer;
BEGIN
  -- For each (client, sku) pair with outbound movements
  FOR r IN
    SELECT client_id, sku, SUM(quantity) AS total_outbound
    FROM inventory_movements
    WHERE type IN ('SALE', 'COLLECTION')
    GROUP BY client_id, sku
    HAVING SUM(quantity) > 0
  LOOP
    consumed_qty := r.total_outbound;

    -- Consume lots FEFO (earliest expiry first, NULL expiry last)
    FOR lot IN
      SELECT id, remaining_quantity
      FROM cabinet_inventory_lots
      WHERE client_id = r.client_id
        AND sku = r.sku
        AND status = 'active'
      ORDER BY expiry_date ASC NULLS LAST, placement_date ASC, id ASC
    LOOP
      EXIT WHEN consumed_qty <= 0;

      IF lot.remaining_quantity <= consumed_qty THEN
        -- Fully consume this lot
        UPDATE cabinet_inventory_lots
        SET remaining_quantity = 0,
            status = 'consumed'
        WHERE id = lot.id;
        consumed_qty := consumed_qty - lot.remaining_quantity;
      ELSE
        -- Partially consume this lot
        UPDATE cabinet_inventory_lots
        SET remaining_quantity = remaining_quantity - consumed_qty
        WHERE id = lot.id;
        consumed_qty := 0;
      END IF;
    END LOOP;
  END LOOP;
END $$;

-- ── Step 3: Mark expired lots ───────────────────────────────────────────────

UPDATE cabinet_inventory_lots
SET status = 'expired'
WHERE status = 'active'
  AND expiry_date IS NOT NULL
  AND expiry_date < CURRENT_DATE;

-- ── Step 4: Verification ────────────────────────────────────────────────────
-- Log any mismatches between lots and cabinet_inventory for debugging

DO $$
DECLARE
  mismatch_count integer;
BEGIN
  SELECT COUNT(*) INTO mismatch_count
  FROM (
    SELECT ci.client_id, ci.sku, ci.available_quantity,
           COALESCE(lots.total_remaining, 0) AS lots_remaining
    FROM cabinet_inventory ci
    LEFT JOIN (
      SELECT client_id, sku, SUM(remaining_quantity) AS total_remaining
      FROM cabinet_inventory_lots
      WHERE status = 'active'
      GROUP BY client_id, sku
    ) lots ON ci.client_id = lots.client_id AND ci.sku = lots.sku
    WHERE ci.available_quantity != COALESCE(lots.total_remaining, 0)
  ) mismatches;

  IF mismatch_count > 0 THEN
    RAISE WARNING 'Lot backfill: % (client,sku) pairs have mismatched quantities between cabinet_inventory and lot totals. This is expected if there were manual adjustments or HOLDING movements.', mismatch_count;
  ELSE
    RAISE NOTICE 'Lot backfill: All quantities match between cabinet_inventory and lot totals.';
  END IF;
END $$;
