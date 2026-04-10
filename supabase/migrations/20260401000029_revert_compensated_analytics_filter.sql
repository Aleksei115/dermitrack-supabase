-- =============================================================================
-- Migration 29: Revert compensated-visit exclusion from analytics VIEW
-- =============================================================================
-- Problem: Migration 27 created analytics.inventory_movements that filters out
-- movements from COMPENSATED visits. This VIEW exists on DEV but not PROD,
-- causing Historico tab KPIs to diverge:
--   Ventas (M1):      DEV $64,861 vs PROD $73,059
--   Creación:         DEV $426,975 vs PROD $438,690
--   Botiquín Activo:  DEV $95,244  vs PROD $106,959
--   Recolección:      DEV $257,304 vs PROD $258,672
--
-- The compensation logic (rpc_compensate_visit_v2) already creates reverse
-- movements, so double-filtering via the VIEW over-subtracts.
--
-- Fix: Remove the WHERE clause from the VIEW, and restore cabinet_inventory
-- quantities that were incorrectly zeroed during compensation of visit
-- 19923779-9632-4a9b-842d-9ed68e3af8a5 (MEXBR172).
-- =============================================================================

BEGIN;

-- Step 1: Replace the analytics VIEW to remove the COMPENSATED filter.
-- Keep the JOIN to visits (no functional harm, maintains schema consistency).
CREATE OR REPLACE VIEW analytics.inventory_movements AS
SELECT im.*
FROM public.inventory_movements im
JOIN public.visits v ON im.visit_id = v.visit_id;

-- Step 2: Restore cabinet_inventory for the compensated visit's 10 PLACEMENTs.
-- The compensation zeroed these rows; add back the original placement quantities.
UPDATE public.cabinet_inventory ci
SET available_quantity = ci.available_quantity + sub.quantity
FROM (
  SELECT client_id, sku, quantity
  FROM public.inventory_movements
  WHERE visit_id = '19923779-9632-4a9b-842d-9ed68e3af8a5'
    AND type = 'PLACEMENT'
) sub
WHERE ci.client_id = sub.client_id
  AND ci.sku = sub.sku;

COMMIT;

NOTIFY pgrst, 'reload schema';
