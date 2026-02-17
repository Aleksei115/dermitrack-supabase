-- Remove 67 botiquin_odv rows that belong to saga VENTA ODVs
-- These are billing/charge ODVs, not physical shipments.
-- They were incorrectly imported under MEXPF13496 when they belong to other clients.
-- Rule: saga VENTA ODVs → ventas_odv only, saga LEV/BOTIQUIN ODVs → botiquin_odv only

DELETE FROM botiquin_odv
WHERE odv_id IN (
  SELECT szl.zoho_id
  FROM saga_zoho_links szl
  WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
);
