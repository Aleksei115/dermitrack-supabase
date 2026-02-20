-- Migration: Fix MEXBR172 Nov 28 data discrepancies
--
-- Issues addressed:
-- 1a. CREACION movements from saga 176a8254 (Nov 28 LEV_POST_CORTE) don't match BOTIQUIN ODV items
--     - P592, P632, X952 shouldn't exist (not in any BOTIQUIN ODV)
--     - Y810 has quantity 3, but ODV DCOdV-35428 has Y810=1
-- 1b. Reconciliation CREACIONs (P567, P630, P146) from migration 20260219000006 need to be
--     moved to the Nov 28 saga instead of being orphaned
-- 1c. ODV DCOdV-35423 (P081:5 VENTA) missing from saga_zoho_links
-- 1d. VENTA ODVs DCOdV-35158 and DCOdV-36315 have items=NULL
-- 1e. 63 phantom saga_zoho_links entries with zoho_id=NULL
-- 1f. inventario_botiquin needs reconciliation after CREACION corrections

BEGIN;

-- ============================================================
-- PREREQUISITE: Add items column to saga_zoho_links if missing
-- (DEV had this from MCP-applied migrations; PROD needs it here)
-- ============================================================
ALTER TABLE saga_zoho_links ADD COLUMN IF NOT EXISTS items jsonb;

-- ============================================================
-- 1a. Delete incorrect CREACIONs and fix Y810 quantity
-- ============================================================

-- Delete P592, P632, X952 CREACIONs that don't exist in any BOTIQUIN ODV
DELETE FROM movimientos_inventario
WHERE id_cliente = 'MEXBR172'
  AND id_saga_transaction = '176a8254-375c-4da6-bdb2-6e5f743268f0'
  AND tipo = 'CREACION'
  AND sku IN ('P592', 'P632', 'X952');

-- Fix Y810 quantity: ODV DCOdV-35428 has Y810=1, not 3
UPDATE movimientos_inventario
SET cantidad = 1,
    cantidad_despues = cantidad_antes + 1
WHERE id_cliente = 'MEXBR172'
  AND id_saga_transaction = '176a8254-375c-4da6-bdb2-6e5f743268f0'
  AND tipo = 'CREACION'
  AND sku = 'Y810';

-- ============================================================
-- 1b. Move reconciliation CREACIONs (P567, P630, P146) to Nov 28 saga
-- ============================================================

-- These were created by migration 20260219000006 with saga=NULL and fecha=Feb 4.
-- They should belong to the Nov 28 LEV_POST_CORTE saga.
UPDATE movimientos_inventario
SET id_saga_transaction = '176a8254-375c-4da6-bdb2-6e5f743268f0',
    fecha_movimiento = '2025-11-28 22:47:54+00'
WHERE id_cliente = 'MEXBR172'
  AND id_saga_transaction IS NULL
  AND tipo = 'CREACION'
  AND sku IN ('P146', 'P567', 'P630');

-- ============================================================
-- 1c. Add missing DCOdV-35423 to saga_zoho_links
-- ============================================================

-- This ODV exists in ventas_odv (P081:5) but wasn't linked to the VENTA saga
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo, items)
VALUES (
  '791de313-cd4e-4dd3-b176-98b87a6883a0',
  'DCOdV-35423',
  'VENTA',
  '[{"sku":"P081","cantidad":5}]'::jsonb
);

-- ============================================================
-- 1d. Populate items in VENTA ODVs that have items=NULL
-- ============================================================

-- DCOdV-35158: items from ventas_odv
UPDATE saga_zoho_links
SET items = '[{"sku":"P040","cantidad":3},{"sku":"P072","cantidad":1},{"sku":"P299","cantidad":1},{"sku":"P592","cantidad":2},{"sku":"S531","cantidad":1},{"sku":"X952","cantidad":1},{"sku":"Y587","cantidad":2}]'::jsonb
WHERE zoho_id = 'DCOdV-35158' AND tipo = 'VENTA';

-- DCOdV-36315: items from ventas_odv
UPDATE saga_zoho_links
SET items = '[{"sku":"P592","cantidad":2},{"sku":"P632","cantidad":1},{"sku":"X952","cantidad":1},{"sku":"Y810","cantidad":2}]'::jsonb
WHERE zoho_id = 'DCOdV-36315' AND tipo = 'VENTA';

-- ============================================================
-- 1e. Delete phantom saga_zoho_links entries (zoho_id=NULL)
-- ============================================================

-- 63 entries created by bulk operations with no actual Zoho ODV
DELETE FROM saga_zoho_links WHERE zoho_id IS NULL;

-- ============================================================
-- 1f. Reconcile inventario_botiquin for MEXBR172
-- ============================================================

-- Recalculate from movements: after corrections, P592/P632/X952 should have 0 stock
-- and Y810 stock changes due to quantity fix
WITH calculated AS (
  SELECT sku,
    GREATEST(0,
      SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END)
    ) as correct_stock
  FROM movimientos_inventario
  WHERE id_cliente = 'MEXBR172'
  GROUP BY sku
)
UPDATE inventario_botiquin inv
SET cantidad_disponible = c.correct_stock,
    ultima_actualizacion = now()
FROM calculated c
WHERE inv.id_cliente = 'MEXBR172'
  AND inv.sku = c.sku
  AND inv.cantidad_disponible != c.correct_stock;

-- Remove zero-stock entries
DELETE FROM inventario_botiquin
WHERE id_cliente = 'MEXBR172' AND cantidad_disponible = 0;

COMMIT;
