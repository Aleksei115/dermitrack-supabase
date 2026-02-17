-- Fix erroneous VENTA sagas and re-run rebuild to clean up stale movements
--
-- Root cause analysis:
-- 1. 12 EMPTY VENTA sagas (0 items, no ODV link) — created by the app even when
--    no sale occurred. They should be CANCELADA.
-- 2. 4 LEGACY VENTA sagas with salida=0 — actually record PERMANENCIA only (all items
--    stayed in botiquin). They generate 0 VENTA movements but cause confusion.
-- 3. 62 VENTA movements exist under RECOLECCION sagas — stale data from a previous
--    rebuild version that incorrectly split RECOLECCION.cantidad_salida into VENTA
--    movements. The current rebuild function is correct but was never re-run.
--
-- Fix:
-- A. Cancel 16 erroneous VENTA sagas (rebuild already skips CANCELADA estado)
-- B. Re-run rebuild — the existing function handles everything correctly:
--    - RECOLECCION sagas: only create RECOLECCION movements (uses current_stock)
--    - VENTA sagas: only create VENTA from cantidad_salida, skip when salida=0
--    - Hybrid VENTA sagas: VENTA from salida, permanencia stays in stock naturally

-- ============================================================
-- Part A: Cancel EMPTY VENTA sagas (12 sagas, 0 items, no ODV)
-- ============================================================
UPDATE saga_transactions SET estado = 'CANCELADA'
WHERE id IN (
  -- MEXEG032: R2 Oct 15, R3 Oct 29, R5 Nov 28
  '6a819e5c-851f-4441-b95b-701a79d81fe6',
  '59921d73-ce57-415c-bf72-f919ab8d2c7a',
  '944e611b-7322-4ae8-bf63-c1cbabf8080a',
  -- MEXHR15497: R2 Oct 15, R3 Oct 30, R5 Nov 28
  '567ec5db-a104-448e-b72d-8b9f01e3232e',
  'e8e37b3a-b910-45fe-a122-0c60e844c2c3',
  '1929e448-d9eb-48bb-a004-c522f966b853',
  -- MEXFS22989: R3 Oct 30, R5 Nov 28
  '4135ca7d-d038-4540-ac68-3818384d3465',
  '596ba3f9-e59a-43f2-aa08-d836eec02ae0',
  -- MEXAF10018: R3 Oct 30, R5 Nov 27
  '958ef26a-ea5e-4153-a92b-340b7d0a1f10',
  'a44b038c-b96b-4b5e-97d4-de27e03a88cd',
  -- MEXAB19703: R2 Oct 15
  '6e02715c-6f7d-4fc4-9c22-48851fb303ac',
  -- MEXJG20850: R2 Oct 15
  '243be1bc-4a81-45a6-be8b-0b671b9168e7'
)
AND tipo IN ('VENTA', 'VENTA_ODV')
AND estado = 'CONFIRMADO';

-- ============================================================
-- Part B: Cancel LEGACY VENTA sagas with total salida=0
-- All items have cantidad_salida=0, cantidad_permanencia=30
-- These are NOT sales — everything stayed in the botiquin
-- ============================================================
UPDATE saga_transactions SET estado = 'CANCELADA'
WHERE id IN (
  '9a2e63c4-b46f-4791-b016-5700efdf9df7',  -- MEXHR15497 R4 Nov 15
  'd55f29e3-fe87-4e39-bbdf-0bc06beb3551',  -- MEXFS22989 R4 Nov 15
  '26dacdc8-56a1-4b6e-b992-333513a1c658',  -- MEXAF10018 R4 Nov 15
  'e3da25f0-0ba0-474e-9e91-ddb0e68c6cad'   -- MEXEG032  R4 Nov 15
)
AND tipo IN ('VENTA', 'VENTA_ODV')
AND estado = 'CONFIRMADO';

-- ============================================================
-- Part C: Re-run rebuild
-- The deployed function already handles everything correctly:
-- - Skips CANCELADA sagas (our 16 cancelled above)
-- - RECOLECCION legacy: drains current_stock, adds back permanencia
-- - VENTA legacy: uses cantidad_salida capped to stock
-- - VENTA NEW format: uses cantidad capped to stock
-- This clears 62 stale VENTA movements under RECOLECCION sagas
-- ============================================================
SELECT * FROM rebuild_movimientos_inventario();
