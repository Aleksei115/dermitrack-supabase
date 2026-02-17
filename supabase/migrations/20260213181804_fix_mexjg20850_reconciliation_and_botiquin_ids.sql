-- ==========================================================================
-- Fix MEXJG20850 (Jaime Oscar Garcia Paz) inventory reconciliation
-- + Fix botiquin_odv id_cliente for Araceli and Jaime
-- + Add DCOdV-35171 to botiquin_odv
-- 2026 data (Visits 6,7) NOT touched
-- ==========================================================================

-- Part 0: Delete wrong V160 VENTA at V3 (V160 was NOT in Botiquin 1, NOT in ventas_odv)
DELETE FROM public.movimientos_inventario WHERE id = 1177;

-- Part 1: Fix V2 PERMANENCIA — delete wrong qty=0 rows under VENTA saga
DELETE FROM public.movimientos_inventario
WHERE id IN (1770,1771,1772,1773,1774,1775,1776,1777,1778,1779,1780);

-- Insert correct V2 PERMANENCIA with V1 stock quantities, NULL saga
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
VALUES
  (NULL,'MEXJG20850','P005',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','P058',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','P086',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','P138',2,2,2,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','P299',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','Q252',2,2,2,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','Q270',2,2,2,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','W181',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','X952',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','Y365',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA'),
  (NULL,'MEXJG20850','Y458',3,3,3,'2025-10-15 20:25:16+00','PERMANENCIA');

-- Part 2: Link V3 orphan CREACION to LEV_POST_CORTE c4029acd
UPDATE public.movimientos_inventario
SET id_saga_transaction = 'c4029acd-85d5-4255-aa23-e8388d46380c'
WHERE id IN (1168,1169,1170,1171,1174,1176,1175,1173,1172);

-- Part 3: Link V4 orphan CREACION (dated 2025-11-01) to LEV_POST_CORTE a743348e
UPDATE public.movimientos_inventario
SET id_saga_transaction = 'a743348e-6c2e-4ae7-8457-a070064acd9c'
WHERE id IN (1181,1180);

-- Part 4: Fix V4 PERMANENCIA — unlink from VENTA saga
UPDATE public.movimientos_inventario
SET id_saga_transaction = NULL
WHERE id IN (1781,1782,1783,1784,1787,1788,1786,1785);

-- Insert missing V4 PERMANENCIA for P134 and P258 (added by V4 LEV_POST_CORTE)
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
VALUES
  (NULL,'MEXJG20850','P134',1,1,1,'2025-11-14 00:00:00+00','PERMANENCIA'),
  (NULL,'MEXJG20850','P258',1,1,1,'2025-11-14 00:00:00+00','PERMANENCIA');

-- Part 5: Link V5 orphan CREACION to LEV_POST_CORTE 0b89372c
UPDATE public.movimientos_inventario
SET id_saga_transaction = '0b89372c-20b5-4244-a866-87ab0db263a0'
WHERE id IN (1195,1200,1192,1198,1196,1193,1197,1201,1199,1194);

-- Part 6: Add missing V5 VENTA V160(1) under RECOLECCION saga (matches ventas_odv DCOdV-35170)
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
VALUES
  ('2446c584-af4d-4437-a5b9-cb7acb768352','MEXJG20850','V160',1,0,1,'2025-11-28 22:02:17.147869+00','VENTA');

-- Part 7: Add DEVOLUCION saga_zoho_links for RECOLECCION sagas
INSERT INTO public.saga_zoho_links
  (id_saga_transaction, zoho_id, tipo, zoho_sync_status, zoho_synced_at)
VALUES
  ('5016f40a-c0bd-43cc-8d20-ff8045b0da4e', NULL, 'DEVOLUCION', 'synced', now()),
  ('2446c584-af4d-4437-a5b9-cb7acb768352', NULL, 'DEVOLUCION', 'synced', now());

-- Part 8: Update existing 2025 saga_zoho_links to synced
UPDATE public.saga_zoho_links
SET zoho_sync_status = 'synced', zoho_synced_at = now()
WHERE id IN (4, 49, 48, 50, 36, 110, 69);

-- Part 9: Update saga estados
UPDATE public.saga_transactions
SET estado = 'COMPLETADO'
WHERE id IN (
  '88fc7961-77bf-416a-aad3-f14513c193f6',  -- LEVANTAMIENTO_INICIAL (V1)
  '5016f40a-c0bd-43cc-8d20-ff8045b0da4e',  -- RECOLECCION (V3)
  '4a50c734-2e4f-4d7f-acc7-5f6e5c22636f',  -- VENTA (V3)
  'c4029acd-85d5-4255-aa23-e8388d46380c',  -- LEV_POST_CORTE (V3)
  '8663e827-673b-44f1-820e-123989f79bf3',  -- VENTA (V4)
  'a743348e-6c2e-4ae7-8457-a070064acd9c',  -- LEV_POST_CORTE (V4)
  '544e2ca5-dc65-4878-aaa9-4af451d0f2da',  -- VENTA (V5)
  '2446c584-af4d-4437-a5b9-cb7acb768352',  -- RECOLECCION (V5)
  '0b89372c-20b5-4244-a866-87ab0db263a0'   -- LEV_POST_CORTE (V5)
)
AND estado = 'CONFIRMADO';

UPDATE public.saga_transactions
SET estado = 'CANCELADA'
WHERE id IN (
  '243be1bc-4a81-45a6-be8b-0b671b9168e7',  -- Empty VENTA (V2)
  '043537c6-f3f4-4e46-b16b-f14fb14bb542'   -- Orphan LEV_POST_CORTE (V2)
)
AND estado = 'CONFIRMADO';

-- ==========================================================================
-- Part 10: Add DCOdV-35171 to botiquin_odv
-- Note: botiquin_odv.id_cliente FK -> clientes.id_cliente (normal zoho ID)
-- ==========================================================================
INSERT INTO public.botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
VALUES
  ('MEXJG20850','P076','DCOdV-35171','2025-11-28',2,'unpaid'),
  ('MEXJG20850','P192','DCOdV-35171','2025-11-28',2,'unpaid'),
  ('MEXJG20850','Q839','DCOdV-35171','2025-11-28',2,'unpaid'),
  ('MEXJG20850','P028','DCOdV-35171','2025-11-28',5,'unpaid'),
  ('MEXJG20850','P161','DCOdV-35171','2025-11-28',1,'unpaid'),
  ('MEXJG20850','P222','DCOdV-35171','2025-11-28',1,'unpaid'),
  ('MEXJG20850','P133','DCOdV-35171','2025-11-28',5,'unpaid'),
  ('MEXJG20850','P328','DCOdV-35171','2025-11-28',2,'unpaid'),
  ('MEXJG20850','P030','DCOdV-35171','2025-11-28',5,'unpaid'),
  ('MEXJG20850','P292','DCOdV-35171','2025-11-28',5,'unpaid');

-- Part 11: botiquin_odv id_cliente check
-- Both Araceli (MEXAB19703) and Edna (MEXEG032) already use the correct normal zoho ID
-- (botiquin_odv FK -> clientes which only has normal IDs, not botiquin IDs)
-- Jaime (MEXJG20850) also already correct. No changes needed.
