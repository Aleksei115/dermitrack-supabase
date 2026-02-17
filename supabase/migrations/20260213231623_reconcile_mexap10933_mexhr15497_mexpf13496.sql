-- Reconciliación de datos para MEXAP10933, MEXHR15497, MEXPF13496
-- Principios:
--   botiquin_odv = verdad para CREACION
--   Saga items = filtro para VENTA (cantidades de saga salida)
--   RECOLECCION = derivada de (stock pre-visita - VENTA), todo a 0 antes de CREACION
-- Anomalías documentadas en plan

------------------------------------------------------------
-- CLEANUP: Remove bad DCOdV-35289 data (wrongly assigned to MEXPF13496)
------------------------------------------------------------
DELETE FROM public.botiquin_odv WHERE odv_id = 'DCOdV-35289' AND id_cliente = 'MEXPF13496';

------------------------------------------------------------
-- CLIENTE 1: MEXAP10933
------------------------------------------------------------

-- 1a. botiquin_odv INSERT — DCOdV-35289 (11 rows)
INSERT INTO public.botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
VALUES
  ('MEXAP10933','Y517','DCOdV-35289','2025-12-01',4,'unpaid'),
  ('MEXAP10933','P024','DCOdV-35289','2025-12-01',4,'unpaid'),
  ('MEXAP10933','P120','DCOdV-35289','2025-12-01',2,'unpaid'),
  ('MEXAP10933','P225','DCOdV-35289','2025-12-01',4,'unpaid'),
  ('MEXAP10933','P226','DCOdV-35289','2025-12-01',3,'unpaid'),
  ('MEXAP10933','S809','DCOdV-35289','2025-12-01',3,'unpaid'),
  ('MEXAP10933','T430','DCOdV-35289','2025-12-01',2,'unpaid'),
  ('MEXAP10933','P632','DCOdV-35289','2025-12-01',2,'unpaid'),
  ('MEXAP10933','R790','DCOdV-35289','2025-12-01',2,'unpaid'),
  ('MEXAP10933','R319','DCOdV-35289','2025-12-01',2,'unpaid'),
  ('MEXAP10933','P048','DCOdV-35289','2025-12-01',2,'unpaid');

-- 1b. saga_zoho_links — 2 DEVOLUCION (RECOL sagas)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('d8255714-ca1d-40f3-b758-392f06615bc6', NULL, 'DEVOLUCION', 'synced'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2', NULL, 'DEVOLUCION', 'synced');

-- 1c. Movimientos rebuild — DELETE 2025 + INSERT 58 rows
DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXAP10933' AND fecha_movimiento < '2026-01-01';

-- V1 (2025-09-23) — 8 CREACION (saga f4b510ee)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','R197','CREACION',3,0,3,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P188','CREACION',2,0,2,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P157','CREACION',3,0,3,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P061','CREACION',5,0,5,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P115','CREACION',5,0,5,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P032','CREACION',5,0,5,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','P070','CREACION',5,0,5,'2025-09-23'),
  ('f4b510ee-84f2-404e-9da6-84ca5b7ca952','MEXAP10933','S531','CREACION',2,0,2,'2025-09-23');

-- V2 (2025-10-15) — 2 VENTA + 6 PERMANENCIA (saga 384353d2)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-33357)
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P032','VENTA',1,5,4,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P070','VENTA',1,5,4,'2025-10-15'),
  -- PERMANENCIA
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','R197','PERMANENCIA',3,3,3,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P188','PERMANENCIA',2,2,2,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P157','PERMANENCIA',3,3,3,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P061','PERMANENCIA',5,5,5,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','P115','PERMANENCIA',5,5,5,'2025-10-15'),
  ('384353d2-be93-4ecd-b243-b31aa3dc2044','MEXAP10933','S531','PERMANENCIA',2,2,2,'2025-10-15');

-- V3 (2025-10-30) — 3 VENTA + 7 RECOLECCION + 7 CREACION (sagas 940de401, d8255714, 0937d8a4)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-33406, saga 940de401)
  ('940de401-8090-4008-b278-430d5773b035','MEXAP10933','P032','VENTA',1,4,3,'2025-10-30'),
  ('940de401-8090-4008-b278-430d5773b035','MEXAP10933','P070','VENTA',4,4,0,'2025-10-30'),
  ('940de401-8090-4008-b278-430d5773b035','MEXAP10933','P115','VENTA',2,5,3,'2025-10-30'),
  -- RECOLECCION (saga d8255714) — all remaining to 0
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','R197','RECOLECCION',3,3,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','P188','RECOLECCION',2,2,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','P157','RECOLECCION',3,3,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','P061','RECOLECCION',5,5,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','P115','RECOLECCION',3,3,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','P032','RECOLECCION',3,3,0,'2025-10-30'),
  ('d8255714-ca1d-40f3-b758-392f06615bc6','MEXAP10933','S531','RECOLECCION',2,2,0,'2025-10-30'),
  -- CREACION (saga 0937d8a4)
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','P220','CREACION',2,0,2,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','Y458','CREACION',4,0,4,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','Y527','CREACION',4,0,4,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','P027','CREACION',5,0,5,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','P138','CREACION',5,0,5,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','Y365','CREACION',5,0,5,'2025-10-30'),
  ('0937d8a4-fd9e-4a15-a5d9-6f60660c208c','MEXAP10933','P005','CREACION',5,0,5,'2025-10-30');

-- V4 (2025-11-15) — 2 VENTA + 5 PERMANENCIA (saga 30e4fa59)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-34391, saga 30e4fa59)
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','Y458','VENTA',2,4,2,'2025-11-15'),
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','Y365','VENTA',2,5,3,'2025-11-15'),
  -- PERMANENCIA
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','P220','PERMANENCIA',2,2,2,'2025-11-15'),
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','Y527','PERMANENCIA',4,4,4,'2025-11-15'),
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','P027','PERMANENCIA',5,5,5,'2025-11-15'),
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','P138','PERMANENCIA',5,5,5,'2025-11-15'),
  ('30e4fa59-54fd-4ade-848c-0fda75ffb8fe','MEXAP10933','P005','PERMANENCIA',5,5,5,'2025-11-15');

-- V5 (2025-11-29) — 1 VENTA + 6 RECOLECCION + 11 CREACION (sagas 74be25d7, e1872356, b9dd1fee)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-35184, saga 74be25d7)
  ('74be25d7-edfd-42b2-874b-eec233856a5e','MEXAP10933','Y458','VENTA',2,2,0,'2025-11-29'),
  -- RECOLECCION (saga e1872356) — all remaining to 0
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','Y527','RECOLECCION',4,4,0,'2025-11-29'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','P220','RECOLECCION',2,2,0,'2025-11-29'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','P027','RECOLECCION',5,5,0,'2025-11-29'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','P138','RECOLECCION',5,5,0,'2025-11-29'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','P005','RECOLECCION',5,5,0,'2025-11-29'),
  ('e1872356-6836-4ebd-91c2-3e8981e91bb2','MEXAP10933','Y365','RECOLECCION',3,3,0,'2025-11-29'),
  -- CREACION (saga b9dd1fee)
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','Y517','CREACION',4,0,4,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P024','CREACION',4,0,4,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P120','CREACION',2,0,2,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P225','CREACION',4,0,4,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P226','CREACION',3,0,3,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','S809','CREACION',3,0,3,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','T430','CREACION',2,0,2,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P632','CREACION',2,0,2,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','R790','CREACION',2,0,2,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','R319','CREACION',2,0,2,'2025-11-29'),
  ('b9dd1fee-a93f-4a0c-b0b7-d098f978e920','MEXAP10933','P048','CREACION',2,0,2,'2025-11-29');
-- MEXAP10933 TOTAL: 8+8+17+7+18 = 58 rows


------------------------------------------------------------
-- CLIENTE 2: MEXHR15497
------------------------------------------------------------

-- 2a. botiquin_odv INSERT — DCOdV-35107 (9 rows)
INSERT INTO public.botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
VALUES
  ('MEXHR15497','Y587','DCOdV-35107','2025-11-27',3,'unpaid'),
  ('MEXHR15497','P158','DCOdV-35107','2025-11-27',2,'unpaid'),
  ('MEXHR15497','Q269','DCOdV-35107','2025-11-27',3,'unpaid'),
  ('MEXHR15497','Y399','DCOdV-35107','2025-11-27',2,'unpaid'),
  ('MEXHR15497','P032','DCOdV-35107','2025-11-27',4,'unpaid'),
  ('MEXHR15497','P021','DCOdV-35107','2025-11-27',4,'unpaid'),
  ('MEXHR15497','P187','DCOdV-35107','2025-11-27',4,'unpaid'),
  ('MEXHR15497','P070','DCOdV-35107','2025-11-27',4,'unpaid'),
  ('MEXHR15497','P113','DCOdV-35107','2025-11-27',4,'unpaid');

-- 2b. saga_zoho_links — 2 DEVOLUCION (RECOL sagas)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('8d971d9e-4548-42a9-851f-4d87201cf27b', NULL, 'DEVOLUCION', 'synced'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2', NULL, 'DEVOLUCION', 'synced');

-- 2c. Movimientos rebuild — DELETE 2025 + INSERT 72 rows
DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXHR15497' AND fecha_movimiento < '2026-01-01';

-- V1 (2025-09-23) — 11 CREACION (saga 69bfb3dc) — anomalía: 33 pcs
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','X952','CREACION',4,0,4,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','W181','CREACION',4,0,4,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P005','CREACION',6,0,6,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P070','CREACION',3,0,3,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P206','CREACION',3,0,3,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P080','CREACION',3,0,3,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P015','CREACION',2,0,2,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P050','CREACION',2,0,2,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P220','CREACION',2,0,2,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P574','CREACION',2,0,2,'2025-09-23'),
  ('69bfb3dc-cd64-4a6b-8c88-23c36e8b29e7','MEXHR15497','P165','CREACION',2,0,2,'2025-09-23');

-- V2 (2025-10-15) — 11 PERMANENCIA (saga 567ec5db, no ventas)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','X952','PERMANENCIA',4,4,4,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','W181','PERMANENCIA',4,4,4,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P005','PERMANENCIA',6,6,6,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P070','PERMANENCIA',3,3,3,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P206','PERMANENCIA',3,3,3,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P080','PERMANENCIA',3,3,3,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P015','PERMANENCIA',2,2,2,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P050','PERMANENCIA',2,2,2,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P220','PERMANENCIA',2,2,2,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P574','PERMANENCIA',2,2,2,'2025-10-15'),
  ('567ec5db-a104-448e-b72d-8b9f01e3232e','MEXHR15497','P165','PERMANENCIA',2,2,2,'2025-10-15');

-- V3 (2025-10-30) — 11 RECOLECCION + 10 CREACION (sagas 8d971d9e, b5a63705)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- RECOLECCION (saga 8d971d9e) — all 33 pcs to 0
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','X952','RECOLECCION',4,4,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','W181','RECOLECCION',4,4,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P005','RECOLECCION',6,6,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P070','RECOLECCION',3,3,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P206','RECOLECCION',3,3,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P080','RECOLECCION',3,3,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P015','RECOLECCION',2,2,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P050','RECOLECCION',2,2,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P220','RECOLECCION',2,2,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P574','RECOLECCION',2,2,0,'2025-10-30'),
  ('8d971d9e-4548-42a9-851f-4d87201cf27b','MEXHR15497','P165','RECOLECCION',2,2,0,'2025-10-30'),
  -- CREACION (saga b5a63705)
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P040','CREACION',4,0,4,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P021','CREACION',4,0,4,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P027','CREACION',4,0,4,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P032','CREACION',4,0,4,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P113','CREACION',4,0,4,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','R044','CREACION',2,0,2,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P086','CREACION',2,0,2,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P158','CREACION',2,0,2,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','P632','CREACION',2,0,2,'2025-10-30'),
  ('b5a63705-27b4-4108-ac26-bb5a6ec5f223','MEXHR15497','Y399','CREACION',2,0,2,'2025-10-30');

-- V4 (2025-11-15) — 10 PERMANENCIA (saga 9a2e63c4, all permanencia)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P040','PERMANENCIA',4,4,4,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P021','PERMANENCIA',4,4,4,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P027','PERMANENCIA',4,4,4,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P032','PERMANENCIA',4,4,4,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P113','PERMANENCIA',4,4,4,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','R044','PERMANENCIA',2,2,2,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P086','PERMANENCIA',2,2,2,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P158','PERMANENCIA',2,2,2,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','P632','PERMANENCIA',2,2,2,'2025-11-15'),
  ('9a2e63c4-b46f-4791-b016-5700efdf9df7','MEXHR15497','Y399','PERMANENCIA',2,2,2,'2025-11-15');

-- V5 (2025-11-28) — 10 RECOLECCION + 9 CREACION (sagas 03c50812, 3673d37a)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- RECOLECCION (saga 03c50812) — all 30 pcs to 0
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P158','RECOLECCION',2,2,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','R044','RECOLECCION',2,2,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P032','RECOLECCION',4,4,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','Y399','RECOLECCION',2,2,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P021','RECOLECCION',4,4,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P040','RECOLECCION',4,4,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P632','RECOLECCION',2,2,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P113','RECOLECCION',4,4,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P027','RECOLECCION',4,4,0,'2025-11-28'),
  ('03c50812-53c8-4f00-a5c5-fde66e1ab3f2','MEXHR15497','P086','RECOLECCION',2,2,0,'2025-11-28'),
  -- CREACION (saga 3673d37a)
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','Y587','CREACION',3,0,3,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P158','CREACION',2,0,2,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','Q269','CREACION',3,0,3,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','Y399','CREACION',2,0,2,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P032','CREACION',4,0,4,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P021','CREACION',4,0,4,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P187','CREACION',4,0,4,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P070','CREACION',4,0,4,'2025-11-28'),
  ('3673d37a-6986-4606-aa84-a52aa94d3fc9','MEXHR15497','P113','CREACION',4,0,4,'2025-11-28');
-- MEXHR15497 TOTAL: 11+11+21+10+19 = 72 rows


------------------------------------------------------------
-- CLIENTE 3: MEXPF13496
------------------------------------------------------------

-- 3a. Crear visita V4 (2025-11-15)
INSERT INTO public.visitas (id_cliente, id_usuario, tipo, estado, created_at, completed_at, saga_status)
VALUES (
  'MEXPF13496',
  'zcrm_5062751000006206019',
  'VISITA_CORTE',
  'COMPLETADO',
  '2025-11-15',
  '2025-11-15',
  'COMPLETED'
);

-- 3b. botiquin_odv INSERT — DCOdV-35109 (11 rows)
INSERT INTO public.botiquin_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
VALUES
  ('MEXPF13496','Y587','DCOdV-35109','2025-11-27',3,'unpaid'),
  ('MEXPF13496','Y399','DCOdV-35109','2025-11-27',2,'unpaid'),
  ('MEXPF13496','P031','DCOdV-35109','2025-11-27',1,'unpaid'),
  ('MEXPF13496','P216','DCOdV-35109','2025-11-27',2,'unpaid'),
  ('MEXPF13496','P630','DCOdV-35109','2025-11-27',2,'unpaid'),
  ('MEXPF13496','Y365','DCOdV-35109','2025-11-27',4,'unpaid'),
  ('MEXPF13496','P292','DCOdV-35109','2025-11-27',4,'unpaid'),
  ('MEXPF13496','P070','DCOdV-35109','2025-11-27',4,'unpaid'),
  ('MEXPF13496','P081','DCOdV-35109','2025-11-27',4,'unpaid'),
  ('MEXPF13496','P087','DCOdV-35109','2025-11-27',2,'unpaid'),
  ('MEXPF13496','P105','DCOdV-35109','2025-11-27',2,'unpaid');

-- 3c. saga_zoho_links — 3 DEVOLUCION (RECOL sagas)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1', NULL, 'DEVOLUCION', 'synced'),
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d', NULL, 'DEVOLUCION', 'synced'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696', NULL, 'DEVOLUCION', 'synced');

-- 3d. Movimientos rebuild — DELETE 2025 + INSERT 77 rows
DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXPF13496' AND fecha_movimiento < '2026-01-01';

-- V1 (2025-09-23) — 12 CREACION (saga d463c8fa)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P115','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P147','CREACION',3,0,3,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P183','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P032','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P061','CREACION',3,0,3,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P168','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P205','CREACION',3,0,3,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P120','CREACION',3,0,3,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','Y458','CREACION',3,0,3,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','Q805','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','R846','CREACION',2,0,2,'2025-09-23'),
  ('d463c8fa-1ff8-4674-8861-b14b2bef5020','MEXPF13496','P574','CREACION',3,0,3,'2025-09-23');

-- V2 (2025-10-15) — 4 VENTA + 8 PERMANENCIA (saga 813e574e)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-33341)
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P032','VENTA',1,2,1,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P061','VENTA',3,3,0,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P205','VENTA',1,3,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','Y458','VENTA',3,3,0,'2025-10-15'),
  -- PERMANENCIA
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P115','PERMANENCIA',2,2,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P147','PERMANENCIA',3,3,3,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P183','PERMANENCIA',2,2,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P168','PERMANENCIA',2,2,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P120','PERMANENCIA',3,3,3,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','Q805','PERMANENCIA',2,2,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','R846','PERMANENCIA',2,2,2,'2025-10-15'),
  ('813e574e-f24f-4687-af6f-fc8be1adbf39','MEXPF13496','P574','PERMANENCIA',3,3,3,'2025-10-15');

-- V3 (2025-10-30) — Fase 1: 2V + 9R + 9C | Fase 2: 2V + 5R = 27
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- Fase 1: VENTA viejo botiquín (DCOdV-33477, saga b6642345)
  ('b6642345-8291-4794-8cbc-4e34bd1846a2','MEXPF13496','P032','VENTA',1,1,0,'2025-10-30'),
  ('b6642345-8291-4794-8cbc-4e34bd1846a2','MEXPF13496','R846','VENTA',1,2,1,'2025-10-30'),
  -- Fase 1: RECOLECCION viejo botiquín (saga 59eac4f8)
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P115','RECOLECCION',2,2,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P147','RECOLECCION',3,3,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P574','RECOLECCION',3,3,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','R846','RECOLECCION',1,1,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P168','RECOLECCION',2,2,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P183','RECOLECCION',2,2,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P120','RECOLECCION',3,3,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','Q805','RECOLECCION',2,2,0,'2025-10-30'),
  ('59eac4f8-0166-4ffd-910e-d8bc2f5a5dd1','MEXPF13496','P205','RECOLECCION',2,2,0,'2025-10-30'),
  -- Fase 1: CREACION nuevo botiquín (saga 5078e16e)
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P005','CREACION',5,0,5,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P027','CREACION',5,0,5,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P040','CREACION',4,0,4,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P077','CREACION',2,0,2,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P165','CREACION',2,0,2,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P206','CREACION',3,0,3,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','P216','CREACION',3,0,3,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','S402','CREACION',3,0,3,'2025-10-30'),
  ('5078e16e-0950-45bd-b6b0-8d2e0acabc5c','MEXPF13496','X952','CREACION',3,0,3,'2025-10-30'),
  -- Fase 2: VENTA nuevo botiquín (DCOdV-34390, saga b6642345)
  ('b6642345-8291-4794-8cbc-4e34bd1846a2','MEXPF13496','P077','VENTA',2,2,0,'2025-11-01'),
  ('b6642345-8291-4794-8cbc-4e34bd1846a2','MEXPF13496','P165','VENTA',1,2,1,'2025-11-01'),
  -- Fase 2: RECOLECCION mid-ciclo (saga 96ee9b36)
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d','MEXPF13496','P216','RECOLECCION',1,3,2,'2025-11-01'),
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d','MEXPF13496','P027','RECOLECCION',2,5,3,'2025-11-01'),
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d','MEXPF13496','P005','RECOLECCION',2,5,3,'2025-11-01'),
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d','MEXPF13496','S402','RECOLECCION',1,3,2,'2025-11-01'),
  ('96ee9b36-f368-416c-8da3-8892bd9cdb9d','MEXPF13496','P206','RECOLECCION',3,3,0,'2025-11-01');
-- After V3: {P005:3, P027:3, P040:4, P165:1, P216:2, S402:2, X952:3} = 18

-- V4 (2025-11-15) — 7 PERMANENCIA (no saga, visita creada para cuadrar)
-- Need visit_id from the inserted visita. Use a subquery approach with NULL saga.
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  (NULL,'MEXPF13496','P005','PERMANENCIA',3,3,3,'2025-11-15'),
  (NULL,'MEXPF13496','P027','PERMANENCIA',3,3,3,'2025-11-15'),
  (NULL,'MEXPF13496','P040','PERMANENCIA',4,4,4,'2025-11-15'),
  (NULL,'MEXPF13496','P165','PERMANENCIA',1,1,1,'2025-11-15'),
  (NULL,'MEXPF13496','P216','PERMANENCIA',2,2,2,'2025-11-15'),
  (NULL,'MEXPF13496','S402','PERMANENCIA',2,2,2,'2025-11-15'),
  (NULL,'MEXPF13496','X952','PERMANENCIA',3,3,3,'2025-11-15');

-- V5 (2025-11-28) — 1 VENTA + 7 RECOLECCION + 11 CREACION (sagas 61534942, 5bd560f6, de9596b4)
INSERT INTO public.movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento)
VALUES
  -- VENTA (DCOdV-35185, saga 61534942)
  ('61534942-59c5-4f3e-b62b-a3c45e815f9c','MEXPF13496','S402','VENTA',1,2,1,'2025-11-28'),
  -- RECOLECCION (saga 5bd560f6) — all remaining to 0
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','P005','RECOLECCION',3,3,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','X952','RECOLECCION',3,3,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','P040','RECOLECCION',4,4,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','P216','RECOLECCION',2,2,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','P027','RECOLECCION',3,3,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','S402','RECOLECCION',1,1,0,'2025-11-28'),
  ('5bd560f6-fd09-4c50-8e07-88919cb94696','MEXPF13496','P165','RECOLECCION',1,1,0,'2025-11-28'),
  -- CREACION (saga de9596b4)
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','Y587','CREACION',3,0,3,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','Y399','CREACION',2,0,2,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P031','CREACION',1,0,1,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P216','CREACION',2,0,2,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P630','CREACION',2,0,2,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','Y365','CREACION',4,0,4,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P292','CREACION',4,0,4,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P070','CREACION',4,0,4,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P081','CREACION',4,0,4,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P087','CREACION',2,0,2,'2025-11-28'),
  ('de9596b4-6dd1-435f-93cd-c34db6d30b31','MEXPF13496','P105','CREACION',2,0,2,'2025-11-28');
-- MEXPF13496 TOTAL: 12+12+27+7+19 = 77 rows
