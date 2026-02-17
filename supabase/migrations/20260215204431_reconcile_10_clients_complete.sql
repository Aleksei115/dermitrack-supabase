-- Reconciliación completa de 10 clientes (2025)
-- Clientes: MEXEG032, MEXAB19703, MEXJG20850, MEXAF10018, MEXBR172,
--           MEXER156, MEXFS22989, MEXAP10933, MEXHR15497, MEXPF13496
-- Solo afecta datos 2025, no toca 2026.

-- ============================================================
-- PHASE 1: Fix botiquin_odv DCOdV-35097 (was MEXPF13496 → MEXAF10018)
-- ============================================================
UPDATE public.botiquin_odv
SET id_cliente = 'MEXAF10018'
WHERE odv_id = 'DCOdV-35097' AND id_cliente = 'MEXPF13496';

-- ============================================================
-- PHASE 2: Create saga_zoho_links for sagas missing them (2025)
-- ============================================================
INSERT INTO public.saga_zoho_links (id_saga_transaction, tipo, zoho_id, zoho_sync_status) VALUES
-- MEXAB19703 V2 VENTA (CANCELADA, no ODV)
('6e02715c-6f7d-4fc4-9c22-48851fb303ac', 'VENTA', NULL, 'synced'),
-- MEXAF10018 V3 VENTA
('958ef26a-ea5e-4153-a92b-340b7d0a1f10', 'VENTA', NULL, 'synced'),
-- MEXAF10018 V4 LEV_POST_CORTE
('ba1ae11f-ff86-4afe-83a8-70213bad9de5', 'BOTIQUIN', NULL, 'synced'),
-- MEXAF10018 V4 VENTA
('26dacdc8-56a1-4b6e-b992-333513a1c658', 'VENTA', NULL, 'synced'),
-- MEXAF10018 V5 VENTA
('a44b038c-b96b-4b5e-97d4-de27e03a88cd', 'VENTA', NULL, 'synced'),
-- MEXAP10933 V2 LEV_POST_CORTE
('9495cac8-6bca-449a-ba54-5e6cd3d6e7c0', 'BOTIQUIN', NULL, 'synced'),
-- MEXAP10933 V4 LEV_POST_CORTE
('dd90dd29-956a-485b-9819-2bd7dfcaae14', 'BOTIQUIN', NULL, 'synced'),
-- MEXBR172 V2 RECOLECCION
('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'DEVOLUCION', NULL, 'synced'),
-- MEXBR172 V4 RECOLECCION
('97699ce5-cdf1-4717-9de3-3ae169993273', 'DEVOLUCION', NULL, 'synced'),
-- MEXBR172 V5 RECOLECCION
('ac45db1d-edfa-4322-a178-b89f1351125a', 'DEVOLUCION', NULL, 'synced'),
-- MEXEG032 V2 LEV_POST_CORTE
('980b7366-cc60-4d36-994c-b111720067b6', 'BOTIQUIN', NULL, 'synced'),
-- MEXEG032 V2 VENTA
('6a819e5c-851f-4441-b95b-701a79d81fe6', 'VENTA', NULL, 'synced'),
-- MEXEG032 V3 VENTA
('59921d73-ce57-415c-bf72-f919ab8d2c7a', 'VENTA', NULL, 'synced'),
-- MEXEG032 V4 LEV_POST_CORTE
('29cf6e6b-3140-4890-bef2-348337ef228b', 'BOTIQUIN', NULL, 'synced'),
-- MEXEG032 V4 VENTA
('e3da25f0-0ba0-474e-9e91-ddb0e68c6cad', 'VENTA', NULL, 'synced'),
-- MEXEG032 V5 VENTA
('944e611b-7322-4ae8-bf63-c1cbabf8080a', 'VENTA', NULL, 'synced'),
-- MEXER156 V2 RECOLECCION
('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'DEVOLUCION', NULL, 'synced'),
-- MEXER156 V3 RECOLECCION
('c32862e1-329a-42a8-adef-3c69056b0aa4', 'DEVOLUCION', NULL, 'synced'),
-- MEXER156 V4 RECOLECCION
('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'DEVOLUCION', NULL, 'synced'),
-- MEXER156 V5 RECOLECCION
('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'DEVOLUCION', NULL, 'synced'),
-- MEXER156 V5 LEV_POST_CORTE
('983fdd1c-62c8-4a71-bfb5-3190fddf399e', 'BOTIQUIN', NULL, 'synced'),
-- MEXFS22989 V3 RECOLECCION
('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'DEVOLUCION', NULL, 'synced'),
-- MEXFS22989 V3 VENTA
('4135ca7d-d038-4540-ac68-3818384d3465', 'VENTA', 'DCOdV-33464', 'synced'),
-- MEXFS22989 V4 VENTA
('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'VENTA', NULL, 'synced'),
-- MEXFS22989 V4 LEV_POST_CORTE
('f9904fb7-7e69-484f-9332-9331f280c587', 'BOTIQUIN', NULL, 'synced'),
-- MEXFS22989 V5 VENTA
('596ba3f9-e59a-43f2-aa08-d836eec02ae0', 'VENTA', NULL, 'synced'),
-- MEXFS22989 V5 RECOLECCION
('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'DEVOLUCION', NULL, 'synced'),
-- MEXHR15497 V2 LEV_POST_CORTE
('d39de36b-7f7e-45f4-9d6e-747d5d6c1c98', 'BOTIQUIN', NULL, 'synced'),
-- MEXHR15497 V2 VENTA
('567ec5db-a104-448e-b72d-8b9f01e3232e', 'VENTA', NULL, 'synced'),
-- MEXHR15497 V3 VENTA
('e8e37b3a-b910-45fe-a122-0c60e844c2c3', 'VENTA', NULL, 'synced'),
-- MEXHR15497 V4 VENTA
('9a2e63c4-b46f-4791-b016-5700efdf9df7', 'VENTA', NULL, 'synced'),
-- MEXHR15497 V4 LEV_POST_CORTE
('85e42d43-0b21-4a0b-a88f-5e3523d3d07e', 'BOTIQUIN', NULL, 'synced'),
-- MEXHR15497 V5 VENTA
('1929e448-d9eb-48bb-a004-c522f966b853', 'VENTA', NULL, 'synced'),
-- MEXJG20850 V2 LEV_POST_CORTE (CANCELADA)
('043537c6-f3f4-4e46-b16b-f14fb14bb542', 'BOTIQUIN', NULL, 'synced'),
-- MEXJG20850 V2 VENTA (CANCELADA)
('243be1bc-4a81-45a6-be8b-0b671b9168e7', 'VENTA', NULL, 'synced'),
-- MEXPF13496 V2 LEV_POST_CORTE
('0d48541a-b0ba-468a-aa51-a6c408072cf4', 'BOTIQUIN', NULL, 'synced');

-- ============================================================
-- PHASE 3: Link orphan CREACION movements to LEV_POST_CORTE sagas
-- ============================================================
-- MEXAF10018
UPDATE public.movimientos_inventario SET id_saga_transaction = 'dd7c8ccd-92f9-467d-bfad-7c6be4aba1d7'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXAF10018' AND fecha_movimiento::date = '2025-10-14';

UPDATE public.movimientos_inventario SET id_saga_transaction = '6890fb56-7494-4258-8515-390e90733732'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXAF10018' AND fecha_movimiento::date = '2025-10-30';

UPDATE public.movimientos_inventario SET id_saga_transaction = 'ba1ae11f-ff86-4afe-83a8-70213bad9de5'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXAF10018' AND fecha_movimiento::date = '2025-11-15';

UPDATE public.movimientos_inventario SET id_saga_transaction = '01904b65-83d2-412f-82c3-f56f2f917d7a'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXAF10018' AND fecha_movimiento::date = '2025-11-27';

-- MEXBR172
UPDATE public.movimientos_inventario SET id_saga_transaction = 'b9c1141e-8b56-4199-a560-7f0dd2759f62'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXBR172' AND fecha_movimiento::date = '2025-10-15';

UPDATE public.movimientos_inventario SET id_saga_transaction = '081c9049-655c-4204-acc2-56829c651979'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXBR172' AND fecha_movimiento::date = '2025-10-29';

UPDATE public.movimientos_inventario SET id_saga_transaction = '05f47cdd-edb1-44db-b1bf-3d5d74d53822'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXBR172' AND fecha_movimiento::date = '2025-11-15';

UPDATE public.movimientos_inventario SET id_saga_transaction = '176a8254-375c-4da6-bdb2-6e5f743268f0'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXBR172' AND fecha_movimiento::date = '2025-11-28';

-- MEXEG032
UPDATE public.movimientos_inventario SET id_saga_transaction = 'beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXEG032' AND fecha_movimiento::date = '2025-11-28';

-- MEXER156
UPDATE public.movimientos_inventario SET id_saga_transaction = 'a2196497-7fae-4bb1-852a-88353baf252d'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXER156' AND fecha_movimiento::date = '2025-10-15';

UPDATE public.movimientos_inventario SET id_saga_transaction = 'f6bcd59d-88ab-4734-a89a-b0c1576b5daf'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXER156' AND fecha_movimiento::date = '2025-10-31';

UPDATE public.movimientos_inventario SET id_saga_transaction = '2323afdd-1b98-4ca1-81a6-c2870546829b'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXER156' AND fecha_movimiento::date = '2025-11-15';

-- MEXFS22989
UPDATE public.movimientos_inventario SET id_saga_transaction = '9a284db4-bbdd-4dd4-9a2a-7e0d7c9db03d'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXFS22989' AND fecha_movimiento::date = '2025-10-15';

UPDATE public.movimientos_inventario SET id_saga_transaction = 'ca7da0d0-0b7b-4341-882c-e0dccfd39102'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXFS22989' AND fecha_movimiento::date = '2025-10-30';

UPDATE public.movimientos_inventario SET id_saga_transaction = '788fcace-c511-41b0-a60c-4754e4b25a8e'
WHERE id_saga_transaction IS NULL AND tipo = 'CREACION' AND id_cliente = 'MEXFS22989' AND fecha_movimiento::date = '2025-11-28';

-- ============================================================
-- PHASE 4: Create missing PERMANENCIA movements
-- ============================================================
-- MEXAF10018 V2
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXAF10018', 'P005', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'P031', 'PERMANENCIA', 2, 2, 2, '2025-10-14'),
  ('MEXAF10018', 'P058', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'P138', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'P206', 'PERMANENCIA', 1, 1, 1, '2025-10-14'),
  ('MEXAF10018', 'P574', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'Q805', 'PERMANENCIA', 1, 1, 1, '2025-10-14'),
  ('MEXAF10018', 'W181', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'X616', 'PERMANENCIA', 2, 2, 2, '2025-10-14'),
  ('MEXAF10018', 'X952', 'PERMANENCIA', 3, 3, 3, '2025-10-14'),
  ('MEXAF10018', 'Y458', 'PERMANENCIA', 3, 3, 3, '2025-10-14');

-- MEXAP10933 V2 + V4
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXAP10933', 'P032', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXAP10933', 'P070', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXAP10933', 'Y365', 'PERMANENCIA', 3, 3, 3, '2025-11-15'),
  ('MEXAP10933', 'Y458', 'PERMANENCIA', 2, 2, 2, '2025-11-15');

-- MEXBR172 V2 + V4 + V5
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXBR172', 'P005', 'PERMANENCIA', 3, 3, 3, '2025-10-15'),
  ('MEXBR172', 'P070', 'PERMANENCIA', 3, 3, 3, '2025-10-15'),
  ('MEXBR172', 'P080', 'PERMANENCIA', 3, 3, 3, '2025-10-15'),
  ('MEXBR172', 'P086', 'PERMANENCIA', 1, 1, 1, '2025-10-15'),
  ('MEXBR172', 'X952', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXBR172', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-10-15'),
  ('MEXBR172', 'P299', 'PERMANENCIA', 2, 2, 2, '2025-11-14'),
  ('MEXBR172', 'P592', 'PERMANENCIA', 1, 1, 1, '2025-11-28'),
  ('MEXBR172', 'X952', 'PERMANENCIA', 1, 1, 1, '2025-11-28'),
  ('MEXBR172', 'Y810', 'PERMANENCIA', 1, 1, 1, '2025-11-28');

-- MEXER156 V2
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXER156', 'P006', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P023', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P030', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P031', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P183', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P202', 'PERMANENCIA', 1, 1, 1, '2025-10-15'),
  ('MEXER156', 'P205', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P206', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'P212', 'PERMANENCIA', 1, 1, 1, '2025-10-15'),
  ('MEXER156', 'R846', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXER156', 'X952', 'PERMANENCIA', 2, 2, 2, '2025-10-15');

-- MEXFS22989 V2 + V3 + V4 + V5
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXFS22989', 'P027', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXFS22989', 'P138', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXFS22989', 'P299', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXFS22989', 'R547', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXFS22989', 'X616', 'PERMANENCIA', 4, 4, 4, '2025-10-15'),
  ('MEXFS22989', 'X952', 'PERMANENCIA', 3, 3, 3, '2025-10-15'),
  ('MEXFS22989', 'Y399', 'PERMANENCIA', 3, 3, 3, '2025-10-15'),
  ('MEXFS22989', 'Y458', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXFS22989', 'Y527', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXFS22989', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-10-30'),
  ('MEXFS22989', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-11-15'),
  ('MEXFS22989', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-11-28');

-- MEXPF13496 V2 + V4 + V5
INSERT INTO public.movimientos_inventario (id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento) VALUES
  ('MEXPF13496', 'P032', 'PERMANENCIA', 1, 1, 1, '2025-10-15'),
  ('MEXPF13496', 'P205', 'PERMANENCIA', 2, 2, 2, '2025-10-15'),
  ('MEXPF13496', 'P005', 'PERMANENCIA', 2, 2, 2, '2025-11-15'),
  ('MEXPF13496', 'P027', 'PERMANENCIA', 2, 2, 2, '2025-11-15'),
  ('MEXPF13496', 'P077', 'PERMANENCIA', 2, 2, 2, '2025-11-15'),
  ('MEXPF13496', 'P165', 'PERMANENCIA', 1, 1, 1, '2025-11-15'),
  ('MEXPF13496', 'P206', 'PERMANENCIA', 3, 3, 3, '2025-11-15'),
  ('MEXPF13496', 'P216', 'PERMANENCIA', 1, 1, 1, '2025-11-15'),
  ('MEXPF13496', 'S402', 'PERMANENCIA', 1, 1, 1, '2025-11-15'),
  ('MEXPF13496', 'P005', 'PERMANENCIA', 2, 2, 2, '2025-11-28'),
  ('MEXPF13496', 'P027', 'PERMANENCIA', 2, 2, 2, '2025-11-28'),
  ('MEXPF13496', 'P077', 'PERMANENCIA', 2, 2, 2, '2025-11-28'),
  ('MEXPF13496', 'P165', 'PERMANENCIA', 1, 1, 1, '2025-11-28'),
  ('MEXPF13496', 'P206', 'PERMANENCIA', 3, 3, 3, '2025-11-28'),
  ('MEXPF13496', 'P216', 'PERMANENCIA', 1, 1, 1, '2025-11-28'),
  ('MEXPF13496', 'S402', 'PERMANENCIA', 1, 1, 1, '2025-11-28');
