-- Reconciliación de datos para MEXFS22989, MEXBR172, MEXER156
-- Corrige botiquin_odv, saga items, saga_zoho_links (DEVOLUCION), y rebuild movimientos_inventario 2025

-- ============================================================
-- 1. BOTIQUIN_ODV UPDATES
-- ============================================================

-- 1a. MEXFS22989: DCOdV-35154 (10 rows) — reassign from MEXPF13496
UPDATE public.botiquin_odv
SET id_cliente = 'MEXFS22989', fecha = '2025-11-28', estado_factura = 'unpaid'
WHERE odv_id = 'DCOdV-35154' AND id_cliente = 'MEXPF13496';

-- 1b. MEXBR172: DCOdV-35155 (5 rows) y DCOdV-35428 (9 rows) — reassign from MEXPF13496
UPDATE public.botiquin_odv
SET id_cliente = 'MEXBR172', estado_factura = 'unpaid'
WHERE odv_id IN ('DCOdV-35155', 'DCOdV-35428') AND id_cliente = 'MEXPF13496';

-- 1c. MEXBR172: DCOdV-36318 (3 rows) — just fix estado_factura
UPDATE public.botiquin_odv
SET estado_factura = 'unpaid'
WHERE odv_id = 'DCOdV-36318' AND id_cliente = 'MEXBR172' AND estado_factura IS NULL;

-- ============================================================
-- 2. SAGA TRANSACTIONS ITEMS UPDATES
-- ============================================================

-- 2a. MEXBR172 V2 RECOLECCION — populate items
UPDATE public.saga_transactions
SET items = '[
  {"sku":"P086","cantidad_salida":1,"cantidad_entrada":0,"cantidad_permanencia":0},
  {"sku":"P031","cantidad_salida":1,"cantidad_entrada":0,"cantidad_permanencia":0}
]'::jsonb
WHERE id = '1451e784-0b5d-4af5-8a1a-886d91dc5b44';

-- 2b. MEXER156 V2 RECOLECCION — populate items
UPDATE public.saga_transactions
SET items = '[
  {"sku":"P092","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
  {"sku":"P082","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
  {"sku":"P165","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
  {"sku":"P233","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0}
]'::jsonb
WHERE id = '1486c9be-fcc8-44af-833e-dc5f021cf8dc';

-- 2c. MEXER156 V5 LEV_POST_CORTE — clear bad items (75 items cumulative sum bug)
UPDATE public.saga_transactions
SET items = '[]'::jsonb,
    metadata = jsonb_set(
      COALESCE(metadata, '{}'::jsonb),
      '{legacy_omitida_reason}',
      '"Items incorrectos (suma acumulada). Pendiente datos ODV correctos."'
    )
WHERE id = '983fdd1c-62c8-4a71-bfb5-3190fddf399e';

-- ============================================================
-- 3. SAGA_ZOHO_LINKS — DEVOLUCION INSERTS (9 total)
-- ============================================================

-- 3a. MEXFS22989 (2 DEVOLUCION)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', NULL, 'DEVOLUCION', 'synced'),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', NULL, 'DEVOLUCION', 'synced');

-- 3b. MEXBR172 (3 DEVOLUCION)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', NULL, 'DEVOLUCION', 'synced'),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', NULL, 'DEVOLUCION', 'synced'),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', NULL, 'DEVOLUCION', 'synced');

-- 3c. MEXER156 (4 DEVOLUCION)
INSERT INTO public.saga_zoho_links (id_saga_transaction, zoho_id, tipo, zoho_sync_status)
VALUES
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', NULL, 'DEVOLUCION', 'synced'),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', NULL, 'DEVOLUCION', 'synced'),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', NULL, 'DEVOLUCION', 'synced'),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', NULL, 'DEVOLUCION', 'synced');

-- ============================================================
-- 4. MOVIMIENTOS INVENTARIO — DELETE 2025 DATA
-- ============================================================

DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXFS22989' AND fecha_movimiento < '2026-01-01';

DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXBR172' AND fecha_movimiento < '2026-01-01';

DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXER156' AND fecha_movimiento < '2026-01-01';

-- ============================================================
-- 5. MOVIMIENTOS INVENTARIO — INSERT MEXFS22989 (71 rows)
-- ============================================================

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, precio_unitario, task_id)
VALUES
  -- V1 (2025-10-01) LEV_INICIAL — 9 CREACION (saga cbd9c08f)
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'Y458', 'CREACION', 3, 0, 3, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'R547', 'CREACION', 2, 0, 2, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'Y399', 'CREACION', 3, 0, 3, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'Y527', 'CREACION', 2, 0, 2, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'P027', 'CREACION', 4, 0, 4, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'P299', 'CREACION', 4, 0, 4, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'X616', 'CREACION', 4, 0, 4, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'P138', 'CREACION', 4, 0, 4, '2025-10-01', NULL, NULL),
  ('cbd9c08f-f6bd-4435-b713-fb947d3f72ca', 'MEXFS22989', 'X952', 'CREACION', 4, 0, 4, '2025-10-01', NULL, NULL),

  -- V2 (2025-10-15) CORTE — 2 RECOL + 9 PERM + 2 CREACION = 13 rows
  -- RECOLECCION (saga null)
  (NULL, 'MEXFS22989', 'Y458', 'RECOLECCION', 1, 3, 2, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'X952', 'RECOLECCION', 1, 4, 3, '2025-10-15', NULL, NULL),
  -- PERMANENCIA (saga null)
  (NULL, 'MEXFS22989', 'Y458', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'R547', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'Y399', 'PERMANENCIA', 3, 3, 3, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'Y527', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'P027', 'PERMANENCIA', 4, 4, 4, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'P299', 'PERMANENCIA', 4, 4, 4, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'X616', 'PERMANENCIA', 4, 4, 4, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'P138', 'PERMANENCIA', 4, 4, 4, '2025-10-15', NULL, NULL),
  (NULL, 'MEXFS22989', 'X952', 'PERMANENCIA', 3, 3, 3, '2025-10-15', NULL, NULL),
  -- CREACION (saga 9a284db4)
  ('9a284db4-bbdd-4dd4-9a2a-7e0d7c9db03d', 'MEXFS22989', 'Y458', 'CREACION', 1, 0, 1, '2025-10-15', NULL, NULL),
  ('9a284db4-bbdd-4dd4-9a2a-7e0d7c9db03d', 'MEXFS22989', 'X952', 'CREACION', 1, 0, 1, '2025-10-15', NULL, NULL),

  -- V3 (2025-10-30) CORTE — 9 RECOL + 10 CREACION = 19 rows
  -- RECOLECCION (saga 18f0fa44)
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'Y458', 'RECOLECCION', 3, 3, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'R547', 'RECOLECCION', 2, 2, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'Y399', 'RECOLECCION', 3, 3, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'Y527', 'RECOLECCION', 2, 2, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'P027', 'RECOLECCION', 4, 4, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'P299', 'RECOLECCION', 4, 4, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'X616', 'RECOLECCION', 4, 4, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'P138', 'RECOLECCION', 4, 4, 0, '2025-10-30', NULL, NULL),
  ('18f0fa44-629e-4fff-a4d5-27eaf3778a34', 'MEXFS22989', 'X952', 'RECOLECCION', 4, 4, 0, '2025-10-30', NULL, NULL),
  -- CREACION (saga ca7da0d0)
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P014', 'CREACION', 5, 0, 5, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P206', 'CREACION', 5, 0, 5, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P058', 'CREACION', 5, 0, 5, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P632', 'CREACION', 1, 0, 1, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P070', 'CREACION', 5, 0, 5, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P086', 'CREACION', 2, 0, 2, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P077', 'CREACION', 2, 0, 2, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P231', 'CREACION', 2, 0, 2, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P574', 'CREACION', 1, 0, 1, '2025-10-30', NULL, NULL),
  ('ca7da0d0-0b7b-4341-882c-e0dccfd39102', 'MEXFS22989', 'P630', 'CREACION', 2, 0, 2, '2025-10-30', NULL, NULL),

  -- V4 (2025-11-15) CORTE — 10 PERMANENCIA (saga d55f29e3)
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P014', 'PERMANENCIA', 5, 5, 5, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P206', 'PERMANENCIA', 5, 5, 5, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P058', 'PERMANENCIA', 5, 5, 5, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P632', 'PERMANENCIA', 1, 1, 1, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P070', 'PERMANENCIA', 5, 5, 5, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P086', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P077', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P231', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P574', 'PERMANENCIA', 1, 1, 1, '2025-11-15', NULL, NULL),
  ('d55f29e3-fe87-4e39-bbdf-0bc06beb3551', 'MEXFS22989', 'P630', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),

  -- V5 (2025-11-28) CORTE — 10 RECOL + 10 CREACION = 20 rows
  -- RECOLECCION (saga ac8b7f58)
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P014', 'RECOLECCION', 5, 5, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P206', 'RECOLECCION', 5, 5, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P058', 'RECOLECCION', 5, 5, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P632', 'RECOLECCION', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P070', 'RECOLECCION', 5, 5, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P086', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P077', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P231', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P574', 'RECOLECCION', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('ac8b7f58-4922-4f22-87b3-6755e12beaea', 'MEXFS22989', 'P630', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  -- CREACION (saga 788fcace)
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P076', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P192', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P043', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P134', 'CREACION', 5, 0, 5, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P161', 'CREACION', 1, 0, 1, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P222', 'CREACION', 1, 0, 1, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P133', 'CREACION', 5, 0, 5, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P328', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P030', 'CREACION', 5, 0, 5, '2025-11-28', NULL, NULL),
  ('788fcace-c511-41b0-a60c-4754e4b25a8e', 'MEXFS22989', 'P292', 'CREACION', 5, 0, 5, '2025-11-28', NULL, NULL);

-- ============================================================
-- 6. MOVIMIENTOS INVENTARIO — INSERT MEXBR172 (100 rows)
-- ============================================================

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, precio_unitario, task_id)
VALUES
  -- V1 (2025-09-27) LEV_INICIAL — 11 CREACION (saga 832599d2)
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P086', 'CREACION', 2, 0, 2, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'Y458', 'CREACION', 2, 0, 2, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P574', 'CREACION', 2, 0, 2, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P031', 'CREACION', 2, 0, 2, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P165', 'CREACION', 2, 0, 2, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P005', 'CREACION', 3, 0, 3, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'W181', 'CREACION', 4, 0, 4, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P080', 'CREACION', 3, 0, 3, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'X952', 'CREACION', 4, 0, 4, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P206', 'CREACION', 3, 0, 3, '2025-09-27', NULL, NULL),
  ('832599d2-2484-4235-bed2-3d90ae02f0a9', 'MEXBR172', 'P070', 'CREACION', 3, 0, 3, '2025-09-27', NULL, NULL),

  -- V2 (2025-10-15) CORTE — 6 VENTA + 2 RECOL + 6 PERM + 5 CREACION = 19 rows
  -- VENTA (saga 25607474)
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'P031', 'VENTA', 1, 2, 1, '2025-10-15', NULL, NULL),
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'P165', 'VENTA', 2, 2, 0, '2025-10-15', NULL, NULL),
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'P206', 'VENTA', 3, 3, 0, '2025-10-15', NULL, NULL),
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'P574', 'VENTA', 2, 2, 0, '2025-10-15', NULL, NULL),
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'W181', 'VENTA', 4, 4, 0, '2025-10-15', NULL, NULL),
  ('25607474-f594-49ee-b273-0c3abadd26fc', 'MEXBR172', 'Y458', 'VENTA', 1, 2, 1, '2025-10-15', NULL, NULL),
  -- RECOLECCION (saga 1451e784)
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P086', 'RECOLECCION', 1, 2, 1, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P031', 'RECOLECCION', 1, 1, 0, '2025-10-15', NULL, NULL),
  -- PERMANENCIA (saga 1451e784)
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P070', 'PERMANENCIA', 3, 3, 3, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P005', 'PERMANENCIA', 3, 3, 3, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P080', 'PERMANENCIA', 3, 3, 3, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'P086', 'PERMANENCIA', 1, 1, 1, '2025-10-15', NULL, NULL),
  ('1451e784-0b5d-4af5-8a1a-886d91dc5b44', 'MEXBR172', 'X952', 'PERMANENCIA', 4, 4, 4, '2025-10-15', NULL, NULL),
  -- CREACION (saga b9c1141e)
  ('b9c1141e-8b56-4199-a560-7f0dd2759f62', 'MEXBR172', 'P216', 'CREACION', 3, 0, 3, '2025-10-15', NULL, NULL),
  ('b9c1141e-8b56-4199-a560-7f0dd2759f62', 'MEXBR172', 'S531', 'CREACION', 3, 0, 3, '2025-10-15', NULL, NULL),
  ('b9c1141e-8b56-4199-a560-7f0dd2759f62', 'MEXBR172', 'P158', 'CREACION', 2, 0, 2, '2025-10-15', NULL, NULL),
  ('b9c1141e-8b56-4199-a560-7f0dd2759f62', 'MEXBR172', 'P014', 'CREACION', 4, 0, 4, '2025-10-15', NULL, NULL),
  ('b9c1141e-8b56-4199-a560-7f0dd2759f62', 'MEXBR172', 'P021', 'CREACION', 3, 0, 3, '2025-10-15', NULL, NULL),

  -- V3 (2025-10-29) CORTE — 8 VENTA + 8 PERM + 6 CREACION = 22 rows
  -- VENTA (saga 6b393d37)
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P070', 'VENTA', 2, 3, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P005', 'VENTA', 2, 3, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P158', 'VENTA', 2, 2, 0, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P080', 'VENTA', 2, 3, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P086', 'VENTA', 1, 1, 0, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P021', 'VENTA', 3, 3, 0, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'X952', 'VENTA', 1, 4, 3, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P014', 'VENTA', 2, 4, 2, '2025-10-29', NULL, NULL),
  -- PERMANENCIA (saga 6b393d37)
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P070', 'PERMANENCIA', 1, 1, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P005', 'PERMANENCIA', 1, 1, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'S531', 'PERMANENCIA', 3, 3, 3, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'Y458', 'PERMANENCIA', 1, 1, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P080', 'PERMANENCIA', 1, 1, 1, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'X952', 'PERMANENCIA', 3, 3, 3, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P014', 'PERMANENCIA', 2, 2, 2, '2025-10-29', NULL, NULL),
  ('6b393d37-9041-4b95-a6f7-bf0234062ecf', 'MEXBR172', 'P216', 'PERMANENCIA', 3, 3, 3, '2025-10-29', NULL, NULL),
  -- CREACION (saga 081c9049)
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'P040', 'CREACION', 3, 0, 3, '2025-10-29', NULL, NULL),
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'P299', 'CREACION', 2, 0, 2, '2025-10-29', NULL, NULL),
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'P081', 'CREACION', 5, 0, 5, '2025-10-29', NULL, NULL),
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'P058', 'CREACION', 2, 0, 2, '2025-10-29', NULL, NULL),
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'Y587', 'CREACION', 2, 0, 2, '2025-10-29', NULL, NULL),
  ('081c9049-655c-4204-acc2-56829c651979', 'MEXBR172', 'P632', 'CREACION', 1, 0, 1, '2025-10-29', NULL, NULL),

  -- V4 (2025-11-14) CORTE — 6 VENTA + 3 RECOL + 7 PERM + 5 CREACION = 21 rows
  -- VENTA (saga 59e1ed77)
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'P005', 'VENTA', 1, 1, 0, '2025-11-14', NULL, NULL),
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'P058', 'VENTA', 2, 2, 0, '2025-11-14', NULL, NULL),
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'P070', 'VENTA', 1, 1, 0, '2025-11-14', NULL, NULL),
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'S531', 'VENTA', 2, 3, 1, '2025-11-14', NULL, NULL),
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'X952', 'VENTA', 1, 3, 2, '2025-11-14', NULL, NULL),
  ('59e1ed77-d242-430c-87d3-8cdf0d37573e', 'MEXBR172', 'Y458', 'VENTA', 1, 1, 0, '2025-11-14', NULL, NULL),
  -- RECOLECCION (saga 97699ce5)
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P080', 'RECOLECCION', 1, 1, 0, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P216', 'RECOLECCION', 3, 3, 0, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P014', 'RECOLECCION', 2, 2, 0, '2025-11-14', NULL, NULL),
  -- PERMANENCIA (saga 97699ce5)
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P081', 'PERMANENCIA', 5, 5, 5, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'Y587', 'PERMANENCIA', 2, 2, 2, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P632', 'PERMANENCIA', 1, 1, 1, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P040', 'PERMANENCIA', 3, 3, 3, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'X952', 'PERMANENCIA', 2, 2, 2, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'S531', 'PERMANENCIA', 1, 1, 1, '2025-11-14', NULL, NULL),
  ('97699ce5-cdf1-4717-9de3-3ae169993273', 'MEXBR172', 'P299', 'PERMANENCIA', 2, 2, 2, '2025-11-14', NULL, NULL),
  -- CREACION (saga 05f47cdd)
  ('05f47cdd-edb1-44db-b1bf-3d5d74d53822', 'MEXBR172', 'P592', 'CREACION', 3, 0, 3, '2025-11-14', NULL, NULL),
  ('05f47cdd-edb1-44db-b1bf-3d5d74d53822', 'MEXBR172', 'P072', 'CREACION', 3, 0, 3, '2025-11-14', NULL, NULL),
  ('05f47cdd-edb1-44db-b1bf-3d5d74d53822', 'MEXBR172', 'S809', 'CREACION', 3, 0, 3, '2025-11-14', NULL, NULL),
  ('05f47cdd-edb1-44db-b1bf-3d5d74d53822', 'MEXBR172', 'Y810', 'CREACION', 3, 0, 3, '2025-11-14', NULL, NULL),
  ('05f47cdd-edb1-44db-b1bf-3d5d74d53822', 'MEXBR172', 'Y601', 'CREACION', 2, 0, 2, '2025-11-14', NULL, NULL),

  -- V5 (2025-11-28) CORTE — 9 VENTA + 6 RECOL + 14 CREACION = 29 rows
  -- VENTA (saga 791de313)
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'P040', 'VENTA', 3, 3, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'P072', 'VENTA', 1, 3, 2, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'P299', 'VENTA', 1, 2, 1, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'P592', 'VENTA', 3, 3, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'P632', 'VENTA', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'S531', 'VENTA', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'X952', 'VENTA', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'Y587', 'VENTA', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('791de313-cd4e-4dd3-b176-98b87a6883a0', 'MEXBR172', 'Y810', 'VENTA', 2, 3, 1, '2025-11-28', NULL, NULL),
  -- RECOLECCION (saga ac45db1d)
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'P081', 'RECOLECCION', 5, 5, 0, '2025-11-28', NULL, NULL),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'P072', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'P299', 'RECOLECCION', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'S809', 'RECOLECCION', 3, 3, 0, '2025-11-28', NULL, NULL),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'Y810', 'RECOLECCION', 1, 1, 0, '2025-11-28', NULL, NULL),
  ('ac45db1d-edfa-4322-a178-b89f1351125a', 'MEXBR172', 'Y601', 'RECOLECCION', 2, 2, 0, '2025-11-28', NULL, NULL),
  -- CREACION (saga 176a8254)
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P567', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P630', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'V160', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P371', 'CREACION', 3, 0, 3, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'Y810', 'CREACION', 1, 0, 1, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P072', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P105', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P141', 'CREACION', 3, 0, 3, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P047', 'CREACION', 2, 0, 2, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P299', 'CREACION', 1, 0, 1, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P085', 'CREACION', 3, 0, 3, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P062', 'CREACION', 3, 0, 3, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'S809', 'CREACION', 3, 0, 3, '2025-11-28', NULL, NULL),
  ('176a8254-375c-4da6-bdb2-6e5f743268f0', 'MEXBR172', 'P146', 'CREACION', 1, 0, 1, '2025-11-28', NULL, NULL);

-- ============================================================
-- 7. MOVIMIENTOS INVENTARIO — INSERT MEXER156 (107 rows)
-- ============================================================

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, precio_unitario, task_id)
VALUES
  -- V1 (2025-09-26) LEV_INICIAL — 15 CREACION (saga 44139245)
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P092', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P023', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P165', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P031', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'R846', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P202', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P030', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P205', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P183', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P206', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P082', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P006', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'X952', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P212', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),
  ('44139245-2a31-4387-b43f-36b660ce1104', 'MEXER156', 'P233', 'CREACION', 2, 0, 2, '2025-09-26', NULL, NULL),

  -- V2 (2025-10-15) CORTE — 2 VENTA + 4 RECOL + 11 PERM + 6 CREACION = 23 rows
  -- VENTA (saga fab72eda)
  ('fab72eda-c0d0-42ab-aaaa-a7cd0014514b', 'MEXER156', 'P202', 'VENTA', 1, 2, 1, '2025-10-15', NULL, NULL),
  ('fab72eda-c0d0-42ab-aaaa-a7cd0014514b', 'MEXER156', 'P212', 'VENTA', 1, 2, 1, '2025-10-15', NULL, NULL),
  -- RECOLECCION (saga 1486c9be)
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P092', 'RECOLECCION', 2, 2, 0, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P082', 'RECOLECCION', 2, 2, 0, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P165', 'RECOLECCION', 2, 2, 0, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P233', 'RECOLECCION', 2, 2, 0, '2025-10-15', NULL, NULL),
  -- PERMANENCIA (saga 1486c9be)
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P023', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P031', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'R846', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P202', 'PERMANENCIA', 1, 1, 1, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P030', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P205', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P183', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P206', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P006', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'X952', 'PERMANENCIA', 2, 2, 2, '2025-10-15', NULL, NULL),
  ('1486c9be-fcc8-44af-833e-dc5f021cf8dc', 'MEXER156', 'P212', 'PERMANENCIA', 1, 1, 1, '2025-10-15', NULL, NULL),
  -- CREACION (saga a2196497)
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'P080', 'CREACION', 2, 0, 2, '2025-10-15', NULL, NULL),
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'P027', 'CREACION', 1, 0, 1, '2025-10-15', NULL, NULL),
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'P113', 'CREACION', 2, 0, 2, '2025-10-15', NULL, NULL),
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'P005', 'CREACION', 1, 0, 1, '2025-10-15', NULL, NULL),
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'P220', 'CREACION', 2, 0, 2, '2025-10-15', NULL, NULL),
  ('a2196497-7fae-4bb1-852a-88353baf252d', 'MEXER156', 'V160', 'CREACION', 2, 0, 2, '2025-10-15', NULL, NULL),

  -- V3 (2025-10-31) CORTE — 3 VENTA + 10 RECOL + 6 PERM + 12 CREACION = 31 rows
  -- VENTA (saga 58ab8acc)
  ('58ab8acc-1c27-4f08-8c0d-39d8c7c5e8de', 'MEXER156', 'P220', 'VENTA', 1, 2, 1, '2025-10-31', NULL, NULL),
  ('58ab8acc-1c27-4f08-8c0d-39d8c7c5e8de', 'MEXER156', 'X952', 'VENTA', 1, 2, 1, '2025-10-31', NULL, NULL),
  ('58ab8acc-1c27-4f08-8c0d-39d8c7c5e8de', 'MEXER156', 'P212', 'VENTA', 1, 1, 0, '2025-10-31', NULL, NULL),
  -- RECOLECCION (saga c32862e1)
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P205', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P030', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P206', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P023', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P031', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P006', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P202', 'RECOLECCION', 1, 1, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'R846', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'X952', 'RECOLECCION', 1, 1, 0, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P183', 'RECOLECCION', 2, 2, 0, '2025-10-31', NULL, NULL),
  -- PERMANENCIA (saga c32862e1) — V160 stays as PERMANENCIA
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P220', 'PERMANENCIA', 1, 1, 1, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P027', 'PERMANENCIA', 1, 1, 1, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P113', 'PERMANENCIA', 2, 2, 2, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P005', 'PERMANENCIA', 1, 1, 1, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'P080', 'PERMANENCIA', 2, 2, 2, '2025-10-31', NULL, NULL),
  ('c32862e1-329a-42a8-adef-3c69056b0aa4', 'MEXER156', 'V160', 'PERMANENCIA', 2, 2, 2, '2025-10-31', NULL, NULL),
  -- CREACION (saga f6bcd59d)
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'R846', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P202', 'CREACION', 1, 0, 1, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P206', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P205', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P006', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P030', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P183', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'X952', 'CREACION', 1, 0, 1, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P023', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P058', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P092', 'CREACION', 3, 0, 3, '2025-10-31', NULL, NULL),
  ('f6bcd59d-88ab-4734-a89a-b0c1576b5daf', 'MEXER156', 'P031', 'CREACION', 2, 0, 2, '2025-10-31', NULL, NULL),

  -- V4 (2025-11-15) CORTE — 5 VENTA + 5 RECOL + 10 PERM + 4 CREACION = 24 rows
  -- VENTA (saga c97c9b7e)
  ('c97c9b7e-d405-47f9-9ca0-ae714bc34844', 'MEXER156', 'P113', 'VENTA', 1, 2, 1, '2025-11-15', NULL, NULL),
  ('c97c9b7e-d405-47f9-9ca0-ae714bc34844', 'MEXER156', 'X952', 'VENTA', 1, 1, 0, '2025-11-15', NULL, NULL),
  ('c97c9b7e-d405-47f9-9ca0-ae714bc34844', 'MEXER156', 'P205', 'VENTA', 1, 2, 1, '2025-11-15', NULL, NULL),
  ('c97c9b7e-d405-47f9-9ca0-ae714bc34844', 'MEXER156', 'V160', 'VENTA', 2, 2, 0, '2025-11-15', NULL, NULL),
  ('c97c9b7e-d405-47f9-9ca0-ae714bc34844', 'MEXER156', 'P220', 'VENTA', 1, 1, 0, '2025-11-15', NULL, NULL),
  -- RECOLECCION (saga e1393a0e)
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P005', 'RECOLECCION', 1, 1, 0, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P027', 'RECOLECCION', 1, 1, 0, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P080', 'RECOLECCION', 2, 2, 0, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P092', 'RECOLECCION', 3, 3, 0, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P113', 'RECOLECCION', 1, 1, 0, '2025-11-15', NULL, NULL),
  -- PERMANENCIA (saga e1393a0e)
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P006', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P205', 'PERMANENCIA', 1, 1, 1, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P202', 'PERMANENCIA', 1, 1, 1, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P183', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P031', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'R846', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P206', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P023', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P030', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  ('e1393a0e-6071-471a-b1f3-6bab51b1675a', 'MEXER156', 'P058', 'PERMANENCIA', 2, 2, 2, '2025-11-15', NULL, NULL),
  -- CREACION (saga 2323afdd)
  ('2323afdd-1b98-4ca1-81a6-c2870546829b', 'MEXER156', 'P226', 'CREACION', 4, 0, 4, '2025-11-15', NULL, NULL),
  ('2323afdd-1b98-4ca1-81a6-c2870546829b', 'MEXER156', 'W181', 'CREACION', 4, 0, 4, '2025-11-15', NULL, NULL),
  ('2323afdd-1b98-4ca1-81a6-c2870546829b', 'MEXER156', 'P217', 'CREACION', 2, 0, 2, '2025-11-15', NULL, NULL),
  ('2323afdd-1b98-4ca1-81a6-c2870546829b', 'MEXER156', 'P180', 'CREACION', 2, 0, 2, '2025-11-15', NULL, NULL),

  -- V5 (2025-12-03) CORTE — 8 VENTA + 6 RECOL = 14 rows (no CREACION)
  -- VENTA (saga da34db4b)
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P058', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P031', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P206', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P205', 'VENTA', 1, 1, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'R846', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P183', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P202', 'VENTA', 1, 1, 0, '2025-12-03', NULL, NULL),
  ('da34db4b-6f63-43d8-bd5c-123bde0d1a57', 'MEXER156', 'P217', 'VENTA', 2, 2, 0, '2025-12-03', NULL, NULL),
  -- RECOLECCION (saga b4bda484)
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'P226', 'RECOLECCION', 4, 4, 0, '2025-12-03', NULL, NULL),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'P023', 'RECOLECCION', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'W181', 'RECOLECCION', 4, 4, 0, '2025-12-03', NULL, NULL),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'P180', 'RECOLECCION', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'P030', 'RECOLECCION', 2, 2, 0, '2025-12-03', NULL, NULL),
  ('b4bda484-b98d-49c7-93b8-a85d5b43de21', 'MEXER156', 'P006', 'RECOLECCION', 2, 2, 0, '2025-12-03', NULL, NULL);
