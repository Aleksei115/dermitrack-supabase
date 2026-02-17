-- Sync 11 missing 2026 movimientos from PROD to DEV
-- 6 VENTA + 5 RECOLECCION rows that exist on PROD but not DEV

-- Insert the 11 missing movimientos
INSERT INTO movimientos_inventario (id_saga_transaction, id_cliente, sku, tipo, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, precio_unitario, task_id)
VALUES
  -- MEXAB19703: 1 VENTA
  ('aa1450f2-34a1-4f0a-bd3c-0eb680e63de3', 'MEXAB19703', 'Y524', 'VENTA', 2, 2, 0, '2026-01-30 19:33:24.99186+00', NULL, NULL),
  -- MEXBR172: 3 VENTA + 1 RECOLECCION
  ('28c9458d-cd3e-4a4c-8e2c-2150502b602e', 'MEXBR172', 'P072', 'VENTA', 2, 2, 0, '2026-02-04 06:08:40.519391+00', NULL, NULL),
  ('28c9458d-cd3e-4a4c-8e2c-2150502b602e', 'MEXBR172', 'P299', 'VENTA', 1, 1, 0, '2026-02-04 06:08:40.519391+00', NULL, NULL),
  ('28c9458d-cd3e-4a4c-8e2c-2150502b602e', 'MEXBR172', 'Y810', 'VENTA', 1, 1, 0, '2026-02-04 06:08:40.519391+00', NULL, NULL),
  ('832045f4-5bfd-464f-8512-d4977f4d7252', 'MEXBR172', 'S809', 'RECOLECCION', 3, 3, 0, '2026-02-04 20:15:12.901385+00', NULL, NULL),
  -- MEXHR15497: 1 VENTA + 4 RECOLECCION
  ('d5ffc17f-6eff-44a4-81d5-c11f643ea33a', 'MEXHR15497', 'P158', 'VENTA', 2, 2, 0, '2026-02-07 05:37:30.139458+00', NULL, NULL),
  ('fe8a7da8-0dc7-47ef-a2b0-3a9125ebb390', 'MEXHR15497', 'P021', 'RECOLECCION', 4, 4, 0, '2026-02-07 05:44:21.177583+00', NULL, NULL),
  ('fe8a7da8-0dc7-47ef-a2b0-3a9125ebb390', 'MEXHR15497', 'P032', 'RECOLECCION', 4, 4, 0, '2026-02-07 05:44:21.177583+00', NULL, NULL),
  ('fe8a7da8-0dc7-47ef-a2b0-3a9125ebb390', 'MEXHR15497', 'P113', 'RECOLECCION', 4, 4, 0, '2026-02-07 05:44:21.177583+00', NULL, NULL),
  ('fe8a7da8-0dc7-47ef-a2b0-3a9125ebb390', 'MEXHR15497', 'Y399', 'RECOLECCION', 2, 2, 0, '2026-02-07 05:44:21.177583+00', NULL, NULL),
  -- MEXPF13496: 1 VENTA
  ('7aab6490-153c-40d6-ad22-9f9a767bd5a6', 'MEXPF13496', 'P216', 'VENTA', 2, 2, 0, '2026-01-29 17:48:03.666546+00', NULL, NULL);

-- Backfill precio_unitario for the new rows
UPDATE movimientos_inventario mi
SET precio_unitario = m.precio
FROM medicamentos m
WHERE m.sku = mi.sku
AND mi.precio_unitario IS NULL;

-- Update inventario_botiquin for affected clients/SKUs
-- VENTA and RECOLECCION both decrease stock
-- MEXAB19703: Y524 -2
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 2, 0)
WHERE id_cliente = 'MEXAB19703' AND sku = 'Y524';

-- MEXBR172: P072 -2, P299 -1, Y810 -1 (VENTA), S809 -3 (RECOLECCION)
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 2, 0)
WHERE id_cliente = 'MEXBR172' AND sku = 'P072';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 1, 0)
WHERE id_cliente = 'MEXBR172' AND sku = 'P299';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 1, 0)
WHERE id_cliente = 'MEXBR172' AND sku = 'Y810';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 3, 0)
WHERE id_cliente = 'MEXBR172' AND sku = 'S809';

-- MEXHR15497: P158 -2 (VENTA), P021 -4, P032 -4, P113 -4, Y399 -2 (RECOLECCION)
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 2, 0)
WHERE id_cliente = 'MEXHR15497' AND sku = 'P158';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 4, 0)
WHERE id_cliente = 'MEXHR15497' AND sku = 'P021';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 4, 0)
WHERE id_cliente = 'MEXHR15497' AND sku = 'P032';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 4, 0)
WHERE id_cliente = 'MEXHR15497' AND sku = 'P113';
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 2, 0)
WHERE id_cliente = 'MEXHR15497' AND sku = 'Y399';

-- MEXPF13496: P216 -2
UPDATE inventario_botiquin SET cantidad_disponible = GREATEST(cantidad_disponible - 2, 0)
WHERE id_cliente = 'MEXPF13496' AND sku = 'P216';
