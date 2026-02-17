-- ============================================================
-- Fix MEXEG032 (Edna Gonzalez Solis) inventory reconciliation
-- DEV-only data fix. All DML targets specific saga UUIDs that
-- only exist on DEV — statements are safe no-ops on PROD.
-- ============================================================

-- Part 2: Fix Visit 3 RECOLECCION (saga 229ac253) — delete doubled, insert correct
DELETE FROM public.movimientos_inventario
WHERE id_saga_transaction = '229ac253-46a8-4a0d-91ea-444fda946d83';

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT v.*
FROM (VALUES
  ('229ac253-46a8-4a0d-91ea-444fda946d83'::uuid,'MEXEG032','P005',4,4,0,'2025-10-29 16:34:10+00'::timestamptz,'RECOLECCION'::tipo_movimiento_inventario),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','P022',2,2,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','P051',2,2,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','P070',4,4,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','P148',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','P206',4,4,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','S402',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','S531',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','S615',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','W832',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','X616',4,4,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','X875',1,1,0,'2025-10-29 16:34:10+00','RECOLECCION'),
  ('229ac253-46a8-4a0d-91ea-444fda946d83','MEXEG032','Y365',4,4,0,'2025-10-29 16:34:10+00','RECOLECCION')
) AS v(id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
WHERE EXISTS (SELECT 1 FROM public.saga_transactions WHERE id = '229ac253-46a8-4a0d-91ea-444fda946d83');

-- Part 3: Fix Visit 3 CREACION (saga 615a5f29) — insert movements (were missing)
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT v.*
FROM (VALUES
  ('615a5f29-0681-4470-ac66-63529cd0a64a'::uuid,'MEXEG032','P022',2,0,2,'2025-10-29 16:34:10+00'::timestamptz,'CREACION'::tipo_movimiento_inventario),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','P051',2,0,2,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','P070',4,0,4,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','P148',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','P206',4,0,4,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','P299',4,0,4,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','S402',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','S531',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','S615',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','W832',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','X616',4,0,4,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','X875',1,0,1,'2025-10-29 16:34:10+00','CREACION'),
  ('615a5f29-0681-4470-ac66-63529cd0a64a','MEXEG032','Y365',4,0,4,'2025-10-29 16:34:10+00','CREACION')
) AS v(id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
WHERE EXISTS (SELECT 1 FROM public.saga_transactions WHERE id = '615a5f29-0681-4470-ac66-63529cd0a64a')
  AND NOT EXISTS (SELECT 1 FROM public.movimientos_inventario WHERE id_saga_transaction = '615a5f29-0681-4470-ac66-63529cd0a64a');

-- Part 4: Create Visit 2 PERMANENCIA saga + movements
INSERT INTO public.saga_transactions
  (id, tipo, id_cliente, id_usuario, items, visit_id, estado, created_at)
SELECT gen_random_uuid(), 'PERMANENCIA', 'MEXEG032', 'admin_aleksei',
  '[{"sku":"P005","cantidad":4},{"sku":"P022","cantidad":2},{"sku":"P051","cantidad":2},{"sku":"P070","cantidad":4},{"sku":"P148","cantidad":1},{"sku":"P206","cantidad":4},{"sku":"S402","cantidad":1},{"sku":"S531","cantidad":1},{"sku":"S615","cantidad":1},{"sku":"W832","cantidad":1},{"sku":"X616","cantidad":4},{"sku":"X875","cantidad":1},{"sku":"Y365","cantidad":4}]'::jsonb,
  'd5c1e53a-2863-4257-8396-9803d8063e76',
  'CONFIRMADO',
  '2025-10-15 16:59:09+00'
WHERE EXISTS (SELECT 1 FROM public.visitas WHERE visit_id = 'd5c1e53a-2863-4257-8396-9803d8063e76')
  AND NOT EXISTS (SELECT 1 FROM public.saga_transactions WHERE visit_id = 'd5c1e53a-2863-4257-8396-9803d8063e76' AND tipo = 'PERMANENCIA' AND id_cliente = 'MEXEG032');

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT st.id, 'MEXEG032', item->>'sku', (item->>'cantidad')::int,
       (item->>'cantidad')::int, (item->>'cantidad')::int,
       '2025-10-15 16:59:09+00', 'PERMANENCIA'
FROM public.saga_transactions st,
     jsonb_array_elements(st.items) AS item
WHERE st.visit_id = 'd5c1e53a-2863-4257-8396-9803d8063e76'
  AND st.tipo = 'PERMANENCIA'
  AND st.id_cliente = 'MEXEG032'
  AND NOT EXISTS (SELECT 1 FROM public.movimientos_inventario mi WHERE mi.id_saga_transaction = st.id);

-- Part 5: Create Visit 4 PERMANENCIA saga + movements
INSERT INTO public.saga_transactions
  (id, tipo, id_cliente, id_usuario, items, visit_id, estado, created_at)
SELECT gen_random_uuid(), 'PERMANENCIA', 'MEXEG032', 'admin_aleksei',
  '[{"sku":"P022","cantidad":2},{"sku":"P051","cantidad":2},{"sku":"P070","cantidad":4},{"sku":"P148","cantidad":1},{"sku":"P206","cantidad":4},{"sku":"P299","cantidad":4},{"sku":"S402","cantidad":1},{"sku":"S531","cantidad":1},{"sku":"S615","cantidad":1},{"sku":"W832","cantidad":1},{"sku":"X616","cantidad":4},{"sku":"X875","cantidad":1},{"sku":"Y365","cantidad":4}]'::jsonb,
  '25afcff6-95f3-4e58-aa0b-382bc807b385',
  'CONFIRMADO',
  '2025-11-15 21:22:36.169379+00'
WHERE EXISTS (SELECT 1 FROM public.visitas WHERE visit_id = '25afcff6-95f3-4e58-aa0b-382bc807b385')
  AND NOT EXISTS (SELECT 1 FROM public.saga_transactions WHERE visit_id = '25afcff6-95f3-4e58-aa0b-382bc807b385' AND tipo = 'PERMANENCIA' AND id_cliente = 'MEXEG032');

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT st.id, 'MEXEG032', item->>'sku', (item->>'cantidad')::int,
       (item->>'cantidad')::int, (item->>'cantidad')::int,
       '2025-11-15 21:22:36.169379+00', 'PERMANENCIA'
FROM public.saga_transactions st,
     jsonb_array_elements(st.items) AS item
WHERE st.visit_id = '25afcff6-95f3-4e58-aa0b-382bc807b385'
  AND st.tipo = 'PERMANENCIA'
  AND st.id_cliente = 'MEXEG032'
  AND NOT EXISTS (SELECT 1 FROM public.movimientos_inventario mi WHERE mi.id_saga_transaction = st.id);

-- Part 6: Complete Visit 5 RECOLECCION (saga d50f6c9f) — add missing 12 rows
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT v.*
FROM (VALUES
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db'::uuid,'MEXEG032','P022',2,2,0,'2025-11-28 21:43:35.257+00'::timestamptz,'RECOLECCION'::tipo_movimiento_inventario),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','P051',2,2,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','P070',4,4,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','P148',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','P206',4,4,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','S402',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','S531',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','S615',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','W832',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','X616',4,4,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','X875',1,1,0,'2025-11-28 21:43:35.257+00','RECOLECCION'),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db','MEXEG032','Y365',4,4,0,'2025-11-28 21:43:35.257+00','RECOLECCION')
) AS v(id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
WHERE EXISTS (SELECT 1 FROM public.saga_transactions WHERE id = 'd50f6c9f-dd78-4d3e-892a-bef951b5a1db')
  AND (SELECT count(*) FROM public.movimientos_inventario WHERE id_saga_transaction = 'd50f6c9f-dd78-4d3e-892a-bef951b5a1db') < 13;

-- Part 7: Create DEVOLUCION saga_zoho_links for RECOLECCION sagas
INSERT INTO public.saga_zoho_links
  (id_saga_transaction, zoho_id, tipo, zoho_sync_status, zoho_synced_at)
SELECT v.*
FROM (VALUES
  ('229ac253-46a8-4a0d-91ea-444fda946d83'::uuid, NULL::text, 'DEVOLUCION'::tipo_zoho_link, 'synced'::text, now()),
  ('d50f6c9f-dd78-4d3e-892a-bef951b5a1db', NULL, 'DEVOLUCION', 'synced', now())
) AS v(id_saga_transaction, zoho_id, tipo, zoho_sync_status, zoho_synced_at)
WHERE EXISTS (SELECT 1 FROM public.saga_transactions WHERE id = v.id_saga_transaction)
  AND NOT EXISTS (SELECT 1 FROM public.saga_zoho_links WHERE id_saga_transaction = v.id_saga_transaction AND tipo = 'DEVOLUCION');

-- Part 8: Update BOTIQUIN saga_zoho_links to synced (DEV IDs 64, 65, 66)
UPDATE public.saga_zoho_links
SET zoho_sync_status = 'synced', zoho_synced_at = now()
WHERE id IN (64, 65, 66)
  AND zoho_sync_status = 'pending';

-- Part 9: Update 2025 saga_transactions to COMPLETADO
UPDATE public.saga_transactions
SET estado = 'COMPLETADO'
WHERE id IN (
  '6e5634aa-3295-4788-b822-ece5253160e8',
  '615a5f29-0681-4470-ac66-63529cd0a64a',
  '229ac253-46a8-4a0d-91ea-444fda946d83',
  'beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0',
  'd50f6c9f-dd78-4d3e-892a-bef951b5a1db'
)
AND estado = 'CONFIRMADO';

-- Part 10: Insert Visit 5 CREACION movements (saga beaff0ea) — were missing
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
SELECT v.*
FROM (VALUES
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0'::uuid,'MEXEG032','P028',4,0,4,'2025-11-28 21:43:35.257+00'::timestamptz,'CREACION'::tipo_movimiento_inventario),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P029',2,0,2,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P055',2,0,2,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P062',4,0,4,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P072',4,0,4,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P077',2,0,2,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P084',2,0,2,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P163',4,0,4,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P233',4,0,4,'2025-11-28 21:43:35.257+00','CREACION'),
  ('beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0','MEXEG032','P537',2,0,2,'2025-11-28 21:43:35.257+00','CREACION')
) AS v(id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
WHERE EXISTS (SELECT 1 FROM public.saga_transactions WHERE id = 'beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0')
  AND NOT EXISTS (SELECT 1 FROM public.movimientos_inventario WHERE id_saga_transaction = 'beaff0ea-23ac-4e52-9215-cfe6eb6d1ab0');
