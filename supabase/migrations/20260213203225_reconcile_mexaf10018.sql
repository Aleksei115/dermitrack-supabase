-- ============================================================================
-- Reconciliación de datos: Cliente MEXAF10018 (ADA MARISA FRANCO GUZMAN)
-- ============================================================================
-- Problemas corregidos:
--   1. 7 visitas canceladas huérfanas (sin sagas ni tasks)
--   2. botiquin_odv DCOdV-35097: fecha y estado_factura incorrectos
--   3. Faltan 2 saga_transactions RECOLECCION (V2 y V4)
--   4. Faltan 4 saga_zoho_links para RECOLECCIONs (V2, V3, V4, V5)
--   5. movimientos_inventario 2025: cantidad_antes/despues incorrectos,
--      movimientos faltantes (V4 CREACION, V5 RECOLECCION), referencias cruzadas
-- ============================================================================

-- ============================================================
-- PASO 1: Eliminar 7 visitas canceladas (2026-01-28/29)
-- Verificado: 0 saga_transactions, 0 visit_tasks cada una
-- ============================================================
DELETE FROM public.visitas WHERE visit_id IN (
  'e9cadfa1-cc9c-4477-bd97-081d58b5e216',
  '1eefaa29-a804-4c9e-be45-4199de1fcbc4',
  '0a2abd4e-5218-4bf9-ae8b-4b9aaa360ec9',
  '8b54d66b-09fa-4371-9ad0-cfe7708911b6',
  '95dd3855-b64d-43c9-81d8-afa59c7036a8',
  '74b978d3-bc3d-4107-ba04-69576d3960a9',
  'f3e93bdd-eb10-4d27-a644-781d0541d967'
);

-- ============================================================
-- PASO 2: Corregir botiquin_odv DCOdV-35097
-- Ya existen 13 rows pero con fecha=2025-01-01 y estado_factura=null
-- ============================================================
UPDATE public.botiquin_odv
SET fecha = '2025-11-27',
    estado_factura = 'unpaid'
WHERE odv_id = 'DCOdV-35097';

-- ============================================================
-- PASO 3: Crear 2 saga_transactions RECOLECCION faltantes
-- ============================================================

-- V2 RECOLECCION (2025-10-14): nada recolectado, todo permaneció
INSERT INTO public.saga_transactions (
  id, tipo, estado, id_cliente, id_usuario, visit_id, items, created_at, updated_at
) VALUES (
  'a1b2c3d4-0002-4000-8000-000000000001',
  'RECOLECCION', 'CONFIRMADO', 'MEXAF10018', 'zcrm_5062751000006203001',
  'ff785123-f136-40c8-b7d1-f6143aa2cdf0',
  '[
    {"sku":"Y458","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"Q805","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":1},
    {"sku":"P574","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"P005","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"W181","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"P058","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"X952","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"P206","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":1},
    {"sku":"X616","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":2},
    {"sku":"P138","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":3},
    {"sku":"P031","cantidad_salida":0,"cantidad_entrada":0,"cantidad_permanencia":2}
  ]'::jsonb,
  '2025-10-14T12:00:00Z', now()
);

-- V4 RECOLECCION (2025-11-15): todo recolectado, nada permaneció
INSERT INTO public.saga_transactions (
  id, tipo, estado, id_cliente, id_usuario, visit_id, items, created_at, updated_at
) VALUES (
  'a1b2c3d4-0004-4000-8000-000000000001',
  'RECOLECCION', 'CONFIRMADO', 'MEXAF10018', 'zcrm_5062751000006203001',
  '1e4d7f88-e9f3-4ff0-96ad-2854e2b7d188',
  '[
    {"sku":"Y399","cantidad_salida":1,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P070","cantidad_salida":1,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P113","cantidad_salida":1,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P015","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P022","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"X522","cantidad_salida":2,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"W832","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P299","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P079","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P095","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P014","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P025","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0},
    {"sku":"P021","cantidad_salida":3,"cantidad_entrada":0,"cantidad_permanencia":0}
  ]'::jsonb,
  '2025-11-15T12:00:00Z', now()
);

-- ============================================================
-- PASO 4: Crear 4 saga_zoho_links para RECOLECCION
-- tipo_zoho_link no tiene RECOLECCION, se usa DEVOLUCION
-- ============================================================
INSERT INTO public.saga_zoho_links (id_saga_transaction, tipo, zoho_id, zoho_sync_status) VALUES
  ('a1b2c3d4-0002-4000-8000-000000000001', 'DEVOLUCION', NULL, 'synced'),
  ('14692bc6-f8fc-402d-9c1f-abf96e1163ac', 'DEVOLUCION', NULL, 'synced'),
  ('a1b2c3d4-0004-4000-8000-000000000001', 'DEVOLUCION', NULL, 'synced'),
  ('f27fa12a-6f13-4128-a21b-036f8aa9fbb5', 'DEVOLUCION', NULL, 'synced');

-- ============================================================
-- PASO 5: Borrar todos los movimientos_inventario 2025
-- Actualmente 91 rows con datos incorrectos
-- ============================================================
DELETE FROM public.movimientos_inventario
WHERE id_cliente = 'MEXAF10018'
  AND fecha_movimiento < '2026-01-01';

-- ============================================================
-- PASO 6: Re-insertar 104 movimientos con antes/despues correctos
-- ============================================================
-- Running stock tracker por SKU (calculado manualmente):
--
-- V1 CREACION (2025-09-29): 11 items, 30 pcs total
--   Y458:3, Q805:2, P574:3, P005:3, W181:3, P058:3, X952:3, P206:2, X616:3, P138:3, P031:2
--
-- V2 (2025-10-14):
--   VENTA: P206:1(2→1), Q805:1(2→1), X616:1(3→2)
--   CREACION: Y399:1(0→1), P070:1(0→1), P113:1(0→1) [from LEV_POST_CORTE dd7c8ccd]
--   PERMANENCIA: 11 items that stayed (Y458:3, Q805:1, P574:3, P005:3, W181:3, P058:3, X952:3, P206:1, X616:2, P138:3, P031:2)
--
-- V3 (2025-10-30):
--   RECOLECCION(salida): Q805:1, X952:3, P031:2, P058:3, P206:1, Y458:3, P574:3, P138:3, W181:3, X616:2, P005:3
--   PERMANENCIA: P070:1, P113:1, Y399:1
--   CREACION: P014:3, P015:2, P021:3, P022:2, P025:3, P079:3, P095:3, P299:3, W832:3, X522:2 [from LEV_POST_CORTE 6890fb56]
--
-- V4 (2025-11-15):
--   RECOLECCION(salida): Y399:1, P070:1, P113:1, P015:2, P022:2, X522:2, W832:3, P299:3, P079:3, P095:3, P014:3, P025:3, P021:3
--   CREACION: P014:3, P015:2, P021:3, P022:2, P025:3, P070:1, P079:3, P095:3, P113:1, P299:3, W832:3, X522:2, Y399:1 [from LEV_POST_CORTE ba1ae11f]
--
-- V5 (2025-11-27):
--   RECOLECCION(salida): same 13 SKUs as V4 CREACION
--   CREACION: P015:2, Y399:1, P022:2, X522:2, W832:3, P113:1, P299:3, P025:3, P014:3, P021:3, P095:3, P079:3, P070:1 [from LEV_POST_CORTE 01904b65]

INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo, precio_unitario, task_id)
VALUES
-- =============================================
-- V1: LEVANTAMIENTO_INICIAL (2025-09-29) — 11 CREACION
-- saga: 0b110ae9 (LEVANTAMIENTO_INICIAL)
-- task: e24462c3 (LEVANTAMIENTO_INICIAL)
-- Stock after: Y458:3, Q805:2, P574:3, P005:3, W181:3, P058:3, X952:3, P206:2, X616:3, P138:3, P031:2 = 30
-- =============================================
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','Y458',3,0,3,'2025-09-29','CREACION',700.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','Q805',2,0,2,'2025-09-29','CREACION',454.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P574',3,0,3,'2025-09-29','CREACION',357.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P005',3,0,3,'2025-09-29','CREACION',255.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','W181',3,0,3,'2025-09-29','CREACION',345.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P058',3,0,3,'2025-09-29','CREACION',377.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','X952',3,0,3,'2025-09-29','CREACION',780.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P206',2,0,2,'2025-09-29','CREACION',485.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','X616',3,0,3,'2025-09-29','CREACION',400.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P138',3,0,3,'2025-09-29','CREACION',289.00,'e24462c3-d876-4765-9977-eeb6d519277d'),
('0b110ae9-be62-472b-8711-98042d8d8b7c','MEXAF10018','P031',2,0,2,'2025-09-29','CREACION',298.00,'e24462c3-d876-4765-9977-eeb6d519277d'),

-- =============================================
-- V2: CORTE (2025-10-14) — 3 VENTA + 3 CREACION + 11 PERMANENCIA = 17
-- =============================================

-- V2 VENTA (3 rows) — saga: d3db40ce (VENTA), task: 442e5ccb (CORTE)
-- Stock before: P206:2, Q805:2, X616:3
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P206',1,2,1,'2025-10-14','VENTA',485.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','Q805',1,2,1,'2025-10-14','VENTA',454.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','X616',1,3,2,'2025-10-14','VENTA',400.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),

-- V2 CREACION (3 rows) — saga: NULL, task: 442e5ccb (CORTE)
-- New items from LEV_POST_CORTE dd7c8ccd: Y399:1, P070:1, P113:1
(NULL,'MEXAF10018','Y399',1,0,1,'2025-10-14','CREACION',445.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
(NULL,'MEXAF10018','P070',1,0,1,'2025-10-14','CREACION',275.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
(NULL,'MEXAF10018','P113',1,0,1,'2025-10-14','CREACION',459.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),

-- V2 PERMANENCIA (11 rows) — saga: d3db40ce (VENTA), task: 442e5ccb (CORTE)
-- Stock: unchanged (the original V1 items that remained minus sold quantities)
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','Y458',0,3,3,'2025-10-14','PERMANENCIA',700.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','Q805',0,1,1,'2025-10-14','PERMANENCIA',454.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P574',0,3,3,'2025-10-14','PERMANENCIA',357.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P005',0,3,3,'2025-10-14','PERMANENCIA',255.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','W181',0,3,3,'2025-10-14','PERMANENCIA',345.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P058',0,3,3,'2025-10-14','PERMANENCIA',377.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','X952',0,3,3,'2025-10-14','PERMANENCIA',780.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P206',0,1,1,'2025-10-14','PERMANENCIA',485.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','X616',0,2,2,'2025-10-14','PERMANENCIA',400.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P138',0,3,3,'2025-10-14','PERMANENCIA',289.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),
('d3db40ce-ca9b-4b43-9a23-56bae2973e47','MEXAF10018','P031',0,2,2,'2025-10-14','PERMANENCIA',298.00,'442e5ccb-72d9-4da6-96c2-ea4275c0f600'),

-- =============================================
-- V3: CORTE (2025-10-30) — 11 RECOLECCION + 3 PERMANENCIA + 10 CREACION = 24
-- =============================================
-- Stock entering V3: Y458:3, Q805:1, P574:3, P005:3, W181:3, P058:3, X952:3, P206:1, X616:2, P138:3, P031:2, Y399:1, P070:1, P113:1 = 30

-- V3 RECOLECCION (11 rows) — saga: 14692bc6, task: aa99f0bc
-- Items collected (salida): everything from V1 batch except P070:1, P113:1, Y399:1 which stay
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','Q805',1,1,0,'2025-10-30','RECOLECCION',454.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','X952',3,3,0,'2025-10-30','RECOLECCION',780.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P031',2,2,0,'2025-10-30','RECOLECCION',298.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P058',3,3,0,'2025-10-30','RECOLECCION',377.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P206',1,1,0,'2025-10-30','RECOLECCION',485.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','Y458',3,3,0,'2025-10-30','RECOLECCION',700.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P574',3,3,0,'2025-10-30','RECOLECCION',357.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P138',3,3,0,'2025-10-30','RECOLECCION',289.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','W181',3,3,0,'2025-10-30','RECOLECCION',345.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','X616',2,2,0,'2025-10-30','RECOLECCION',400.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P005',3,3,0,'2025-10-30','RECOLECCION',255.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),

-- V3 PERMANENCIA (3 rows) — saga: 14692bc6 (RECOLECCION), task: aa99f0bc
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P070',1,1,1,'2025-10-30','PERMANENCIA',275.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','P113',1,1,1,'2025-10-30','PERMANENCIA',459.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),
('14692bc6-f8fc-402d-9c1f-abf96e1163ac','MEXAF10018','Y399',1,1,1,'2025-10-30','PERMANENCIA',445.00,'aa99f0bc-aa17-431a-9765-86522d58738c'),

-- V3 CREACION (10 rows) — saga: NULL, task: 92897257 (CORTE)
-- New items from LEV_POST_CORTE 6890fb56
-- Stock after: P070:1, P113:1, Y399:1 (permanencia) + new 10 items = 30
(NULL,'MEXAF10018','P014',3,0,3,'2025-10-30','CREACION',244.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P015',2,0,2,'2025-10-30','CREACION',298.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P021',3,0,3,'2025-10-30','CREACION',258.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P022',2,0,2,'2025-10-30','CREACION',250.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P025',3,0,3,'2025-10-30','CREACION',363.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P079',3,0,3,'2025-10-30','CREACION',275.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P095',3,0,3,'2025-10-30','CREACION',269.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','P299',3,0,3,'2025-10-30','CREACION',485.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','W832',3,0,3,'2025-10-30','CREACION',567.00,'92897257-c925-4e59-a0be-b6b7377d4584'),
(NULL,'MEXAF10018','X522',2,0,2,'2025-10-30','CREACION',600.00,'92897257-c925-4e59-a0be-b6b7377d4584'),

-- =============================================
-- V4: CORTE (2025-11-15) — 13 RECOLECCION + 13 CREACION = 26
-- =============================================
-- Stock entering V4: P070:1, P113:1, Y399:1, P014:3, P015:2, P021:3, P022:2, P025:3, P079:3, P095:3, P299:3, W832:3, X522:2 = 30

-- V4 RECOLECCION (13 rows) — saga: a1b2c3d4-0004 (new), task: 362feb72
-- Everything recolectado, 0 permanencia
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','Y399',1,1,0,'2025-11-15','RECOLECCION',445.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P070',1,1,0,'2025-11-15','RECOLECCION',275.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P113',1,1,0,'2025-11-15','RECOLECCION',459.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P015',2,2,0,'2025-11-15','RECOLECCION',298.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P022',2,2,0,'2025-11-15','RECOLECCION',250.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','X522',2,2,0,'2025-11-15','RECOLECCION',600.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','W832',3,3,0,'2025-11-15','RECOLECCION',567.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P299',3,3,0,'2025-11-15','RECOLECCION',485.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P079',3,3,0,'2025-11-15','RECOLECCION',275.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P095',3,3,0,'2025-11-15','RECOLECCION',269.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P014',3,3,0,'2025-11-15','RECOLECCION',244.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P025',3,3,0,'2025-11-15','RECOLECCION',363.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),
('a1b2c3d4-0004-4000-8000-000000000001','MEXAF10018','P021',3,3,0,'2025-11-15','RECOLECCION',258.00,'362feb72-c89e-4328-b598-e1f78aa5a4ed'),

-- V4 CREACION (13 rows) — saga: NULL, task: a399bb33 (CORTE)
-- New items from LEV_POST_CORTE ba1ae11f
-- Stock after: same 13 SKUs = 30
(NULL,'MEXAF10018','P014',3,0,3,'2025-11-15','CREACION',244.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P015',2,0,2,'2025-11-15','CREACION',298.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P021',3,0,3,'2025-11-15','CREACION',258.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P022',2,0,2,'2025-11-15','CREACION',250.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P025',3,0,3,'2025-11-15','CREACION',363.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P070',1,0,1,'2025-11-15','CREACION',275.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P079',3,0,3,'2025-11-15','CREACION',275.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P095',3,0,3,'2025-11-15','CREACION',269.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P113',1,0,1,'2025-11-15','CREACION',459.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','P299',3,0,3,'2025-11-15','CREACION',485.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','W832',3,0,3,'2025-11-15','CREACION',567.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','X522',2,0,2,'2025-11-15','CREACION',600.00,'a399bb33-5214-470f-98ba-c60340be5919'),
(NULL,'MEXAF10018','Y399',1,0,1,'2025-11-15','CREACION',445.00,'a399bb33-5214-470f-98ba-c60340be5919'),

-- =============================================
-- V5: CORTE (2025-11-27) — 13 RECOLECCION + 13 CREACION = 26
-- =============================================
-- Stock entering V5: P014:3, P015:2, P021:3, P022:2, P025:3, P070:1, P079:3, P095:3, P113:1, P299:3, W832:3, X522:2, Y399:1 = 30

-- V5 RECOLECCION (13 rows) — saga: f27fa12a, task: a35fa148
-- Everything recolectado
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P014',3,3,0,'2025-11-27','RECOLECCION',244.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P015',2,2,0,'2025-11-27','RECOLECCION',298.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P021',3,3,0,'2025-11-27','RECOLECCION',258.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P022',2,2,0,'2025-11-27','RECOLECCION',250.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P025',3,3,0,'2025-11-27','RECOLECCION',363.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P070',1,1,0,'2025-11-27','RECOLECCION',275.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P079',3,3,0,'2025-11-27','RECOLECCION',275.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P095',3,3,0,'2025-11-27','RECOLECCION',269.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P113',1,1,0,'2025-11-27','RECOLECCION',459.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','P299',3,3,0,'2025-11-27','RECOLECCION',485.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','W832',3,3,0,'2025-11-27','RECOLECCION',567.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','X522',2,2,0,'2025-11-27','RECOLECCION',600.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),
('f27fa12a-6f13-4128-a21b-036f8aa9fbb5','MEXAF10018','Y399',1,1,0,'2025-11-27','RECOLECCION',445.00,'a35fa148-ccc1-4b7e-adfc-c5bfa9145f9a'),

-- V5 CREACION (13 rows) — saga: NULL, task: b613385f (CORTE)
-- New items from LEV_POST_CORTE 01904b65
-- Final stock 2025: P014:3, P015:2, P021:3, P022:2, P025:3, P070:1, P079:3, P095:3, P113:1, P299:3, W832:3, X522:2, Y399:1 = 30
(NULL,'MEXAF10018','P014',3,0,3,'2025-11-27','CREACION',244.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P015',2,0,2,'2025-11-27','CREACION',298.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P021',3,0,3,'2025-11-27','CREACION',258.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P022',2,0,2,'2025-11-27','CREACION',250.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P025',3,0,3,'2025-11-27','CREACION',363.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P070',1,0,1,'2025-11-27','CREACION',275.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P079',3,0,3,'2025-11-27','CREACION',275.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P095',3,0,3,'2025-11-27','CREACION',269.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P113',1,0,1,'2025-11-27','CREACION',459.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','P299',3,0,3,'2025-11-27','CREACION',485.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','W832',3,0,3,'2025-11-27','CREACION',567.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','X522',2,0,2,'2025-11-27','CREACION',600.00,'b613385f-cba3-4dec-b45a-060f4539200a'),
(NULL,'MEXAF10018','Y399',1,0,1,'2025-11-27','CREACION',445.00,'b613385f-cba3-4dec-b45a-060f4539200a');
