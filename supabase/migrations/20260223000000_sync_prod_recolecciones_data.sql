-- Sync PROD recolecciones data to match DEV
-- 8 recolecciones have items/evidencias/firmas in DEV but not in PROD
-- (2f456dad already exists in PROD)
-- Also removes 2 orphaned inventario_botiquin rows for MEXJG20850

BEGIN;

-- ============================================================
-- 1. recolecciones_items (85 rows across 8 recolecciones)
-- PK is (recoleccion_id, sku) — ON CONFLICT DO NOTHING for safety
-- ============================================================

INSERT INTO recolecciones_items (recoleccion_id, sku, cantidad) VALUES
  -- 909f8758: 9 items
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P031', 1),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P070', 4),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P081', 4),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P087', 2),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P105', 2),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P292', 4),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'P630', 2),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'Y365', 4),
  ('909f8758-c467-4b42-aece-14a5488c0321', 'Y399', 1),
  -- 5833b89c: 18 items
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P029', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P055', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P082', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P120', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P156', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P202', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P208', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P212', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P213', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P214', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P298', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'P529', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'W656', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'X616', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'X952', 2),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'X998', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'Y399', 1),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'Y527', 2),
  -- 5e41800b: 8 items
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'P024', 3),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'P120', 2),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'P225', 4),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'P226', 3),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'R319', 2),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'R790', 2),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'S809', 2),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'Y517', 4),
  -- 0b08cc39: 10 items
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P030', 5),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P043', 2),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P076', 2),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P133', 5),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P134', 5),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P161', 1),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P192', 2),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P222', 1),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P292', 4),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'P328', 2),
  -- 45e5d63d: 9 items
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P030', 4),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P076', 2),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P133', 5),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P134', 5),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P161', 1),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P192', 2),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P222', 1),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P292', 4),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'P328', 1),
  -- 9c15a588: 10 items
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P028', 5),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P030', 3),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P076', 2),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P133', 5),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P161', 1),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P192', 2),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P222', 1),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P292', 4),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'P328', 2),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'Q839', 2),
  -- 4dd095db: 13 items
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P014', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P015', 2),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P021', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P022', 2),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P025', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P070', 1),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P079', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P095', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P113', 1),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'P299', 3),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'W832', 2),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'X522', 2),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'Y399', 1),
  -- 95fbd849: 8 items
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'P021', 4),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'P032', 4),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'P070', 4),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'P113', 4),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'P187', 2),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'Q269', 2),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'Y399', 2),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'Y587', 3)
ON CONFLICT DO NOTHING;

-- ============================================================
-- 2. recolecciones_evidencias (8 rows)
-- PK is evidencia_id — ON CONFLICT DO NOTHING for safety
-- ============================================================

INSERT INTO recolecciones_evidencias (evidencia_id, recoleccion_id, storage_path, mime_type, created_at, metadata) VALUES
  ('94b94cbf-7627-478a-813c-110cfaa5b06f', '909f8758-c467-4b42-aece-14a5488c0321', 'recolecciones/909f8758-c467-4b42-aece-14a5488c0321/evidencias/evidencia_1769817185714_1ys4hd4tj_0.jpg', NULL, '2026-01-30 23:53:09.173035+00', '{}'),
  ('6ecdd567-d065-455e-b3d5-7b43d4192c63', '5833b89c-11e9-4de9-b23a-378fb41adb46', 'recolecciones/5833b89c-11e9-4de9-b23a-378fb41adb46/evidencias/evidencia_1770236022688_dfcglqywx_0.jpg', NULL, '2026-02-04 20:13:45.552278+00', '{}'),
  ('7e09520a-ce45-4060-8083-c07af3418bb3', '5e41800b-74d1-44c0-95ba-1251cba60abb', 'recolecciones/5e41800b-74d1-44c0-95ba-1251cba60abb/evidencias/evidencia_1769817007501_rutdof8ay_0.jpg', NULL, '2026-01-30 23:50:11.010753+00', '{}'),
  ('caff1d2e-ed7c-4071-87c4-ce620b646cd6', '0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'recolecciones/0b08cc39-9dfc-49c4-91ba-5295a1a094b5/evidencias/evidencia_1770405255894_eeeqxdj3o_0.jpg', NULL, '2026-02-06 19:14:18.232602+00', '{}'),
  ('5ce30096-5bc7-4403-bcfa-a6e446f2ccd5', '45e5d63d-21a1-4770-9424-115496cb55b2', 'recolecciones/45e5d63d-21a1-4770-9424-115496cb55b2/evidencias/evidencia_1770405412857_mg0o7ua2e_0.jpg', NULL, '2026-02-06 19:16:55.647557+00', '{}'),
  ('03a03ac0-04a2-47e8-9522-16e29bd83baf', '9c15a588-90e7-421d-b902-fb74f14788b3', 'recolecciones/9c15a588-90e7-421d-b902-fb74f14788b3/evidencias/evidencia_1770405578782_pri25writ_0.jpg', NULL, '2026-02-06 19:19:41.486597+00', '{}'),
  ('63077621-5cd1-41ec-ace8-7bf823baa94a', '4dd095db-405c-4734-8bfb-f4bdc48b887f', 'recolecciones/4dd095db-405c-4734-8bfb-f4bdc48b887f/evidencias/evidencia_1770236326407_8x6ka755c_0.jpg', NULL, '2026-02-04 20:18:48.19714+00', '{}'),
  ('23b72f6e-b3af-4033-b1d1-59eea6f25740', '95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'recolecciones/95fbd849-d3c2-4f07-a07b-429f9a8427ca/evidencias/evidencia_1770443060183_xvpbyme6w_0.jpg', NULL, '2026-02-07 05:44:21.177583+00', '{}')
ON CONFLICT DO NOTHING;

-- ============================================================
-- 3. recolecciones_firmas (8 rows)
-- PK is recoleccion_id — ON CONFLICT DO NOTHING for safety
-- ============================================================

INSERT INTO recolecciones_firmas (recoleccion_id, storage_path, signed_at, device_info) VALUES
  ('909f8758-c467-4b42-aece-14a5488c0321', 'recolecciones/909f8758-c467-4b42-aece-14a5488c0321/firma.png', '2026-01-30 23:53:09.173035+00', '{}'),
  ('5833b89c-11e9-4de9-b23a-378fb41adb46', 'recolecciones/5833b89c-11e9-4de9-b23a-378fb41adb46/firma.png', '2026-02-04 20:13:45.552278+00', '{}'),
  ('5e41800b-74d1-44c0-95ba-1251cba60abb', 'recolecciones/5e41800b-74d1-44c0-95ba-1251cba60abb/firma.png', '2026-01-30 23:50:11.010753+00', '{}'),
  ('0b08cc39-9dfc-49c4-91ba-5295a1a094b5', 'recolecciones/0b08cc39-9dfc-49c4-91ba-5295a1a094b5/firma.png', '2026-02-06 19:14:18.232602+00', '{}'),
  ('45e5d63d-21a1-4770-9424-115496cb55b2', 'recolecciones/45e5d63d-21a1-4770-9424-115496cb55b2/firma.png', '2026-02-06 19:16:55.647557+00', '{}'),
  ('9c15a588-90e7-421d-b902-fb74f14788b3', 'recolecciones/9c15a588-90e7-421d-b902-fb74f14788b3/firma.png', '2026-02-06 19:19:41.486597+00', '{}'),
  ('4dd095db-405c-4734-8bfb-f4bdc48b887f', 'recolecciones/4dd095db-405c-4734-8bfb-f4bdc48b887f/firma.png', '2026-02-04 20:18:48.19714+00', '{}'),
  ('95fbd849-d3c2-4f07-a07b-429f9a8427ca', 'recolecciones/95fbd849-d3c2-4f07-a07b-429f9a8427ca/firma.png', '2026-02-07 05:44:21.177583+00', '{}')
ON CONFLICT DO NOTHING;

-- ============================================================
-- 4. Remove 2 orphaned inventario_botiquin rows for MEXJG20850
-- These exist in PROD but not in DEV (P134, P258 both with qty=0)
-- ============================================================

DELETE FROM inventario_botiquin
WHERE id_cliente = 'MEXJG20850'
  AND sku IN ('P134', 'P258');

COMMIT;
