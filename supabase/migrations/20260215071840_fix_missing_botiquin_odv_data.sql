-- Fix missing botiquin_odv data caused by silently failed migrations
-- Affects: MEXEG032, MEXFS22989, MEXBR172, MEXAB19703, MEXJG20850

-- =============================================================================
-- Parte 1: MEXEG032 — INSERT botiquin_odv (36 rows total, 3 ODVs)
-- Causa: migración insert_edna_gonzalez_odv_data.sql nunca se aplicó
-- =============================================================================

-- DCOdV-31303 (V1, 13 rows)
INSERT INTO public.botiquin_odv (odv_id, fecha, sku, id_cliente, cantidad, estado_factura)
VALUES
  ('DCOdV-31303','2025-09-24','S402','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','S531','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','P051','MEXEG032',2,'Open'),
  ('DCOdV-31303','2025-09-24','S615','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','W832','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','X875','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','P022','MEXEG032',2,'Open'),
  ('DCOdV-31303','2025-09-24','P148','MEXEG032',1,'Open'),
  ('DCOdV-31303','2025-09-24','P206','MEXEG032',4,'Open'),
  ('DCOdV-31303','2025-09-24','P070','MEXEG032',4,'Open'),
  ('DCOdV-31303','2025-09-24','P181','MEXEG032',4,'Open'),
  ('DCOdV-31303','2025-09-24','P005','MEXEG032',4,'Open'),
  ('DCOdV-31303','2025-09-24','X616','MEXEG032',4,'Open')
ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

-- DCOdV-33409 (V3, 13 rows)
INSERT INTO public.botiquin_odv (odv_id, fecha, sku, id_cliente, cantidad, estado_factura)
VALUES
  ('DCOdV-33409','2025-10-30','W832','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','X875','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','S615','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','P148','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','P022','MEXEG032',2,'Open'),
  ('DCOdV-33409','2025-10-30','P051','MEXEG032',2,'Open'),
  ('DCOdV-33409','2025-10-30','S531','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','S402','MEXEG032',1,'Open'),
  ('DCOdV-33409','2025-10-30','P070','MEXEG032',4,'Open'),
  ('DCOdV-33409','2025-10-30','X616','MEXEG032',4,'Open'),
  ('DCOdV-33409','2025-10-30','P299','MEXEG032',4,'Open'),
  ('DCOdV-33409','2025-10-30','Y365','MEXEG032',4,'Open'),
  ('DCOdV-33409','2025-10-30','P206','MEXEG032',4,'Open')
ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

-- DCOdV-35165 (V5, 10 rows)
INSERT INTO public.botiquin_odv (odv_id, fecha, sku, id_cliente, cantidad, estado_factura)
VALUES
  ('DCOdV-35165','2025-11-28','P028','MEXEG032',4,'Open'),
  ('DCOdV-35165','2025-11-28','P029','MEXEG032',2,'Open'),
  ('DCOdV-35165','2025-11-28','P055','MEXEG032',2,'Open'),
  ('DCOdV-35165','2025-11-28','P062','MEXEG032',4,'Open'),
  ('DCOdV-35165','2025-11-28','P072','MEXEG032',4,'Open'),
  ('DCOdV-35165','2025-11-28','P077','MEXEG032',2,'Open'),
  ('DCOdV-35165','2025-11-28','P084','MEXEG032',2,'Open'),
  ('DCOdV-35165','2025-11-28','P163','MEXEG032',4,'Open'),
  ('DCOdV-35165','2025-11-28','P233','MEXEG032',4,'Open'),
  ('DCOdV-35165','2025-11-28','P537','MEXEG032',2,'Open')
ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

-- =============================================================================
-- Parte 2: MEXFS22989 — Reasignar DCOdV-35154 de MEXPF13496
-- Causa: UPDATE en migración reconcile_mexfs22989_mexbr172_mexer156.sql silently failed
-- =============================================================================

UPDATE public.botiquin_odv
SET id_cliente = 'MEXFS22989', fecha = '2025-11-28', estado_factura = 'unpaid'
WHERE odv_id = 'DCOdV-35154' AND id_cliente = 'MEXPF13496';

-- =============================================================================
-- Parte 3: MEXBR172 — Reasignar DCOdV-35155 y DCOdV-35428 de MEXPF13496
-- Causa: misma migración silently failed
-- =============================================================================

UPDATE public.botiquin_odv
SET id_cliente = 'MEXBR172', estado_factura = 'unpaid'
WHERE odv_id IN ('DCOdV-35155', 'DCOdV-35428') AND id_cliente = 'MEXPF13496';

-- =============================================================================
-- Parte 4: MEXBR172 DCOdV-36318 — Fix estado_factura NULL
-- =============================================================================

UPDATE public.botiquin_odv
SET estado_factura = 'unpaid'
WHERE odv_id = 'DCOdV-36318' AND id_cliente = 'MEXBR172' AND estado_factura IS NULL;

-- =============================================================================
-- Parte 5: MEXAB19703 Y365 — INSERT bajo DCOdV-31301 (V1 ODV)
-- Causa: Y365 tiene CREACION en V1 pero no aparecía en el ODV original
-- =============================================================================

INSERT INTO public.botiquin_odv (odv_id, fecha, sku, id_cliente, cantidad, estado_factura)
VALUES ('DCOdV-31301','2025-10-03','Y365','MEXAB19703',3,'unpaid')
ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;

-- =============================================================================
-- Parte 6: MEXJG20850 Y365 — INSERT bajo DCOdV-31106 (V1 ODV)
-- Causa: Y365 tiene CREACION en V1 pero no aparecía en el ODV original
-- =============================================================================

INSERT INTO public.botiquin_odv (odv_id, fecha, sku, id_cliente, cantidad, estado_factura)
VALUES ('DCOdV-31106','2025-10-01','Y365','MEXJG20850',3,'unpaid')
ON CONFLICT (odv_id, id_cliente, sku) DO NOTHING;
