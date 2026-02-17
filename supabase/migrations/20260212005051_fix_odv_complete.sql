-- ============================================================
-- Fix ODV ↔ Visitas: INSERT missing ODVs, fix saga_zoho_links
-- ============================================================
-- Sources: Excel "venta botiquin", Zoho CSVs, DB investigation
-- Scope: Pre-2026 visits only
-- ============================================================

-- ============================================================
-- STEP 1: INSERT 6 ODVs faltantes en ventas_odv (15 filas)
-- ============================================================

-- DCOdV-35185 — MEXPF13496 — 1 línea
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35185', 'MEXPF13496', 'S402', 1, '2025-11-28'::date, 479.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35185' AND id_cliente = 'MEXPF13496' AND sku = 'S402'
);

-- DCOdV-34450 — MEXJG20850 — 2 líneas
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-34450', 'MEXJG20850', 'P231', 1, '2025-11-17'::date, 288.00, 'Open'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-34450' AND id_cliente = 'MEXJG20850' AND sku = 'P231'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-34450', 'MEXJG20850', 'P040', 1, '2025-11-17'::date, 244.83, 'Open'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-34450' AND id_cliente = 'MEXJG20850' AND sku = 'P040'
);

-- DCOdV-35170 — MEXJG20850 — 2 líneas
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35170', 'MEXJG20850', 'P258', 1, '2025-11-28'::date, 459.00, 'Open'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35170' AND id_cliente = 'MEXJG20850' AND sku = 'P258'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35170', 'MEXJG20850', 'V160', 1, '2025-11-28'::date, 626.00, 'Open'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35170' AND id_cliente = 'MEXJG20850' AND sku = 'V160'
);

-- DCOdV-35184 — MEXAP10933 — 1 línea (Y458 = código Zoho actual de CREMA INB OIL FREE)
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35184', 'MEXAP10933', 'Y458', 2, '2025-11-28'::date, 700.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35184' AND id_cliente = 'MEXAP10933' AND sku = 'Y458'
);

-- DCOdV-35100 — MEXER156 — 8 líneas
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P058', 2, '2025-11-27'::date, 325.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P058'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P031', 2, '2025-11-27'::date, 298.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P031'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P206', 2, '2025-11-27'::date, 418.11, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P206'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P205', 1, '2025-11-27'::date, 253.45, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P205'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'R846', 2, '2025-11-27'::date, 360.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'R846'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P183', 2, '2025-11-27'::date, 225.87, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P183'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P202', 1, '2025-11-27'::date, 318.11, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P202'
);

INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-35100', 'MEXER156', 'P217', 2, '2025-11-27'::date, 256.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-35100' AND id_cliente = 'MEXER156' AND sku = 'P217'
);

-- DCOdV-34332 — MEXAB19703 — 1 línea (solo P077 per Excel)
INSERT INTO ventas_odv (odv_id, id_cliente, sku, cantidad, fecha, precio, estado_factura)
SELECT 'DCOdV-34332', 'MEXAB19703', 'P077', 1, '2025-11-14'::date, 238.00, 'Closed'
WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv WHERE odv_id = 'DCOdV-34332' AND id_cliente = 'MEXAB19703' AND sku = 'P077'
);


-- ============================================================
-- STEP 2: DELETE link duplicado
-- ============================================================
-- id=44: DCOdV-34391 duplicado en saga 74be25d7 (MEXAP10933 Nov-29)
-- El link correcto ya existe: id=14 en saga de visita Nov-15
DELETE FROM saga_zoho_links WHERE id = 44;


-- ============================================================
-- STEP 3: SWAP MEXJG20850 links (ids 49, 50)
-- ============================================================
-- id=49 estaba DCOdV-34450 → debe ser DCOdV-33465 (visit Oct-30, saga 4a50c734)
-- id=50 estaba DCOdV-35170 → debe ser DCOdV-34450 (visit Nov-14, saga 8663e827)
UPDATE saga_zoho_links SET zoho_id = 'DCOdV-33465' WHERE id = 49;
UPDATE saga_zoho_links SET zoho_id = 'DCOdV-34450' WHERE id = 50;


-- ============================================================
-- STEP 4: INSERT 6 nuevos saga_zoho_links
-- ============================================================

-- DCOdV-35170 → MEXJG20850 Nov-28, saga 544e2ca5
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT '544e2ca5-dc65-4878-aaa9-4af451d0f2da', 'DCOdV-35170', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = '544e2ca5-dc65-4878-aaa9-4af451d0f2da' AND zoho_id = 'DCOdV-35170'
);

-- DCOdV-34332 → MEXAB19703 Nov-15, saga ade1daf2
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT 'ade1daf2-493c-4b16-b1a0-b93c3e7699ba', 'DCOdV-34332', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = 'ade1daf2-493c-4b16-b1a0-b93c3e7699ba' AND zoho_id = 'DCOdV-34332'
);

-- DCOdV-35184 → MEXAP10933 Nov-29, saga 74be25d7 (reusing saga from deleted link_id 44)
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT '74be25d7-edfd-42b2-874b-eec233856a5e', 'DCOdV-35184', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = '74be25d7-edfd-42b2-874b-eec233856a5e' AND zoho_id = 'DCOdV-35184'
);

-- DCOdV-34390 → MEXPF13496 Oct-30, saga b6642345 (2nd link, already has DCOdV-33477)
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT 'b6642345-8291-4794-8cbc-4e34bd1846a2', 'DCOdV-34390', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = 'b6642345-8291-4794-8cbc-4e34bd1846a2' AND zoho_id = 'DCOdV-34390'
);

-- DCOdV-33466 → MEXAB19703 Oct-31, saga d57da800 (RECOLECCION saga with VENTA movements)
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT 'd57da800-4987-4a71-8ec4-bf3e8aca3ad2', 'DCOdV-33466', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = 'd57da800-4987-4a71-8ec4-bf3e8aca3ad2' AND zoho_id = 'DCOdV-33466'
);

-- DCOdV-35185 → MEXPF13496 Nov-28, saga 61534942 (0 movements)
INSERT INTO saga_zoho_links (id_saga_transaction, zoho_id, tipo)
SELECT '61534942-59c5-4f3e-b62b-a3c45e815f9c', 'DCOdV-35185', 'VENTA'
WHERE NOT EXISTS (
    SELECT 1 FROM saga_zoho_links WHERE id_saga_transaction = '61534942-59c5-4f3e-b62b-a3c45e815f9c' AND zoho_id = 'DCOdV-35185'
);
