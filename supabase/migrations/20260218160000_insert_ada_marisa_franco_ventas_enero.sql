-- Import ventas ODV for Ada Marisa Franco Guzman (MEXAF10018) - Enero 2026
-- Source: ADA_MARISA_FRANCO_QRY_Ventas_Direccion_Comercial (32).csv

INSERT INTO ventas_odv (id_cliente, sku, odv_id, fecha, cantidad, precio, estado_factura)
SELECT v.*
FROM (VALUES
  ('MEXAF10018', 'W832', 'DCOdV-38577', '2026-01-30'::date, 1, 539.00, 'Open')
) AS v(id_cliente, sku, odv_id, fecha, cantidad, precio, estado_factura)
WHERE NOT EXISTS (
  SELECT 1 FROM ventas_odv vo WHERE vo.odv_id = v.odv_id AND vo.sku = v.sku
);
