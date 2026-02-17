-- Migración de ventas enero 2026 (38 filas nuevas)
-- Idempotente: usa NOT EXISTS para evitar duplicados si ya fueron insertadas

INSERT INTO ventas_odv (id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
SELECT v.id_cliente, v.sku, v.odv_id, v.fecha::date, v.cantidad, v.estado_factura
FROM (VALUES
  ('MEXBR172',   'P043', 'DCOdV-38105', '2026-01-23', 5,  'Open'),
  ('MEXBR172',   'U347', 'DCOdV-38105', '2026-01-23', 5,  'Open'),
  ('MEXAP10933', 'P054', 'DCOdV-38211', '2026-01-26', 1,  'Open'),
  ('MEXAP10933', 'P069', 'DCOdV-38211', '2026-01-26', 2,  'Open'),
  ('MEXHR15497', 'P017', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXHR15497', 'P031', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXHR15497', 'P055', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXHR15497', 'P061', 'DCOdV-38389', '2026-01-28', 1,  'Open'),
  ('MEXHR15497', 'P106', 'DCOdV-38389', '2026-01-28', 1,  'Open'),
  ('MEXHR15497', 'P138', 'DCOdV-38389', '2026-01-28', 1,  'Open'),
  ('MEXHR15497', 'P148', 'DCOdV-38389', '2026-01-28', 3,  'Open'),
  ('MEXHR15497', 'P191', 'DCOdV-38389', '2026-01-28', 3,  'Open'),
  ('MEXHR15497', 'P301', 'DCOdV-38389', '2026-01-28', 5,  'Open'),
  ('MEXHR15497', 'Q805', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXHR15497', 'R846', 'DCOdV-38389', '2026-01-28', 4,  'Open'),
  ('MEXHR15497', 'S531', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXHR15497', 'S615', 'DCOdV-38389', '2026-01-28', 3,  'Open'),
  ('MEXHR15497', 'S829', 'DCOdV-38389', '2026-01-28', 3,  'Open'),
  ('MEXHR15497', 'T430', 'DCOdV-38389', '2026-01-28', 1,  'Open'),
  ('MEXHR15497', 'V160', 'DCOdV-38389', '2026-01-28', 2,  'Open'),
  ('MEXAB19703', 'R319', 'DCOdV-38414', '2026-01-29', 5,  'Open'),
  ('MEXPF13496', 'P216', 'DCOdV-38444', '2026-01-29', 2,  'Open'),
  ('MEXPF13496', 'Y399', 'DCOdV-38444', '2026-01-29', 1,  'Open'),
  ('MEXPF13496', 'Y587', 'DCOdV-38444', '2026-01-29', 3,  'Open'),
  ('MEXHR15497', 'P158', 'DCOdV-38476', '2026-01-29', 2,  'Open'),
  ('MEXHR15497', 'P187', 'DCOdV-38476', '2026-01-29', 2,  'Open'),
  ('MEXAP10933', 'P024', 'DCOdV-38490', '2026-01-29', 1,  'Open'),
  ('MEXAP10933', 'P048', 'DCOdV-38490', '2026-01-29', 2,  'Open'),
  ('MEXAP10933', 'P632', 'DCOdV-38490', '2026-01-29', 2,  'Open'),
  ('MEXAP10933', 'S809', 'DCOdV-38490', '2026-01-29', 1,  'Open'),
  ('MEXAP10933', 'T430', 'DCOdV-38490', '2026-01-29', 2,  'Open'),
  ('MEXFS22989', 'P292', 'DCOdV-38532', '2026-01-30', 1,  'Open'),
  ('MEXAB19703', 'P030', 'DCOdV-38544', '2026-01-30', 1,  'Open'),
  ('MEXAB19703', 'P292', 'DCOdV-38544', '2026-01-30', 1,  'Open'),
  ('MEXAB19703', 'P328', 'DCOdV-38544', '2026-01-30', 1,  'Open'),
  ('MEXAB19703', 'Y524', 'DCOdV-38544', '2026-01-30', 2,  'Open'),
  ('MEXJG20850', 'P030', 'DCOdV-38564', '2026-01-30', 2,  'Open'),
  ('MEXJG20850', 'P292', 'DCOdV-38564', '2026-01-30', 1,  'Open')
) AS v(id_cliente, sku, odv_id, fecha, cantidad, estado_factura)
WHERE NOT EXISTS (
  SELECT 1 FROM ventas_odv vo
  WHERE vo.odv_id = v.odv_id
    AND vo.sku = v.sku
    AND vo.id_cliente = v.id_cliente
);
