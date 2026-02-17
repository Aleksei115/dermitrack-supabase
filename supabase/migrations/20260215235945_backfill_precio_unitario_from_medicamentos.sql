-- Backfill precio_unitario from medicamentos.precio where NULL
UPDATE movimientos_inventario mi
SET precio_unitario = m.precio
FROM medicamentos m
WHERE m.sku = mi.sku
AND mi.precio_unitario IS NULL;
