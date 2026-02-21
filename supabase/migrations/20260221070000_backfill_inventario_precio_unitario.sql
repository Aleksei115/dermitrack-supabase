-- Backfill: 11 inventario_botiquin rows (all MEXBR172) have NULL precio_unitario.
-- Set them from medicamentos.precio so all RPCs get correct values without fallback.

UPDATE inventario_botiquin ib
SET precio_unitario = med.precio
FROM medicamentos med
WHERE ib.sku = med.sku
  AND ib.precio_unitario IS NULL;
