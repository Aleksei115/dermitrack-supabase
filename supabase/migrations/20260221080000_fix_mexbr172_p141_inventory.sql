-- Fix corrupt inventory data for MEXBR172 / SKU P141
-- Root cause: race condition between rpc_confirm_saga_pivot and
-- trigger_generate_movements_from_saga caused duplicate movements
-- with wrong cantidad_antes/cantidad_despues values.
--
-- CREACION: 3 units (correct)
-- VENTA: 3 units but cantidad_antes=0, cantidad_despues=2 (corrupt)
--   Should be: cantidad_antes=3, cantidad_despues=0
-- inventario_botiquin: cantidad_disponible=2 (should be 0, i.e. no row)

-- 1. Fix corrupt VENTA movement: cantidad_antes should be 3, cantidad_despues should be 0
UPDATE movimientos_inventario
SET cantidad_antes = 3, cantidad_despues = 0
WHERE id_cliente = 'MEXBR172' AND sku = 'P141' AND tipo = 'VENTA';

-- 2. Delete phantom inventory row (3 created - 3 sold = 0 stock)
DELETE FROM inventario_botiquin
WHERE id_cliente = 'MEXBR172' AND sku = 'P141';

-- 3. Cleanup: remove zero-qty rows for inactive MEXER156
DELETE FROM inventario_botiquin
WHERE id_cliente = 'MEXER156' AND cantidad_disponible = 0;
