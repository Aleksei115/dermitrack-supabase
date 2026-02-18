-- Reconciliar datos de MEXBR172: insertar CREACION faltantes y sincronizar inventario_botiquin
-- Contexto: 3 SKUs (P146, P567, P630) tenian VENTA/RECOLECCION sin CREACION previa
-- y 4 SKUs (Y810, X952, P592, P632) tenian stock en movimientos pero 0 en inventario
-- Nota: cantidad_despues en movimientos es unreliable (no forma running totals consistentes),
-- por lo que usamos SUM(CREACION) - SUM(VENTA) - SUM(RECOLECCION) para calcular stock correcto.

-- 1. Insertar movimientos CREACION faltantes para SKUs con stock negativo
-- (id_saga_transaction es nullable — usamos NULL para reconciliaciones manuales)
INSERT INTO movimientos_inventario (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo, precio_unitario)
SELECT
  NULL, 'MEXBR172', sku,
  abs(stock_mov) as cantidad,
  0 as cantidad_antes,
  abs(stock_mov) as cantidad_despues,
  (SELECT MIN(fecha_movimiento) FROM movimientos_inventario WHERE id_cliente = 'MEXBR172' AND sku = t.sku) - interval '1 second',
  'CREACION',
  (SELECT precio FROM medicamentos WHERE sku = t.sku)
FROM (
  SELECT sku,
    SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END) as stock_mov
  FROM movimientos_inventario
  WHERE id_cliente = 'MEXBR172'
  GROUP BY sku
  HAVING SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END) < 0
) t;

-- 2. Reconciliar inventario_botiquin usando stock CALCULADO (no cantidad_despues)
-- Actualizar entries existentes
WITH calculated AS (
  SELECT sku,
    GREATEST(0,
      SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END)
    ) as correct_stock
  FROM movimientos_inventario
  WHERE id_cliente = 'MEXBR172'
  GROUP BY sku
)
UPDATE inventario_botiquin inv
SET cantidad_disponible = c.correct_stock,
    ultima_actualizacion = now()
FROM calculated c
WHERE inv.id_cliente = 'MEXBR172'
  AND inv.sku = c.sku
  AND inv.cantidad_disponible != c.correct_stock;

-- 3. Insertar entries faltantes para SKUs con stock positivo sin entry en inventario
WITH calculated AS (
  SELECT sku,
    GREATEST(0,
      SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
      - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END)
    ) as correct_stock
  FROM movimientos_inventario
  WHERE id_cliente = 'MEXBR172'
  GROUP BY sku
  HAVING GREATEST(0,
    SUM(CASE WHEN tipo = 'CREACION' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'VENTA' THEN cantidad ELSE 0 END)
    - SUM(CASE WHEN tipo = 'RECOLECCION' THEN cantidad ELSE 0 END)
  ) > 0
)
INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
SELECT 'MEXBR172', c.sku, c.correct_stock, now()
FROM calculated c
WHERE NOT EXISTS (
  SELECT 1 FROM inventario_botiquin inv
  WHERE inv.id_cliente = 'MEXBR172' AND inv.sku = c.sku
)
ON CONFLICT (id_cliente, sku) DO UPDATE SET
  cantidad_disponible = EXCLUDED.cantidad_disponible,
  ultima_actualizacion = now();

-- 4. Limpiar inventario con cantidad 0
DELETE FROM inventario_botiquin
WHERE id_cliente = 'MEXBR172' AND cantidad_disponible = 0;

NOTIFY pgrst, 'reload schema';
