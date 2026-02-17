-- Eliminar columna redundante id_cliente_zoho_normal de clientes.
-- id_cliente (PK) ya ES el zoho_normal ID. La columna era una copia idéntica.
--
-- Paso 1: Recrear vista que depende de la columna (sin ella)
-- Paso 2: Eliminar columna

-- DROP + CREATE (CREATE OR REPLACE can't remove columns from a view)
DROP VIEW IF EXISTS public.v_clientes_con_inventario;
CREATE VIEW public.v_clientes_con_inventario
WITH (security_invoker = true)
AS
SELECT
  c.id_cliente,
  c.nombre_cliente,
  c.id_zona,
  c.id_usuario,
  c.activo,
  c.estado,
  c.facturacion_promedio,
  c.facturacion_total,
  c.meses_con_venta,
  c.rango,
  c.id_cliente_zoho_botiquin,
  COALESCE(inv.total_inventario, 0::bigint) AS total_inventario,
  COALESCE(inv.total_inventario, 0::bigint) > 0 AS tiene_botiquin_activo
FROM clientes c
LEFT JOIN (
  SELECT id_cliente, sum(cantidad_disponible) AS total_inventario
  FROM inventario_botiquin
  GROUP BY id_cliente
) inv ON c.id_cliente = inv.id_cliente
ORDER BY c.nombre_cliente;

-- Drop the redundant column
ALTER TABLE public.clientes DROP COLUMN IF EXISTS id_cliente_zoho_normal;
