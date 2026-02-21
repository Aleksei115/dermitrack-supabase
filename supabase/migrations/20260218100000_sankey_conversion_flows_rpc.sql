-- Sankey conversion flows RPC
-- Returns M2 (Venta Recurrente) and M3 (Venta por Exposición) flows per (cliente, sku)
-- Uses same criteria as get_impacto_botiquin_resumen()

CREATE OR REPLACE FUNCTION analytics.get_sankey_conversion_flows()
RETURNS TABLE (
  id_cliente   varchar,
  nombre_cliente varchar,
  sku          text,
  producto     text,
  categoria    text,      -- 'M2' or 'M3'
  valor_odv    numeric,
  cantidad_odv numeric,
  num_transacciones bigint
)
LANGUAGE sql STABLE
SET search_path = public
AS $$
  -- M2: SKUs with VENTA in botiquín that later have ODV purchases (excluding saga-linked ODVs)
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    a.sku,
    COALESCE(m.producto, a.sku) AS producto,
    'M2'::text AS categoria,
    COALESCE(SUM(v.cantidad * v.precio), 0) AS valor_odv,
    COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
    COUNT(v.*) AS num_transacciones
  FROM (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_venta
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, mi.sku
  ) a
  JOIN ventas_odv v ON v.id_cliente = a.id_cliente
                    AND v.sku = a.sku
                    AND v.fecha > a.first_venta
                    AND v.odv_id NOT IN (
                      SELECT szl.zoho_id FROM saga_zoho_links szl
                      WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
                    )
  JOIN clientes c ON c.id_cliente = a.id_cliente
  LEFT JOIN medicamentos m ON m.sku = a.sku
  GROUP BY c.id_cliente, c.nombre_cliente, a.sku, m.producto

  UNION ALL

  -- M3: SKUs with CREACION only (no VENTA) that later have ODV purchases (no prior ODV)
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    cs.sku,
    COALESCE(m.producto, cs.sku) AS producto,
    'M3'::text AS categoria,
    COALESCE(SUM(v.cantidad * v.precio), 0) AS valor_odv,
    COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
    COUNT(v.*) AS num_transacciones
  FROM (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_creacion
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'CREACION'
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = mi.id_cliente
          AND mi2.sku = mi.sku
          AND mi2.tipo = 'VENTA'
      )
    GROUP BY mi.id_cliente, mi.sku
  ) cs
  JOIN ventas_odv v ON v.id_cliente = cs.id_cliente
                    AND v.sku = cs.sku
                    AND v.fecha > cs.first_creacion
  JOIN clientes c ON c.id_cliente = cs.id_cliente
  LEFT JOIN medicamentos m ON m.sku = cs.sku
  WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv v2
    WHERE v2.id_cliente = cs.id_cliente
      AND v2.sku = cs.sku
      AND v2.fecha <= cs.first_creacion
  )
  GROUP BY c.id_cliente, c.nombre_cliente, cs.sku, m.producto;
$$;

-- Public schema wrapper (SECURITY DEFINER)
CREATE OR REPLACE FUNCTION public.get_sankey_conversion_flows()
RETURNS TABLE (
  id_cliente   varchar,
  nombre_cliente varchar,
  sku          text,
  producto     text,
  categoria    text,
  valor_odv    numeric,
  cantidad_odv numeric,
  num_transacciones bigint
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
  SELECT * FROM analytics.get_sankey_conversion_flows();
$$;
