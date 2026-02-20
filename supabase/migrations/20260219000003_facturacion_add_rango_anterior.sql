-- Add rango_anterior to get_facturacion_composicion()
-- The clientes table has rango (previous/anterior) and rango_actual (current)

DROP FUNCTION IF EXISTS get_facturacion_composicion();

CREATE OR REPLACE FUNCTION get_facturacion_composicion()
RETURNS TABLE(
  id_cliente varchar,
  nombre_cliente varchar,
  rango_actual varchar,
  rango_anterior varchar,
  activo boolean,
  baseline numeric,
  facturacion_actual numeric,
  current_m1 numeric,
  current_m2 numeric,
  current_m3 numeric,
  current_unlinked numeric,
  pct_crecimiento numeric,
  pct_vinculado numeric,
  valor_vinculado numeric,
  piezas_vinculadas bigint,
  skus_vinculados bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.id_cliente
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
  ),
  m1_impacto AS (
    SELECT mi.id_cliente,
      SUM(mi.cantidad * m.precio) AS m1_valor,
      SUM(mi.cantidad) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente
  ),
  first_venta AS (
    SELECT id_cliente, sku, MIN(fecha_movimiento::date) AS first_venta
    FROM movimientos_inventario
    WHERE tipo = 'VENTA'
    GROUP BY id_cliente, sku
  ),
  first_creacion AS (
    SELECT mi.id_cliente, mi.sku, MIN(mi.fecha_movimiento::date) AS first_creacion
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'CREACION'
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = mi.id_cliente AND mi2.sku = mi.sku AND mi2.tipo = 'VENTA'
      )
    GROUP BY mi.id_cliente, mi.sku
  ),
  prior_odv AS (
    SELECT DISTINCT v.id_cliente, v.sku
    FROM ventas_odv v
    JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku
    WHERE v.fecha <= fc.first_creacion
  ),
  categorized AS (
    SELECT
      v.id_cliente,
      v.sku,
      v.fecha,
      v.cantidad,
      v.cantidad * v.precio AS line_total,
      CASE
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.fecha > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.fecha > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM ventas_odv v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.id_cliente = m1.id_cliente
    LEFT JOIN first_venta fv ON v.id_cliente = fv.id_cliente AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.id_cliente = po.id_cliente AND v.sku = po.sku
    WHERE v.precio > 0
  ),
  totals AS (
    SELECT
      id_cliente,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M1'), 0) AS m1_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M2'), 0) AS m2_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M3'), 0) AS m3_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'UNLINKED'), 0) AS unlinked_total,
      COALESCE(SUM(line_total), 0) AS grand_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_valor,
      COALESCE(SUM(cantidad) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_piezas,
      COUNT(DISTINCT sku) FILTER (WHERE categoria IN ('M2','M3')) AS m2m3_skus
    FROM categorized
    GROUP BY id_cliente
  )
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    c.rango_actual,
    c.rango AS rango_anterior,
    c.activo,
    COALESCE(c.facturacion_promedio, 0)::numeric AS baseline,
    COALESCE(c.facturacion_actual, 0)::numeric AS facturacion_actual,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.facturacion_actual, 0) * t.m1_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m1,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.facturacion_actual, 0) * t.m2_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m2,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.facturacion_actual, 0) * t.m3_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m3,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.facturacion_actual, 0) * t.unlinked_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_unlinked,
    CASE WHEN COALESCE(c.facturacion_promedio, 0) > 0
      THEN ROUND(((COALESCE(c.facturacion_actual, 0) - c.facturacion_promedio) / c.facturacion_promedio * 100)::numeric, 1)
      ELSE NULL END AS pct_crecimiento,
    CASE WHEN t.grand_total > 0
      THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
      ELSE 0 END AS pct_vinculado,
    (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
    (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clientes c
  LEFT JOIN totals t ON c.id_cliente = t.id_cliente
  LEFT JOIN m1_impacto m1i ON c.id_cliente = m1i.id_cliente
  WHERE c.rango_actual IS NOT NULL
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$$;

NOTIFY pgrst, 'reload schema';
