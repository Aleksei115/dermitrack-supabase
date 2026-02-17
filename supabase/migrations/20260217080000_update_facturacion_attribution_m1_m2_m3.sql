-- Update get_facturacion_attribution to split into M1/M2/M3 metrics:
-- M1: Ventas directas botiquín (movimientos VENTA)
-- M2: Convertidas a recurrente (SKU tuvo VENTA en botiquín, luego compra ODV)
-- M3: Vinculadas a exposición (SKU solo tuvo CREACION, sin VENTA, luego compra ODV)
DROP FUNCTION IF EXISTS public.get_facturacion_attribution();

CREATE OR REPLACE FUNCTION public.get_facturacion_attribution()
RETURNS TABLE (
  id_cliente varchar,
  nombre_cliente varchar,
  rango_actual varchar,
  activo boolean,
  facturacion_promedio numeric,
  facturacion_actual numeric,
  delta numeric,
  pct_cambio numeric,
  avg_m1 numeric,
  avg_m2 numeric,
  avg_m3 numeric,
  pct_ingreso_botiquin numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  WITH botiquin_monthly AS (
    SELECT
      mi.id_cliente,
      COUNT(DISTINCT date_trunc('month', mi.fecha_movimiento)) AS meses_actividad,
      SUM(mi.cantidad * mi.precio_unitario) AS total_m1
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA' AND mi.precio_unitario > 0
    GROUP BY mi.id_cliente
  ),
  sku_con_venta AS (
    SELECT DISTINCT id_cliente, sku
    FROM movimientos_inventario
    WHERE tipo = 'VENTA'
  ),
  first_creacion AS (
    SELECT id_cliente, sku, MIN(fecha_movimiento)::date AS primera_creacion
    FROM movimientos_inventario
    WHERE tipo = 'CREACION'
    GROUP BY id_cliente, sku
  ),
  m2_data AS (
    SELECT v.id_cliente,
      COUNT(DISTINCT date_trunc('month', v.fecha)) AS meses,
      SUM(v.cantidad * v.precio) AS total
    FROM ventas_odv v
    JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku AND v.fecha > fc.primera_creacion
    JOIN sku_con_venta sv ON v.id_cliente = sv.id_cliente AND v.sku = sv.sku
    WHERE v.precio > 0
    GROUP BY v.id_cliente
  ),
  m3_data AS (
    SELECT v.id_cliente,
      COUNT(DISTINCT date_trunc('month', v.fecha)) AS meses,
      SUM(v.cantidad * v.precio) AS total
    FROM ventas_odv v
    JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku AND v.fecha > fc.primera_creacion
    LEFT JOIN sku_con_venta sv ON v.id_cliente = sv.id_cliente AND v.sku = sv.sku
    WHERE v.precio > 0 AND sv.sku IS NULL
    GROUP BY v.id_cliente
  )
  SELECT
    c.id_cliente, c.nombre_cliente, c.rango_actual, c.activo,
    COALESCE(c.facturacion_promedio, 0)::numeric AS facturacion_promedio,
    COALESCE(c.facturacion_actual, 0)::numeric AS facturacion_actual,
    (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0))::numeric AS delta,
    CASE WHEN COALESCE(c.facturacion_promedio, 0) > 0
      THEN ROUND(((COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) / c.facturacion_promedio * 100)::numeric, 1)
      ELSE NULL END AS pct_cambio,
    CASE WHEN COALESCE(bm.meses_actividad, 0) > 0
      THEN ROUND((bm.total_m1 / bm.meses_actividad)::numeric, 2) ELSE 0 END AS avg_m1,
    CASE WHEN COALESCE(m2.meses, 0) > 0
      THEN ROUND((m2.total / m2.meses)::numeric, 2) ELSE 0 END AS avg_m2,
    CASE WHEN COALESCE(m3.meses, 0) > 0
      THEN ROUND((m3.total / m3.meses)::numeric, 2) ELSE 0 END AS avg_m3,
    CASE WHEN (COALESCE(c.facturacion_actual, 0) +
      CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_m1 / bm.meses_actividad) ELSE 0 END) > 0
      THEN ROUND((
        (CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_m1 / bm.meses_actividad) ELSE 0 END
         + CASE WHEN COALESCE(m2.meses, 0) > 0 THEN (m2.total / m2.meses) ELSE 0 END
         + CASE WHEN COALESCE(m3.meses, 0) > 0 THEN (m3.total / m3.meses) ELSE 0 END)
        / (COALESCE(c.facturacion_actual, 0)
         + CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_m1 / bm.meses_actividad) ELSE 0 END)
        * 100
      )::numeric, 1) ELSE 0 END AS pct_ingreso_botiquin
  FROM clientes c
  LEFT JOIN botiquin_monthly bm ON c.id_cliente = bm.id_cliente
  LEFT JOIN m2_data m2 ON c.id_cliente = m2.id_cliente
  LEFT JOIN m3_data m3 ON c.id_cliente = m3.id_cliente
  WHERE c.rango_actual IS NOT NULL
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$$;
