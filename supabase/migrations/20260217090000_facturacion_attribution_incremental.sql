-- Rewrite get_facturacion_attribution() to compute INCREMENTAL M2/M3
-- Instead of total ODV from linked SKUs, this computes the delta (after - before)
-- for each SKU category relative to the botiquín start date.
DROP FUNCTION IF EXISTS get_facturacion_attribution();

CREATE OR REPLACE FUNCTION get_facturacion_attribution()
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, rango_actual varchar, activo boolean,
  facturacion_promedio numeric, facturacion_actual numeric, delta numeric, pct_cambio numeric,
  avg_m1 numeric, delta_m2 numeric, delta_m3 numeric, delta_base numeric,
  pct_crecimiento_botiquin numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH first_bot AS (
    SELECT id_cliente, MIN(fecha_movimiento)::date AS primera_fecha
    FROM movimientos_inventario
    WHERE tipo = 'CREACION'
    GROUP BY id_cliente
  ),
  m1_monthly AS (
    SELECT mi.id_cliente,
      ROUND((SUM(mi.cantidad * mi.precio_unitario) /
        NULLIF(COUNT(DISTINCT date_trunc('month', mi.fecha_movimiento)), 0))::numeric, 2) AS avg_m1
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA' AND mi.precio_unitario > 0
    GROUP BY mi.id_cliente
  ),
  sku_con_venta AS (
    SELECT DISTINCT id_cliente, sku
    FROM movimientos_inventario WHERE tipo = 'VENTA'
  ),
  sku_solo_creacion AS (
    SELECT DISTINCT mi.id_cliente, mi.sku
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'CREACION'
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = mi.id_cliente AND mi2.sku = mi.sku AND mi2.tipo = 'VENTA'
      )
  ),
  odv_antes AS (
    SELECT v.id_cliente,
      COUNT(DISTINCT date_trunc('month', v.fecha)) AS meses,
      COALESCE(SUM(CASE WHEN sv.sku IS NOT NULL THEN v.cantidad * v.precio END), 0) AS m2_total,
      COALESCE(SUM(CASE WHEN sc.sku IS NOT NULL THEN v.cantidad * v.precio END), 0) AS m3_total
    FROM ventas_odv v
    JOIN first_bot fb ON v.id_cliente = fb.id_cliente
    LEFT JOIN sku_con_venta sv ON v.id_cliente = sv.id_cliente AND v.sku = sv.sku
    LEFT JOIN sku_solo_creacion sc ON v.id_cliente = sc.id_cliente AND v.sku = sc.sku
    WHERE v.precio > 0 AND v.fecha < fb.primera_fecha
    GROUP BY v.id_cliente
  ),
  odv_despues AS (
    SELECT v.id_cliente,
      COUNT(DISTINCT date_trunc('month', v.fecha)) AS meses,
      COALESCE(SUM(CASE WHEN sv.sku IS NOT NULL THEN v.cantidad * v.precio END), 0) AS m2_total,
      COALESCE(SUM(CASE WHEN sc.sku IS NOT NULL THEN v.cantidad * v.precio END), 0) AS m3_total
    FROM ventas_odv v
    JOIN first_bot fb ON v.id_cliente = fb.id_cliente
    LEFT JOIN sku_con_venta sv ON v.id_cliente = sv.id_cliente AND v.sku = sv.sku
    LEFT JOIN sku_solo_creacion sc ON v.id_cliente = sc.id_cliente AND v.sku = sc.sku
    WHERE v.precio > 0 AND v.fecha >= fb.primera_fecha
    GROUP BY v.id_cliente
  ),
  computed AS (
    SELECT
      c.id_cliente, c.nombre_cliente, c.rango_actual, c.activo,
      COALESCE(c.facturacion_promedio, 0)::numeric AS facturacion_promedio,
      COALESCE(c.facturacion_actual, 0)::numeric AS facturacion_actual,
      (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0))::numeric AS delta,
      CASE WHEN COALESCE(c.facturacion_promedio, 0) > 0
        THEN ROUND(((COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) / c.facturacion_promedio * 100)::numeric, 1)
        ELSE NULL END AS pct_cambio,
      COALESCE(m1.avg_m1, 0)::numeric AS avg_m1,
      ROUND((COALESCE(od.m2_total / NULLIF(od.meses, 0), 0) - COALESCE(oa.m2_total / NULLIF(oa.meses, 0), 0))::numeric, 2) AS delta_m2,
      ROUND((COALESCE(od.m3_total / NULLIF(od.meses, 0), 0) - COALESCE(oa.m3_total / NULLIF(oa.meses, 0), 0))::numeric, 2) AS delta_m3
    FROM clientes c
    LEFT JOIN m1_monthly m1 ON c.id_cliente = m1.id_cliente
    LEFT JOIN odv_antes oa ON c.id_cliente = oa.id_cliente
    LEFT JOIN odv_despues od ON c.id_cliente = od.id_cliente
    WHERE c.rango_actual IS NOT NULL
  )
  SELECT
    id_cliente, nombre_cliente, rango_actual, activo,
    facturacion_promedio, facturacion_actual, delta, pct_cambio,
    avg_m1, delta_m2, delta_m3,
    (delta - delta_m2 - delta_m3)::numeric AS delta_base,
    CASE WHEN (GREATEST(avg_m1, 0) + GREATEST(delta_m2, 0) + GREATEST(delta_m3, 0) + GREATEST(delta - delta_m2 - delta_m3, 0)) > 0
      THEN ROUND((
        (GREATEST(avg_m1, 0) + GREATEST(delta_m2, 0) + GREATEST(delta_m3, 0)) /
        (GREATEST(avg_m1, 0) + GREATEST(delta_m2, 0) + GREATEST(delta_m3, 0) + GREATEST(delta - delta_m2 - delta_m3, 0))
        * 100)::numeric, 1)
      ELSE 0 END AS pct_crecimiento_botiquin
  FROM computed
  ORDER BY delta DESC;
$$;
