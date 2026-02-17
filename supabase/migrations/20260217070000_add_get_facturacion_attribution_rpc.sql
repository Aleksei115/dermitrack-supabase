-- RPC: get_facturacion_attribution
-- Returns per-doctor facturacion breakdown with botiquin attribution metrics
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
  avg_botiquin_mensual numeric,
  avg_conversiones_m2 numeric,
  pct_atribucion_total numeric
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
      SUM(mi.cantidad * mi.precio_unitario) AS total_botiquin_venta
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA'
      AND mi.precio_unitario > 0
    GROUP BY mi.id_cliente
  ),
  first_creacion AS (
    SELECT
      id_cliente,
      sku,
      MIN(fecha_movimiento) AS primera_creacion
    FROM movimientos_inventario
    WHERE tipo = 'CREACION'
    GROUP BY id_cliente, sku
  ),
  conversiones_m2 AS (
    SELECT
      v.id_cliente,
      COUNT(DISTINCT date_trunc('month', v.fecha)) AS meses_con_conversion,
      SUM(v.cantidad * v.precio) AS total_conversiones
    FROM ventas_odv v
    INNER JOIN first_creacion fc
      ON v.id_cliente = fc.id_cliente
      AND v.sku = fc.sku
      AND v.fecha > fc.primera_creacion::date
    WHERE v.precio > 0
    GROUP BY v.id_cliente
  )
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    c.rango_actual,
    c.activo,
    COALESCE(c.facturacion_promedio, 0)::numeric AS facturacion_promedio,
    COALESCE(c.facturacion_actual, 0)::numeric AS facturacion_actual,
    (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0))::numeric AS delta,
    CASE WHEN COALESCE(c.facturacion_promedio, 0) > 0
      THEN ROUND(((COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) / c.facturacion_promedio * 100)::numeric, 1)
      ELSE NULL
    END AS pct_cambio,
    CASE WHEN COALESCE(bm.meses_actividad, 0) > 0
      THEN ROUND((bm.total_botiquin_venta / bm.meses_actividad)::numeric, 2)
      ELSE 0
    END AS avg_botiquin_mensual,
    CASE WHEN COALESCE(cm.meses_con_conversion, 0) > 0
      THEN ROUND((cm.total_conversiones / cm.meses_con_conversion)::numeric, 2)
      ELSE 0
    END AS avg_conversiones_m2,
    CASE WHEN (COALESCE(c.facturacion_actual, 0) +
      CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_botiquin_venta / bm.meses_actividad) ELSE 0 END) > 0
      THEN ROUND((
        (CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_botiquin_venta / bm.meses_actividad) ELSE 0 END
         + CASE WHEN COALESCE(cm.meses_con_conversion, 0) > 0 THEN (cm.total_conversiones / cm.meses_con_conversion) ELSE 0 END)
        / (COALESCE(c.facturacion_actual, 0)
         + CASE WHEN COALESCE(bm.meses_actividad, 0) > 0 THEN (bm.total_botiquin_venta / bm.meses_actividad) ELSE 0 END)
        * 100
      )::numeric, 1)
      ELSE 0
    END AS pct_atribucion_total
  FROM clientes c
  LEFT JOIN botiquin_monthly bm ON c.id_cliente = bm.id_cliente
  LEFT JOIN conversiones_m2 cm ON c.id_cliente = cm.id_cliente
  WHERE c.rango_actual IS NOT NULL
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$$;
