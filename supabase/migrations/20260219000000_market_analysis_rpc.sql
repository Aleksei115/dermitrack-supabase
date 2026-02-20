-- Market Analysis RPC: unified view for Análisis de Mercado tab
-- Returns one row per (id_cliente, sku) with aggregated movement metrics + M2 conversion data.
-- stock_activo_pz comes from inventario_botiquin.cantidad_disponible (source of truth).
-- Includes valor columns for all 3 movement types (for treemap visualization).

CREATE OR REPLACE FUNCTION analytics.get_market_analysis()
RETURNS TABLE(
  id_cliente        varchar,
  sku               varchar,
  producto          varchar,
  marca             varchar,
  padecimiento      varchar,
  es_top            boolean,
  venta_pz          bigint,
  venta_valor       numeric,
  creacion_pz       bigint,
  creacion_valor    numeric,
  recoleccion_pz    bigint,
  recoleccion_valor numeric,
  stock_activo_pz   bigint,
  conversiones_m2   bigint,
  revenue_m2        numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  -- Padecimiento lookup (same pattern as clasificacion_base)
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku)
      mp.sku,
      p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  -- Aggregate movimientos_inventario by (id_cliente, sku, tipo)
  movements AS (
    SELECT
      mi.id_cliente,
      mi.sku,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::bigint             AS venta_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0)         AS venta_valor,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)::bigint           AS creacion_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'CREACION'), 0)      AS creacion_valor,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::bigint        AS recoleccion_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)   AS recoleccion_valor
    FROM movimientos_inventario mi
    JOIN medicamentos med ON med.sku = mi.sku
    GROUP BY mi.id_cliente, mi.sku
  ),
  -- M2 conversions from clasificacion_base
  m2_counts AS (
    SELECT
      cb.id_cliente,
      cb.sku,
      COUNT(*)::bigint AS conversiones_m2,
      COALESCE(SUM(cb.revenue_odv), 0) AS revenue_m2
    FROM analytics.clasificacion_base() cb
    WHERE cb.m_type = 'M2'
    GROUP BY cb.id_cliente, cb.sku
  )
  SELECT
    mv.id_cliente::varchar,
    mv.sku::varchar,
    med.producto::varchar,
    med.marca::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    med.top AS es_top,
    mv.venta_pz,
    mv.venta_valor,
    mv.creacion_pz,
    mv.creacion_valor,
    mv.recoleccion_pz,
    mv.recoleccion_valor,
    COALESCE(ib.cantidad_disponible, 0)::bigint AS stock_activo_pz,
    COALESCE(m2.conversiones_m2, 0)::bigint,
    COALESCE(m2.revenue_m2, 0)::numeric
  FROM movements mv
  JOIN medicamentos med ON med.sku = mv.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
  LEFT JOIN m2_counts m2 ON m2.id_cliente = mv.id_cliente AND m2.sku = mv.sku
  LEFT JOIN inventario_botiquin ib ON ib.id_cliente = mv.id_cliente AND ib.sku = mv.sku;
$$;

-- Public wrapper (exposed via PostgREST)
CREATE OR REPLACE FUNCTION public.get_market_analysis()
RETURNS TABLE(
  id_cliente        varchar,
  sku               varchar,
  producto          varchar,
  marca             varchar,
  padecimiento      varchar,
  es_top            boolean,
  venta_pz          bigint,
  venta_valor       numeric,
  creacion_pz       bigint,
  creacion_valor    numeric,
  recoleccion_pz    bigint,
  recoleccion_valor numeric,
  stock_activo_pz   bigint,
  conversiones_m2   bigint,
  revenue_m2        numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_market_analysis();
$$;

NOTIFY pgrst, 'reload schema';
