-- =============================================================================
-- Migration: Add 5 global filter params to ALL analytics RPCs
-- Filters: p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin
-- Also fixes:
--   1. get_corte_historico_data: add SKU unique counts to KPIs
--   2. get_corte_logistica_data: fix cantidad_colocada (use SUM CREACION, not stock)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. clasificacion_base — FOUNDATION (called by many downstream RPCs)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.clasificacion_base(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente         varchar,
  nombre_cliente     varchar,
  sku                varchar,
  producto           varchar,
  padecimiento       varchar,
  marca              varchar,
  es_top             boolean,
  m_type             text,
  first_event_date   date,
  revenue_botiquin   numeric,
  revenue_odv        numeric,
  cantidad_odv       numeric,
  num_transacciones_odv bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  saga_odv_ids AS (
    SELECT DISTINCT szl.zoho_id
    FROM saga_zoho_links szl
    WHERE szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku)
      mp.sku,
      p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  m1_pairs AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_venta,
           SUM(mi.cantidad * m.precio) AS revenue_botiquin
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
    GROUP BY mi.id_cliente, mi.sku
  ),
  m2_agg AS (
    SELECT mp.id_cliente, mp.sku,
           COALESCE(SUM(v.cantidad * v.precio), 0) AS revenue_odv,
           COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m1_pairs mp
    JOIN ventas_odv v ON v.id_cliente = mp.id_cliente
                      AND v.sku = mp.sku
                      AND v.fecha >= mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    GROUP BY mp.id_cliente, mp.sku
    HAVING COALESCE(SUM(v.cantidad * v.precio), 0) > 0
  ),
  m3_candidates AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_creacion
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'CREACION'
      AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = mi.id_cliente
          AND mi2.sku = mi.sku
          AND mi2.tipo = 'VENTA'
      )
    GROUP BY mi.id_cliente, mi.sku
  ),
  m3_agg AS (
    SELECT mc.id_cliente, mc.sku, mc.first_creacion,
           COALESCE(SUM(v.cantidad * v.precio), 0) AS revenue_odv,
           COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc
    JOIN ventas_odv v ON v.id_cliente = mc.id_cliente
                      AND v.sku = mc.sku
                      AND v.fecha >= mc.first_creacion
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
      AND NOT EXISTS (
        SELECT 1 FROM ventas_odv v2
        WHERE v2.id_cliente = mc.id_cliente
          AND v2.sku = mc.sku
          AND v2.fecha <= mc.first_creacion
      )
    GROUP BY mc.id_cliente, mc.sku, mc.first_creacion
    HAVING COALESCE(SUM(v.cantidad * v.precio), 0) > 0
  )

  -- M1 rows
  SELECT
    mp.id_cliente::varchar,
    c.nombre_cliente::varchar,
    mp.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top AS es_top,
    'M1'::text AS m_type,
    mp.first_venta AS first_event_date,
    mp.revenue_botiquin,
    0::numeric AS revenue_odv,
    0::numeric AS cantidad_odv,
    0::bigint AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN clientes c ON c.id_cliente = mp.id_cliente
  JOIN medicamentos m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku

  UNION ALL

  -- M2 rows
  SELECT
    m2.id_cliente::varchar,
    c.nombre_cliente::varchar,
    m2.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top,
    'M2'::text,
    mp.first_venta,
    0::numeric,
    m2.revenue_odv,
    m2.cantidad_odv,
    m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.id_cliente = m2.id_cliente AND mp.sku = m2.sku
  JOIN clientes c ON c.id_cliente = m2.id_cliente
  JOIN medicamentos m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku

  UNION ALL

  -- M3 rows
  SELECT
    m3.id_cliente::varchar,
    c.nombre_cliente::varchar,
    m3.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top,
    'M3'::text,
    m3.first_creacion,
    0::numeric,
    m3.revenue_odv,
    m3.cantidad_odv,
    m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clientes c ON c.id_cliente = m3.id_cliente
  JOIN medicamentos m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.clasificacion_base(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente         varchar,
  nombre_cliente     varchar,
  sku                varchar,
  producto           varchar,
  padecimiento       varchar,
  marca              varchar,
  es_top             boolean,
  m_type             text,
  first_event_date   date,
  revenue_botiquin   numeric,
  revenue_odv        numeric,
  cantidad_odv       numeric,
  num_transacciones_odv bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_impacto_botiquin_resumen — uses clasificacion_base
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_impacto_botiquin_resumen(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  adopciones integer, revenue_adopciones numeric,
  conversiones integer, revenue_conversiones numeric,
  exposiciones integer, revenue_exposiciones numeric,
  crosssell_pares integer, revenue_crosssell numeric,
  revenue_total_impacto numeric, revenue_total_odv numeric,
  porcentaje_impacto numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin)),
  m1 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev FROM base WHERE m_type = 'M1'),
  m2 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M2'),
  m3 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M3'),
  total_odv AS (SELECT COALESCE(SUM(cantidad * precio), 0) AS rev FROM ventas_odv)
  SELECT m1.cnt, m1.rev, m2.cnt, m2.rev, m3.cnt, m3.rev, 0::int, 0::numeric,
    (m1.rev + m2.rev + m3.rev), t.rev,
    CASE WHEN t.rev > 0 THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1) ELSE 0 END
  FROM m1, m2, m3, total_odv t;
$$;

CREATE OR REPLACE FUNCTION public.get_impacto_botiquin_resumen(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  adopciones integer, revenue_adopciones numeric,
  conversiones integer, revenue_conversiones numeric,
  exposiciones integer, revenue_exposiciones numeric,
  crosssell_pares integer, revenue_crosssell numeric,
  revenue_total_impacto numeric, revenue_total_odv numeric,
  porcentaje_impacto numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_impacto_botiquin_resumen(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. get_impacto_detalle — uses clasificacion_base
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_impacto_detalle(
  p_metrica text,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  cantidad integer, precio numeric, valor numeric, fecha date, detalle text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF p_metrica = 'M1' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           ROUND(b.revenue_botiquin / NULLIF(m.precio, 0))::int AS cantidad,
           m.precio,
           b.revenue_botiquin AS valor,
           b.first_event_date AS fecha,
           'Adopción en botiquín'::text AS detalle
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
    JOIN medicamentos m ON m.sku = b.sku
    WHERE b.m_type = 'M1'
    ORDER BY b.revenue_botiquin DESC;

  ELSIF p_metrica = 'M2' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           b.cantidad_odv::int AS cantidad,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS precio,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS fecha,
           ('ODV después de botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
    JOIN LATERAL (
      SELECT MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
        AND v.fecha > b.first_event_date
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    ) odv_first ON true
    WHERE b.m_type = 'M2'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metrica = 'M3' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           b.cantidad_odv::int AS cantidad,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS precio,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS fecha,
           ('Exposición post-botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
    JOIN LATERAL (
      SELECT MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
        AND v.fecha > b.first_event_date
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    ) odv_first ON true
    WHERE b.m_type = 'M3'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metrica = 'M4' THEN
    RETURN;

  ELSE
    RAISE EXCEPTION 'Métrica inválida: %. Use M1, M2, M3 o M4.', p_metrica;
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_impacto_detalle(
  p_metrica text,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  cantidad integer, precio numeric, valor numeric, fecha date, detalle text
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_impacto_detalle(p_metrica, p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. get_sankey_conversion_flows — uses clasificacion_base
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_sankey_conversion_flows(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku text, producto text,
  categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT b.id_cliente::varchar, b.nombre_cliente::varchar, b.sku::text, b.producto::text,
    b.m_type::text AS categoria, b.revenue_odv AS valor_odv, b.cantidad_odv, b.num_transacciones_odv AS num_transacciones
  FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
  WHERE b.m_type IN ('M2', 'M3');
$$;

CREATE OR REPLACE FUNCTION public.get_sankey_conversion_flows(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku text, producto text,
  categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_sankey_conversion_flows(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. get_top_converting_skus — uses clasificacion_base
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_top_converting_skus(
  p_limit integer DEFAULT 10,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  sku varchar, producto varchar, conversiones integer,
  avg_dias integer, roi numeric, valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base_m2 AS (
    SELECT b.id_cliente, b.sku, b.revenue_odv, b.first_event_date AS first_venta
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b WHERE b.m_type = 'M2'
  ),
  with_invest AS (
    SELECT bm.id_cliente, bm.sku, bm.revenue_odv, bm.first_venta,
      (SELECT MIN(mi.fecha_movimiento::date) FROM movimientos_inventario mi WHERE mi.id_cliente = bm.id_cliente AND mi.sku = bm.sku AND mi.tipo = 'CREACION') AS first_creacion,
      (SELECT COALESCE(SUM(mi.cantidad * m.precio), 0) FROM movimientos_inventario mi JOIN medicamentos m ON m.sku = mi.sku WHERE mi.id_cliente = bm.id_cliente AND mi.sku = bm.sku AND mi.tipo = 'CREACION') AS invest
    FROM base_m2 bm
  )
  SELECT wi.sku, m.producto, COUNT(*)::int AS conversiones,
    ROUND(AVG(GREATEST(0, wi.first_venta - wi.first_creacion)))::int AS avg_dias,
    CASE WHEN SUM(wi.invest) > 0 THEN ROUND(SUM(wi.revenue_odv) / SUM(wi.invest), 1) ELSE 0 END AS roi,
    SUM(wi.revenue_odv) AS valor_generado
  FROM with_invest wi JOIN medicamentos m ON m.sku = wi.sku
  GROUP BY wi.sku, m.producto ORDER BY valor_generado DESC LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public.get_top_converting_skus(
  p_limit integer DEFAULT 10,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  sku varchar, producto varchar, conversiones integer,
  avg_dias integer, roi numeric, valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_top_converting_skus(p_limit, p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. get_conversion_metrics — uses clasificacion_base (public only)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_conversion_metrics(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin))
  SELECT (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
         (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
         COALESCE((SELECT SUM(revenue_odv) FROM base WHERE m_type = 'M2'), 0)::numeric;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. get_conversion_details — uses clasificacion_base (public only)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_conversion_details(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  m_type text, id_cliente varchar, nombre_cliente varchar, sku varchar,
  producto varchar, fecha_botiquin date, fecha_primera_odv date,
  dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT
    b.m_type::text,
    b.id_cliente, b.nombre_cliente, b.sku, b.producto,
    b.first_event_date AS fecha_botiquin,
    odv_first.first_odv AS fecha_primera_odv,
    (odv_first.first_odv - b.first_event_date)::int AS dias_conversion,
    b.num_transacciones_odv AS num_ventas_odv,
    b.cantidad_odv::bigint AS total_piezas,
    b.revenue_odv AS valor_generado
  FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
  JOIN LATERAL (
    SELECT MIN(v.fecha) AS first_odv
    FROM ventas_odv v
    WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
      AND v.fecha >= b.first_event_date
      AND v.odv_id NOT IN (
        SELECT szl.zoho_id FROM saga_zoho_links szl
        WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
      )
  ) odv_first ON true
  WHERE b.m_type IN ('M2', 'M3')
  ORDER BY b.revenue_odv DESC;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. get_market_analysis — uses clasificacion_base
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_market_analysis(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, sku varchar, producto varchar, marca varchar,
  padecimiento varchar, es_top boolean,
  venta_pz bigint, venta_valor numeric,
  creacion_pz bigint, creacion_valor numeric,
  recoleccion_pz bigint, recoleccion_valor numeric,
  stock_activo_pz bigint,
  conversiones_m2 bigint, revenue_m2 numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku)
      mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  movements AS (
    SELECT
      mi.id_cliente, mi.sku,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::bigint             AS venta_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0)         AS venta_valor,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)::bigint           AS creacion_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'CREACION'), 0)      AS creacion_valor,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::bigint        AS recoleccion_pz,
      COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)   AS recoleccion_valor
    FROM movimientos_inventario mi
    JOIN medicamentos med ON med.sku = mi.sku
    WHERE (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
    GROUP BY mi.id_cliente, mi.sku
  ),
  m2_counts AS (
    SELECT cb.id_cliente, cb.sku,
      COUNT(*)::bigint AS conversiones_m2,
      COALESCE(SUM(cb.revenue_odv), 0) AS revenue_m2
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) cb
    WHERE cb.m_type = 'M2'
    GROUP BY cb.id_cliente, cb.sku
  )
  SELECT
    mv.id_cliente::varchar, mv.sku::varchar, med.producto::varchar, med.marca::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, med.top AS es_top,
    mv.venta_pz, mv.venta_valor,
    mv.creacion_pz, mv.creacion_valor,
    mv.recoleccion_pz, mv.recoleccion_valor,
    COALESCE(ib.cantidad_disponible, 0)::bigint AS stock_activo_pz,
    COALESCE(m2.conversiones_m2, 0)::bigint,
    COALESCE(m2.revenue_m2, 0)::numeric
  FROM movements mv
  JOIN medicamentos med ON med.sku = mv.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
  LEFT JOIN m2_counts m2 ON m2.id_cliente = mv.id_cliente AND m2.sku = mv.sku
  LEFT JOIN inventario_botiquin ib ON ib.id_cliente = mv.id_cliente AND ib.sku = mv.sku;
$$;

CREATE OR REPLACE FUNCTION public.get_market_analysis(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, sku varchar, producto varchar, marca varchar,
  padecimiento varchar, es_top boolean,
  venta_pz bigint, venta_valor numeric,
  creacion_pz bigint, creacion_valor numeric,
  recoleccion_pz bigint, recoleccion_valor numeric,
  stock_activo_pz bigint,
  conversiones_m2 bigint, revenue_m2 numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_market_analysis(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. get_brand_performance — standalone (movimientos_inventario)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_brand_performance(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(marca varchar, valor numeric, piezas integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT m.marca,
         SUM(mi.cantidad * m.precio) AS valor,
         SUM(mi.cantidad)::int AS piezas
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  WHERE mi.tipo = 'VENTA'
    AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
  GROUP BY m.marca
  ORDER BY valor DESC;
$$;

CREATE OR REPLACE FUNCTION public.get_brand_performance(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(marca varchar, valor numeric, piezas integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_brand_performance(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 10. get_padecimiento_performance — standalone
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_padecimiento_performance(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, valor numeric, piezas integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
         SUM(mi.cantidad * m.precio) AS valor,
         SUM(mi.cantidad)::int AS piezas
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
  WHERE mi.tipo = 'VENTA'
    AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
  GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ORDER BY valor DESC;
$$;

CREATE OR REPLACE FUNCTION public.get_padecimiento_performance(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, valor numeric, piezas integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_padecimiento_performance(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 11. get_product_interest — standalone
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_product_interest(
  p_limit integer DEFAULT 15,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(producto varchar, venta integer, creacion integer, recoleccion integer, stock_activo integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT
    CASE WHEN LENGTH(m.producto) > 20 THEN LEFT(m.producto, 20) || '...' ELSE m.producto END AS producto,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::int AS venta,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)::int AS creacion,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::int AS recoleccion,
    GREATEST(0,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)
    )::int AS stock_activo
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  WHERE mi.tipo IN ('VENTA', 'CREACION', 'RECOLECCION')
    AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
  GROUP BY m.producto
  ORDER BY (
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0) +
    GREATEST(0,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)
    )
  ) DESC
  LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public.get_product_interest(
  p_limit integer DEFAULT 15,
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(producto varchar, venta integer, creacion integer, recoleccion integer, stock_activo integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_product_interest(p_limit, p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 12. get_opportunity_matrix — standalone
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_opportunity_matrix(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, venta integer, recoleccion integer, valor numeric, converted_qty integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  botiquin_by_pad AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::int AS venta,
           COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::int AS recoleccion,
           COALESCE(SUM(mi.cantidad * m.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0) AS valor
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.tipo IN ('VENTA', 'RECOLECCION')
      AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ),
  converted AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(v.cantidad), 0)::int AS converted_qty
    FROM (
      SELECT mi.id_cliente, mi.sku,
             MIN(mi.fecha_movimiento::date) AS first_venta
      FROM movimientos_inventario mi
      WHERE mi.tipo = 'VENTA'
        AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
        AND mi.sku IN (SELECT sku FROM filtered_skus)
      GROUP BY mi.id_cliente, mi.sku
    ) bv
    JOIN ventas_odv v ON v.id_cliente = bv.id_cliente AND v.sku = bv.sku AND v.fecha > bv.first_venta
    LEFT JOIN sku_padecimiento sp ON sp.sku = bv.sku
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  )
  SELECT bp.padecimiento, bp.venta, bp.recoleccion, bp.valor,
         COALESCE(cv.converted_qty, 0)::int AS converted_qty
  FROM botiquin_by_pad bp
  LEFT JOIN converted cv ON cv.padecimiento = bp.padecimiento
  ORDER BY bp.valor DESC;
$$;

CREATE OR REPLACE FUNCTION public.get_opportunity_matrix(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, venta integer, recoleccion integer, valor numeric, converted_qty integer)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_opportunity_matrix(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 13. get_yoy_padecimiento — standalone
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_yoy_padecimiento(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, anio integer, valor numeric, crecimiento numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
BEGIN
  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  yearly AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           EXTRACT(YEAR FROM mi.fecha_movimiento)::int AS anio,
           SUM(mi.cantidad * m.precio) AS valor
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mi.id_cliente = ANY(p_medicos))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS'), EXTRACT(YEAR FROM mi.fecha_movimiento)::int
  ),
  with_prev AS (
    SELECT y.padecimiento, y.anio, y.valor,
           LAG(y.valor) OVER (PARTITION BY y.padecimiento ORDER BY y.anio) AS prev_valor
    FROM yearly y
  )
  SELECT wp.padecimiento, wp.anio, wp.valor,
         CASE WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor > 0
              THEN ROUND(((wp.valor - wp.prev_valor) / wp.prev_valor) * 100)
              WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor = 0 AND wp.valor > 0
              THEN 100::numeric
              ELSE NULL
         END AS crecimiento
  FROM with_prev wp
  ORDER BY wp.padecimiento, wp.anio;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_yoy_padecimiento(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(padecimiento varchar, anio integer, valor numeric, crecimiento numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_yoy_padecimiento(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 14. get_ranking_medicos_completo — standalone
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_ranking_medicos_completo(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_cliente VARCHAR, id_cliente VARCHAR, activo BOOLEAN,
  rango VARCHAR, rango_actual VARCHAR, facturacion_actual NUMERIC,
  facturacion NUMERIC, piezas INT, valor NUMERIC, unique_skus INT
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $$
BEGIN
  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  clientes_con_botiquin AS (
    SELECT DISTINCT mi.id_cliente
    FROM movimientos_inventario mi
    UNION
    SELECT DISTINCT st.id_cliente
    FROM saga_transactions st
    WHERE st.tipo = 'LEVANTAMIENTO_INICIAL'
  ),
  ventas AS (
    SELECT
      mi.id_cliente,
      SUM(mi.cantidad)::int AS piezas,
      SUM(mi.cantidad * m.precio) AS valor,
      COUNT(DISTINCT mi.sku)::int AS unique_skus
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
    GROUP BY mi.id_cliente
  )
  SELECT
    c.nombre_cliente,
    c.id_cliente,
    c.activo,
    COALESCE(c.rango, 'N/A')::VARCHAR AS rango,
    c.rango_actual,
    COALESCE(c.facturacion_actual, 0) AS facturacion_actual,
    COALESCE(c.facturacion_promedio, 0) AS facturacion,
    COALESCE(v.piezas, 0) AS piezas,
    COALESCE(v.valor, 0) AS valor,
    COALESCE(v.unique_skus, 0) AS unique_skus
  FROM clientes_con_botiquin cb
  JOIN clientes c ON c.id_cliente = cb.id_cliente
  LEFT JOIN ventas v ON v.id_cliente = cb.id_cliente
  WHERE (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ORDER BY COALESCE(v.valor, 0) DESC;
END;
$$;

CREATE OR REPLACE FUNCTION public.get_ranking_medicos_completo(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_cliente VARCHAR, id_cliente VARCHAR, activo BOOLEAN,
  rango VARCHAR, rango_actual VARCHAR, facturacion_actual NUMERIC,
  facturacion NUMERIC, piezas INT, valor NUMERIC, unique_skus INT
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_ranking_medicos_completo(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 15. get_facturacion_composicion — standalone (public only, complex)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_facturacion_composicion(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, rango_actual varchar,
  rango_anterior varchar, activo boolean,
  baseline numeric, facturacion_actual numeric,
  current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric,
  pct_crecimiento numeric, pct_vinculado numeric,
  valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
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
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
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
      AND v.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
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
    AND (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 16. get_corte_historico_data — FIX: add SKU unique counts to KPIs
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_historico_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_result json;
BEGIN
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'CREACION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.cantidad_disponible * med.precio), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_unicos
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE inv.cantidad_disponible > 0
      AND (p_medicos IS NULL OR inv.id_cliente = ANY(p_medicos))
      AND (p_marcas IS NULL OR med.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'RECOLECCION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.id_cliente,
      MIN(mov.fecha_movimiento::date) AS fecha_visita
    FROM movimientos_inventario mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mov.id_saga_transaction, mov.id_cliente
  ),
  visita_rows AS (
    SELECT
      c.id_cliente,
      c.nombre_cliente,
      vb.fecha_visita::text AS fecha_visita,
      COUNT(DISTINCT mov.sku)::int AS skus_unicos,
      COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor_venta,
      COALESCE(SUM(mov.cantidad), 0)::int AS piezas_venta
    FROM visitas_base vb
    JOIN movimientos_inventario mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON vb.id_cliente = c.id_cliente
    WHERE mov.tipo = 'VENTA'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR vb.fecha_visita >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR vb.fecha_visita <= p_fecha_fin)
    GROUP BY c.id_cliente, c.nombre_cliente, vb.fecha_visita
    ORDER BY vb.fecha_visita ASC, c.nombre_cliente
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'valor_venta_m1', (SELECT valor FROM kpi_venta_m1),
      'skus_venta_m1', (SELECT skus_unicos FROM kpi_venta_m1),
      'valor_creacion', (SELECT valor FROM kpi_creacion),
      'skus_creacion', (SELECT skus_unicos FROM kpi_creacion),
      'stock_activo', (SELECT valor FROM kpi_stock),
      'skus_stock', (SELECT skus_unicos FROM kpi_stock),
      'valor_recoleccion', (SELECT valor FROM kpi_recoleccion),
      'skus_recoleccion', (SELECT skus_unicos FROM kpi_recoleccion)
    ),
    'visitas', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$$;

-- Public wrapper already has filters, just re-create to match
CREATE OR REPLACE FUNCTION public.get_corte_historico_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_corte_historico_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 17. get_corte_logistica_data — FIX: cantidad_colocada = SUM(CREACION), not stock
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text, nombre_cliente varchar, id_cliente varchar,
  fecha_visita text, sku varchar, producto varchar,
  cantidad_colocada integer, qty_venta integer, qty_recoleccion integer,
  total_corte integer, destino text, saga_estado text,
  odv_botiquin text, odv_venta text,
  recoleccion_id uuid, recoleccion_estado text,
  evidencia_paths text[], firma_path text, observaciones text, quien_recibio text
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
#variable_conflict use_column
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  IF p_fecha_inicio IS NOT NULL AND p_fecha_fin IS NOT NULL THEN
    v_fecha_inicio := p_fecha_inicio;
    v_fecha_fin := p_fecha_fin;
  ELSE
    SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
    FROM get_corte_actual_rango() r;
  END IF;

  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT
    u.nombre::text                                                          AS nombre_asesor,
    c.nombre_cliente,
    mov.id_cliente,
    TO_CHAR(mov.fecha_movimiento, 'YYYY-MM-DD')                            AS fecha_visita,
    mov.sku,
    med.producto,
    -- FIX: cantidad_colocada = total CREACION pieces for this (cliente, sku), not current stock
    (SELECT COALESCE(SUM(m2.cantidad), 0)
     FROM movimientos_inventario m2
     WHERE m2.id_cliente = mov.id_cliente
       AND m2.sku = mov.sku
       AND m2.tipo = 'CREACION')::int                                       AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquin
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_zoho_link
       FROM movimientos_inventario m_cre
       WHERE m_cre.id_cliente = mov.id_cliente
         AND m_cre.sku = mov.sku
         AND m_cre.tipo = 'CREACION'
         AND m_cre.id_saga_zoho_link IS NOT NULL
         AND m_cre.fecha_movimiento <=
           CASE WHEN mov.tipo = 'RECOLECCION'
             THEN mov.fecha_movimiento - interval '1 second'
             ELSE mov.fecha_movimiento
           END
       ORDER BY m_cre.fecha_movimiento DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id = latest_cre.id_saga_zoho_link
       AND szl.tipo = 'BOTIQUIN'
       AND szl.zoho_id IS NOT NULL)                                         AS odv_botiquin,
    -- ODV Venta
    (SELECT szl.zoho_id
     FROM saga_zoho_links szl
     WHERE szl.id = mov.id_saga_zoho_link
       AND szl.tipo = 'VENTA')                                             AS odv_venta,
    rcl.recoleccion_id,
    rcl.estado::text                                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM recolecciones_evidencias re
     WHERE re.recoleccion_id = rcl.recoleccion_id)                         AS evidencia_paths,
    (SELECT rf.storage_path
     FROM recolecciones_firmas rf
     WHERE rf.recoleccion_id = rcl.recoleccion_id
     LIMIT 1)                                                              AS firma_path,
    rcl.cedis_observaciones                                                AS observaciones,
    rcl.cedis_responsable_nombre                                           AS quien_recibio
  FROM movimientos_inventario mov
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN saga_transactions st ON mov.id_saga_transaction = st.id
  LEFT JOIN visitas v            ON st.visit_id = v.visit_id
  LEFT JOIN usuarios u           ON v.id_usuario = u.id_usuario
  LEFT JOIN recolecciones rcl    ON v.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo IN ('VENTA', 'RECOLECCION')
    AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY mov.fecha_movimiento DESC, c.nombre_cliente, mov.sku;
END;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text, nombre_cliente varchar, id_cliente varchar,
  fecha_visita text, sku varchar, producto varchar,
  cantidad_colocada integer, qty_venta integer, qty_recoleccion integer,
  total_corte integer, destino text, saga_estado text,
  odv_botiquin text, odv_venta text,
  recoleccion_id uuid, recoleccion_estado text,
  evidencia_paths text[], firma_path text, observaciones text, quien_recibio text
)
LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- Reload PostgREST schema cache
-- ─────────────────────────────────────────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
