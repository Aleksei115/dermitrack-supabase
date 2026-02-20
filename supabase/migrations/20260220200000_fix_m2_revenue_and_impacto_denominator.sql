-- =============================================================================
-- Fix 1: clasificacion_base — M2 rows now emit mp.revenue_botiquin (was 0)
-- Fix 2: get_impacto_botiquin_resumen — M2 sums revenue_botiquin + revenue_odv
-- Fix 3: get_historico_conversiones_evolucion — DISTINCT in botiquin_linked to prevent
--         M1+M2 duplication inflating valor_total (was $78k higher than impacto denominator)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. clasificacion_base: Fix M2 revenue_botiquin (was hardcoded 0::numeric)
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

  -- M2 rows (now includes revenue_botiquin from m1_pairs)
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
    mp.revenue_botiquin,  -- FIX: was 0::numeric, now carries botiquín revenue
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
-- 2. get_impacto_botiquin_resumen: M2 revenue_conversiones stays ODV-only,
--    but revenue_total_impacto includes M2 botiquín revenue separately
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
  base AS (SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin)),
  m1 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev FROM base WHERE m_type = 'M1'),
  m2 AS (SELECT COUNT(*)::int AS cnt,
         COALESCE(SUM(revenue_odv), 0) AS rev_odv,
         COALESCE(SUM(revenue_botiquin), 0) AS rev_botiquin
         FROM base WHERE m_type = 'M2'),
  m3 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M3'),
  total_odv AS (
    SELECT COALESCE(SUM(v.cantidad * v.precio), 0) AS rev
    FROM ventas_odv v
    WHERE (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
      AND v.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
  )
  SELECT m1.cnt, m1.rev,
    m2.cnt, m2.rev_odv,
    m3.cnt, m3.rev,
    0::int, 0::numeric,
    (m1.rev + m2.rev_odv + m2.rev_botiquin + m3.rev), t.rev,
    CASE WHEN t.rev > 0 THEN ROUND(((m1.rev + m2.rev_odv + m2.rev_botiquin + m3.rev) / t.rev) * 100, 1) ELSE 0 END
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
-- 3. Re-deploy get_historico_conversiones_evolucion (sync DEV with migration)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_agrupacion text DEFAULT 'day',
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS TABLE(
  fecha_grupo date, fecha_label text, pares_total int, pares_botiquin int,
  pares_directo int, valor_total numeric, valor_botiquin numeric, valor_directo numeric,
  num_transacciones int, num_clientes int
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $function$
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
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  botiquin_linked AS (
    SELECT DISTINCT b.id_cliente, b.sku, b.first_event_date
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
    WHERE b.m_type IN ('M1', 'M2', 'M3')
      AND (p_medicos IS NULL OR b.id_cliente = ANY(p_medicos))
      AND b.sku IN (SELECT fs.sku FROM filtered_skus fs)
  ),
  ventas_clasificadas AS (
    SELECT
      v.id_cliente, v.sku, v.fecha, v.cantidad, v.precio,
      (v.cantidad * COALESCE(v.precio, 0)) as valor_venta,
      CASE
        WHEN bl.id_cliente IS NOT NULL
             AND v.fecha >= bl.first_event_date THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', v.fecha)::DATE
        ELSE v.fecha::DATE
      END as fecha_agrupada
    FROM ventas_odv v
    LEFT JOIN botiquin_linked bl ON v.id_cliente = bl.id_cliente AND v.sku = bl.sku
    WHERE (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
      AND (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
      AND v.sku IN (SELECT fs.sku FROM filtered_skus fs)
  )
  SELECT
    vc.fecha_agrupada as fecha_grupo,
    CASE
      WHEN p_agrupacion = 'week' THEN 'Sem ' || to_char(vc.fecha_agrupada, 'DD/MM')
      ELSE to_char(vc.fecha_agrupada, 'DD Mon')
    END as fecha_label,
    COUNT(DISTINCT vc.id_cliente || '-' || vc.sku)::INT as pares_total,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as pares_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as pares_directo,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC as valor_total,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_botiquin,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_directo,
    COUNT(*)::INT as num_transacciones,
    COUNT(DISTINCT vc.id_cliente)::INT as num_clientes
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$function$;

NOTIFY pgrst, 'reload schema';
