-- =============================================================================
-- Fix 1: get_impacto_botiquin_resumen — revert M2 double-counting
--        M2 rows are a SUBSET of M1, so M1.rev already includes their botiquín
--        revenue. Adding rev_botiquin again inflates revenue_total_impacto.
-- Fix 2: get_conversion_details — add valor_botiquin column (M2 botiquín revenue)
-- Fix 3: get_conversion_metrics — add valor_botiquin column (SUM of M2 botiquín)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. get_impacto_botiquin_resumen: Revert M2 CTE to SUM(revenue_odv) only
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
  m2 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M2'),
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
    m2.cnt, m2.rev,
    m3.cnt, m3.rev,
    0::int, 0::numeric,
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
-- 2. get_conversion_metrics: Add valor_botiquin (requires DROP + CREATE)
-- ─────────────────────────────────────────────────────────────────────────────

DROP FUNCTION IF EXISTS public.get_conversion_metrics(varchar[], varchar[], varchar[], date, date);

CREATE FUNCTION public.get_conversion_metrics(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric, valor_botiquin numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin))
  SELECT (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
         (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
         COALESCE((SELECT SUM(revenue_odv) FROM base WHERE m_type = 'M2'), 0)::numeric,
         COALESCE((SELECT SUM(revenue_botiquin) FROM base WHERE m_type = 'M2'), 0)::numeric;
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. get_conversion_details: Add valor_botiquin (requires DROP + CREATE)
-- ─────────────────────────────────────────────────────────────────────────────

DROP FUNCTION IF EXISTS public.get_conversion_details(varchar[], varchar[], varchar[], date, date);

CREATE FUNCTION public.get_conversion_details(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(
  m_type text, id_cliente varchar, nombre_cliente varchar, sku varchar,
  producto varchar, fecha_botiquin date, fecha_primera_odv date,
  dias_conversion integer, num_ventas_odv bigint, total_piezas bigint,
  valor_generado numeric, valor_botiquin numeric
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
    b.revenue_odv AS valor_generado,
    b.revenue_botiquin AS valor_botiquin
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

NOTIFY pgrst, 'reload schema';
