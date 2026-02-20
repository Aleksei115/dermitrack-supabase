-- =============================================================================
-- Fix 1: DROP ALL old 0-param overloads to resolve ambiguity (both schemas)
-- Fix 2: Redefine get_historico_conversiones_evolucion (pass filters + count pairs)
-- Fix 3: Amplify get_filtros_disponibles with fecha_primer_levantamiento
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. DROP ALL old overloads that conflict with new 5-param DEFAULT NULL versions
-- ─────────────────────────────────────────────────────────────────────────────

-- analytics schema
DROP FUNCTION IF EXISTS analytics.clasificacion_base();
DROP FUNCTION IF EXISTS analytics.get_brand_performance();
DROP FUNCTION IF EXISTS analytics.get_impacto_botiquin_resumen();
DROP FUNCTION IF EXISTS analytics.get_impacto_detalle(text);
DROP FUNCTION IF EXISTS analytics.get_market_analysis();
DROP FUNCTION IF EXISTS analytics.get_opportunity_matrix();
DROP FUNCTION IF EXISTS analytics.get_padecimiento_performance();
DROP FUNCTION IF EXISTS analytics.get_product_interest(integer);
DROP FUNCTION IF EXISTS analytics.get_ranking_medicos_completo();
DROP FUNCTION IF EXISTS analytics.get_sankey_conversion_flows();
DROP FUNCTION IF EXISTS analytics.get_top_converting_skus(integer);
DROP FUNCTION IF EXISTS analytics.get_yoy_padecimiento();

-- public schema
DROP FUNCTION IF EXISTS public.clasificacion_base();
DROP FUNCTION IF EXISTS public.get_brand_performance();
DROP FUNCTION IF EXISTS public.get_conversion_details();
DROP FUNCTION IF EXISTS public.get_conversion_metrics();
DROP FUNCTION IF EXISTS public.get_facturacion_composicion();
DROP FUNCTION IF EXISTS public.get_impacto_botiquin_resumen();
DROP FUNCTION IF EXISTS public.get_impacto_detalle(text);
DROP FUNCTION IF EXISTS public.get_market_analysis();
DROP FUNCTION IF EXISTS public.get_opportunity_matrix();
DROP FUNCTION IF EXISTS public.get_padecimiento_performance();
DROP FUNCTION IF EXISTS public.get_product_interest(integer);
DROP FUNCTION IF EXISTS public.get_ranking_medicos_completo();
DROP FUNCTION IF EXISTS public.get_sankey_conversion_flows();
DROP FUNCTION IF EXISTS public.get_top_converting_skus(integer);
DROP FUNCTION IF EXISTS public.get_yoy_padecimiento();

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Redefine get_historico_conversiones_evolucion
--    - DROP old 3-param version
--    - Pass filters to clasificacion_base()
--    - Fix skus_unicos_total to count pairs (id_cliente||'-'||sku) not just sku
-- ─────────────────────────────────────────────────────────────────────────────

DROP FUNCTION IF EXISTS public.get_historico_conversiones_evolucion(date, date, text);
DROP FUNCTION IF EXISTS public.get_historico_conversiones_evolucion(date, date, text, varchar[], varchar[], varchar[]);

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
    SELECT b.id_cliente, b.sku, b.first_event_date
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

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Fix get_impacto_botiquin_resumen: filter total_odv by same params
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
-- 4. Amplify get_filtros_disponibles with fecha_primer_levantamiento
--    Must DROP first because return type changed (added column)
-- ─────────────────────────────────────────────────────────────────────────────

DROP FUNCTION IF EXISTS public.get_filtros_disponibles();

CREATE OR REPLACE FUNCTION public.get_filtros_disponibles()
RETURNS TABLE(marcas varchar[], medicos jsonb, padecimientos varchar[], fecha_primer_levantamiento date)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
     FROM medicamentos m WHERE m.marca IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
     FROM clientes c WHERE c.activo = true),
    (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
     FROM padecimientos p)::varchar[],
    (SELECT MIN(fecha_movimiento)::date
     FROM movimientos_inventario WHERE tipo = 'CREACION');
END;
$$;

NOTIFY pgrst, 'reload schema';
