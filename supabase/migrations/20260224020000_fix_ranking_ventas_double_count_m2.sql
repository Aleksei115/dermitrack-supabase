-- 20260224020000_fix_ranking_ventas_double_count_m2.sql
-- Fix: ventas_botiquin was counting M2 revenue_botiquin + M2 revenue_odv (double count)
--
-- Root cause: chatbot RPCs queried clasificacion_base() directly and reimplemented
-- M1/M2/M3 aggregation logic, leading to M2 botiquín revenue being added to the total.
--
-- Fix: Rewrite both RPCs as thin aggregation wrappers over analytics.get_impacto_detalle(),
-- which is the same function used by the analytics dashboard. This guarantees the numbers
-- shown in the chatbot always match the dashboard (single source of truth).

-- ============================================================================
-- 1. chatbot.get_ranking_ventas_completo — wrapper over get_impacto_detalle
-- ============================================================================
CREATE OR REPLACE FUNCTION chatbot.get_ranking_ventas_completo(
  p_limite INTEGER DEFAULT 20
)
RETURNS TABLE(
  sku VARCHAR,
  descripcion TEXT,
  marca VARCHAR,
  piezas_botiquin INTEGER,
  piezas_conversion INTEGER,
  piezas_exposicion INTEGER,
  piezas_totales INTEGER,
  ventas_botiquin NUMERIC,
  ventas_conversion NUMERIC,
  ventas_exposicion NUMERIC,
  ventas_totales NUMERIC
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  WITH impacts AS (
    SELECT d.sku, d.cantidad, d.valor, 'M1'::text AS tipo
    FROM analytics.get_impacto_detalle('M1') d
    UNION ALL
    SELECT d.sku, d.cantidad, d.valor, 'M2'::text
    FROM analytics.get_impacto_detalle('M2') d
    UNION ALL
    SELECT d.sku, d.cantidad, d.valor, 'M3'::text
    FROM analytics.get_impacto_detalle('M3') d
  )
  SELECT
    i.sku::VARCHAR,
    m.descripcion::TEXT,
    m.marca::VARCHAR,
    SUM(CASE WHEN i.tipo = 'M1' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_botiquin,
    SUM(CASE WHEN i.tipo = 'M2' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_conversion,
    SUM(CASE WHEN i.tipo = 'M3' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_exposicion,
    SUM(i.cantidad)::INTEGER AS piezas_totales,
    ROUND(SUM(CASE WHEN i.tipo = 'M1' THEN i.valor ELSE 0 END), 2) AS ventas_botiquin,
    ROUND(SUM(CASE WHEN i.tipo = 'M2' THEN i.valor ELSE 0 END), 2) AS ventas_conversion,
    ROUND(SUM(CASE WHEN i.tipo = 'M3' THEN i.valor ELSE 0 END), 2) AS ventas_exposicion,
    ROUND(SUM(i.valor), 2) AS ventas_totales
  FROM impacts i
  JOIN medicamentos m ON m.sku = i.sku
  GROUP BY i.sku, m.descripcion, m.marca
  ORDER BY SUM(i.valor) DESC
  LIMIT p_limite;
END;
$$;

-- ============================================================================
-- 2. chatbot.get_rendimiento_marcas_completo — wrapper over get_impacto_detalle
-- ============================================================================
CREATE OR REPLACE FUNCTION chatbot.get_rendimiento_marcas_completo()
RETURNS TABLE(
  marca VARCHAR,
  piezas_botiquin INTEGER,
  piezas_conversion INTEGER,
  piezas_exposicion INTEGER,
  piezas_totales INTEGER,
  ventas_botiquin NUMERIC,
  ventas_conversion NUMERIC,
  ventas_exposicion NUMERIC,
  ventas_totales NUMERIC
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  WITH impacts AS (
    SELECT d.sku, d.cantidad, d.valor, 'M1'::text AS tipo
    FROM analytics.get_impacto_detalle('M1') d
    UNION ALL
    SELECT d.sku, d.cantidad, d.valor, 'M2'::text
    FROM analytics.get_impacto_detalle('M2') d
    UNION ALL
    SELECT d.sku, d.cantidad, d.valor, 'M3'::text
    FROM analytics.get_impacto_detalle('M3') d
  )
  SELECT
    m.marca::VARCHAR,
    SUM(CASE WHEN i.tipo = 'M1' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_botiquin,
    SUM(CASE WHEN i.tipo = 'M2' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_conversion,
    SUM(CASE WHEN i.tipo = 'M3' THEN i.cantidad ELSE 0 END)::INTEGER AS piezas_exposicion,
    SUM(i.cantidad)::INTEGER AS piezas_totales,
    ROUND(SUM(CASE WHEN i.tipo = 'M1' THEN i.valor ELSE 0 END), 2) AS ventas_botiquin,
    ROUND(SUM(CASE WHEN i.tipo = 'M2' THEN i.valor ELSE 0 END), 2) AS ventas_conversion,
    ROUND(SUM(CASE WHEN i.tipo = 'M3' THEN i.valor ELSE 0 END), 2) AS ventas_exposicion,
    ROUND(SUM(i.valor), 2) AS ventas_totales
  FROM impacts i
  JOIN medicamentos m ON m.sku = i.sku
  GROUP BY m.marca
  ORDER BY SUM(i.valor) DESC;
END;
$$;

-- Notify PostgREST to reload schema cache
NOTIFY pgrst, 'reload schema';
