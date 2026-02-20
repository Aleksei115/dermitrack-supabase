-- Fix get_historico_conversiones_evolucion:
-- 1. Was only using M1 pairs (no ODV by definition) → now includes M1+M2+M3
-- 2. Used strict > on fecha, missing same-day sales → now uses >=
-- 3. Excluded saga VENTA ODVs, but they ARE botiquín sales → removed saga exclusion
--    (saga exclusion belongs in clasificacion_base for M-type classification,
--     not here where we just classify ODV channel as botiquín vs directa)

CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_agrupacion text DEFAULT 'day',
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS TABLE(
  fecha_grupo date, fecha_label text, skus_unicos_total int, skus_unicos_botiquin int,
  skus_unicos_directo int, valor_total numeric, valor_botiquin numeric, valor_directo numeric,
  num_transacciones int, num_clientes int
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $function$
BEGIN
  RETURN QUERY
  WITH
  -- Padecimiento dedup (1:1 per sku)
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  -- SKUs passing marca + padecimiento filters
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  -- All botiquín-linked (cliente, sku) pairs: M1 + M2 + M3
  -- M1: botiquín VENTA, no organic ODV yet (saga ODVs still count here)
  -- M2: botiquín VENTA → organic ODV conversion
  -- M3: CREACION exposure → ODV
  botiquin_linked AS (
    SELECT b.id_cliente, b.sku, b.first_event_date
    FROM analytics.clasificacion_base() b
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
    COUNT(DISTINCT vc.sku)::INT as skus_unicos_total,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as skus_unicos_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as skus_unicos_directo,
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
