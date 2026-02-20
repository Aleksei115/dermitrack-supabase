-- Consolidated dashboard RPCs: get_dashboard_static() and get_dashboard_data()
-- Reduces 19 parallel HTTP requests to 2 (static + analytics) on mount,
-- and 1 per filter change. Shared CTEs eliminate redundant table scans.
-- Existing individual RPCs are NOT dropped — remain available for debugging and mobile app.

--------------------------------------------------------------------------------
-- A) get_dashboard_static(): called ONCE on mount (no filter params)
--    Returns corteFiltros + corteStatsGenerales in a single JSON object
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
RETURNS json
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_filtros json;
  v_stats json;
  v_row record;
BEGIN
  -- 1. Filtros disponibles
  SELECT row_to_json(f) INTO v_filtros
  FROM (
    SELECT
      (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
       FROM medicamentos m WHERE m.marca IS NOT NULL) AS marcas,
      (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
       FROM clientes c WHERE c.activo = true) AS medicos,
      (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
       FROM padecimientos p) AS padecimientos,
      (SELECT MIN(fecha_movimiento)::date
       FROM movimientos_inventario WHERE tipo = 'CREACION') AS "fechaPrimerLevantamiento"
  ) f;

  -- 2. Corte stats generales con comparación
  SELECT row_to_json(s) INTO v_stats
  FROM (
    SELECT
      r.fecha_inicio AS "fechaInicio",
      r.fecha_fin AS "fechaFin",
      r.dias_corte AS "diasCorte",
      r.total_medicos_visitados AS "totalMedicosVisitados",
      r.total_movimientos AS "totalMovimientos",
      r.piezas_venta AS "piezasVenta",
      r.piezas_creacion AS "piezasCreacion",
      r.piezas_recoleccion AS "piezasRecoleccion",
      r.valor_venta AS "valorVenta",
      r.valor_creacion AS "valorCreacion",
      r.valor_recoleccion AS "valorRecoleccion",
      r.medicos_con_venta AS "medicosConVenta",
      r.medicos_sin_venta AS "medicosSinVenta",
      r.valor_venta_anterior AS "valorVentaAnterior",
      r.valor_creacion_anterior AS "valorCreacionAnterior",
      r.valor_recoleccion_anterior AS "valorRecoleccionAnterior",
      r.promedio_por_medico_anterior AS "promedioPorMedicoAnterior",
      r.porcentaje_cambio_venta AS "porcentajeCambioVenta",
      r.porcentaje_cambio_creacion AS "porcentajeCambioCreacion",
      r.porcentaje_cambio_recoleccion AS "porcentajeCambioRecoleccion",
      r.porcentaje_cambio_promedio AS "porcentajeCambioPromedio"
    FROM get_corte_stats_generales_con_comparacion() r
    LIMIT 1
  ) s;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats
  );
END;
$$;

--------------------------------------------------------------------------------
-- B) get_dashboard_data(): called on every filter change
--    Returns 6 sections in a single JSON object with SHARED CTEs
--    Language SQL for optimal query planner (single execution plan)
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION analytics.get_dashboard_data(
  p_medicos     varchar[] DEFAULT NULL,
  p_marcas      varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date     DEFAULT NULL,
  p_fecha_fin    date     DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
WITH
-- ═══════════════════════════════════════════════════════════
-- SHARED FOUNDATION CTEs (computed ONCE, reused by all sections)
-- ═══════════════════════════════════════════════════════════

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

saga_odv_ids AS (
  SELECT DISTINCT szl.zoho_id
  FROM saga_zoho_links szl
  WHERE szl.tipo = 'VENTA'
    AND szl.zoho_id IS NOT NULL
),

-- ═══════════════════════════════════════════════════════════
-- CLASSIFICATION CTEs (M1/M2/M3 — single source of truth)
-- ═══════════════════════════════════════════════════════════

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
),

-- Unified classification (M1 + M2 + M3)
clasificacion AS (
  -- M1 rows
  SELECT
    mp.id_cliente, c.nombre_cliente, mp.sku, m.producto,
    COALESCE(sp.padecimiento, 'OTROS') AS padecimiento,
    m.marca, m.top AS es_top,
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
    m2.id_cliente, c.nombre_cliente, m2.sku, m.producto,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.marca, m.top,
    'M2'::text,
    mp.first_venta,
    mp.revenue_botiquin,
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
    m3.id_cliente, c.nombre_cliente, m3.sku, m.producto,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.marca, m.top,
    'M3'::text,
    m3.first_creacion,
    0::numeric,
    m3.revenue_odv,
    m3.cantidad_odv,
    m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clientes c ON c.id_cliente = m3.id_cliente
  JOIN medicamentos m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 1: clasificacionBase (full rows for StatCardsRow)
-- ═══════════════════════════════════════════════════════════

clasificacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT id_cliente, nombre_cliente, sku, producto, padecimiento,
           marca, es_top, m_type, first_event_date,
           revenue_botiquin, revenue_odv, cantidad_odv, num_transacciones_odv
    FROM clasificacion
  ) r
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 2: impactoResumen (M1/M2/M3 counts + revenue summary)
-- ═══════════════════════════════════════════════════════════

impacto_m1 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev
  FROM clasificacion WHERE m_type = 'M1'
),
impacto_m2 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M2'
),
impacto_m3 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M3'
),
total_odv AS (
  SELECT COALESCE(SUM(v.cantidad * v.precio), 0) AS rev
  FROM ventas_odv v
  WHERE (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
),
impacto_json AS (
  SELECT row_to_json(r) AS val
  FROM (
    SELECT m1.cnt AS adopciones, m1.rev AS revenue_adopciones,
           m2.cnt AS conversiones, m2.rev AS revenue_conversiones,
           m3.cnt AS exposiciones, m3.rev AS revenue_exposiciones,
           0::int AS crosssell_pares, 0::numeric AS revenue_crosssell,
           (m1.rev + m2.rev + m3.rev) AS revenue_total_impacto,
           t.rev AS revenue_total_odv,
           CASE WHEN t.rev > 0
             THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1)
             ELSE 0 END AS porcentaje_impacto
    FROM impacto_m1 m1, impacto_m2 m2, impacto_m3 m3, total_odv t
  ) r
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 3: marketAnalysis (movements + M2 conversions per client-SKU)
-- ═══════════════════════════════════════════════════════════

movements AS (
  SELECT
    mi.id_cliente, mi.sku,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::bigint           AS venta_pz,
    COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0)       AS venta_valor,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)::bigint         AS creacion_pz,
    COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'CREACION'), 0)    AS creacion_valor,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::bigint      AS recoleccion_pz,
    COALESCE(SUM(mi.cantidad * med.precio) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0) AS recoleccion_valor
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
  FROM clasificacion cb
  WHERE cb.m_type = 'M2'
  GROUP BY cb.id_cliente, cb.sku
),
market_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      mv.id_cliente, mv.sku, med.producto, med.marca,
      COALESCE(sp.padecimiento, 'OTROS') AS padecimiento, med.top AS es_top,
      mv.venta_pz, mv.venta_valor,
      mv.creacion_pz, mv.creacion_valor,
      mv.recoleccion_pz, mv.recoleccion_valor,
      COALESCE(ib.cantidad_disponible, 0)::bigint AS stock_activo_pz,
      COALESCE(m2c.conversiones_m2, 0)::bigint AS conversiones_m2,
      COALESCE(m2c.revenue_m2, 0)::numeric AS revenue_m2
    FROM movements mv
    JOIN medicamentos med ON med.sku = mv.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
    LEFT JOIN m2_counts m2c ON m2c.id_cliente = mv.id_cliente AND m2c.sku = mv.sku
    LEFT JOIN inventario_botiquin ib ON ib.id_cliente = mv.id_cliente AND ib.sku = mv.sku
  ) r
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 4: conversionDetails (M2+M3 with first ODV date + days)
-- ═══════════════════════════════════════════════════════════

conversion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r."valorGenerado" DESC), '[]'::json) AS val
  FROM (
    SELECT
      b.m_type AS "mType",
      b.id_cliente AS "idCliente",
      b.nombre_cliente AS "nombreCliente",
      b.sku,
      b.producto,
      b.first_event_date AS "fechaBotiquin",
      odv_first.first_odv AS "fechaPrimeraOdv",
      (odv_first.first_odv - b.first_event_date)::int AS "diasConversion",
      b.num_transacciones_odv AS "numVentasOdv",
      b.cantidad_odv::bigint AS "totalPiezas",
      b.revenue_odv AS "valorGenerado",
      b.revenue_botiquin AS "valorBotiquin"
    FROM clasificacion b
    JOIN LATERAL (
      SELECT MIN(v.fecha) AS first_odv
      FROM ventas_odv v
      WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
        AND v.fecha >= b.first_event_date
        AND v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    ) odv_first ON true
    WHERE b.m_type IN ('M2', 'M3')
  ) r
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 5: facturacionComposicion (per-client billing breakdown)
-- ═══════════════════════════════════════════════════════════

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
      WHEN m1o.odv_id IS NOT NULL THEN 'M1'
      WHEN fv.sku IS NOT NULL AND v.fecha > fv.first_venta THEN 'M2'
      WHEN fc.sku IS NOT NULL AND v.fecha > fc.first_creacion AND po.sku IS NULL THEN 'M3'
      ELSE 'UNLINKED'
    END AS categoria
  FROM ventas_odv v
  LEFT JOIN m1_odv_ids m1o ON v.odv_id = m1o.odv_id AND v.id_cliente = m1o.id_cliente
  LEFT JOIN first_venta fv ON v.id_cliente = fv.id_cliente AND v.sku = fv.sku
  LEFT JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku
  LEFT JOIN prior_odv po ON v.id_cliente = po.id_cliente AND v.sku = po.sku
  WHERE v.precio > 0
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
),
fact_totals AS (
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
),
facturacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.diff DESC), '[]'::json) AS val
  FROM (
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
      (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados,
      -- sort helper
      (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) AS diff
    FROM clientes c
    LEFT JOIN fact_totals t ON c.id_cliente = t.id_cliente
    LEFT JOIN m1_impacto m1i ON c.id_cliente = m1i.id_cliente
    WHERE c.rango_actual IS NOT NULL
      AND (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ) r
),

-- ═══════════════════════════════════════════════════════════
-- SECTION 6: sankeyFlows (M2+M3 flow data for Sankey diagram)
-- ═══════════════════════════════════════════════════════════

sankey_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      b.id_cliente, b.nombre_cliente, b.sku, b.producto,
      b.m_type AS categoria,
      b.revenue_odv AS valor_odv,
      b.cantidad_odv,
      b.num_transacciones_odv AS num_transacciones
    FROM clasificacion b
    WHERE b.m_type IN ('M2', 'M3')
  ) r
)

-- ═══════════════════════════════════════════════════════════
-- FINAL: assemble all sections into one JSON object
-- ═══════════════════════════════════════════════════════════

SELECT json_build_object(
  'clasificacionBase', c.val,
  'impactoResumen', i.val,
  'marketAnalysis', m.val,
  'conversionDetails', cv.val,
  'facturacionComposicion', f.val,
  'sankeyFlows', s.val
)
FROM clasificacion_json c, impacto_json i, market_json m,
     conversion_json cv, facturacion_json f, sankey_json s;
$function$;

--------------------------------------------------------------------------------
-- C) Debug helper (PL/pgSQL for row counts inspection)
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION analytics.debug_dashboard_data(
  p_medicos     varchar[] DEFAULT NULL,
  p_marcas      varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date     DEFAULT NULL,
  p_fecha_fin    date     DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  result json;
  counts json;
BEGIN
  result := analytics.get_dashboard_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
  counts := json_build_object(
    'clasificacion_count', json_array_length(result->'clasificacionBase'),
    'market_count', json_array_length(result->'marketAnalysis'),
    'conversion_count', json_array_length(result->'conversionDetails'),
    'facturacion_count', json_array_length(result->'facturacionComposicion'),
    'sankey_count', json_array_length(result->'sankeyFlows')
  );
  RETURN json_build_object('_debug', counts, '_data', result);
END;
$$;

--------------------------------------------------------------------------------
-- D) Public wrappers (SECURITY DEFINER for PostgREST access)
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.get_dashboard_static()
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public', 'analytics'
AS $$ SELECT analytics.get_dashboard_static(); $$;

CREATE OR REPLACE FUNCTION public.get_dashboard_data(
  p_medicos     varchar[] DEFAULT NULL,
  p_marcas      varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date     DEFAULT NULL,
  p_fecha_fin    date     DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public', 'analytics'
AS $$ SELECT analytics.get_dashboard_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin); $$;

CREATE OR REPLACE FUNCTION public.debug_dashboard_data(
  p_medicos     varchar[] DEFAULT NULL,
  p_marcas      varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date     DEFAULT NULL,
  p_fecha_fin    date     DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public', 'analytics'
AS $$ SELECT analytics.debug_dashboard_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin); $$;

-- Grant execute on new functions
GRANT EXECUTE ON FUNCTION analytics.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_dashboard_data(varchar[], varchar[], varchar[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.debug_dashboard_data(varchar[], varchar[], varchar[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_dashboard_data(varchar[], varchar[], varchar[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.debug_dashboard_data(varchar[], varchar[], varchar[], date, date) TO authenticated, anon;

-- Notify PostgREST to pick up new functions
NOTIFY pgrst, 'reload schema';
