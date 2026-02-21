-- Fix: Exclude voided (completed-then-cancelled) visits from analytics
--
-- Problem: When a visit is cancelled after completion (completed_at IS NOT NULL),
-- analytics functions fall back to the previous corte's data, polluting "current" aggregations.
--
-- Solution: A reusable "voided_clients" CTE identifies clients whose latest meaningful visit
-- is CANCELADO with completed_at (voided). These clients are excluded from current data.
-- Never-started cancellations (completed_at IS NULL) are simply skipped in ranking.

-- ============================================================
-- 1. analytics.get_corte_actual_data — add voided_clients CTE
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_corte_actual_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  -- Identify clients whose current cycle was voided (completed then cancelled)
  voided_clients AS (
    SELECT sub.id_cliente
    FROM (
      SELECT DISTINCT ON (v.id_cliente) v.id_cliente, v.estado
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado NOT IN ('PROGRAMADO')
        AND NOT (v.estado = 'CANCELADO' AND v.completed_at IS NULL)
      ORDER BY v.id_cliente, v.corte_number DESC
    ) sub
    WHERE sub.estado = 'CANCELADO'
  ),
  -- Rank all completed visits per active doctor using corte_number
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      v.corte_number,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
      AND (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  ),
  -- All active clients for pending detection
  all_active_clients AS (
    SELECT c.id_cliente, c.nombre_cliente
    FROM clientes c
    WHERE c.activo = TRUE
      AND (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ),
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
  -- Current corte movements via saga_transactions from current_visits
  current_mov AS (
    SELECT mov.id_cliente, ac.nombre_cliente, mov.sku, mov.tipo, mov.cantidad, COALESCE(mov.precio_unitario, 0) AS precio
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.id_cliente = mov.id_cliente
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Previous corte movements via saga_transactions from prev_visits
  prev_mov AS (
    SELECT mov.id_cliente, mov.sku, mov.tipo, mov.cantidad, COALESCE(mov.precio_unitario, 0) AS precio
    FROM prev_visits pv
    JOIN saga_transactions st ON st.visit_id = pv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI aggregation (current corte)
  kpi_stats AS (
    SELECT
      COALESCE(COUNT(DISTINCT mov.id_cliente), 0)::int AS total_medicos_visitados,
      COALESCE(COUNT(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.id_cliente END), 0)::int AS medicos_con_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM current_mov mov
  ),
  -- Previous corte totals for % change (consistent filters)
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM prev_mov mov
  ),
  -- Previous per-medico venta (for VS Anterior column)
  prev_medico_stats AS (
    SELECT mov.id_cliente,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta
    FROM prev_mov mov
    GROUP BY mov.id_cliente
  ),
  -- Visited doctors with movement data
  visited_medico_rows AS (
    SELECT
      mov.id_cliente,
      mov.nombre_cliente,
      cv.fecha_visita::text AS fecha_visita,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion,
      STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.sku END, ', ') AS skus_vendidos,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END), 0) > 0 AS tiene_venta,
      pms.valor_venta AS valor_venta_anterior,
      CASE
        WHEN pms.valor_venta IS NOT NULL AND pms.valor_venta > 0
          THEN ROUND(((COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0) - pms.valor_venta) / pms.valor_venta * 100)::numeric, 1)
        WHEN (pms.valor_venta IS NULL OR pms.valor_venta = 0)
          AND COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END), 0) > 0
          THEN 100.0
        ELSE NULL
      END AS porcentaje_cambio
    FROM current_mov mov
    JOIN current_visits cv ON cv.id_cliente = mov.id_cliente
    LEFT JOIN prev_medico_stats pms ON mov.id_cliente = pms.id_cliente
    GROUP BY mov.id_cliente, mov.nombre_cliente, cv.fecha_visita, pms.valor_venta
  ),
  -- Pending doctors: active clients without a current completed visit
  pending_medico_rows AS (
    SELECT
      ac.id_cliente,
      ac.nombre_cliente,
      NULL::text AS fecha_visita,
      0::int AS piezas_venta,
      0::int AS piezas_creacion,
      0::int AS piezas_recoleccion,
      0::numeric AS valor_venta,
      0::numeric AS valor_creacion,
      0::numeric AS valor_recoleccion,
      NULL::text AS skus_vendidos,
      false AS tiene_venta,
      NULL::numeric AS valor_venta_anterior,
      NULL::numeric AS porcentaje_cambio
    FROM all_active_clients ac
    WHERE NOT EXISTS (SELECT 1 FROM current_visits cv WHERE cv.id_cliente = ac.id_cliente)
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'fecha_inicio', (SELECT MIN(cv.fecha_visita) FROM current_visits cv),
      'fecha_fin', (SELECT MAX(cv.fecha_visita) FROM current_visits cv),
      'dias_corte', COALESCE((SELECT MAX(cv.fecha_visita) - MIN(cv.fecha_visita) + 1 FROM current_visits cv), 0),
      'total_medicos_visitados', k.total_medicos_visitados,
      'medicos_con_venta', k.medicos_con_venta,
      'medicos_sin_venta', k.total_medicos_visitados - k.medicos_con_venta,
      'piezas_venta', k.piezas_venta,
      'valor_venta', k.valor_venta,
      'piezas_creacion', k.piezas_creacion,
      'valor_creacion', k.valor_creacion,
      'piezas_recoleccion', k.piezas_recoleccion,
      'valor_recoleccion', k.valor_recoleccion,
      'porcentaje_cambio_venta',
        CASE WHEN p.valor_venta > 0
          THEN ROUND(((k.valor_venta - p.valor_venta) / p.valor_venta * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_creacion',
        CASE WHEN p.valor_creacion > 0
          THEN ROUND(((k.valor_creacion - p.valor_creacion) / p.valor_creacion * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_recoleccion',
        CASE WHEN p.valor_recoleccion > 0
          THEN ROUND(((k.valor_recoleccion - p.valor_recoleccion) / p.valor_recoleccion * 100)::numeric, 1)
          ELSE NULL END
    ),
    'medicos', COALESCE(
      (SELECT json_agg(row_to_json(sub) ORDER BY sub.fecha_visita IS NULL ASC, sub.valor_venta DESC, sub.nombre_cliente)
       FROM (
         SELECT * FROM visited_medico_rows
         UNION ALL
         SELECT * FROM pending_medico_rows
       ) sub),
      '[]'::json)
  ) INTO v_result
  FROM kpi_stats k
  CROSS JOIN prev_stats p;

  RETURN v_result;
END;
$function$;

-- ============================================================
-- 2. public.get_corte_actual_data — wrapper
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_corte_actual_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT analytics.get_corte_actual_data(p_medicos, p_marcas, p_padecimientos);
$function$;

-- ============================================================
-- 3. analytics.get_dashboard_static — fix corteProgress query
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filtros json;
  v_stats json;
  v_progress json;
BEGIN
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

  -- Progress: voided visits (CANCELADO + completed_at) stay in ranking → not counted as completaron.
  -- Never-started cancellations (CANCELADO + NULL completed_at) are skipped → fall back to previous COMPLETADO.
  SELECT json_build_object(
    'completaron', COUNT(*) FILTER (WHERE lv.estado = 'COMPLETADO'),
    'pendientes', COUNT(*) FILTER (WHERE lv.estado IN ('EN_CURSO','PENDIENTE')),
    'total', (SELECT COUNT(*) FROM clientes WHERE activo = TRUE)
  ) INTO v_progress
  FROM (
    SELECT DISTINCT ON (v.id_cliente) v.estado
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado NOT IN ('PROGRAMADO')
      AND NOT (v.estado = 'CANCELADO' AND v.completed_at IS NULL)
    ORDER BY v.id_cliente, v.corte_number DESC
  ) lv;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats,
    'corteProgress', v_progress
  );
END;
$function$;

-- ============================================================
-- 4. public.get_dashboard_static — wrapper
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$;

-- ============================================================
-- 5. public.get_corte_actual_rango — rewrite with corte_number
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_corte_actual_rango()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.id_cliente
    FROM (
      SELECT DISTINCT ON (v.id_cliente) v.id_cliente, v.estado
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado NOT IN ('PROGRAMADO')
        AND NOT (v.estado = 'CANCELADO' AND v.completed_at IS NULL)
      ORDER BY v.id_cliente, v.corte_number DESC
    ) sub
    WHERE sub.estado = 'CANCELADO'
  ),
  ranked_visits AS (
    SELECT
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  )
  SELECT
    MIN(cv.fecha_visita),
    MAX(cv.fecha_visita),
    COALESCE(MAX(cv.fecha_visita) - MIN(cv.fecha_visita) + 1, 0)::int
  FROM current_visits cv;
END;
$function$;

-- ============================================================
-- 6. public.get_corte_anterior_rango — rewrite with corte_number
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_corte_anterior_rango()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.id_cliente
    FROM (
      SELECT DISTINCT ON (v.id_cliente) v.id_cliente, v.estado
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado NOT IN ('PROGRAMADO')
        AND NOT (v.estado = 'CANCELADO' AND v.completed_at IS NULL)
      ORDER BY v.id_cliente, v.corte_number DESC
    ) sub
    WHERE sub.estado = 'CANCELADO'
  ),
  ranked_visits AS (
    SELECT
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  )
  SELECT
    MIN(pv.fecha_visita),
    MAX(pv.fecha_visita),
    COALESCE(MAX(pv.fecha_visita) - MIN(pv.fecha_visita) + 1, 0)::int
  FROM prev_visits pv;
END;
$function$;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
