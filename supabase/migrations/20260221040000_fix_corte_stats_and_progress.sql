-- Fix: corteProgress picking wrong visit, stats_generales using date-based filtering,
-- and logistica using old 5-param date-based logic instead of visit-based.
--
-- Problem 1: corteProgress DISTINCT ON picks MEXBR172's EN_CURSO corte 6 over COMPLETADO corte 5
-- Problem 2: get_corte_stats_generales_con_comparacion uses fecha_movimiento BETWEEN, misses most doctors
-- Problem 3: get_corte_logistica_data still has 5-param date-based logic (migration recorded but SQL not applied)
--
-- Solution: Rewrite all three to use visit→saga→movement chain with voided_clients + ranked_visits CTEs.

-- ============================================================
-- 1. Drop old 5-param get_corte_logistica_data (both schemas)
-- ============================================================
DROP FUNCTION IF EXISTS public.get_corte_logistica_data(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS analytics.get_corte_logistica_data(character varying[], character varying[], character varying[], date, date);

-- ============================================================
-- 2. Create new 3-param analytics.get_corte_logistica_data (visit-based)
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text,
  nombre_cliente character varying,
  id_cliente character varying,
  fecha_visita text,
  sku character varying,
  producto character varying,
  cantidad_colocada integer,
  qty_venta integer,
  qty_recoleccion integer,
  total_corte integer,
  destino text,
  saga_estado text,
  odv_botiquin text,
  odv_venta text,
  recoleccion_id uuid,
  recoleccion_estado text,
  evidencia_paths text[],
  firma_path text,
  observaciones text,
  quien_recibio text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
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
  -- Rank completed visits by corte_number DESC
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.id_usuario,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
      AND (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  -- Padecimiento dedup
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  -- SKU filter by marca + padecimiento
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
    TO_CHAR(cv.fecha_visita, 'YYYY-MM-DD')                                 AS fecha_visita,
    mov.sku,
    med.producto,
    -- Cantidad colocada: CREACION of that SKU in the same visit
    (SELECT COALESCE(SUM(m_cre.cantidad), 0)
     FROM movimientos_inventario m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = cv.visit_id
       AND m_cre.id_cliente = mov.id_cliente
       AND m_cre.sku = mov.sku
       AND m_cre.tipo = 'CREACION')::int                                    AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquin: trace per-SKU to most recent CREACION with a BOTIQUIN ODV
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_transaction
       FROM movimientos_inventario m_cre
       WHERE m_cre.id_cliente = mov.id_cliente
         AND m_cre.sku = mov.sku
         AND m_cre.tipo = 'CREACION'
         AND m_cre.fecha_movimiento <= mov.fecha_movimiento
         AND EXISTS (
           SELECT 1 FROM saga_zoho_links szl_chk
           WHERE szl_chk.id_saga_transaction = m_cre.id_saga_transaction
             AND szl_chk.tipo = 'BOTIQUIN'
             AND szl_chk.zoho_id IS NOT NULL
         )
       ORDER BY m_cre.fecha_movimiento DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = latest_cre.id_saga_transaction
       AND szl.tipo = 'BOTIQUIN'
       AND szl.zoho_id IS NOT NULL
       AND EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       ))                                                                   AS odv_botiquin,
    -- ODV Venta: visit-based join, per-SKU filtering via items jsonb
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM saga_transactions st_mov
     JOIN saga_transactions st_ven ON st_ven.visit_id = st_mov.visit_id
       AND st_ven.tipo = 'VENTA'
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = st_ven.id
       AND szl.tipo = 'VENTA'
       AND szl.zoho_id IS NOT NULL
     WHERE st_mov.id = mov.id_saga_transaction
       AND (szl.items IS NULL OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       )))                                                                  AS odv_venta,
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
  FROM current_visits cv
  JOIN saga_transactions st ON st.visit_id = cv.visit_id
  JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN usuarios u   ON cv.id_usuario = u.id_usuario
  LEFT JOIN recolecciones rcl ON cv.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.tipo IN ('VENTA', 'RECOLECCION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY c.nombre_cliente, mov.sku;
END;
$function$;

-- ============================================================
-- 3. Public wrapper for get_corte_logistica_data (3-param)
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor text,
  nombre_cliente character varying,
  id_cliente character varying,
  fecha_visita text,
  sku character varying,
  producto character varying,
  cantidad_colocada integer,
  qty_venta integer,
  qty_recoleccion integer,
  total_corte integer,
  destino text,
  saga_estado text,
  odv_botiquin text,
  odv_venta text,
  recoleccion_id uuid,
  recoleccion_estado text,
  evidencia_paths text[],
  firma_path text,
  observaciones text,
  quien_recibio text
)
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos);
$function$;

-- ============================================================
-- 4. Rewrite get_corte_stats_generales_con_comparacion (visit-based)
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(
  fecha_inicio date,
  fecha_fin date,
  dias_corte integer,
  total_medicos_visitados integer,
  total_movimientos integer,
  piezas_venta integer,
  piezas_creacion integer,
  piezas_recoleccion integer,
  valor_venta numeric,
  valor_creacion numeric,
  valor_recoleccion numeric,
  medicos_con_venta integer,
  medicos_sin_venta integer,
  valor_venta_anterior numeric,
  valor_creacion_anterior numeric,
  valor_recoleccion_anterior numeric,
  promedio_por_medico_anterior numeric,
  porcentaje_cambio_venta numeric,
  porcentaje_cambio_creacion numeric,
  porcentaje_cambio_recoleccion numeric,
  porcentaje_cambio_promedio numeric
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
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
      v.visit_id,
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
  ),
  current_visits AS (SELECT * FROM ranked_visits WHERE rn = 1),
  prev_visits AS (SELECT * FROM ranked_visits WHERE rn = 2),
  date_bounds AS (
    SELECT MIN(cv.fecha_visita) AS fi, MAX(cv.fecha_visita) AS ff
    FROM current_visits cv
  )
  SELECT db.fi, db.ff INTO v_fecha_inicio, v_fecha_fin
  FROM date_bounds db;

  -- Previous corte values via prev_visits → saga → movements
  SELECT
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0)
  INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
  FROM (
    SELECT rv.visit_id FROM (
      SELECT v.visit_id, v.id_cliente,
        ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado = 'COMPLETADO' AND v.completed_at IS NOT NULL
        AND v.id_cliente NOT IN (
          SELECT sub.id_cliente FROM (
            SELECT DISTINCT ON (v2.id_cliente) v2.id_cliente, v2.estado
            FROM visitas v2 JOIN clientes c2 ON c2.id_cliente = v2.id_cliente AND c2.activo = TRUE
            WHERE v2.estado NOT IN ('PROGRAMADO') AND NOT (v2.estado = 'CANCELADO' AND v2.completed_at IS NULL)
            ORDER BY v2.id_cliente, v2.corte_number DESC
          ) sub WHERE sub.estado = 'CANCELADO'
        )
    ) rv WHERE rv.rn = 2
  ) pv
  JOIN saga_transactions st ON st.visit_id = pv.visit_id
  JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
  JOIN medicamentos med ON mov.sku = med.sku;

  SELECT COUNT(DISTINCT pv.id_cliente)
  INTO v_ant_medicos_con_venta
  FROM (
    SELECT rv.visit_id, rv.id_cliente FROM (
      SELECT v.visit_id, v.id_cliente,
        ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado = 'COMPLETADO' AND v.completed_at IS NOT NULL
        AND v.id_cliente NOT IN (
          SELECT sub.id_cliente FROM (
            SELECT DISTINCT ON (v2.id_cliente) v2.id_cliente, v2.estado
            FROM visitas v2 JOIN clientes c2 ON c2.id_cliente = v2.id_cliente AND c2.activo = TRUE
            WHERE v2.estado NOT IN ('PROGRAMADO') AND NOT (v2.estado = 'CANCELADO' AND v2.completed_at IS NULL)
            ORDER BY v2.id_cliente, v2.corte_number DESC
          ) sub WHERE sub.estado = 'CANCELADO'
        )
    ) rv WHERE rv.rn = 2
  ) pv
  JOIN saga_transactions st ON st.visit_id = pv.visit_id
  JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
  WHERE mov.tipo = 'VENTA';

  -- Current corte via current_visits → saga → movements
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
    SELECT v.visit_id, v.id_cliente, v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO' AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
  ),
  current_visits AS (SELECT * FROM ranked_visits WHERE rn = 1),
  medicos_visitados AS (
    SELECT DISTINCT mov.id_cliente
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT mov.id_cliente
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.tipo = 'VENTA'
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int AS total_mov,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int AS pz_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int AS pz_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int AS pz_recoleccion,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END) AS val_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END) AS val_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END) AS val_recoleccion
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    COALESCE(v_fecha_fin - v_fecha_inicio + 1, 0)::int,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    v_ant_val_venta,
    v_ant_val_creacion,
    v_ant_val_recoleccion,
    CASE WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
      THEN v_ant_val_venta / v_ant_medicos_con_venta
      ELSE NULL
    END,
    CASE WHEN v_ant_val_venta IS NOT NULL AND v_ant_val_venta > 0
      THEN ROUND(((COALESCE(s.val_venta, 0) - v_ant_val_venta) / v_ant_val_venta * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_creacion IS NOT NULL AND v_ant_val_creacion > 0
      THEN ROUND(((COALESCE(s.val_creacion, 0) - v_ant_val_creacion) / v_ant_val_creacion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_recoleccion IS NOT NULL AND v_ant_val_recoleccion > 0
      THEN ROUND(((COALESCE(s.val_recoleccion, 0) - v_ant_val_recoleccion) / v_ant_val_recoleccion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE
      WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
           AND (SELECT COUNT(*)::int FROM medicos_con_venta_actual) > 0
           AND v_ant_val_venta > 0 THEN
        ROUND((
          (COALESCE(s.val_venta, 0) / (SELECT COUNT(*)::int FROM medicos_con_venta_actual)) -
          (v_ant_val_venta / v_ant_medicos_con_venta)
        ) / (v_ant_val_venta / v_ant_medicos_con_venta) * 100, 1)
      ELSE NULL
    END
  FROM stats_actual s;
END;
$function$;

-- ============================================================
-- 5. Fix corteProgress in analytics.get_dashboard_static
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

  -- Progress: use ranked_completados instead of DISTINCT ON to avoid picking EN_CURSO over COMPLETADO
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
  ranked_completados AS (
    SELECT v.id_cliente,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
  )
  SELECT json_build_object(
    'completaron', (SELECT COUNT(DISTINCT id_cliente) FROM ranked_completados WHERE rn = 1),
    'pendientes', (SELECT COUNT(*) FROM clientes WHERE activo = TRUE)
      - (SELECT COUNT(DISTINCT id_cliente) FROM ranked_completados WHERE rn = 1)
      - (SELECT COUNT(*) FROM voided_clients),
    'cancelados', (SELECT COUNT(*) FROM voided_clients),
    'total', (SELECT COUNT(*) FROM clientes WHERE activo = TRUE)
  ) INTO v_progress;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats,
    'corteProgress', v_progress
  );
END;
$function$;

-- ============================================================
-- 6. Public wrapper for get_dashboard_static (refresh)
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$;

-- ============================================================
-- 7. GRANTs
-- ============================================================
GRANT EXECUTE ON FUNCTION analytics.get_corte_logistica_data(character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_logistica_data(character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_stats_generales_con_comparacion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_dashboard_static() TO authenticated, anon;

-- ============================================================
-- 8. Reload PostgREST schema cache
-- ============================================================
NOTIFY pgrst, 'reload schema';
