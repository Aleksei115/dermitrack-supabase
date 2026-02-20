-- =============================================================================
-- Migration: Rewrite Corte Mensual RPCs — Visit-based model
--
-- Replaces gap-detection (>3 day gaps in movimientos_inventario) with
-- deterministic visit-based approach:
--   "Corte actual" = última VISITA_CORTE COMPLETADA de cada médico activo
--   "Corte anterior" = penúltima VISITA_CORTE COMPLETADA
--
-- Chain: visitas.visit_id → saga_transactions.visit_id → movimientos_inventario.id_saga_transaction
--
-- Changes:
--   1. get_corte_actual_rango() — visit-based range
--   2. get_corte_anterior_rango() — visit-based (penultimate)
--   3. analytics.get_corte_actual_data() — 3 params, visit-based, pending doctors
--   4. analytics.get_corte_logistica_data() — 3 params, visit-based
--   5. get_corte_stats_generales_con_comparacion() — fix c.activo=TRUE in prev_stats
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. get_corte_actual_rango() — derive range from last completed corte visits
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_corte_actual_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH ranked_visits AS (
    SELECT
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.completed_at DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  )
  SELECT
    MIN(cv.fecha_visita),
    MAX(cv.fecha_visita),
    (MAX(cv.fecha_visita) - MIN(cv.fecha_visita) + 1)::int
  FROM current_visits cv;
END;
$function$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. get_corte_anterior_rango() — penultimate corte visits
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_corte_anterior_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH ranked_visits AS (
    SELECT
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.completed_at DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  )
  SELECT
    MIN(pv.fecha_visita),
    MAX(pv.fecha_visita),
    (MAX(pv.fecha_visita) - MIN(pv.fecha_visita) + 1)::int
  FROM prev_visits pv;
END;
$function$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. analytics.get_corte_actual_data() — visit-based, 3 params (no dates)
--    Includes pending doctors (active clients without current corte visit)
-- ─────────────────────────────────────────────────────────────────────────────

-- Drop old 5-param versions (signature change)
DROP FUNCTION IF EXISTS public.get_corte_actual_data(varchar[], varchar[], varchar[], date, date);
DROP FUNCTION IF EXISTS analytics.get_corte_actual_data(varchar[], varchar[], varchar[], date, date);

CREATE OR REPLACE FUNCTION analytics.get_corte_actual_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_result json;
BEGIN
  WITH
  -- Rank all completed VISITA_CORTE per active doctor
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.completed_at DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
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
    SELECT mov.id_cliente, ac.nombre_cliente, mov.sku, mov.tipo, mov.cantidad, med.precio
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.id_cliente = mov.id_cliente
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Previous corte movements via saga_transactions from prev_visits
  prev_mov AS (
    SELECT mov.id_cliente, mov.sku, mov.tipo, mov.cantidad, med.precio
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
  -- Previous per-médico venta (for VS Anterior column)
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
  -- Pending doctors: active clients without a current corte visit
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
$$;

-- Public wrapper (3 params)
CREATE OR REPLACE FUNCTION public.get_corte_actual_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_corte_actual_data(p_medicos, p_marcas, p_padecimientos);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. analytics.get_corte_logistica_data() — visit-based, 3 params (no dates)
-- ─────────────────────────────────────────────────────────────────────────────

-- Drop old 5-param versions (signature change)
DROP FUNCTION IF EXISTS public.get_corte_logistica_data(varchar[], varchar[], varchar[], date, date);
DROP FUNCTION IF EXISTS analytics.get_corte_logistica_data(varchar[], varchar[], varchar[], date, date);

CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
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
BEGIN
  RETURN QUERY
  WITH
  -- Rank completed VISITA_CORTE per active doctor
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.id_usuario,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.completed_at DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
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
    cv.fecha_visita::text                                                   AS fecha_visita,
    mov.sku,
    med.producto,
    -- cantidad_colocada = total CREACION pieces for this (cliente, sku)
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
    -- ODV Botiquin: latest CREACION zoho link for this (cliente, sku)
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
  FROM current_visits cv
  JOIN saga_transactions st ON st.visit_id = cv.visit_id
  JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med ON mov.sku = med.sku
  LEFT JOIN usuarios u ON u.id_usuario = cv.id_usuario
  LEFT JOIN recolecciones rcl ON cv.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.tipo IN ('VENTA', 'RECOLECCION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY cv.fecha_visita DESC, c.nombre_cliente, mov.sku;
END;
$$;

-- Public wrapper (3 params)
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
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
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos);
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. get_corte_stats_generales_con_comparacion() — fix c.activo=TRUE in prev_stats
--    (rango functions already rewritten above, this fixes the activo filter bug)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(
  fecha_inicio date, fecha_fin date, dias_corte integer,
  total_medicos_visitados integer, total_movimientos integer,
  piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer,
  valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric,
  medicos_con_venta integer, medicos_sin_venta integer,
  valor_venta_anterior numeric, valor_creacion_anterior numeric,
  valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric,
  porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric,
  porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_ant_fecha_inicio date;
  v_ant_fecha_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  -- Get current cut range (now visit-based)
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;

  -- Get previous cut range (now visit-based)
  SELECT r.fecha_inicio, r.fecha_fin INTO v_ant_fecha_inicio, v_ant_fecha_fin
  FROM get_corte_anterior_rango() r;

  -- Previous cut values — FIX: now filters c.activo = TRUE
  IF v_ant_fecha_inicio IS NOT NULL THEN
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin
      AND c.activo = TRUE;

    SELECT COUNT(DISTINCT mov.id_cliente) INTO v_ant_medicos_con_venta
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin
      AND mov.tipo = 'VENTA'
      AND c.activo = TRUE;
  ELSE
    v_ant_val_venta := NULL;
    v_ant_val_creacion := NULL;
    v_ant_val_recoleccion := NULL;
    v_ant_medicos_con_venta := NULL;
  END IF;

  RETURN QUERY
  WITH
  -- Current corte: only active clients
  medicos_visitados AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND c.activo = TRUE
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.tipo = 'VENTA'
      AND c.activo = TRUE
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int as total_mov,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int as pz_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int as pz_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int as pz_recoleccion,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END) as val_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END) as val_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END) as val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND c.activo = TRUE
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    (v_fecha_fin - v_fecha_inicio + 1)::int,
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
    -- Previous corte values (now filtered by activo = TRUE)
    v_ant_val_venta,
    v_ant_val_creacion,
    v_ant_val_recoleccion,
    CASE WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
      THEN v_ant_val_venta / v_ant_medicos_con_venta
      ELSE NULL
    END,
    -- Percentage changes
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

-- ─────────────────────────────────────────────────────────────────────────────
-- Reload PostgREST schema cache
-- ─────────────────────────────────────────────────────────────────────────────

NOTIFY pgrst, 'reload schema';
