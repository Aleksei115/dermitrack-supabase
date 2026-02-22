-- ============================================================================
-- Migration: Corte range per-client algorithm
--
-- Replaces the ±10 fixed-window algorithm with a per-client approach:
--   1. For each active client, find their most recent completed visit
--   2. Start = MIN(created_at) of those visits
--   3. End = MAX(completed_at) of those visits
--
-- This correctly handles:
--   - Tight cortes (Feb 3-7) → Jan 29 to Feb 7
--   - Wide cortes with stragglers (Nov 28 + Dec 15 new doctor) → Nov 27 to Dec 15
--   - No arbitrary gap thresholds or fixed windows
--
-- Functions modified:
--   1. analytics.get_corte_actual_rango()
--   2. analytics.get_corte_anterior_stats()
--   3. analytics.get_corte_stats_generales_con_comparacion()
-- ============================================================================


-- ─── 1. get_corte_actual_rango() ─────────────────────────────────────────────
-- Per-client latest completed visit → MIN(created_at) to MAX(completed_at)

CREATE OR REPLACE FUNCTION analytics.get_corte_actual_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  WITH latest_per_client AS (
    SELECT DISTINCT ON (v.id_cliente)
      v.created_at::date AS fecha_creacion,
      v.completed_at::date AS fecha_completado
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
    ORDER BY v.id_cliente, v.completed_at DESC
  )
  SELECT
    MIN(lpc.fecha_creacion)  AS fecha_inicio,
    MAX(lpc.fecha_completado) AS fecha_fin,
    COALESCE(MAX(lpc.fecha_completado) - MIN(lpc.fecha_creacion) + 1, 0)::int AS dias_corte
  FROM latest_per_client lpc;
END;
$fn$;


-- ─── 2. get_corte_anterior_stats() ──────────────────────────────────────────
-- Per-client latest completed visit BEFORE current corte start
-- → MIN(created_at) to MAX(completed_at) of those visits

CREATE OR REPLACE FUNCTION analytics.get_corte_anterior_stats()
RETURNS TABLE(fecha_inicio date, fecha_fin date, id_cliente character varying, nombre_cliente character varying, valor_venta numeric, piezas_venta integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_corte_actual_inicio date;
  v_prev_inicio date;
  v_prev_fin date;
BEGIN
  -- Get current corte start
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM analytics.get_corte_actual_rango() r;

  -- Per-client latest visit before current corte start
  SELECT MIN(sq.fecha_creacion), MAX(sq.fecha_completado)
  INTO v_prev_inicio, v_prev_fin
  FROM (
    SELECT DISTINCT ON (v.id_cliente)
      v.created_at::date AS fecha_creacion,
      v.completed_at::date AS fecha_completado
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.completed_at::date < v_corte_actual_inicio
    ORDER BY v.id_cliente, v.completed_at DESC
  ) sq;

  IF v_prev_inicio IS NULL THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    v_prev_inicio,
    v_prev_fin,
    c.id_cliente,
    c.nombre_cliente,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin
  GROUP BY c.id_cliente, c.nombre_cliente;
END;
$fn$;


-- ─── 3. get_corte_stats_generales_con_comparacion() ─────────────────────────
-- Current: uses get_corte_actual_rango() (per-client)
-- Previous: per-client latest visit before current start → MIN/MAX

CREATE OR REPLACE FUNCTION analytics.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_dias_corte int;
  v_prev_inicio date;
  v_prev_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  -- ── Current corte window (per-client algorithm) ──
  SELECT r.fecha_inicio, r.fecha_fin, r.dias_corte
  INTO v_fecha_inicio, v_fecha_fin, v_dias_corte
  FROM analytics.get_corte_actual_rango() r;

  -- ── Previous corte window: per-client latest before current start ──
  SELECT MIN(sq.fecha_creacion), MAX(sq.fecha_completado)
  INTO v_prev_inicio, v_prev_fin
  FROM (
    SELECT DISTINCT ON (v.id_cliente)
      v.created_at::date AS fecha_creacion,
      v.completed_at::date AS fecha_completado
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.completed_at::date < v_fecha_inicio
    ORDER BY v.id_cliente, v.completed_at DESC
  ) sq;

  IF v_prev_inicio IS NOT NULL THEN
    -- Previous period aggregate stats
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, med.precio, 0) ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin;

    -- Previous period doctors with sales
    SELECT COUNT(DISTINCT mov.id_cliente)
    INTO v_ant_medicos_con_venta
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin
      AND mov.tipo = 'VENTA';
  END IF;

  -- ── Return current stats with comparison ──
  RETURN QUERY
  WITH
  current_movements AS (
    SELECT mov.*
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_visitados AS (
    SELECT DISTINCT cm.id_cliente FROM current_movements cm
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT cm.id_cliente FROM current_movements cm WHERE cm.tipo = 'VENTA'
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int AS total_mov,
      SUM(CASE WHEN cm.tipo = 'VENTA' THEN cm.cantidad ELSE 0 END)::int AS pz_venta,
      SUM(CASE WHEN cm.tipo = 'CREACION' THEN cm.cantidad ELSE 0 END)::int AS pz_creacion,
      SUM(CASE WHEN cm.tipo = 'RECOLECCION' THEN cm.cantidad ELSE 0 END)::int AS pz_recoleccion,
      SUM(CASE WHEN cm.tipo = 'VENTA' THEN cm.cantidad * COALESCE(cm.precio_unitario, med.precio, 0) ELSE 0 END) AS val_venta,
      SUM(CASE WHEN cm.tipo = 'CREACION' THEN cm.cantidad * COALESCE(cm.precio_unitario, med.precio, 0) ELSE 0 END) AS val_creacion,
      SUM(CASE WHEN cm.tipo = 'RECOLECCION' THEN cm.cantidad * COALESCE(cm.precio_unitario, med.precio, 0) ELSE 0 END) AS val_recoleccion
    FROM current_movements cm
    JOIN medicamentos med ON cm.sku = med.sku
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    v_dias_corte,
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
$fn$;


-- ─── Grants ─────────────────────────────────────────────────────────────────
GRANT EXECUTE ON FUNCTION analytics.get_corte_actual_rango() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_anterior_stats() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_stats_generales_con_comparacion() TO authenticated, anon;

-- ─── Reload PostgREST schema cache ──────────────────────────────────────────
NOTIFY pgrst, 'reload schema';
