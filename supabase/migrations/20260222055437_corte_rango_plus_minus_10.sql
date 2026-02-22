-- ============================================================================
-- Migration: Corte range ±10 day window algorithm
--
-- Replaces the fragile voided_clients/ranked_visits/rn=1 logic with a simple
-- deterministic algorithm: MAX(completed_at) ± 10 days.
--
-- Corte visits typically happen around the 14-15th of each month, so this
-- creates a consistent ~21-day window centered on the most recent activity.
--
-- Functions modified:
--   1. analytics.get_corte_actual_rango()              — complete rewrite
--   2. analytics.get_corte_anterior_stats()             — date-based previous window
--   3. analytics.get_corte_stats_generales_con_comparacion() — date-range filtering
--
-- Public wrappers unchanged (they delegate to analytics versions).
-- ============================================================================


-- ─── 1. get_corte_actual_rango() ─────────────────────────────────────────────
-- Old: voided_clients → ranked_visits → rn=1 → MIN/MAX
-- New: MAX(completed_at) ± 10

CREATE OR REPLACE FUNCTION analytics.get_corte_actual_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  SELECT
    (MAX(v.completed_at::date) - 10)::date AS fecha_inicio,
    (MAX(v.completed_at::date) + 10)::date AS fecha_fin,
    21 AS dias_corte
  FROM visitas v
  JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
  WHERE v.estado = 'COMPLETADO'
    AND v.completed_at IS NOT NULL;
END;
$fn$;


-- ─── 2. get_corte_anterior_stats() ──────────────────────────────────────────
-- Old: max VENTA movement before current start, walk backwards with 3-day gaps
-- New: max completed visit before current window start, ±10

CREATE OR REPLACE FUNCTION analytics.get_corte_anterior_stats()
RETURNS TABLE(fecha_inicio date, fecha_fin date, id_cliente character varying, nombre_cliente character varying, valor_venta numeric, piezas_venta integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_corte_actual_inicio date;
  v_prev_max_fecha date;
  v_prev_inicio date;
  v_prev_fin date;
BEGIN
  -- Get current corte start
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM analytics.get_corte_actual_rango() r;

  -- Find the max completed visit date BEFORE current corte start
  SELECT MAX(v.completed_at::date) INTO v_prev_max_fecha
  FROM visitas v
  JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
  WHERE v.estado = 'COMPLETADO'
    AND v.completed_at IS NOT NULL
    AND v.completed_at::date < v_corte_actual_inicio;

  IF v_prev_max_fecha IS NULL THEN
    RETURN;
  END IF;

  -- Previous corte window: ±10 from that max date
  v_prev_inicio := v_prev_max_fecha - 10;
  v_prev_fin := v_prev_max_fecha + 10;

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
-- Old: Duplicated voided_clients/ranked_visits inline, rn=1 for current, rn=2 for previous
-- New: Uses get_corte_actual_rango() for current window, ±10 from max visit before window for previous

CREATE OR REPLACE FUNCTION analytics.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_dias_corte int;
  v_prev_max_fecha date;
  v_prev_inicio date;
  v_prev_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  -- ── Current corte window from ±10 algorithm ──
  SELECT r.fecha_inicio, r.fecha_fin, r.dias_corte
  INTO v_fecha_inicio, v_fecha_fin, v_dias_corte
  FROM analytics.get_corte_actual_rango() r;

  -- ── Previous corte window: max completed visit before current start, ±10 ──
  SELECT MAX(v.completed_at::date) INTO v_prev_max_fecha
  FROM visitas v
  JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
  WHERE v.estado = 'COMPLETADO'
    AND v.completed_at IS NOT NULL
    AND v.completed_at::date < v_fecha_inicio;

  IF v_prev_max_fecha IS NOT NULL THEN
    v_prev_inicio := v_prev_max_fecha - 10;
    v_prev_fin := v_prev_max_fecha + 10;

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
