-- Fix: fecha_inicio should use MIN(created_at) not MIN(completed_at)
-- Visits created Jan 29-30 but completed Feb 3-7 were missing from the date range.
-- Changes: ranked_visits adds fecha_creacion, date_bounds uses MIN(fecha_creacion) for fi.

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
      v.created_at::date AS fecha_creacion,
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
    SELECT MIN(cv.fecha_creacion) AS fi, MAX(cv.fecha_visita) AS ff
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

GRANT EXECUTE ON FUNCTION public.get_corte_stats_generales_con_comparacion() TO authenticated, anon;
NOTIFY pgrst, 'reload schema';
