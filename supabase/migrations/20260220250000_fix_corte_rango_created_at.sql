-- Fix: Use created_at (visit start date) instead of completed_at (visit finish date)
-- for corte date range calculations.
--
-- completed_at reflects when the rep finished ALL tasks, which can be days after
-- the actual visit. created_at aligns with fecha_movimiento and the real visit date.

CREATE OR REPLACE FUNCTION public.get_corte_actual_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH ranked_visits AS (
    SELECT
      v.id_cliente,
      v.created_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.created_at DESC) AS rn
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
$$;

CREATE OR REPLACE FUNCTION public.get_corte_anterior_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  WITH ranked_visits AS (
    SELECT
      v.id_cliente,
      v.created_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.created_at DESC) AS rn
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
$$;

NOTIFY pgrst, 'reload schema';
