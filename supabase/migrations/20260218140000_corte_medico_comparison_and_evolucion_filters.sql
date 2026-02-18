-- ============================================================
-- 1. Update get_corte_actual_data: add per-médico VS Anterior comparison
-- 2. Update get_historico_conversiones_evolucion: add médicos/marcas/padecimientos filters
-- ============================================================

-- ──────────────────────────────────────────────
-- 1. analytics.get_corte_actual_data — add prev_medico_stats CTE
-- ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION analytics.get_corte_actual_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_ant_fecha_inicio date;
  v_ant_fecha_fin date;
  v_result json;
BEGIN
  -- Date range: use params or auto-detect from corte actual
  IF p_fecha_inicio IS NOT NULL AND p_fecha_fin IS NOT NULL THEN
    v_fecha_inicio := p_fecha_inicio;
    v_fecha_fin := p_fecha_fin;
  ELSE
    SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
    FROM get_corte_actual_rango() r;
  END IF;

  -- Previous corte range for comparison
  SELECT r.fecha_inicio, r.fecha_fin INTO v_ant_fecha_inicio, v_ant_fecha_fin
  FROM get_corte_anterior_rango() r;

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
  -- Current corte movements (fully filtered)
  current_mov AS (
    SELECT mov.id_cliente, c.nombre_cliente, mov.sku, mov.tipo, mov.cantidad,
           mov.fecha_movimiento, med.precio
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND c.activo = TRUE
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI aggregation
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
  -- Previous corte for % change (same filters applied)
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE v_ant_fecha_inicio IS NOT NULL
      AND mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Previous corte per-médico venta (for VS Anterior column)
  prev_medico_stats AS (
    SELECT mov.id_cliente,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0)::numeric AS valor_venta
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE v_ant_fecha_inicio IS NOT NULL
      AND mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT fs.sku FROM filtered_skus fs)
    GROUP BY mov.id_cliente
  ),
  -- Per-medico breakdown for grid (with VS Anterior)
  medico_rows AS (
    SELECT
      mov.id_cliente,
      mov.nombre_cliente,
      MAX(mov.fecha_movimiento::date)::text AS fecha_visita,
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
    LEFT JOIN prev_medico_stats pms ON mov.id_cliente = pms.id_cliente
    GROUP BY mov.id_cliente, mov.nombre_cliente, pms.valor_venta
    ORDER BY COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0) DESC,
             mov.nombre_cliente
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'fecha_inicio', v_fecha_inicio,
      'fecha_fin', v_fecha_fin,
      'dias_corte', (v_fecha_fin - v_fecha_inicio + 1),
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
        CASE WHEN v_ant_fecha_inicio IS NOT NULL AND p.valor_venta > 0
          THEN ROUND(((k.valor_venta - p.valor_venta) / p.valor_venta * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_creacion',
        CASE WHEN v_ant_fecha_inicio IS NOT NULL AND p.valor_creacion > 0
          THEN ROUND(((k.valor_creacion - p.valor_creacion) / p.valor_creacion * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_recoleccion',
        CASE WHEN v_ant_fecha_inicio IS NOT NULL AND p.valor_recoleccion > 0
          THEN ROUND(((k.valor_recoleccion - p.valor_recoleccion) / p.valor_recoleccion * 100)::numeric, 1)
          ELSE NULL END
    ),
    'medicos', COALESCE((SELECT json_agg(row_to_json(mr)) FROM medico_rows mr), '[]'::json)
  ) INTO v_result
  FROM kpi_stats k
  CROSS JOIN prev_stats p;

  RETURN v_result;
END;
$$;

-- Public wrapper (unchanged signature)
CREATE OR REPLACE FUNCTION public.get_corte_actual_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_corte_actual_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ──────────────────────────────────────────────
-- 2. Update get_historico_conversiones_evolucion — add médicos/marcas/padecimientos filters
-- ──────────────────────────────────────────────
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
  -- Use base M1 pairs to identify botiquín-linked (cliente, sku)
  base_m1 AS (
    SELECT b.id_cliente, b.sku, b.first_event_date AS first_venta
    FROM analytics.clasificacion_base() b
    WHERE b.m_type = 'M1'
      AND (p_medicos IS NULL OR b.id_cliente = ANY(p_medicos))
      AND b.sku IN (SELECT fs.sku FROM filtered_skus fs)
  ),
  saga_ids AS (
    SELECT DISTINCT szl.zoho_id
    FROM saga_zoho_links szl
    WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
  ),
  ventas_clasificadas AS (
    SELECT
      v.id_cliente, v.sku, v.fecha, v.cantidad, v.precio,
      (v.cantidad * COALESCE(v.precio, 0)) as valor_venta,
      CASE
        WHEN bm.id_cliente IS NOT NULL
             AND v.fecha > bm.first_venta
             AND v.odv_id NOT IN (SELECT zoho_id FROM saga_ids) THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', v.fecha)::DATE
        ELSE v.fecha::DATE
      END as fecha_agrupada
    FROM ventas_odv v
    LEFT JOIN base_m1 bm ON v.id_cliente = bm.id_cliente AND v.sku = bm.sku
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
