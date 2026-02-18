-- ============================================================
-- Backend-driven filtering for Corte Mensual tabs
-- 4 RPCs: filtros + one per tab. All accept 5 filter params.
-- ============================================================

-- ──────────────────────────────────────────────
-- 0. get_filtros_disponibles() — ALL options globally (not corte-limited)
-- ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_filtros_disponibles()
RETURNS TABLE(marcas varchar[], medicos jsonb, padecimientos varchar[])
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
     FROM medicamentos m WHERE m.marca IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
     FROM clientes c WHERE c.activo = true),
    (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
     FROM padecimientos p)::varchar[];
END;
$$;

-- ──────────────────────────────────────────────
-- 1. analytics.get_corte_actual_data()
--    Combines KPIs + per-medico grid for Corte Actual tab
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
  -- Per-medico breakdown for grid
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
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END), 0) > 0 AS tiene_venta
    FROM current_mov mov
    GROUP BY mov.id_cliente, mov.nombre_cliente
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

-- Public wrapper
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
-- 2. analytics.get_corte_historico_data()
--    KPIs (all-time) + per-visit grid (date-filtered)
-- ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION analytics.get_corte_historico_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_result json;
BEGIN
  WITH
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
  ),
  -- KPI: Ventas M1 (ALL-TIME, filtered by medicos/marcas/padecimientos only)
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI: Creacion ALL-TIME
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'CREACION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI: Stock Activo (current inventory value)
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.cantidad_disponible * med.precio), 0)::numeric AS valor
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE (p_medicos IS NULL OR inv.id_cliente = ANY(p_medicos))
      AND (p_marcas IS NULL OR med.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  -- KPI: Recoleccion ALL-TIME
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'RECOLECCION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Visitas grouped by saga_transaction (date-filtered)
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.id_cliente,
      MIN(mov.fecha_movimiento::date) AS fecha_visita
    FROM movimientos_inventario mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mov.id_saga_transaction, mov.id_cliente
  ),
  visita_rows AS (
    SELECT
      c.id_cliente,
      c.nombre_cliente,
      vb.fecha_visita::text AS fecha_visita,
      COUNT(DISTINCT mov.sku)::int AS skus_unicos,
      COALESCE(SUM(mov.cantidad * med.precio), 0)::numeric AS valor_venta,
      COALESCE(SUM(mov.cantidad), 0)::int AS piezas_venta
    FROM visitas_base vb
    JOIN movimientos_inventario mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON vb.id_cliente = c.id_cliente
    WHERE mov.tipo = 'VENTA'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR vb.fecha_visita >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR vb.fecha_visita <= p_fecha_fin)
    GROUP BY c.id_cliente, c.nombre_cliente, vb.fecha_visita
    ORDER BY vb.fecha_visita ASC, c.nombre_cliente
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'valor_venta_m1', (SELECT valor FROM kpi_venta_m1),
      'valor_creacion', (SELECT valor FROM kpi_creacion),
      'stock_activo', (SELECT valor FROM kpi_stock),
      'valor_recoleccion', (SELECT valor FROM kpi_recoleccion)
    ),
    'visitas', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_historico_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_corte_historico_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

-- ──────────────────────────────────────────────
-- 3. analytics.get_corte_logistica_data()
--    Detailed logistica rows (based on existing get_corte_logistica_detalle)
-- ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
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
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
#variable_conflict use_column
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Date range: params or auto-detect
  IF p_fecha_inicio IS NOT NULL AND p_fecha_fin IS NOT NULL THEN
    v_fecha_inicio := p_fecha_inicio;
    v_fecha_fin := p_fecha_fin;
  ELSE
    SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
    FROM get_corte_actual_rango() r;
  END IF;

  RETURN QUERY
  WITH
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
    TO_CHAR(mov.fecha_movimiento, 'YYYY-MM-DD')                            AS fecha_visita,
    mov.sku,
    med.producto,
    COALESCE(inv.cantidad_disponible, 0)::int                              AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquin: Zoho link from LEVANTAMIENTO_INICIAL saga
    (SELECT szl.zoho_id
     FROM saga_zoho_links szl
     JOIN saga_transactions st_lev ON szl.id_saga_transaction = st_lev.id
     WHERE st_lev.id_cliente = mov.id_cliente
       AND st_lev.tipo = 'LEVANTAMIENTO_INICIAL'
       AND st_lev.estado NOT IN ('CANCELADA', 'FALLIDA')
     LIMIT 1)                                                              AS odv_botiquin,
    -- ODV Venta: from saga_zoho_links for this movement's saga
    (SELECT szl.zoho_id
     FROM saga_zoho_links szl
     WHERE szl.id_saga_transaction = mov.id_saga_transaction
       AND szl.tipo = 'VENTA'
     LIMIT 1)                                                              AS odv_venta,
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
  FROM movimientos_inventario mov
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN saga_transactions st ON mov.id_saga_transaction = st.id
  LEFT JOIN visitas v            ON st.visit_id = v.visit_id
  LEFT JOIN usuarios u           ON v.id_usuario = u.id_usuario
  LEFT JOIN inventario_botiquin inv ON mov.id_cliente = inv.id_cliente AND mov.sku = inv.sku
  LEFT JOIN recolecciones rcl    ON v.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo IN ('VENTA', 'RECOLECCION')
    AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY mov.fecha_movimiento DESC, c.nombre_cliente, mov.sku;
END;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
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
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

NOTIFY pgrst, 'reload schema';
