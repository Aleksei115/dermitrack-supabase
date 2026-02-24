-- =============================================================================
-- Migration: Remove all medicamentos.precio references from analytics RPCs
-- =============================================================================
-- All monetary calculations must use precio_unitario from the source tables:
--   - movimientos_inventario.precio_unitario (captured at transaction time)
--   - inventario_botiquin.precio_unitario (latest stock price)
--   - ventas_odv.precio (ODV transaction price)
-- NEVER use medicamentos.precio (stale catalog price).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Schema fix: ensure inventario_botiquin.precio_unitario column exists
--    (was added directly to DB but never captured in a migration)
-- ---------------------------------------------------------------------------
ALTER TABLE public.inventario_botiquin
  ADD COLUMN IF NOT EXISTS precio_unitario numeric(10,2);

-- ---------------------------------------------------------------------------
-- 2. Backfill: ventas_odv id_venta=1424 (sku=U347) missing precio
--    Using 268.00 from existing ventas_odv records for same SKU
-- ---------------------------------------------------------------------------
UPDATE ventas_odv
SET precio = 268.00
WHERE id_venta = 1424 AND sku = 'U347' AND precio IS NULL;

-- ---------------------------------------------------------------------------
-- 3. Rewrite analytics.get_botiquin_data()
--    CHANGE: COALESCE(mov.precio_unitario, med.precio) → mov.precio_unitario
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_botiquin_data()
RETURNS TABLE(
  sku character varying,
  id_movimiento bigint,
  tipo_movimiento text,
  cantidad integer,
  fecha_movimiento text,
  id_lote text,
  fecha_ingreso text,
  cantidad_inicial integer,
  cantidad_disponible integer,
  id_cliente character varying,
  nombre_cliente character varying,
  rango character varying,
  facturacion_promedio numeric,
  facturacion_total numeric,
  producto character varying,
  precio numeric,
  marca character varying,
  top boolean,
  padecimiento character varying
)
LANGUAGE plpgsql SECURITY DEFINER STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT
    med.sku,
    mov.id as id_movimiento,
    CAST(mov.tipo AS TEXT) AS tipo_movimiento,
    mov.cantidad,
    TO_CHAR(mov.fecha_movimiento, 'DD/MM/YYYY') AS fecha_movimiento,
    mov.id::TEXT as id_lote,
    TO_CHAR(mov.fecha_movimiento, 'DD/MM/YYYY') AS fecha_ingreso,
    COALESCE(inv.cantidad_disponible, 0)::INTEGER as cantidad_inicial,
    COALESCE(inv.cantidad_disponible, 0)::INTEGER as cantidad_disponible,
    mov.id_cliente,
    c.nombre_cliente,
    c.rango,
    c.facturacion_promedio,
    c.facturacion_total,
    med.producto,
    mov.precio_unitario AS precio,
    med.marca,
    med.top,
    p.nombre as padecimiento
  FROM movimientos_inventario mov
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med ON mov.sku = med.sku
  LEFT JOIN inventario_botiquin inv
    ON mov.id_cliente = inv.id_cliente AND mov.sku = inv.sku
  LEFT JOIN medicamento_padecimientos mp ON mov.sku = mp.sku
  LEFT JOIN padecimientos p ON mp.id_padecimiento = p.id_padecimiento;
END;
$$;

-- ---------------------------------------------------------------------------
-- 4. Rewrite analytics.get_corte_anterior_stats()
--    CHANGE: COALESCE(mov.precio_unitario, med.precio, 0) → COALESCE(mov.precio_unitario, 0)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_corte_anterior_stats()
RETURNS TABLE(
  fecha_inicio date,
  fecha_fin date,
  id_cliente character varying,
  nombre_cliente character varying,
  valor_venta numeric,
  piezas_venta integer
)
LANGUAGE plpgsql SECURITY DEFINER STABLE
AS $$
DECLARE
  v_corte_actual_inicio date;
  v_prev_inicio date;
  v_prev_fin date;
BEGIN
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM analytics.get_corte_actual_rango() r;

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
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin
  GROUP BY c.id_cliente, c.nombre_cliente;
END;
$$;

-- ---------------------------------------------------------------------------
-- 5. Rewrite analytics.get_corte_historico_data()
--    CHANGE: kpi_stock uses COALESCE(inv.precio_unitario, med.precio, 0)
--            → COALESCE(inv.precio_unitario, 0)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_corte_historico_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER VOLATILE
AS $$
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
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    WHERE mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    WHERE mov.tipo = 'CREACION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.cantidad_disponible * COALESCE(inv.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_unicos
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE inv.cantidad_disponible > 0
      AND (p_medicos IS NULL OR inv.id_cliente = ANY(p_medicos))
      AND (p_marcas IS NULL OR med.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    WHERE mov.tipo = 'RECOLECCION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
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
      COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor_venta,
      COALESCE(SUM(mov.cantidad), 0)::int AS piezas_venta
    FROM visitas_base vb
    JOIN movimientos_inventario mov ON vb.id_saga_transaction = mov.id_saga_transaction
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
      'skus_venta_m1', (SELECT skus_unicos FROM kpi_venta_m1),
      'valor_creacion', (SELECT valor FROM kpi_creacion),
      'skus_creacion', (SELECT skus_unicos FROM kpi_creacion),
      'stock_activo', (SELECT valor FROM kpi_stock),
      'skus_stock', (SELECT skus_unicos FROM kpi_stock),
      'valor_recoleccion', (SELECT valor FROM kpi_recoleccion),
      'skus_recoleccion', (SELECT skus_unicos FROM kpi_recoleccion)
    ),
    'visitas', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$$;

-- ---------------------------------------------------------------------------
-- 6. Rewrite analytics.get_corte_stats_generales_con_comparacion()
--    CHANGE: All COALESCE(..., med.precio, 0) → COALESCE(..., 0)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_corte_stats_generales_con_comparacion()
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
LANGUAGE plpgsql SECURITY DEFINER STABLE
AS $$
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
  SELECT r.fecha_inicio, r.fecha_fin, r.dias_corte
  INTO v_fecha_inicio, v_fecha_fin, v_dias_corte
  FROM analytics.get_corte_actual_rango() r;

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
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin;

    SELECT COUNT(DISTINCT mov.id_cliente)
    INTO v_ant_medicos_con_venta
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente AND c.activo = TRUE
    WHERE mov.fecha_movimiento::date BETWEEN v_prev_inicio AND v_prev_fin
      AND mov.tipo = 'VENTA';
  END IF;

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
      SUM(CASE WHEN cm.tipo = 'VENTA' THEN cm.cantidad * COALESCE(cm.precio_unitario, 0) ELSE 0 END) AS val_venta,
      SUM(CASE WHEN cm.tipo = 'CREACION' THEN cm.cantidad * COALESCE(cm.precio_unitario, 0) ELSE 0 END) AS val_creacion,
      SUM(CASE WHEN cm.tipo = 'RECOLECCION' THEN cm.cantidad * COALESCE(cm.precio_unitario, 0) ELSE 0 END) AS val_recoleccion
    FROM current_movements cm
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
$$;

-- ---------------------------------------------------------------------------
-- 7. Rewrite analytics.get_facturacion_composicion_legacy()
--    CHANGE: m1_impacto CTE uses SUM(mi.cantidad * m.precio)
--            → SUM(mi.cantidad * COALESCE(mi.precio_unitario, 0))
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_facturacion_composicion_legacy()
RETURNS TABLE(
  id_cliente character varying,
  nombre_cliente character varying,
  rango_actual character varying,
  activo boolean,
  baseline numeric,
  facturacion_actual numeric,
  current_m1 numeric,
  current_m2 numeric,
  current_m3 numeric,
  current_unlinked numeric,
  pct_crecimiento numeric,
  pct_vinculado numeric,
  valor_vinculado numeric,
  piezas_vinculadas bigint,
  skus_vinculados bigint
)
LANGUAGE sql SECURITY DEFINER STABLE
AS $$
  WITH
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.id_cliente
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
  ),
  m1_impacto AS (
    SELECT mi.id_cliente,
      SUM(mi.cantidad * COALESCE(mi.precio_unitario, 0)) AS m1_valor,
      SUM(mi.cantidad) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA'
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
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.fecha > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.fecha > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM ventas_odv v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.id_cliente = m1.id_cliente
    LEFT JOIN first_venta fv ON v.id_cliente = fv.id_cliente AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.id_cliente = fc.id_cliente AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.id_cliente = po.id_cliente AND v.sku = po.sku
    WHERE v.precio > 0
  ),
  totals AS (
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
  )
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    c.rango_actual,
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
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clientes c
  LEFT JOIN totals t ON c.id_cliente = t.id_cliente
  LEFT JOIN m1_impacto m1i ON c.id_cliente = m1i.id_cliente
  WHERE c.rango_actual IS NOT NULL
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$$;

-- ---------------------------------------------------------------------------
-- 8. Rewrite analytics.get_recoleccion_activa()
--    CHANGE: SUM(bi.cantidad * med.precio) → use inventario_botiquin.precio_unitario
--    (BORRADOR items are still in botiquin, so use stock price)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_recoleccion_activa()
RETURNS json
LANGUAGE plpgsql SECURITY DEFINER STABLE
AS $$
DECLARE
  result json;
BEGIN
  WITH borrador_items AS (
    SELECT
      st.id_cliente,
      (item->>'sku')::text as sku,
      (item->>'cantidad')::int as cantidad
    FROM saga_transactions st,
    jsonb_array_elements(st.items) as item
    WHERE st.tipo = 'RECOLECCION'
      AND st.estado = 'BORRADOR'
  )
  SELECT json_build_object(
    'total_piezas', COALESCE(SUM(bi.cantidad), 0)::bigint,
    'valor_total', COALESCE(SUM(bi.cantidad * COALESCE(inv.precio_unitario, 0)), 0),
    'num_clientes', COALESCE(COUNT(DISTINCT bi.id_cliente), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN clientes c ON bi.id_cliente = c.id_cliente
  LEFT JOIN inventario_botiquin inv ON bi.id_cliente = inv.id_cliente AND bi.sku = inv.sku
  WHERE c.activo = TRUE;

  RETURN COALESCE(result, json_build_object('total_piezas', 0, 'valor_total', 0, 'num_clientes', 0));
END;
$$;

-- ---------------------------------------------------------------------------
-- 9. Notify PostgREST to reload schema cache
-- ---------------------------------------------------------------------------
NOTIFY pgrst, 'reload schema';
