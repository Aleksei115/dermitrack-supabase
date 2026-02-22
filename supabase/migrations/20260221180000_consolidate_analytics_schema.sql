-- ═══════════════════════════════════════════════════════════════════════════════
-- Migration: Consolidate All Analytics RPCs into `analytics` Schema
-- ═══════════════════════════════════════════════════════════════════════════════
--
-- Moves 21 functions into analytics schema (17 dashboard RPCs + 4 internal helpers)
-- Drops 5 unused analytics functions + their public wrappers + 1 orphaned helper
-- Replaces 17 public dashboard RPCs with thin wrappers delegating to analytics.*
-- Drops 4 public internal helpers (no external callers)
-- No frontend changes — dashboard calls public wrappers which keep same signature
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- Phase 1: DROP unused functions
-- ═══════════════════════════════════════════════════════════════════════════════

-- Public wrappers of unused analytics functions
DROP FUNCTION IF EXISTS public.debug_dashboard_data(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS public.get_doctor_performance(integer);
DROP FUNCTION IF EXISTS public.get_ranking_medicos_completo(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS public.get_top_converting_skus(integer, character varying[], character varying[], character varying[], date, date);

-- Unused analytics functions
DROP FUNCTION IF EXISTS analytics._padecimiento_for_sku(character varying);
DROP FUNCTION IF EXISTS analytics.debug_dashboard_data(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS analytics.get_doctor_performance(integer);
DROP FUNCTION IF EXISTS analytics.get_ranking_medicos_completo(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS analytics.get_top_converting_skus(integer, character varying[], character varying[], character varying[], date, date);

-- Orphaned helper (zero callers)
DROP FUNCTION IF EXISTS public.get_corte_anterior_rango();


-- ═══════════════════════════════════════════════════════════════════════════════
-- Phase 2: CREATE analytics versions (21 functions)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── 2a: Foundation helpers (internal — no public wrapper needed) ─────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_actual_rango()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_corte_anterior_stats()
RETURNS TABLE(fecha_inicio date, fecha_fin date, id_cliente character varying, nombre_cliente character varying, valor_venta numeric, piezas_venta integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_corte_actual_inicio date;
  v_corte_anterior_fin date;
  v_corte_anterior_inicio date;
  v_prev_fecha date;
BEGIN
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM analytics.get_corte_actual_rango() r;

  SELECT MAX(fecha_movimiento::date) INTO v_corte_anterior_fin
  FROM movimientos_inventario
  WHERE fecha_movimiento::date < v_corte_actual_inicio
    AND tipo = 'VENTA';

  IF v_corte_anterior_fin IS NULL THEN
    RETURN;
  END IF;

  v_corte_anterior_inicio := v_corte_anterior_fin;

  FOR v_prev_fecha IN
    SELECT DISTINCT fecha_movimiento::date
    FROM movimientos_inventario
    WHERE fecha_movimiento::date <= v_corte_anterior_fin
      AND tipo = 'VENTA'
    ORDER BY fecha_movimiento::date DESC
  LOOP
    IF v_corte_anterior_inicio - v_prev_fecha > 3 THEN
      EXIT;
    END IF;
    v_corte_anterior_inicio := v_prev_fecha;
  END LOOP;

  RETURN QUERY
  SELECT
    v_corte_anterior_inicio as fecha_inicio,
    v_corte_anterior_fin as fecha_fin,
    c.id_cliente,
    c.nombre_cliente,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0) as valor_venta,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int as piezas_venta
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_corte_anterior_inicio AND v_corte_anterior_fin
  GROUP BY c.id_cliente, c.nombre_cliente;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_corte_filtros_disponibles()
RETURNS TABLE(marcas character varying[], medicos jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_corte_actual_rango() r;

  RETURN QUERY
  SELECT
    ARRAY_AGG(DISTINCT med.marca)::varchar[] as marcas,
    jsonb_agg(DISTINCT jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)) as medicos
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_corte_stats_generales()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_corte_actual_rango() r;

  RETURN QUERY
  WITH medicos_visitados AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_con_venta AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.tipo = 'VENTA'
  ),
  stats AS (
    SELECT
      COUNT(*)::int as total_mov,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int as pz_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int as pz_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int as pz_recoleccion,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as val_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as val_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
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
    (SELECT COUNT(*)::int FROM medicos_con_venta),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta)
  FROM stats s;
END;
$fn$;


-- ─── 2b: Simple dashboard RPCs (no internal helper deps) ────────────────────

CREATE OR REPLACE FUNCTION analytics.get_botiquin_data()
RETURNS TABLE(sku character varying, id_movimiento bigint, tipo_movimiento text, cantidad integer, fecha_movimiento text, id_lote text, fecha_ingreso text, cantidad_inicial integer, cantidad_disponible integer, id_cliente character varying, nombre_cliente character varying, rango character varying, facturacion_promedio numeric, facturacion_total numeric, producto character varying, precio numeric, marca character varying, top boolean, padecimiento character varying)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
    COALESCE(mov.precio_unitario, med.precio) AS precio,
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
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_recurring_data()
RETURNS TABLE(id_cliente character varying, sku character varying, fecha date, cantidad integer, precio numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  SELECT v.id_cliente, v.sku, v.fecha, v.cantidad, v.precio
  FROM ventas_odv v;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_balance_metrics()
RETURNS TABLE(concepto text, valor_creado numeric, valor_ventas numeric, valor_recoleccion numeric, valor_permanencia_entrada numeric, valor_permanencia_virtual numeric, valor_calculado_total numeric, diferencia numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  WITH metricas_inventario AS (
    SELECT
      SUM(inv.cantidad_disponible * COALESCE(inv.precio_unitario, 0)) as total_stock_vivo
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    JOIN clientes c ON inv.id_cliente = c.id_cliente
    WHERE c.activo = TRUE
  ),
  metricas_movimientos AS (
    SELECT
      SUM(CASE WHEN mov.tipo = 'CREACION'
        THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as total_creado_historico,
      SUM(CASE WHEN mov.tipo = 'PERMANENCIA'
        THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as total_permanencia_entrada,
      SUM(CASE WHEN mov.tipo = 'VENTA'
        THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as total_ventas,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION'
        THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) as total_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
  )
  SELECT
    'BALANCE_GLOBAL_SISTEMA'::TEXT as concepto,
    COALESCE(M.total_creado_historico, 0) as valor_creado,
    COALESCE(M.total_ventas, 0) as valor_ventas,
    COALESCE(M.total_recoleccion, 0) as valor_recoleccion,
    COALESCE(M.total_permanencia_entrada, 0) as valor_permanencia_entrada,
    COALESCE(I.total_stock_vivo, 0) as valor_permanencia_virtual,
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as valor_calculado_total,
    COALESCE(M.total_creado_historico, 0) -
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as diferencia
  FROM metricas_inventario I
  CROSS JOIN metricas_movimientos M;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_recoleccion_activa()
RETURNS json
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
    'valor_total', COALESCE(SUM(bi.cantidad * med.precio), 0),
    'num_clientes', COALESCE(COUNT(DISTINCT bi.id_cliente), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN medicamentos med ON bi.sku = med.sku
  JOIN clientes c ON bi.id_cliente = c.id_cliente
  WHERE c.activo = TRUE;

  RETURN COALESCE(result, json_build_object('total_piezas', 0, 'valor_total', 0, 'num_clientes', 0));
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_filtros_disponibles()
RETURNS TABLE(marcas character varying[], medicos jsonb, padecimientos character varying[], fecha_primer_levantamiento date)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
     FROM medicamentos m WHERE m.marca IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
     FROM clientes c WHERE c.activo = true),
    (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
     FROM padecimientos p)::varchar[],
    (SELECT MIN(fecha_movimiento)::date
     FROM movimientos_inventario WHERE tipo = 'CREACION');
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_historico_skus_valor_por_visita(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_id_cliente character varying DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, piezas_venta integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  WITH visitas AS (
    SELECT
      mov.id_saga_transaction,
      mov.id_cliente,
      MIN(mov.fecha_movimiento::date) as fecha_visita
    FROM movimientos_inventario mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.tipo = 'VENTA'
    GROUP BY mov.id_saga_transaction, mov.id_cliente
  )
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    v.fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0) as valor_venta,
    SUM(mov.cantidad)::int as piezas_venta
  FROM visitas v
  JOIN movimientos_inventario mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON v.id_cliente = c.id_cliente
  WHERE mov.tipo = 'VENTA'
    AND (p_fecha_inicio IS NULL OR v.fecha_visita >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR v.fecha_visita <= p_fecha_fin)
    AND (p_id_cliente IS NULL OR v.id_cliente = p_id_cliente)
  GROUP BY c.id_cliente, c.nombre_cliente, v.fecha_visita
  ORDER BY v.fecha_visita ASC, c.nombre_cliente;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_crosssell_significancia()
RETURNS TABLE(exposed_total integer, exposed_with_crosssell integer, exposed_conversion_pct numeric, unexposed_total integer, unexposed_with_crosssell integer, unexposed_conversion_pct numeric, chi_squared numeric, significancia text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_a int;
  v_b int;
  v_c int;
  v_d int;
  v_n int;
  v_chi numeric;
BEGIN
  WITH exposure AS (
    SELECT mi.id_cliente, mp.id_padecimiento,
           MIN(mi.fecha_movimiento::date) AS first_exposure
    FROM movimientos_inventario mi
    JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
    WHERE mi.tipo = 'CREACION'
    GROUP BY mi.id_cliente, mp.id_padecimiento
  ),
  all_combos AS (
    SELECT d.id_cliente, p.id_padecimiento
    FROM (SELECT DISTINCT id_cliente FROM ventas_odv) d
    CROSS JOIN (SELECT DISTINCT id_padecimiento FROM medicamento_padecimientos) p
  ),
  analysis AS (
    SELECT ac.id_cliente, ac.id_padecimiento,
           e.first_exposure IS NOT NULL AS is_exposed,
           EXISTS (
             SELECT 1 FROM ventas_odv v
             JOIN medicamento_padecimientos mp ON mp.sku = v.sku
               AND mp.id_padecimiento = ac.id_padecimiento
             WHERE v.id_cliente = ac.id_cliente
               AND e.first_exposure IS NOT NULL
               AND v.fecha > e.first_exposure
               AND NOT EXISTS (
                 SELECT 1 FROM movimientos_inventario mi2
                 WHERE mi2.id_cliente = ac.id_cliente AND mi2.sku = v.sku
               )
               AND NOT EXISTS (
                 SELECT 1 FROM ventas_odv v2
                 WHERE v2.id_cliente = ac.id_cliente AND v2.sku = v.sku
                   AND v2.fecha <= e.first_exposure
               )
               AND v.odv_id NOT IN (
                 SELECT szl.zoho_id FROM saga_zoho_links szl
                 WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
               )
           ) AS has_cross_sell
    FROM all_combos ac
    LEFT JOIN exposure e ON e.id_cliente = ac.id_cliente
      AND e.id_padecimiento = ac.id_padecimiento
  )
  SELECT
    COUNT(*) FILTER (WHERE is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE is_exposed AND NOT has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND NOT has_cross_sell)
  INTO v_a, v_b, v_c, v_d
  FROM analysis;

  v_n := v_a + v_b + v_c + v_d;

  IF (v_a + v_b) > 0 AND (v_c + v_d) > 0 AND (v_a + v_c) > 0 AND (v_b + v_d) > 0 THEN
    v_chi := v_n::numeric
      * POWER(GREATEST(ABS(v_a::numeric * v_d - v_b::numeric * v_c) - v_n::numeric / 2, 0), 2)
      / ((v_a + v_b)::numeric * (v_c + v_d) * (v_a + v_c) * (v_b + v_d));
  ELSE
    v_chi := 0;
  END IF;

  RETURN QUERY SELECT
    (v_a + v_b)::int,
    v_a,
    CASE WHEN (v_a + v_b) > 0
      THEN ROUND(v_a::numeric / (v_a + v_b) * 100, 1)
      ELSE 0::numeric END,
    (v_c + v_d)::int,
    v_c,
    CASE WHEN (v_c + v_d) > 0
      THEN ROUND(v_c::numeric / (v_c + v_d) * 100, 1)
      ELSE 0::numeric END,
    ROUND(v_chi, 2),
    CASE
      WHEN v_chi > 10.83 THEN 'ALTA (p < 0.001)'
      WHEN v_chi > 6.64 THEN 'MEDIA (p < 0.01)'
      WHEN v_chi > 3.84 THEN 'BAJA (p < 0.05)'
      ELSE 'NO SIGNIFICATIVA'
    END;
END;
$fn$;


-- ─── 2c: Functions depending on analytics.get_corte_actual_rango ─────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_stats_por_medico()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, skus_creados text, skus_recolectados text, tiene_venta boolean)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_corte_actual_rango() r;

  RETURN QUERY
  WITH visitas_en_corte AS (
    SELECT DISTINCT
      mov.id_cliente,
      mov.id_saga_transaction,
      MIN(mov.fecha_movimiento::date) as fecha_saga
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.id_saga_transaction IS NOT NULL
    GROUP BY mov.id_cliente, mov.id_saga_transaction
  )
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    MAX(v.fecha_saga) as fecha_visita,
    SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int,
    SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int,
    SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'CREACION' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.sku END, ', '),
    SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END) > 0
  FROM visitas_en_corte v
  JOIN movimientos_inventario mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON v.id_cliente = c.id_cliente
  GROUP BY c.id_cliente, c.nombre_cliente
  ORDER BY SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END) DESC,
           c.nombre_cliente;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_corte_skus_valor_por_visita(
  p_id_cliente character varying DEFAULT NULL,
  p_marca character varying DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, marca character varying)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_corte_actual_rango() r;

  RETURN QUERY
  SELECT
    c.id_cliente,
    c.nombre_cliente,
    mov.fecha_movimiento::date as fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * COALESCE(mov.precio_unitario, 0) ELSE 0 END), 0) as valor_venta,
    med.marca
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo = 'VENTA'
    AND (p_id_cliente IS NULL OR c.id_cliente = p_id_cliente)
    AND (p_marca IS NULL OR med.marca = p_marca)
  GROUP BY c.id_cliente, c.nombre_cliente, mov.fecha_movimiento::date, med.marca
  ORDER BY valor_venta DESC;
END;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_detalle()
RETURNS TABLE(nombre_asesor text, nombre_cliente text, id_cliente text, fecha_visita date, sku text, producto text, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_estado text, odv_botiquin text, odv_venta text, recoleccion_id text, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_corte_actual_rango() r;

  RETURN QUERY
  WITH
  latest_visits AS (
    SELECT DISTINCT ON (v.id_cliente)
      v.visit_id,
      v.id_cliente,
      v.id_usuario,
      v.created_at::date AS fecha_visita
    FROM visitas v
    JOIN clientes cl ON cl.id_cliente = v.id_cliente AND cl.activo = TRUE
    WHERE v.created_at::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND v.estado NOT IN ('CANCELADO')
    ORDER BY v.id_cliente, v.created_at DESC
  ),
  visit_sagas AS (
    SELECT
      lv.visit_id,
      lv.id_cliente,
      st.id AS saga_id,
      st.tipo AS saga_tipo,
      st.estado AS saga_estado
    FROM latest_visits lv
    JOIN saga_transactions st ON st.visit_id = lv.visit_id
  ),
  creaciones AS (
    SELECT
      vs.id_cliente,
      mi.sku,
      SUM(mi.cantidad)::int AS cantidad_colocada,
      vs.saga_id,
      vs.saga_estado
    FROM visit_sagas vs
    JOIN movimientos_inventario mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'LEV_POST_CORTE'
      AND mi.tipo = 'CREACION'
    GROUP BY vs.id_cliente, mi.sku, vs.saga_id, vs.saga_estado
  ),
  ventas AS (
    SELECT
      vs.id_cliente,
      mi.sku,
      SUM(mi.cantidad)::int AS qty_venta,
      vs.saga_id
    FROM visit_sagas vs
    JOIN movimientos_inventario mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'VENTA'
      AND mi.tipo = 'VENTA'
    GROUP BY vs.id_cliente, mi.sku, vs.saga_id
  ),
  recol AS (
    SELECT
      r.id_cliente,
      r.recoleccion_id,
      r.estado AS recoleccion_estado,
      r.cedis_observaciones,
      r.cedis_responsable_nombre
    FROM latest_visits lv
    JOIN recolecciones r ON r.visit_id = lv.visit_id
  ),
  recol_items AS (
    SELECT
      rec.id_cliente,
      ri.sku,
      SUM(ri.cantidad)::int AS qty_recoleccion,
      rec.recoleccion_id,
      rec.recoleccion_estado,
      rec.cedis_observaciones,
      rec.cedis_responsable_nombre
    FROM recol rec
    JOIN recolecciones_items ri ON ri.recoleccion_id = rec.recoleccion_id
    GROUP BY rec.id_cliente, ri.sku, rec.recoleccion_id, rec.recoleccion_estado,
             rec.cedis_observaciones, rec.cedis_responsable_nombre
  ),
  zoho_botiquin AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'LEV_POST_CORTE' AND szl.tipo = 'BOTIQUIN'
    GROUP BY vs.saga_id
  ),
  zoho_venta AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'VENTA' AND szl.tipo = 'VENTA'
    GROUP BY vs.saga_id
  ),
  evidencias AS (
    SELECT re.recoleccion_id, array_agg(re.storage_path) AS paths
    FROM recolecciones_evidencias re
    GROUP BY re.recoleccion_id
  ),
  firmas AS (
    SELECT rf.recoleccion_id, rf.storage_path
    FROM recolecciones_firmas rf
  ),
  combined AS (
    SELECT
      c.id_cliente,
      c.sku,
      c.cantidad_colocada,
      c.saga_id AS saga_creacion,
      c.saga_estado,
      COALESCE(v.qty_venta, 0) AS qty_venta,
      v.saga_id AS saga_venta,
      COALESCE(ri.qty_recoleccion, 0) AS qty_recoleccion,
      ri.recoleccion_id,
      ri.recoleccion_estado,
      ri.cedis_observaciones,
      ri.cedis_responsable_nombre
    FROM creaciones c
    LEFT JOIN ventas v ON v.id_cliente = c.id_cliente AND v.sku = c.sku
    LEFT JOIN recol_items ri ON ri.id_cliente = c.id_cliente AND ri.sku = c.sku
  )
  SELECT
    u.nombre::text                                    AS nombre_asesor,
    cl.nombre_cliente::text                           AS nombre_cliente,
    cl.id_cliente::text                               AS id_cliente,
    lv.fecha_visita                                   AS fecha_visita,
    cb.sku::text                                      AS sku,
    med.producto::text                                AS producto,
    cb.cantidad_colocada                              AS cantidad_colocada,
    cb.qty_venta                                      AS qty_venta,
    cb.qty_recoleccion                                AS qty_recoleccion,
    cb.qty_venta + cb.qty_recoleccion                 AS total_corte,
    CASE
      WHEN cb.qty_venta > 0 AND cb.qty_recoleccion > 0 THEN 'VENTA+RECOLECCION'
      WHEN cb.qty_venta > 0 THEN 'VENTA'
      WHEN cb.qty_recoleccion > 0 THEN 'RECOLECCION'
      ELSE 'PENDIENTE'
    END                                               AS destino,
    cb.saga_estado::text                              AS saga_estado,
    zb.odv                                            AS odv_botiquin,
    zv.odv                                            AS odv_venta,
    cb.recoleccion_id::text                           AS recoleccion_id,
    cb.recoleccion_estado::text                       AS recoleccion_estado,
    ev.paths                                          AS evidencia_paths,
    fi.storage_path                                   AS firma_path,
    cb.cedis_observaciones::text                      AS observaciones,
    cb.cedis_responsable_nombre::text                 AS quien_recibio
  FROM combined cb
  JOIN latest_visits lv ON lv.id_cliente = cb.id_cliente
  JOIN clientes cl ON cl.id_cliente = cb.id_cliente
  JOIN medicamentos med ON med.sku = cb.sku
  LEFT JOIN usuarios u ON u.id_usuario = lv.id_usuario
  LEFT JOIN zoho_botiquin zb ON zb.saga_id = cb.saga_creacion
  LEFT JOIN zoho_venta zv ON zv.saga_id = cb.saga_venta
  LEFT JOIN evidencias ev ON ev.recoleccion_id = cb.recoleccion_id
  LEFT JOIN firmas fi ON fi.recoleccion_id = cb.recoleccion_id
  ORDER BY cl.nombre_cliente, cb.sku;
END;
$fn$;


-- ─── 2d: Functions with cross-calls ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_stats_por_medico_con_comparacion()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, tiene_venta boolean, valor_venta_anterior numeric, porcentaje_cambio numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
  RETURN QUERY
  WITH corte_actual AS (
    SELECT * FROM analytics.get_corte_stats_por_medico()
  ),
  corte_anterior AS (
    SELECT * FROM analytics.get_corte_anterior_stats()
  )
  SELECT
    ca.id_cliente,
    ca.nombre_cliente,
    ca.fecha_visita,
    ca.piezas_venta,
    ca.piezas_creacion,
    ca.piezas_recoleccion,
    ca.valor_venta,
    ca.valor_creacion,
    ca.valor_recoleccion,
    ca.skus_vendidos,
    ca.tiene_venta,
    COALESCE(cp.valor_venta, 0) as valor_venta_anterior,
    CASE
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta > 0 THEN 100.00
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta = 0 THEN 0.00
      ELSE ROUND(((ca.valor_venta - COALESCE(cp.valor_venta, 0)) / cp.valor_venta * 100), 1)
    END as porcentaje_cambio
  FROM corte_actual ca
  LEFT JOIN corte_anterior cp ON ca.id_cliente = cp.id_cliente
  ORDER BY ca.valor_venta DESC;
END;
$fn$;


-- ─── 2e: Complex standalone ─────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
$fn$;


-- ─── 2f: Functions using analytics.clasificacion_base ────────────────────────

CREATE OR REPLACE FUNCTION analytics.get_conversion_metrics(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric, valor_botiquin numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin))
  SELECT (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
         (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
         COALESCE((SELECT SUM(revenue_odv) FROM base WHERE m_type = 'M2'), 0)::numeric,
         COALESCE((SELECT SUM(revenue_botiquin) FROM base WHERE m_type = 'M2'), 0)::numeric;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_conversion_details(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(m_type text, id_cliente character varying, nombre_cliente character varying, sku character varying, producto character varying, fecha_botiquin date, fecha_primera_odv date, dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric, valor_botiquin numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
  SELECT b.m_type::text, b.id_cliente, b.nombre_cliente, b.sku, b.producto,
    b.first_event_date, odv_first.first_odv, (odv_first.first_odv - b.first_event_date)::int,
    b.num_transacciones_odv, b.cantidad_odv::bigint, b.revenue_odv, b.revenue_botiquin
  FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
  JOIN LATERAL (
    SELECT MIN(v.fecha) AS first_odv FROM ventas_odv v
    WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku AND v.fecha >= b.first_event_date
      AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL)
  ) odv_first ON true
  WHERE b.m_type IN ('M2', 'M3')
  ORDER BY b.revenue_odv DESC;
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_agrupacion text DEFAULT 'day',
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
RETURNS TABLE(fecha_grupo date, fecha_label text, pares_total integer, pares_botiquin integer, pares_directo integer, valor_total numeric, valor_botiquin numeric, valor_directo numeric, num_transacciones integer, num_clientes integer)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
BEGIN
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
  ),
  botiquin_linked AS (
    SELECT DISTINCT b.id_cliente, b.sku, b.first_event_date
    FROM analytics.clasificacion_base(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin) b
    WHERE b.m_type IN ('M1', 'M2', 'M3')
      AND (p_medicos IS NULL OR b.id_cliente = ANY(p_medicos))
      AND b.sku IN (SELECT fs.sku FROM filtered_skus fs)
  ),
  ventas_clasificadas AS (
    SELECT
      v.id_cliente, v.sku, v.fecha, v.cantidad, v.precio,
      (v.cantidad * COALESCE(v.precio, 0)) as valor_venta,
      CASE
        WHEN bl.id_cliente IS NOT NULL
             AND v.fecha >= bl.first_event_date THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', v.fecha)::DATE
        ELSE v.fecha::DATE
      END as fecha_agrupada
    FROM ventas_odv v
    LEFT JOIN botiquin_linked bl ON v.id_cliente = bl.id_cliente AND v.sku = bl.sku
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
    COUNT(DISTINCT vc.id_cliente || '-' || vc.sku)::INT as pares_total,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as pares_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as pares_directo,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC as valor_total,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_botiquin,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_directo,
    COUNT(*)::INT as num_transacciones,
    COUNT(DISTINCT vc.id_cliente)::INT as num_clientes
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$fn$;


-- ─── 2g: Facturacion overloads ──────────────────────────────────────────────

-- NOTE: Named _legacy to avoid PostgreSQL overload ambiguity with the 5-param version
-- (both match zero-arg calls when all params have DEFAULTs)
CREATE OR REPLACE FUNCTION analytics.get_facturacion_composicion_legacy()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, rango_actual character varying, activo boolean, baseline numeric, facturacion_actual numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
      SUM(mi.cantidad * m.precio) AS m1_valor,
      SUM(mi.cantidad) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
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
$fn$;


CREATE OR REPLACE FUNCTION analytics.get_facturacion_composicion(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, rango_actual character varying, rango_anterior character varying, activo boolean, baseline numeric, facturacion_actual numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
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
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::date >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::date <= p_fecha_fin)
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
      AND v.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
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
    c.rango AS rango_anterior,
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
    AND (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ORDER BY (COALESCE(c.facturacion_actual, 0) - COALESCE(c.facturacion_promedio, 0)) DESC;
$fn$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- Phase 3: Replace public dashboard RPCs with thin wrappers (17)
--          + DROP public internal helpers (4)
-- ═══════════════════════════════════════════════════════════════════════════════

-- ─── 3a: Thin public wrappers ───────────────────────────────────────────────

CREATE OR REPLACE FUNCTION public.get_botiquin_data()
RETURNS TABLE(sku character varying, id_movimiento bigint, tipo_movimiento text, cantidad integer, fecha_movimiento text, id_lote text, fecha_ingreso text, cantidad_inicial integer, cantidad_disponible integer, id_cliente character varying, nombre_cliente character varying, rango character varying, facturacion_promedio numeric, facturacion_total numeric, producto character varying, precio numeric, marca character varying, top boolean, padecimiento character varying)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_botiquin_data(); $$;

CREATE OR REPLACE FUNCTION public.get_recurring_data()
RETURNS TABLE(id_cliente character varying, sku character varying, fecha date, cantidad integer, precio numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_recurring_data(); $$;

CREATE OR REPLACE FUNCTION public.get_balance_metrics()
RETURNS TABLE(concepto text, valor_creado numeric, valor_ventas numeric, valor_recoleccion numeric, valor_permanencia_entrada numeric, valor_permanencia_virtual numeric, valor_calculado_total numeric, diferencia numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_balance_metrics(); $$;

CREATE OR REPLACE FUNCTION public.get_recoleccion_activa()
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT analytics.get_recoleccion_activa(); $$;

CREATE OR REPLACE FUNCTION public.get_filtros_disponibles()
RETURNS TABLE(marcas character varying[], medicos jsonb, padecimientos character varying[], fecha_primer_levantamiento date)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_filtros_disponibles(); $$;

CREATE OR REPLACE FUNCTION public.get_corte_stats_generales_con_comparacion()
RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_corte_stats_generales_con_comparacion(); $$;

CREATE OR REPLACE FUNCTION public.get_corte_stats_por_medico()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, skus_creados text, skus_recolectados text, tiene_venta boolean)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_corte_stats_por_medico(); $$;

CREATE OR REPLACE FUNCTION public.get_corte_stats_por_medico_con_comparacion()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, tiene_venta boolean, valor_venta_anterior numeric, porcentaje_cambio numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_corte_stats_por_medico_con_comparacion(); $$;

CREATE OR REPLACE FUNCTION public.get_corte_skus_valor_por_visita(
  p_id_cliente character varying DEFAULT NULL,
  p_marca character varying DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, marca character varying)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_corte_skus_valor_por_visita(p_id_cliente, p_marca); $$;

CREATE OR REPLACE FUNCTION public.get_corte_logistica_detalle()
RETURNS TABLE(nombre_asesor text, nombre_cliente text, id_cliente text, fecha_visita date, sku text, producto text, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_estado text, odv_botiquin text, odv_venta text, recoleccion_id text, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_corte_logistica_detalle(); $$;

CREATE OR REPLACE FUNCTION public.get_historico_skus_valor_por_visita(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_id_cliente character varying DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, piezas_venta integer)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_historico_skus_valor_por_visita(p_fecha_inicio, p_fecha_fin, p_id_cliente); $$;

CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_agrupacion text DEFAULT 'day',
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
RETURNS TABLE(fecha_grupo date, fecha_label text, pares_total integer, pares_botiquin integer, pares_directo integer, valor_total numeric, valor_botiquin numeric, valor_directo numeric, num_transacciones integer, num_clientes integer)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_historico_conversiones_evolucion(p_fecha_inicio, p_fecha_fin, p_agrupacion, p_medicos, p_marcas, p_padecimientos); $$;

CREATE OR REPLACE FUNCTION public.get_conversion_metrics(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric, valor_botiquin numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_conversion_metrics(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin); $$;

CREATE OR REPLACE FUNCTION public.get_conversion_details(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(m_type text, id_cliente character varying, nombre_cliente character varying, sku character varying, producto character varying, fecha_botiquin date, fecha_primera_odv date, dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric, valor_botiquin numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_conversion_details(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin); $$;

CREATE OR REPLACE FUNCTION public.get_crosssell_significancia()
RETURNS TABLE(exposed_total integer, exposed_with_crosssell integer, exposed_conversion_pct numeric, unexposed_total integer, unexposed_with_crosssell integer, unexposed_conversion_pct numeric, chi_squared numeric, significancia text)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_crosssell_significancia(); $$;

CREATE OR REPLACE FUNCTION public.get_facturacion_composicion()
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, rango_actual character varying, activo boolean, baseline numeric, facturacion_actual numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_facturacion_composicion_legacy(); $$;

CREATE OR REPLACE FUNCTION public.get_facturacion_composicion(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, rango_actual character varying, rango_anterior character varying, activo boolean, baseline numeric, facturacion_actual numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$ SELECT * FROM analytics.get_facturacion_composicion(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin); $$;


-- ─── 3b: DROP public internal helpers (no external callers) ─────────────────

DROP FUNCTION IF EXISTS public.get_corte_actual_rango();
DROP FUNCTION IF EXISTS public.get_corte_anterior_stats();
DROP FUNCTION IF EXISTS public.get_corte_filtros_disponibles();
DROP FUNCTION IF EXISTS public.get_corte_stats_generales();


-- ═══════════════════════════════════════════════════════════════════════════════
-- Phase 4: Update analytics.get_dashboard_static to use qualified call
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
RETURNS json
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $fn$
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
    FROM analytics.get_corte_stats_generales_con_comparacion() r
    LIMIT 1
  ) s;

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
$fn$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- Phase 5: GRANTs + NOTIFY
-- ═══════════════════════════════════════════════════════════════════════════════

-- Grant on new analytics functions
GRANT EXECUTE ON FUNCTION analytics.get_corte_actual_rango() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_anterior_stats() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_filtros_disponibles() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_stats_generales() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_botiquin_data() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_recurring_data() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_balance_metrics() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_recoleccion_activa() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_filtros_disponibles() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_historico_skus_valor_por_visita(date, date, character varying) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_crosssell_significancia() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_stats_por_medico() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_skus_valor_por_visita(character varying, character varying) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_logistica_detalle() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_stats_por_medico_con_comparacion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_corte_stats_generales_con_comparacion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_conversion_details(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_historico_conversiones_evolucion(date, date, text, character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_facturacion_composicion_legacy() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_facturacion_composicion(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;

-- Grant on public wrappers (permissions preserved by CREATE OR REPLACE, but explicit for clarity)
GRANT EXECUTE ON FUNCTION public.get_botiquin_data() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_recurring_data() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_balance_metrics() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_recoleccion_activa() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_filtros_disponibles() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_stats_generales_con_comparacion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_stats_por_medico() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_stats_por_medico_con_comparacion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_skus_valor_por_visita(character varying, character varying) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_logistica_detalle() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_historico_skus_valor_por_visita(date, date, character varying) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_historico_conversiones_evolucion(date, date, text, character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_conversion_details(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_crosssell_significancia() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_facturacion_composicion() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_facturacion_composicion(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
