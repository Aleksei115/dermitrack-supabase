-- Refactor all downstream RPCs to delegate to analytics.clasificacion_base()
-- Fixes: >= vs > bug, missing saga exclusion, inconsistent M1/M2/M3 logic
-- M4 (cross-sell) eliminated — columns return 0/empty
-- Return types are preserved for frontend compatibility

----------------------------------------------------------------------
-- 1. analytics.get_impacto_botiquin_resumen
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_impacto_botiquin_resumen()
RETURNS TABLE(
  adopciones int, revenue_adopciones numeric,
  conversiones int, revenue_conversiones numeric,
  exposiciones int, revenue_exposiciones numeric,
  crosssell_pares int, revenue_crosssell numeric,
  revenue_total_impacto numeric, revenue_total_odv numeric, porcentaje_impacto numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base()),
  m1 AS (
    SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev FROM base WHERE m_type = 'M1'
  ),
  m2 AS (
    SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M2'
  ),
  m3 AS (
    SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M3'
  ),
  total_odv AS (
    SELECT COALESCE(SUM(cantidad * precio), 0) AS rev FROM ventas_odv
  )
  SELECT
    m1.cnt, m1.rev,
    m2.cnt, m2.rev,
    m3.cnt, m3.rev,
    0::int, 0::numeric,  -- M4 eliminated
    (m1.rev + m2.rev + m3.rev),
    t.rev,
    CASE WHEN t.rev > 0
      THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1)
      ELSE 0
    END
  FROM m1, m2, m3, total_odv t;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_impacto_botiquin_resumen()
RETURNS TABLE(
  adopciones int, revenue_adopciones numeric,
  conversiones int, revenue_conversiones numeric,
  exposiciones int, revenue_exposiciones numeric,
  crosssell_pares int, revenue_crosssell numeric,
  revenue_total_impacto numeric, revenue_total_odv numeric, porcentaje_impacto numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_impacto_botiquin_resumen();
$$;

----------------------------------------------------------------------
-- 2. analytics.get_impacto_detalle(p_metrica)
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_impacto_detalle(p_metrica text)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  cantidad int, precio numeric, valor numeric, fecha date, detalle text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public AS $function$
BEGIN
  IF p_metrica = 'M1' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           ROUND(b.revenue_botiquin / NULLIF(m.precio, 0))::int AS cantidad,
           m.precio,
           b.revenue_botiquin AS valor,
           b.first_event_date AS fecha,
           'Adopción en botiquín'::text AS detalle
    FROM analytics.clasificacion_base() b
    JOIN medicamentos m ON m.sku = b.sku
    WHERE b.m_type = 'M1'
    ORDER BY b.revenue_botiquin DESC;

  ELSIF p_metrica = 'M2' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           b.cantidad_odv::int AS cantidad,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS precio,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS fecha,
           ('ODV después de botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base() b
    JOIN LATERAL (
      SELECT MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
        AND v.fecha > b.first_event_date
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    ) odv_first ON true
    WHERE b.m_type = 'M2'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metrica = 'M3' THEN
    RETURN QUERY
    SELECT b.id_cliente, b.nombre_cliente, b.sku, b.producto,
           b.cantidad_odv::int AS cantidad,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS precio,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS fecha,
           ('Exposición post-botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base() b
    JOIN LATERAL (
      SELECT MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
        AND v.fecha > b.first_event_date
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    ) odv_first ON true
    WHERE b.m_type = 'M3'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metrica = 'M4' THEN
    -- M4 eliminated — return empty result
    RETURN;

  ELSE
    RAISE EXCEPTION 'Métrica inválida: %. Use M1, M2, M3 o M4.', p_metrica;
  END IF;
END;
$function$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_impacto_detalle(p_metrica text)
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  cantidad int, precio numeric, valor numeric, fecha date, detalle text
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_impacto_detalle(p_metrica);
$$;

----------------------------------------------------------------------
-- 3. public.get_conversion_metrics
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_conversion_metrics()
RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base())
  SELECT
    (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
    (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
    COALESCE((SELECT SUM(revenue_odv) FROM base WHERE m_type = 'M2'), 0)::numeric;
$$;

----------------------------------------------------------------------
-- 4. public.get_conversion_details
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_conversion_details()
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  fecha_botiquin date, fecha_primera_odv date, dias_conversion int,
  num_ventas_odv bigint, total_piezas bigint, valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT
    b.id_cliente, b.nombre_cliente, b.sku, b.producto,
    b.first_event_date AS fecha_botiquin,
    odv_first.first_odv AS fecha_primera_odv,
    (odv_first.first_odv - b.first_event_date)::int AS dias_conversion,
    b.num_transacciones_odv AS num_ventas_odv,
    b.cantidad_odv::bigint AS total_piezas,
    b.revenue_odv AS valor_generado
  FROM analytics.clasificacion_base() b
  JOIN LATERAL (
    SELECT MIN(v.fecha) AS first_odv
    FROM ventas_odv v
    WHERE v.id_cliente = b.id_cliente AND v.sku = b.sku
      AND v.fecha > b.first_event_date
      AND v.odv_id NOT IN (
        SELECT szl.zoho_id FROM saga_zoho_links szl
        WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
      )
  ) odv_first ON true
  WHERE b.m_type = 'M2'
  ORDER BY b.revenue_odv DESC;
$$;

----------------------------------------------------------------------
-- 5. analytics.get_sankey_conversion_flows
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_sankey_conversion_flows()
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku text, producto text,
  categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT
    b.id_cliente::varchar,
    b.nombre_cliente::varchar,
    b.sku::text,
    b.producto::text,
    b.m_type::text AS categoria,
    b.revenue_odv AS valor_odv,
    b.cantidad_odv,
    b.num_transacciones_odv AS num_transacciones
  FROM analytics.clasificacion_base() b
  WHERE b.m_type IN ('M2', 'M3');
$$;

-- Public wrapper (already exists, just ensure signature matches)
CREATE OR REPLACE FUNCTION public.get_sankey_conversion_flows()
RETURNS TABLE(
  id_cliente varchar, nombre_cliente varchar, sku text, producto text,
  categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_sankey_conversion_flows();
$$;

----------------------------------------------------------------------
-- 6. analytics.get_top_converting_skus
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_top_converting_skus(p_limit int DEFAULT 10)
RETURNS TABLE(sku varchar, producto varchar, conversiones int, avg_dias int, roi numeric, valor_generado numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base_m2 AS (
    SELECT b.id_cliente, b.sku, b.revenue_odv, b.first_event_date AS first_venta
    FROM analytics.clasificacion_base() b
    WHERE b.m_type = 'M2'
  ),
  with_invest AS (
    SELECT bm.id_cliente, bm.sku, bm.revenue_odv, bm.first_venta,
           (SELECT MIN(mi.fecha_movimiento::date)
            FROM movimientos_inventario mi
            WHERE mi.id_cliente = bm.id_cliente AND mi.sku = bm.sku AND mi.tipo = 'CREACION'
           ) AS first_creacion,
           (SELECT COALESCE(SUM(mi.cantidad * m.precio), 0)
            FROM movimientos_inventario mi
            JOIN medicamentos m ON m.sku = mi.sku
            WHERE mi.id_cliente = bm.id_cliente AND mi.sku = bm.sku AND mi.tipo = 'CREACION'
           ) AS invest
    FROM base_m2 bm
  )
  SELECT wi.sku, m.producto,
         COUNT(*)::int AS conversiones,
         ROUND(AVG(GREATEST(0, wi.first_venta - wi.first_creacion)))::int AS avg_dias,
         CASE WHEN SUM(wi.invest) > 0
              THEN ROUND(SUM(wi.revenue_odv) / SUM(wi.invest), 1)
              ELSE 0
         END AS roi,
         SUM(wi.revenue_odv) AS valor_generado
  FROM with_invest wi
  JOIN medicamentos m ON m.sku = wi.sku
  GROUP BY wi.sku, m.producto
  ORDER BY valor_generado DESC
  LIMIT p_limit;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_top_converting_skus(p_limit int DEFAULT 10)
RETURNS TABLE(sku varchar, producto varchar, conversiones int, avg_dias int, roi numeric, valor_generado numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_top_converting_skus(p_limit);
$$;

----------------------------------------------------------------------
-- 7. analytics.get_top_conversion_mix
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_top_conversion_mix()
RETURNS TABLE(
  top_conversiones int, top_adopciones int, top_pct numeric,
  no_top_conversiones int, no_top_adopciones int, no_top_pct numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH base AS (SELECT * FROM analytics.clasificacion_base()),
  stats AS (
    SELECT
      COUNT(*) FILTER (WHERE m_type = 'M1' AND es_top) AS top_adop,
      COUNT(*) FILTER (WHERE m_type = 'M1' AND NOT es_top) AS no_top_adop,
      COUNT(*) FILTER (WHERE m_type = 'M2' AND es_top) AS top_conv,
      COUNT(*) FILTER (WHERE m_type = 'M2' AND NOT es_top) AS no_top_conv
    FROM base
  )
  SELECT
    s.top_conv::int,
    s.top_adop::int,
    CASE WHEN s.top_adop > 0 THEN ROUND(s.top_conv::numeric / s.top_adop * 100, 1) ELSE 0 END,
    s.no_top_conv::int,
    s.no_top_adop::int,
    CASE WHEN s.no_top_adop > 0 THEN ROUND(s.no_top_conv::numeric / s.no_top_adop * 100, 1) ELSE 0 END
  FROM stats s;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_top_conversion_mix()
RETURNS TABLE(
  top_conversiones int, top_adopciones int, top_pct numeric,
  no_top_conversiones int, no_top_adopciones int, no_top_pct numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_top_conversion_mix();
$$;

----------------------------------------------------------------------
-- 8. public.get_historico_conversiones_evolucion
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL, p_fecha_fin date DEFAULT NULL, p_agrupacion text DEFAULT 'day'
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
  -- Use base M1 pairs to identify botiquín-linked (cliente, sku)
  base_m1 AS (
    SELECT b.id_cliente, b.sku, b.first_event_date AS first_venta
    FROM analytics.clasificacion_base() b
    WHERE b.m_type = 'M1'
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
