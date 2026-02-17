-- Include M1 (botiquín sales from movimientos_inventario VENTA) in the
-- conversiones evolution function so that "Bot" = M1+M2+M3.
-- This makes the "Bot" KPI match the top-card "Dinero generado por Botiquín".
CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL,
  p_agrupacion text DEFAULT 'day'
)
RETURNS TABLE(
  fecha_grupo date, fecha_label text,
  skus_unicos_total int, skus_unicos_botiquin int, skus_unicos_directo int,
  valor_total numeric, valor_botiquin numeric, valor_directo numeric,
  num_transacciones int, num_clientes int
) LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  RETURN QUERY
  WITH primera_interaccion_botiquin AS (
    -- M2: Primera VENTA en botiquín por cliente+sku
    SELECT m.id_cliente, m.sku, MIN(m.fecha_movimiento::DATE) as primera_fecha
    FROM movimientos_inventario m
    WHERE m.tipo = 'VENTA'
    GROUP BY m.id_cliente, m.sku

    UNION

    -- M3: Primera CREACIÓN por cliente+sku que NO tienen VENTA en botiquín
    SELECT m.id_cliente, m.sku, MIN(m.fecha_movimiento::DATE) as primera_fecha
    FROM movimientos_inventario m
    WHERE m.tipo = 'CREACION'
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario m2
        WHERE m2.id_cliente = m.id_cliente AND m2.sku = m.sku AND m2.tipo = 'VENTA'
      )
    GROUP BY m.id_cliente, m.sku
  ),
  -- M1: Ventas directas del botiquín (movimientos_inventario VENTA)
  ventas_botiquin AS (
    SELECT
      mi.id_cliente,
      mi.sku,
      mi.fecha_movimiento::DATE as fecha,
      mi.cantidad,
      med.precio,
      (mi.cantidad * med.precio) as valor_venta,
      TRUE as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', mi.fecha_movimiento::DATE)::DATE
        ELSE mi.fecha_movimiento::DATE
      END as fecha_agrupada
    FROM movimientos_inventario mi
    JOIN medicamentos med ON med.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
      AND (p_fecha_inicio IS NULL OR mi.fecha_movimiento::DATE >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR mi.fecha_movimiento::DATE <= p_fecha_fin)
  ),
  -- ODV sales classified as M2+M3 (botiquín) or direct
  ventas_odv_clasificadas AS (
    SELECT
      v.id_cliente,
      v.sku,
      v.fecha,
      v.cantidad,
      v.precio,
      (v.cantidad * COALESCE(v.precio, 0)) as valor_venta,
      CASE
        WHEN pb.id_cliente IS NOT NULL AND v.fecha > pb.primera_fecha THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', v.fecha)::DATE
        ELSE v.fecha::DATE
      END as fecha_agrupada
    FROM ventas_odv v
    LEFT JOIN primera_interaccion_botiquin pb
      ON v.id_cliente = pb.id_cliente AND v.sku = pb.sku
    WHERE (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
  ),
  -- Combine M1 + ODV
  todas_ventas AS (
    SELECT * FROM ventas_botiquin
    UNION ALL
    SELECT * FROM ventas_odv_clasificadas
  )
  SELECT
    tv.fecha_agrupada,
    CASE
      WHEN p_agrupacion = 'week' THEN 'Sem ' || to_char(tv.fecha_agrupada, 'DD/MM')
      ELSE to_char(tv.fecha_agrupada, 'DD Mon')
    END,
    COUNT(DISTINCT tv.sku)::INT,
    COUNT(DISTINCT CASE WHEN tv.es_de_botiquin THEN tv.id_cliente || '-' || tv.sku END)::INT,
    COUNT(DISTINCT CASE WHEN NOT tv.es_de_botiquin THEN tv.id_cliente || '-' || tv.sku END)::INT,
    COALESCE(SUM(tv.valor_venta), 0)::NUMERIC,
    COALESCE(SUM(CASE WHEN tv.es_de_botiquin THEN tv.valor_venta ELSE 0 END), 0)::NUMERIC,
    COALESCE(SUM(CASE WHEN NOT tv.es_de_botiquin THEN tv.valor_venta ELSE 0 END), 0)::NUMERIC,
    COUNT(*)::INT,
    COUNT(DISTINCT tv.id_cliente)::INT
  FROM todas_ventas tv
  GROUP BY tv.fecha_agrupada
  ORDER BY tv.fecha_agrupada ASC;
END;
$$;
