-- Fix get_historico_conversiones_evolucion:
-- 1. Include M3 attribution (CREACION without VENTA) alongside M2
-- 2. Use fecha > (strict) instead of >= to match impacto RPC logic
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
  ventas_clasificadas AS (
    SELECT
      v.id_cliente, v.sku, v.fecha, v.cantidad, v.precio,
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
  )
  SELECT
    vc.fecha_agrupada,
    CASE
      WHEN p_agrupacion = 'week' THEN 'Sem ' || to_char(vc.fecha_agrupada, 'DD/MM')
      ELSE to_char(vc.fecha_agrupada, 'DD Mon')
    END,
    COUNT(DISTINCT vc.sku)::INT,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC,
    COUNT(*)::INT,
    COUNT(DISTINCT vc.id_cliente)::INT
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$$;
