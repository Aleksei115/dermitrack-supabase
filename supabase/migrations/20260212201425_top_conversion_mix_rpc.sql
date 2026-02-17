-- RPC: Mix de conversión TOP vs No-TOP
-- Calcula el porcentaje de conversión separado por productos TOP y No-TOP
CREATE OR REPLACE FUNCTION analytics.get_top_conversion_mix()
RETURNS TABLE(
  top_conversiones int,
  top_adopciones int,
  top_pct numeric,
  no_top_conversiones int,
  no_top_adopciones int,
  no_top_pct numeric
) LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH adopciones AS (
    -- Pares (cliente, sku) con VENTA en botiquín
    SELECT DISTINCT mi.id_cliente, mi.sku, m.top
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
  ),
  conversiones AS (
    -- Adopciones que tienen ODV posterior
    SELECT a.id_cliente, a.sku, a.top
    FROM adopciones a
    WHERE EXISTS (
      SELECT 1 FROM ventas_odv v
      WHERE v.id_cliente = a.id_cliente AND v.sku = a.sku
        AND v.fecha > (
          SELECT MIN(mi2.fecha_movimiento)::date
          FROM movimientos_inventario mi2
          WHERE mi2.id_cliente = a.id_cliente AND mi2.sku = a.sku AND mi2.tipo = 'VENTA'
        )
    )
  ),
  stats AS (
    SELECT
      COUNT(*) FILTER (WHERE top) AS top_adop,
      COUNT(*) FILTER (WHERE NOT top) AS no_top_adop
    FROM adopciones
  ),
  conv_stats AS (
    SELECT
      COUNT(*) FILTER (WHERE top) AS top_conv,
      COUNT(*) FILTER (WHERE NOT top) AS no_top_conv
    FROM conversiones
  )
  SELECT
    cs.top_conv::int,
    s.top_adop::int,
    CASE WHEN s.top_adop > 0 THEN ROUND(cs.top_conv::numeric / s.top_adop * 100, 1) ELSE 0 END,
    cs.no_top_conv::int,
    s.no_top_adop::int,
    CASE WHEN s.no_top_adop > 0 THEN ROUND(cs.no_top_conv::numeric / s.no_top_adop * 100, 1) ELSE 0 END
  FROM stats s, conv_stats cs;
$$;
