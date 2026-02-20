-----------------------------------------------------------------------
-- Extend get_conversion_details() to include both M2 and M3
-- Adds m_type column; M2 = Conversión, M3 = Exposición
-- Uses >= for first_event_date consistency with clasificacion_base
-----------------------------------------------------------------------

DROP FUNCTION IF EXISTS public.get_conversion_details();

CREATE OR REPLACE FUNCTION public.get_conversion_details()
RETURNS TABLE(
  m_type text,
  id_cliente varchar, nombre_cliente varchar, sku varchar, producto varchar,
  fecha_botiquin date, fecha_primera_odv date, dias_conversion int,
  num_ventas_odv bigint, total_piezas bigint, valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT
    b.m_type::text,
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
      AND v.fecha >= b.first_event_date
      AND v.odv_id NOT IN (
        SELECT szl.zoho_id FROM saga_zoho_links szl
        WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
      )
  ) odv_first ON true
  WHERE b.m_type IN ('M2', 'M3')
  ORDER BY b.revenue_odv DESC;
$$;

NOTIFY pgrst, 'reload schema';
