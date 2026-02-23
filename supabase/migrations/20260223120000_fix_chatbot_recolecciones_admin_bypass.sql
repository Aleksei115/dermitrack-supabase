-- Fix: chatbot.get_recolecciones_usuario() missing admin bypass
--
-- All other chatbot RPCs (get_inventario_doctor, get_movimientos_doctor,
-- get_ventas_odv_usuario) have a p_is_admin parameter that lets OWNER/ADMIN
-- users query across all asesores. This function was the only outlier,
-- causing SyntIA to return "no hay datos" for admins asking about
-- recolecciones from other asesores.

-- Drop old 3-arg version to avoid overload ambiguity
DROP FUNCTION IF EXISTS chatbot.get_recolecciones_usuario(VARCHAR, VARCHAR, INT);

CREATE OR REPLACE FUNCTION chatbot.get_recolecciones_usuario(
   p_id_usuario VARCHAR,
   p_id_cliente VARCHAR DEFAULT NULL,
   p_limit INT DEFAULT 20,
   p_is_admin BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
  recoleccion_id UUID, id_cliente VARCHAR, nombre_cliente VARCHAR,
  estado TEXT, created_at TIMESTAMPTZ, entregada_at TIMESTAMPTZ,
  cedis_observaciones TEXT, items JSON
)
LANGUAGE sql STABLE SECURITY DEFINER AS $$
  SELECT r.recoleccion_id, r.id_cliente, c.nombre_cliente,
    r.estado, r.created_at, r.entregada_at,
    r.cedis_observaciones,
    (SELECT COALESCE(json_agg(json_build_object(
      'sku', ri.sku, 'cantidad', ri.cantidad,
      'producto', m.descripcion
    )), '[]'::json)
    FROM recolecciones_items ri
    LEFT JOIN medicamentos m ON m.sku = ri.sku
    WHERE ri.recoleccion_id = r.recoleccion_id) as items
  FROM recolecciones r
  JOIN clientes c ON c.id_cliente = r.id_cliente
  WHERE (p_is_admin OR r.id_usuario = p_id_usuario)
    AND (p_id_cliente IS NULL OR r.id_cliente = p_id_cliente)
  ORDER BY r.created_at DESC
  LIMIT p_limit;
$$;

GRANT EXECUTE ON FUNCTION chatbot.get_recolecciones_usuario(VARCHAR, VARCHAR, INT, BOOLEAN) TO service_role;

NOTIFY pgrst, 'reload schema';
