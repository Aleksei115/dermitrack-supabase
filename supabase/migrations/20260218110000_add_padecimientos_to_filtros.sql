-- Add padecimientos to the corteFiltros RPC so the filter panel
-- can populate the padecimientos dropdown from actual corte data.
CREATE OR REPLACE FUNCTION public.get_corte_filtros_disponibles()
RETURNS TABLE(marcas character varying[], medicos jsonb, padecimientos character varying[])
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;

  RETURN QUERY
  SELECT
    ARRAY_AGG(DISTINCT med.marca)::varchar[] as marcas,
    jsonb_agg(DISTINCT jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)) as medicos,
    ARRAY_AGG(DISTINCT pad.nombre)::varchar[] as padecimientos
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  LEFT JOIN medicamento_padecimientos mp ON mov.sku = mp.sku
  LEFT JOIN padecimientos pad ON mp.padecimiento_id = pad.id
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin;
END;
$function$;

NOTIFY pgrst, 'reload schema';
