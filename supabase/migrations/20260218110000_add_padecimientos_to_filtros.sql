-- Add padecimientos array to get_corte_filtros_disponibles return type
CREATE OR REPLACE FUNCTION public.get_corte_filtros_disponibles()
RETURNS TABLE(marcas varchar[], medicos jsonb, padecimientos varchar[])
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
    ARRAY_AGG(DISTINCT med.padecimiento)::varchar[] as padecimientos
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin;
END;
$function$;
