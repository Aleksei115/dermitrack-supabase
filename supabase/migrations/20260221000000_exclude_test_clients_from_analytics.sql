-- Exclude TEST_% clients from analytics filter dropdowns
-- TEST_OWNER_001 (Dr. Test Owner) stays in DB but is hidden from all analytics

-- 1. public.get_filtros_disponibles() — filter dropdowns
CREATE OR REPLACE FUNCTION public.get_filtros_disponibles()
 RETURNS TABLE(marcas character varying[], medicos jsonb, padecimientos character varying[], fecha_primer_levantamiento date)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
     FROM medicamentos m WHERE m.marca IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
     FROM clientes c WHERE c.activo = true AND c.id_cliente NOT LIKE 'TEST_%'),
    (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
     FROM padecimientos p)::varchar[],
    (SELECT MIN(fecha_movimiento)::date
     FROM movimientos_inventario WHERE tipo = 'CREACION');
END;
$function$;

-- 2. analytics.get_dashboard_static() — consolidated dashboard static data
CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filtros json;
  v_stats json;
BEGIN
  SELECT row_to_json(f) INTO v_filtros
  FROM (
    SELECT
      (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
       FROM medicamentos m WHERE m.marca IS NOT NULL) AS marcas,
      (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
       FROM clientes c WHERE c.activo = true AND c.id_cliente NOT LIKE 'TEST_%') AS medicos,
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
    FROM get_corte_stats_generales_con_comparacion() r
    LIMIT 1
  ) s;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats
  );
END;
$function$;

-- 3. Re-create public wrapper for get_dashboard_static
CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$;

-- Permissions
GRANT EXECUTE ON FUNCTION public.get_filtros_disponibles() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_dashboard_static() TO authenticated, anon;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
