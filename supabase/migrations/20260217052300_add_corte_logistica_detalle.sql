-- Ensure get_corte_actual_rango exists (dependency, safe CREATE OR REPLACE)
CREATE OR REPLACE FUNCTION public.get_corte_actual_rango()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_fin date;
  v_fecha_inicio date;
  v_prev_fecha date;
BEGIN
  SELECT MAX(fecha_movimiento::date) INTO v_fecha_fin
  FROM movimientos_inventario;

  v_fecha_inicio := v_fecha_fin;

  FOR v_prev_fecha IN
    SELECT DISTINCT fecha_movimiento::date
    FROM movimientos_inventario
    WHERE fecha_movimiento::date <= v_fecha_fin
    ORDER BY fecha_movimiento::date DESC
  LOOP
    IF v_fecha_inicio - v_prev_fecha > 3 THEN
      EXIT;
    END IF;
    v_fecha_inicio := v_prev_fecha;
  END LOOP;

  RETURN QUERY SELECT v_fecha_inicio, v_fecha_fin, (v_fecha_fin - v_fecha_inicio + 1)::int;
END;
$function$;

-- Logística detalle: one row per VENTA/RECOLECCION movement in the current corte
CREATE OR REPLACE FUNCTION public.get_corte_logistica_detalle()
 RETURNS TABLE(
  nombre_asesor text,
  nombre_cliente character varying,
  id_cliente character varying,
  fecha_visita text,
  sku character varying,
  producto character varying,
  cantidad_colocada integer,
  qty_venta integer,
  qty_recoleccion integer,
  total_corte integer,
  destino text,
  saga_estado text,
  odv_botiquin text,
  odv_venta text,
  recoleccion_id uuid,
  recoleccion_estado text,
  evidencia_paths text[],
  firma_path text,
  observaciones text,
  quien_recibio text
 )
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
    u.nombre::text                                                          AS nombre_asesor,
    c.nombre_cliente,
    mov.id_cliente,
    TO_CHAR(mov.fecha_movimiento, 'YYYY-MM-DD')                            AS fecha_visita,
    mov.sku,
    med.producto,
    COALESCE(inv.cantidad_disponible, 0)::int                              AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquín: Zoho link from the LEVANTAMIENTO_INICIAL saga for this client
    (SELECT szl.zoho_id
     FROM saga_zoho_links szl
     JOIN saga_transactions st_lev ON szl.id_saga_transaction = st_lev.id
     WHERE st_lev.id_cliente = mov.id_cliente
       AND st_lev.tipo = 'LEVANTAMIENTO_INICIAL'
       AND st_lev.cancelada = false
     LIMIT 1)                                                              AS odv_botiquin,
    -- ODV Venta: from the saga transaction that generated this movement
    st.zoho_odv_id::text                                                   AS odv_venta,
    -- Recoleccion details (only populated for RECOLECCION movements)
    rcl.recoleccion_id,
    rcl.estado::text                                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM recolecciones_evidencias re
     WHERE re.recoleccion_id = rcl.recoleccion_id)                         AS evidencia_paths,
    (SELECT rf.storage_path
     FROM recolecciones_firmas rf
     WHERE rf.recoleccion_id = rcl.recoleccion_id
     LIMIT 1)                                                              AS firma_path,
    rcl.cedis_observaciones                                                AS observaciones,
    rcl.cedis_responsable_nombre                                           AS quien_recibio
  FROM movimientos_inventario mov
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN saga_transactions st ON mov.id_saga_transaction = st.id
  LEFT JOIN visitas v            ON st.visit_id = v.visit_id
  LEFT JOIN usuarios u           ON v.id_usuario = u.id_usuario
  LEFT JOIN inventario_botiquin inv ON mov.id_cliente = inv.id_cliente AND mov.sku = inv.sku
  LEFT JOIN recolecciones rcl    ON v.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo IN ('VENTA', 'RECOLECCION')
  ORDER BY mov.fecha_movimiento DESC, c.nombre_cliente, mov.sku;
END;
$function$;
