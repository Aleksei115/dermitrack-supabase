-- RPC: get_corte_logistica_detalle
-- Returns one row per SKU per doctor's corte visit.
-- Follows: latest visita_corte → saga_transactions → movimientos + recolecciones → evidencias/firmas.

CREATE OR REPLACE FUNCTION public.get_corte_logistica_detalle()
RETURNS TABLE (
  nombre_asesor       TEXT,
  nombre_cliente      TEXT,
  id_cliente          TEXT,
  fecha_visita        DATE,
  sku                 TEXT,
  producto            TEXT,
  cantidad_colocada   INT,
  qty_venta           INT,
  qty_recoleccion     INT,
  total_corte         INT,
  destino             TEXT,
  saga_estado         TEXT,
  odv_botiquin        TEXT,
  odv_venta           TEXT,
  recoleccion_id      TEXT,
  recoleccion_estado  TEXT,
  evidencia_paths     TEXT[],
  firma_path          TEXT,
  observaciones       TEXT,
  quien_recibio       TEXT
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
AS $$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Get current corte period
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;

  RETURN QUERY
  WITH
  -- Latest corte visit per doctor
  latest_visits AS (
    SELECT DISTINCT ON (v.id_cliente)
      v.visit_id,
      v.id_cliente,
      v.id_usuario,
      v.created_at::date AS fecha_visita
    FROM visitas v
    JOIN clientes cl ON cl.id_cliente = v.id_cliente AND cl.activo = TRUE
    WHERE v.created_at::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND v.estado NOT IN ('CANCELADO')
    ORDER BY v.id_cliente, v.created_at DESC
  ),

  -- All sagas per visit
  visit_sagas AS (
    SELECT
      lv.visit_id,
      lv.id_cliente,
      st.id AS saga_id,
      st.tipo AS saga_tipo,
      st.estado AS saga_estado
    FROM latest_visits lv
    JOIN saga_transactions st ON st.visit_id = lv.visit_id
  ),

  -- CREACION movements (from LEV_POST_CORTE sagas)
  creaciones AS (
    SELECT
      vs.id_cliente,
      mi.sku,
      SUM(mi.cantidad)::int AS cantidad_colocada,
      vs.saga_id,
      vs.saga_estado
    FROM visit_sagas vs
    JOIN movimientos_inventario mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'LEV_POST_CORTE'
      AND mi.tipo = 'CREACION'
    GROUP BY vs.id_cliente, mi.sku, vs.saga_id, vs.saga_estado
  ),

  -- VENTA movements (from VENTA sagas)
  ventas AS (
    SELECT
      vs.id_cliente,
      mi.sku,
      SUM(mi.cantidad)::int AS qty_venta,
      vs.saga_id
    FROM visit_sagas vs
    JOIN movimientos_inventario mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'VENTA'
      AND mi.tipo = 'VENTA'
    GROUP BY vs.id_cliente, mi.sku, vs.saga_id
  ),

  -- Recolecciones per visit
  recol AS (
    SELECT
      r.id_cliente,
      r.recoleccion_id,
      r.estado AS recoleccion_estado,
      r.cedis_observaciones,
      r.cedis_responsable_nombre
    FROM latest_visits lv
    JOIN recolecciones r ON r.visit_id = lv.visit_id
  ),

  -- Recoleccion items aggregated
  recol_items AS (
    SELECT
      rec.id_cliente,
      ri.sku,
      SUM(ri.cantidad)::int AS qty_recoleccion,
      rec.recoleccion_id,
      rec.recoleccion_estado,
      rec.cedis_observaciones,
      rec.cedis_responsable_nombre
    FROM recol rec
    JOIN recolecciones_items ri ON ri.recoleccion_id = rec.recoleccion_id
    GROUP BY rec.id_cliente, ri.sku, rec.recoleccion_id, rec.recoleccion_estado,
             rec.cedis_observaciones, rec.cedis_responsable_nombre
  ),

  -- Zoho ODV links
  zoho_botiquin AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'LEV_POST_CORTE' AND szl.tipo = 'BOTIQUIN'
    GROUP BY vs.saga_id
  ),
  zoho_venta AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_tipo = 'VENTA' AND szl.tipo = 'VENTA'
    GROUP BY vs.saga_id
  ),

  -- Evidencias and firmas
  evidencias AS (
    SELECT re.recoleccion_id, array_agg(re.storage_path) AS paths
    FROM recolecciones_evidencias re
    GROUP BY re.recoleccion_id
  ),
  firmas AS (
    SELECT rf.recoleccion_id, rf.storage_path
    FROM recolecciones_firmas rf
  ),

  -- Combine: start from creaciones (every SKU placed), left join ventas + recolecciones
  combined AS (
    SELECT
      c.id_cliente,
      c.sku,
      c.cantidad_colocada,
      c.saga_id AS saga_creacion,
      c.saga_estado,
      COALESCE(v.qty_venta, 0) AS qty_venta,
      v.saga_id AS saga_venta,
      COALESCE(ri.qty_recoleccion, 0) AS qty_recoleccion,
      ri.recoleccion_id,
      ri.recoleccion_estado,
      ri.cedis_observaciones,
      ri.cedis_responsable_nombre
    FROM creaciones c
    LEFT JOIN ventas v ON v.id_cliente = c.id_cliente AND v.sku = c.sku
    LEFT JOIN recol_items ri ON ri.id_cliente = c.id_cliente AND ri.sku = c.sku
  )

  SELECT
    u.nombre::text                                    AS nombre_asesor,
    cl.nombre_cliente::text                           AS nombre_cliente,
    cl.id_cliente::text                               AS id_cliente,
    lv.fecha_visita                                   AS fecha_visita,
    cb.sku::text                                      AS sku,
    med.producto::text                                AS producto,
    cb.cantidad_colocada                              AS cantidad_colocada,
    cb.qty_venta                                      AS qty_venta,
    cb.qty_recoleccion                                AS qty_recoleccion,
    cb.qty_venta + cb.qty_recoleccion                 AS total_corte,
    CASE
      WHEN cb.qty_venta > 0 AND cb.qty_recoleccion > 0 THEN 'VENTA+RECOLECCION'
      WHEN cb.qty_venta > 0 THEN 'VENTA'
      WHEN cb.qty_recoleccion > 0 THEN 'RECOLECCION'
      ELSE 'PENDIENTE'
    END                                               AS destino,
    cb.saga_estado::text                              AS saga_estado,
    zb.odv                                            AS odv_botiquin,
    zv.odv                                            AS odv_venta,
    cb.recoleccion_id::text                           AS recoleccion_id,
    cb.recoleccion_estado::text                       AS recoleccion_estado,
    ev.paths                                          AS evidencia_paths,
    fi.storage_path                                   AS firma_path,
    cb.cedis_observaciones::text                      AS observaciones,
    cb.cedis_responsable_nombre::text                 AS quien_recibio
  FROM combined cb
  JOIN latest_visits lv ON lv.id_cliente = cb.id_cliente
  JOIN clientes cl ON cl.id_cliente = cb.id_cliente
  JOIN medicamentos med ON med.sku = cb.sku
  LEFT JOIN usuarios u ON u.id_usuario = lv.id_usuario
  LEFT JOIN zoho_botiquin zb ON zb.saga_id = cb.saga_creacion
  LEFT JOIN zoho_venta zv ON zv.saga_id = cb.saga_venta
  LEFT JOIN evidencias ev ON ev.recoleccion_id = cb.recoleccion_id
  LEFT JOIN firmas fi ON fi.recoleccion_id = cb.recoleccion_id
  ORDER BY cl.nombre_cliente, cb.sku;
END;
$$;
