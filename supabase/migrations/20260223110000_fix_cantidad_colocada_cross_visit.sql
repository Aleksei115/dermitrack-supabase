-- Fix cantidad_colocada in Corte > Logística tab
-- Bug: subquery restricted to current corte visit (cv.visit_id), but original
-- placements (CREACION) happen in a PREVIOUS visit — either VISITA_LEVANTAMIENTO_INICIAL
-- (first corte) or a previous VISITA_CORTE's LEV_POST_CORTE (subsequent cortes).
-- Fix: prev_placement_visits CTE finds the most recent completed visit of ANY type
-- with CREACION movements, before the current corte visit, per client.

CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos   varchar[] DEFAULT NULL,
  p_marcas    varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor     text,
  nombre_cliente    varchar,
  id_cliente        varchar,
  fecha_visita      text,
  sku               varchar,
  producto          varchar,
  cantidad_colocada integer,
  qty_venta         integer,
  qty_recoleccion   integer,
  total_corte       integer,
  destino           text,
  saga_estado       text,
  odv_botiquin      text,
  odv_venta         text,
  recoleccion_id    uuid,
  recoleccion_estado text,
  evidencia_paths   text[],
  firma_path        text,
  observaciones     text,
  quien_recibio     text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.id_cliente
    FROM (
      SELECT DISTINCT ON (v.id_cliente) v.id_cliente, v.estado
      FROM visitas v
      JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
      WHERE v.estado NOT IN ('PROGRAMADO')
        AND NOT (v.estado = 'CANCELADO' AND v.completed_at IS NULL)
      ORDER BY v.id_cliente, v.corte_number DESC
    ) sub
    WHERE sub.estado = 'CANCELADO'
  ),
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.id_usuario,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.id_cliente NOT IN (SELECT id_cliente FROM voided_clients)
      AND (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  -- Find the most recent completed visit (any type) with CREACION movements,
  -- completed before the current corte visit, per client.
  -- Covers both VISITA_LEVANTAMIENTO_INICIAL (first corte) and previous VISITA_CORTE.
  prev_placement_visits AS (
    SELECT DISTINCT ON (cv.id_cliente)
      cv.id_cliente,
      v.visit_id
    FROM current_visits cv
    JOIN visitas v_cur ON v_cur.visit_id = cv.visit_id
    JOIN visitas v ON v.id_cliente = cv.id_cliente
      AND v.visit_id != cv.visit_id
      AND v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND v.completed_at < v_cur.completed_at
    WHERE EXISTS (
      SELECT 1 FROM saga_transactions st
      JOIN movimientos_inventario mi ON mi.id_saga_transaction = st.id
      WHERE st.visit_id = v.visit_id
        AND mi.tipo = 'CREACION'
        AND mi.id_cliente = cv.id_cliente
    )
    ORDER BY cv.id_cliente, v.completed_at DESC
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  )
  SELECT
    u.nombre::text                                                          AS nombre_asesor,
    c.nombre_cliente,
    mov.id_cliente,
    TO_CHAR(cv.fecha_visita, 'YYYY-MM-DD')                                 AS fecha_visita,
    mov.sku,
    med.producto,
    (SELECT COALESCE(SUM(m_cre.cantidad), 0)
     FROM movimientos_inventario m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = ppv.visit_id
       AND m_cre.id_cliente = mov.id_cliente
       AND m_cre.sku = mov.sku
       AND m_cre.tipo = 'CREACION')::int                                    AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_transaction
       FROM movimientos_inventario m_cre
       WHERE m_cre.id_cliente = mov.id_cliente
         AND m_cre.sku = mov.sku
         AND m_cre.tipo = 'CREACION'
         AND m_cre.fecha_movimiento <= mov.fecha_movimiento
         AND EXISTS (
           SELECT 1 FROM saga_zoho_links szl_chk
           WHERE szl_chk.id_saga_transaction = m_cre.id_saga_transaction
             AND szl_chk.tipo = 'BOTIQUIN'
             AND szl_chk.zoho_id IS NOT NULL
         )
       ORDER BY m_cre.fecha_movimiento DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = latest_cre.id_saga_transaction
       AND szl.tipo = 'BOTIQUIN'
       AND szl.zoho_id IS NOT NULL
       AND EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       ))                                                                   AS odv_botiquin,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM saga_transactions st_mov
     JOIN saga_transactions st_ven ON st_ven.visit_id = st_mov.visit_id
       AND st_ven.tipo = 'VENTA'
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = st_ven.id
       AND szl.tipo = 'VENTA'
       AND szl.zoho_id IS NOT NULL
     WHERE st_mov.id = mov.id_saga_transaction
       AND (szl.items IS NULL OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       )))                                                                  AS odv_venta,
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
  FROM current_visits cv
  LEFT JOIN prev_placement_visits ppv ON ppv.id_cliente = cv.id_cliente
  JOIN saga_transactions st ON st.visit_id = cv.visit_id
  JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN usuarios u   ON cv.id_usuario = u.id_usuario
  LEFT JOIN recolecciones rcl ON cv.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.tipo IN ('VENTA', 'RECOLECCION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY c.nombre_cliente, mov.sku;
END;
$function$;

-- Public wrapper (pass-through, unchanged signature)
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos   varchar[] DEFAULT NULL,
  p_marcas    varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL
)
RETURNS TABLE(
  nombre_asesor     text,
  nombre_cliente    varchar,
  id_cliente        varchar,
  fecha_visita      text,
  sku               varchar,
  producto          varchar,
  cantidad_colocada integer,
  qty_venta         integer,
  qty_recoleccion   integer,
  total_corte       integer,
  destino           text,
  saga_estado       text,
  odv_botiquin      text,
  odv_venta         text,
  recoleccion_id    uuid,
  recoleccion_estado text,
  evidencia_paths   text[],
  firma_path        text,
  observaciones     text,
  quien_recibio     text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos);
$function$;

NOTIFY pgrst, 'reload schema';
