-- Migration: Fix RECOLECCION odv_botiquin tracing + cleanup duplicate PERMANENCIA sagas
--
-- Fix 1: Delete orphan duplicate PERMANENCIA sagas (sagas with no movements
--         where the same visit already has another PERMANENCIA saga with movements).
-- Fix 2: RECOLECCION movements should trace odv_botiquin to the CREACION from
--         the PREVIOUS cycle, never from the same date. Uses strict < for RECOLECCION
--         while keeping <= for VENTA (which can sell items placed same visit).

-- ============================================================
-- Fix 1: Delete orphan duplicate PERMANENCIA sagas
-- ============================================================
DELETE FROM saga_transactions
WHERE tipo = 'PERMANENCIA'
  AND id NOT IN (
    SELECT DISTINCT id_saga_transaction
    FROM movimientos_inventario
    WHERE id_saga_transaction IS NOT NULL
  )
  AND visit_id IN (
    SELECT visit_id FROM saga_transactions
    WHERE tipo = 'PERMANENCIA'
    GROUP BY visit_id
    HAVING COUNT(*) > 1
  );

-- ============================================================
-- Fix 2: Update analytics function — strict date for RECOLECCION
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
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
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
#variable_conflict use_column
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Date range: params or auto-detect
  IF p_fecha_inicio IS NOT NULL AND p_fecha_fin IS NOT NULL THEN
    v_fecha_inicio := p_fecha_inicio;
    v_fecha_fin := p_fecha_fin;
  ELSE
    SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
    FROM get_corte_actual_rango() r;
  END IF;

  RETURN QUERY
  WITH
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
    TO_CHAR(mov.fecha_movimiento, 'YYYY-MM-DD')                            AS fecha_visita,
    mov.sku,
    med.producto,
    -- Cantidad colocada: CREACION de ese SKU en la misma visita
    (SELECT COALESCE(SUM(m_cre.cantidad), 0)
     FROM movimientos_inventario m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = st.visit_id
       AND m_cre.id_cliente = mov.id_cliente
       AND m_cre.sku = mov.sku
       AND m_cre.tipo = 'CREACION')::int                                    AS cantidad_colocada,
    CASE WHEN mov.tipo = 'VENTA'       THEN mov.cantidad ELSE 0 END        AS qty_venta,
    CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END        AS qty_recoleccion,
    mov.cantidad                                                           AS total_corte,
    mov.tipo::text                                                         AS destino,
    st.estado::text                                                        AS saga_estado,
    -- ODV Botiquin: trace per-SKU to most recent CREACION with a BOTIQUIN ODV (cross-visit)
    -- FIX: RECOLECCION uses strict < (must trace to PREVIOUS cycle, not same date)
    --      VENTA keeps <= (can sell items placed in same visit)
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_transaction
       FROM movimientos_inventario m_cre
       WHERE m_cre.id_cliente = mov.id_cliente
         AND m_cre.sku = mov.sku
         AND m_cre.tipo = 'CREACION'
         AND m_cre.fecha_movimiento <=
           CASE WHEN mov.tipo = 'RECOLECCION'
             THEN mov.fecha_movimiento - interval '1 second'
             ELSE mov.fecha_movimiento
           END
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
         WHERE elem->>'sku' = mov.sku
       ))                                                                   AS odv_botiquin,
    -- ODV Venta: visit-based join, per-SKU filtering via items jsonb
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
         WHERE elem->>'sku' = mov.sku
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
  FROM movimientos_inventario mov
  JOIN clientes c        ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med  ON mov.sku = med.sku
  LEFT JOIN saga_transactions st ON mov.id_saga_transaction = st.id
  LEFT JOIN visitas v            ON st.visit_id = v.visit_id
  LEFT JOIN usuarios u           ON v.id_usuario = u.id_usuario
  LEFT JOIN recolecciones rcl    ON v.visit_id = rcl.visit_id AND mov.id_cliente = rcl.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo IN ('VENTA', 'RECOLECCION')
    AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY mov.fecha_movimiento DESC, c.nombre_cliente, mov.sku;
END;
$$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_logistica_data(
  p_medicos varchar[] DEFAULT NULL,
  p_marcas varchar[] DEFAULT NULL,
  p_padecimientos varchar[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
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
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.get_corte_logistica_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$$;

NOTIFY pgrst, 'reload schema';
