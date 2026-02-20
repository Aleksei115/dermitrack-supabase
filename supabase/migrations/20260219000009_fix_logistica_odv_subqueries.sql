-- Migration: Fix get_corte_logistica_data() ODV subqueries
--
-- Changes:
-- 1. Populate items on BOTIQUIN ODVs where items IS NULL (38 of 51 rows, Sep-Dec 2025)
--    by deriving from CREACION movements of the same saga
-- 2. odv_botiquin: trace per-SKU to most recent CREACION of that SKU for the same client,
--    then find the BOTIQUIN ODV from that saga (cross-visit tracing)
-- 3. odv_venta: visit-based join filtered by SKU via items jsonb (unchanged)

-- ============================================================
-- Step 1: Populate items in BOTIQUIN ODVs with items=NULL
-- Derives items from CREACION movements of the same saga_transaction
-- ============================================================
UPDATE saga_zoho_links szl
SET items = sub.derived_items
FROM (
  SELECT szl2.id, jsonb_agg(
    jsonb_build_object('sku', m.sku, 'cantidad', m.cantidad)
    ORDER BY m.sku
  ) as derived_items
  FROM saga_zoho_links szl2
  JOIN movimientos_inventario m ON m.id_saga_transaction = szl2.id_saga_transaction
    AND m.tipo = 'CREACION'
  WHERE szl2.tipo = 'BOTIQUIN'
    AND szl2.zoho_id IS NOT NULL
    AND szl2.items IS NULL
  GROUP BY szl2.id
) sub
WHERE szl.id = sub.id;

-- ============================================================
-- Step 2: Analytics function (main implementation)
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

-- ============================================================
-- Public wrapper (SECURITY DEFINER for PostgREST access)
-- ============================================================
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
