-- ============================================================================
-- PHASE 5: Recreate Analytics Functions (analytics schema)
-- ============================================================================
-- All function bodies updated with new English table/column/enum names.
-- ============================================================================

CREATE OR REPLACE FUNCTION analytics.clasificacion_base(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, padecimiento character varying, brand character varying, es_top boolean, m_type text, first_event_date date, revenue_botiquin numeric, revenue_odv numeric, cantidad_odv numeric, num_transacciones_odv bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  saga_odv_ids AS (
    SELECT DISTINCT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  m1_pairs AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_venta,
           SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS revenue_botiquin
    FROM inventory_movements mi JOIN medications m ON m.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id, mi.sku
  ),
  m2_agg AS (
    SELECT mp.client_id, mp.sku, COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv, COUNT(v.*) AS num_transacciones_odv
    FROM m1_pairs mp JOIN odv_sales v ON v.client_id = mp.client_id AND v.sku = mp.sku AND v.date >= mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    GROUP BY mp.client_id, mp.sku HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  ),
  m3_candidates AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi WHERE mi.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
      AND NOT EXISTS (SELECT 1 FROM inventory_movements mi2 WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE')
    GROUP BY mi.client_id, mi.sku
  ),
  m3_agg AS (
    SELECT mc.client_id, mc.sku, mc.first_creacion, COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv, COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc JOIN odv_sales v ON v.client_id = mc.client_id AND v.sku = mc.sku AND v.date >= mc.first_creacion
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
      AND NOT EXISTS (SELECT 1 FROM odv_sales v2 WHERE v2.client_id = mc.client_id AND v2.sku = mc.sku AND v2.date <= mc.first_creacion)
    GROUP BY mc.client_id, mc.sku, mc.first_creacion HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  )
  SELECT mp.client_id::varchar, c.client_name::varchar, mp.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M1'::text, mp.first_venta,
    mp.revenue_botiquin, 0::numeric, 0::numeric, 0::bigint
  FROM m1_pairs mp JOIN clients c ON c.client_id = mp.client_id JOIN medications m ON m.sku = mp.sku LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku
  UNION ALL
  SELECT m2.client_id::varchar, c.client_name::varchar, m2.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M2'::text, mp.first_venta,
    mp.revenue_botiquin, m2.revenue_odv, m2.cantidad_odv, m2.num_transacciones_odv
  FROM m2_agg m2 JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id JOIN medications m ON m.sku = m2.sku LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku
  UNION ALL
  SELECT m3.client_id::varchar, c.client_name::varchar, m3.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M3'::text, m3.first_creacion,
    0::numeric, m3.revenue_odv, m3.cantidad_odv, m3.num_transacciones_odv
  FROM m3_agg m3 JOIN clients c ON c.client_id = m3.client_id JOIN medications m ON m.sku = m3.sku LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_client_audit(p_client character varying)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_cliente json;
  v_visitas json;
  v_ciclo json;
  v_grafo_nodos json;
  v_grafo_aristas json;
  v_resumen json;
  v_anomalias_count int := 0;
BEGIN
  -- 1. Client Info
  SELECT json_build_object('id', c.client_id, 'name', c.client_name)
  INTO v_cliente
  FROM clients c
  WHERE c.client_id = p_client;

  IF v_cliente IS NULL THEN
    RETURN json_build_object('error', 'Cliente no encontrado');
  END IF;

  -- ── _av: All completed visits with sagas (even if 0 movements) ──
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    COALESCE(v.completed_at, v.created_at)::date as fecha_visita,
    COALESCE(v.type::text, 'DESCONOCIDO') as tipo_visita,
    ROW_NUMBER() OVER (ORDER BY v.corte_number, v.created_at, v.visit_id) as visit_num
  FROM visits v
  WHERE v.client_id = p_client
    AND v.status = 'COMPLETED'
    AND EXISTS (SELECT 1 FROM saga_transactions st WHERE st.visit_id = v.visit_id);

  -- 2. Visitas with sagas, movements, anomalies
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'visita_tipo', av.tipo_visita,
      'sagas', (
        SELECT COALESCE(json_agg(sr ORDER BY sr->>'saga_type'), '[]'::json)
        FROM (
          SELECT json_build_object(
            'saga_type', st.type::text,
            'saga_status', st.status::text,
            'odv_botiquin', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.type = 'CABINET'
                AND szl.zoho_id IS NOT NULL
            ),
            'odv_venta', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.type = 'SALE'
                AND szl.zoho_id IS NOT NULL
            ),
            'sync_status', (
              SELECT szl.zoho_sync_status
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.zoho_id IS NOT NULL
              LIMIT 1
            ),
            'movimientos', (
              SELECT COALESCE(json_agg(
                json_build_object(
                  'mov_id', m.id,
                  'sku', m.sku,
                  'product', med.product,
                  'type', m.type::text,
                  'quantity', m.quantity,
                  'date', TO_CHAR(m.movement_date, 'YYYY-MM-DD'),
                  'zoho_link_id', m.id_saga_zoho_link,
                  'odv', (SELECT szl2.zoho_id FROM saga_zoho_links szl2 WHERE szl2.id = m.id_saga_zoho_link)
                ) ORDER BY m.sku, m.type
              ), '[]'::json)
              FROM inventory_movements m
              JOIN medications med ON m.sku = med.sku
              WHERE m.id_saga_transaction = st.id
                AND m.client_id = p_client
            ),
            'anomalias', (
              SELECT COALESCE(json_agg(d.msg), '[]'::json)
              FROM (
                SELECT 'MOVIMIENTO_DUPLICADO: ' || m.sku || ' ' || m.type::text
                       || ' aparece ' || COUNT(*) || ' veces' as msg
                FROM inventory_movements m
                WHERE m.id_saga_transaction = st.id
                  AND m.client_id = p_client
                GROUP BY m.sku, m.type
                HAVING COUNT(*) > 1
              ) d
            )
          ) as sr
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
        ) saga_sub
      ),
      'inventario_piezas', (
        SELECT COALESCE(SUM(
          CASE m.type
            WHEN 'PLACEMENT' THEN m.quantity
            WHEN 'SALE' THEN -m.quantity
            WHEN 'COLLECTION' THEN -m.quantity
            ELSE 0
          END
        ), 0)
        FROM inventory_movements m
        JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m.client_id = p_client
          AND av2.fecha_visita <= av.fecha_visita
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
      ),
      'inventario_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM inventory_movements m
          JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m.client_id = p_client
            AND av2.fecha_visita <= av.fecha_visita
            AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
          GROUP BY m.sku
          HAVING SUM(
            CASE m.type
              WHEN 'PLACEMENT' THEN m.quantity
              WHEN 'SALE' THEN -m.quantity
              WHEN 'COLLECTION' THEN -m.quantity
              ELSE 0
            END
          ) > 0
        ) sc
      ),
      'anomalias', (
        SELECT COALESCE(json_agg(va.msg), '[]'::json)
        FROM (
          SELECT 'PERMANENCIA_DUPLICADA: ' || COUNT(*)
                 || ' sagas PERMANENCIA' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.type = 'HOLDING'
            AND EXISTS (
              SELECT 1 FROM inventory_movements m
              WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
            )
          HAVING COUNT(*) > 1

          UNION ALL

          SELECT 'ODV_MISSING: saga ' || st.type::text || ' sin ODV BOTIQUIN' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.type IN ('INITIAL_PLACEMENT', 'CUTOFF_RENEWAL', 'POST_CUTOFF_PLACEMENT')
            AND EXISTS (
              SELECT 1 FROM inventory_movements m
              WHERE m.id_saga_transaction = st.id
                AND m.client_id = p_client
                AND m.type = 'PLACEMENT'
            )
            AND NOT EXISTS (
              SELECT 1 FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.type = 'CABINET'
                AND szl.zoho_id IS NOT NULL
            )

          UNION ALL

          SELECT 'SYNC_PENDING: ODV ' || szl.zoho_id
                 || ' status=' || COALESCE(szl.zoho_sync_status, 'null') as msg
          FROM saga_zoho_links szl
          JOIN saga_transactions st ON szl.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND szl.zoho_id IS NOT NULL
            AND COALESCE(szl.zoho_sync_status, '') != 'synced'
            AND EXISTS (
              SELECT 1 FROM inventory_movements m
              WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
            )

          UNION ALL

          SELECT 'VTA_SIN_CREACION: ' || m.sku || ' ' || m.type::text
                 || ' sin CREACION previa en botiquin' as msg
          FROM inventory_movements m
          JOIN saga_transactions st ON m.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND m.client_id = p_client
            AND m.type IN ('SALE', 'COLLECTION')
            AND NOT EXISTS (
              SELECT 1
              FROM inventory_movements m2
              JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
              JOIN _av av2 ON st2.visit_id = av2.visit_id
              WHERE m2.client_id = p_client
                AND m2.sku = m.sku
                AND m2.type = 'PLACEMENT'
                AND av2.visit_num <= av.visit_num
            )
        ) va
      )
    ) as vr
    FROM _av av
  ) visit_rows;

  -- 3. SKU Lifecycle
  SELECT COALESCE(json_agg(sr ORDER BY sr->>'sku'), '[]'::json)
  INTO v_ciclo
  FROM (
    SELECT json_build_object(
      'sku', sub.sku,
      'product', sub.product,
      'eventos', sub.eventos,
      'estado_actual', CASE
        WHEN sub.last_tipo = 'COLLECTION' THEN 'RECOLECTADO'
        WHEN sub.last_tipo = 'SALE' THEN 'VENDIDO'
        ELSE 'ACTIVE'
      END
    ) as sr
    FROM (
      SELECT
        m.sku,
        MAX(med.product) as product,
        json_agg(
          json_build_object(
            'visit_num', av.visit_num,
            'date', TO_CHAR(m.movement_date, 'YYYY-MM-DD'),
            'type', m.type::text,
            'quantity', m.quantity,
            'odv', (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.id = m.id_saga_zoho_link)
          ) ORDER BY av.visit_num, m.type
        ) as eventos,
        (
          SELECT m2.type::text
          FROM inventory_movements m2
          JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m2.client_id = p_client AND m2.sku = m.sku
            AND m2.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
          ORDER BY av2.visit_num DESC, m2.movement_date DESC
          LIMIT 1
        ) as last_tipo
      FROM inventory_movements m
      JOIN medications med ON m.sku = med.sku
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.client_id = p_client
      GROUP BY m.sku
    ) sub
  ) ciclo_rows;

  -- 4. Graph Nodes
  SELECT COALESCE(json_agg(n ORDER BY n->>'id'), '[]'::json)
  INTO v_grafo_nodos
  FROM (
    -- Visit nodes
    SELECT json_build_object(
      'id', 'v' || av.visit_num,
      'type', 'visita',
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'label', 'V' || av.visit_num || ' ' || TO_CHAR(av.fecha_visita, 'Mon DD'),
      'visita_tipo', av.tipo_visita
    ) as n
    FROM _av av

    UNION ALL

    -- ODV Botiquin nodes
    SELECT json_build_object(
      'id', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'odv',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'piezas', SUM(m.quantity),
      'skus_count', COUNT(DISTINCT m.sku)
    ) as n
    FROM inventory_movements m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND szl.type = 'CABINET'
      AND szl.zoho_id IS NOT NULL
      AND m.type = 'PLACEMENT'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- ODV Venta nodes (NEW)
    SELECT json_build_object(
      'id', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'odv_venta',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'piezas', SUM(m.quantity),
      'skus_count', COUNT(DISTINCT m.sku)
    ) as n
    FROM inventory_movements m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
      AND m.type = 'SALE'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- SKU nodes
    SELECT DISTINCT ON (m.sku)
      json_build_object(
        'id', 'sku-' || m.sku,
        'type', 'sku',
        'label', m.sku,
        'product', med.product
      ) as n
    FROM inventory_movements m
    JOIN medications med ON m.sku = med.sku
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
  ) all_nodes;

  -- 5. Graph Edges
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- CREACION: ODV Botiquin → Visit
    SELECT json_build_object(
      'source', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'target', 'v' || av.visit_num,
      'type', 'PLACEMENT',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND m.type = 'PLACEMENT'
      AND szl.type = 'CABINET'
      AND szl.zoho_id IS NOT NULL
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- PERMANENCIA: Visit → Next Visit
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'v' || (av.visit_num + 1),
      'type', 'HOLDING',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'HOLDING'
      AND EXISTS (SELECT 1 FROM _av av2 WHERE av2.visit_num = av.visit_num + 1)
    GROUP BY av.visit_num

    UNION ALL

    -- RECOLECCION: Visit → REC sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'rec-v' || av.visit_num,
      'type', 'COLLECTION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'COLLECTION'
    GROUP BY av.visit_num

    UNION ALL

    -- VENTA: Visit → VTA sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'vta-v' || av.visit_num,
      'type', 'SALE',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'SALE'
    GROUP BY av.visit_num

    UNION ALL

    -- ODV_VENTA: VTA sink → ODV Venta node (NEW)
    SELECT json_build_object(
      'source', 'vta-v' || av.visit_num,
      'target', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'ODV_VENTA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND m.type = 'SALE'
      AND szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- SKU-level edges
    SELECT json_build_object(
      'source', CASE
        WHEN m.type = 'PLACEMENT' THEN 'sku-' || m.sku
        ELSE 'v' || av.visit_num
      END,
      'target', CASE
        WHEN m.type = 'PLACEMENT' THEN 'v' || av.visit_num
        ELSE 'sku-' || m.sku
      END,
      'type', 'sku_' || LOWER(m.type::text),
      'label', SUBSTR(m.type::text, 1, 3) || '(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
    GROUP BY m.sku, m.type, av.visit_num
  ) all_edges;

  -- 6. Count anomalies
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.type = 'HOLDING'
      AND EXISTS (
        SELECT 1 FROM inventory_movements m
        WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
      )
    GROUP BY av.visit_id
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
    GROUP BY st.id, m.sku, m.type
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.type IN ('INITIAL_PLACEMENT', 'CUTOFF_RENEWAL', 'POST_CUTOFF_PLACEMENT')
      AND EXISTS (
        SELECT 1 FROM inventory_movements m
        WHERE m.id_saga_transaction = st.id
          AND m.client_id = p_client
          AND m.type = 'PLACEMENT'
      )
      AND NOT EXISTS (
        SELECT 1 FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = st.id
          AND szl.type = 'CABINET'
          AND szl.zoho_id IS NOT NULL
      )

    UNION ALL

    SELECT 1
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND m.type IN ('SALE', 'COLLECTION')
      AND NOT EXISTS (
        SELECT 1
        FROM inventory_movements m2
        JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m2.client_id = p_client
          AND m2.sku = m.sku
          AND m2.type = 'PLACEMENT'
          AND av2.visit_num <= av.visit_num
      )
    GROUP BY m.sku

    UNION ALL

    SELECT 1
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE szl.zoho_id IS NOT NULL
      AND COALESCE(szl.zoho_sync_status, '') != 'synced'
      AND EXISTS (
        SELECT 1 FROM inventory_movements m
        WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
      )
  ) anomalies;

  -- 7. Summary
  SELECT json_build_object(
    'total_visitas', (SELECT COUNT(*) FROM _av),
    'total_skus_historico', (
      SELECT COUNT(DISTINCT m.sku)
      FROM inventory_movements m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.client_id = p_client
    ),
    'inventario_actual_piezas', (
      SELECT COALESCE(SUM(
        CASE m.type
          WHEN 'PLACEMENT' THEN m.quantity
          WHEN 'SALE' THEN -m.quantity
          WHEN 'COLLECTION' THEN -m.quantity
          ELSE 0
        END
      ), 0)
      FROM inventory_movements m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.client_id = p_client
        AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
    ),
    'inventario_actual_skus', (
      SELECT COUNT(*) FROM (
        SELECT m.sku
        FROM inventory_movements m
        JOIN saga_transactions st ON m.id_saga_transaction = st.id
        JOIN _av av ON st.visit_id = av.visit_id
        WHERE m.client_id = p_client
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
        GROUP BY m.sku
        HAVING SUM(
          CASE m.type
            WHEN 'PLACEMENT' THEN m.quantity
            WHEN 'SALE' THEN -m.quantity
            WHEN 'COLLECTION' THEN -m.quantity
            ELSE 0
          END
        ) > 0
      ) active
    ),
    'total_anomalias', v_anomalias_count,
    'todas_odv_botiquin', (
      SELECT COALESCE(json_agg(DISTINCT szl.zoho_id ORDER BY szl.zoho_id), '[]'::json)
      FROM saga_zoho_links szl
      JOIN saga_transactions st ON szl.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE szl.zoho_id IS NOT NULL AND szl.type = 'CABINET'
        AND EXISTS (
          SELECT 1 FROM inventory_movements m
          WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
        )
    ),
    'todas_odv_venta', (
      SELECT COALESCE(json_agg(DISTINCT szl.zoho_id ORDER BY szl.zoho_id), '[]'::json)
      FROM saga_zoho_links szl
      JOIN saga_transactions st ON szl.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE szl.zoho_id IS NOT NULL AND szl.type = 'SALE'
        AND EXISTS (
          SELECT 1 FROM inventory_movements m
          WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
        )
    )
  ) INTO v_resumen;

  -- 8. Return combined result
  RETURN json_build_object(
    'cliente', v_cliente,
    'visits', COALESCE(v_visitas, '[]'::json),
    'ciclo_vida_skus', COALESCE(v_ciclo, '[]'::json),
    'grafo', json_build_object(
      'nodos', COALESCE(v_grafo_nodos, '[]'::json),
      'aristas', COALESCE(v_grafo_aristas, '[]'::json)
    ),
    'resumen', v_resumen
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_balance_metrics()
 RETURNS TABLE(concepto text, valor_creado numeric, valor_ventas numeric, valor_recoleccion numeric, valor_permanencia_entrada numeric, valor_permanencia_virtual numeric, valor_calculado_total numeric, diferencia numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH metricas_inventario AS (
    SELECT
      SUM(inv.available_quantity * COALESCE(inv.unit_price, 0)) as total_stock_vivo
    FROM cabinet_inventory inv
    JOIN medications med ON inv.sku = med.sku
    JOIN clients c ON inv.client_id = c.client_id
    WHERE c.active = TRUE
  ),
  metricas_movimientos AS (
    SELECT
      SUM(CASE WHEN mov.type = 'PLACEMENT'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_creado_historico,
      SUM(CASE WHEN mov.type = 'HOLDING'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_permanencia_entrada,
      SUM(CASE WHEN mov.type = 'SALE'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_ventas,
      SUM(CASE WHEN mov.type = 'COLLECTION'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_recoleccion
    FROM inventory_movements mov
    JOIN medications med ON mov.sku = med.sku
  )
  SELECT
    'BALANCE_GLOBAL_SISTEMA'::TEXT as concepto,
    COALESCE(M.total_creado_historico, 0) as valor_creado,
    COALESCE(M.total_ventas, 0) as valor_ventas,
    COALESCE(M.total_recoleccion, 0) as valor_recoleccion,
    COALESCE(M.total_permanencia_entrada, 0) as valor_permanencia_entrada,
    COALESCE(I.total_stock_vivo, 0) as valor_permanencia_virtual,
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as valor_calculado_total,
    COALESCE(M.total_creado_historico, 0) -
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as diferencia
  FROM metricas_inventario I
  CROSS JOIN metricas_movimientos M;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cabinet_data()
 RETURNS TABLE(sku character varying, id_movimiento bigint, movement_type text, quantity integer, movement_date text, id_lote text, intake_date text, cantidad_inicial integer, available_quantity integer, client_id character varying, client_name character varying, tier character varying, avg_billing numeric, total_billing numeric, product character varying, price numeric, brand character varying, top boolean, padecimiento character varying)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    med.sku,
    mov.id as id_movimiento,
    CAST(mov.type AS TEXT) AS movement_type,
    mov.quantity,
    TO_CHAR(mov.movement_date, 'DD/MM/YYYY') AS movement_date,
    mov.id::TEXT as id_lote,
    TO_CHAR(mov.movement_date, 'DD/MM/YYYY') AS intake_date,
    COALESCE(inv.available_quantity, 0)::INTEGER as cantidad_inicial,
    COALESCE(inv.available_quantity, 0)::INTEGER as available_quantity,
    mov.client_id,
    c.client_name,
    c.tier,
    c.avg_billing,
    c.total_billing,
    med.product,
    mov.unit_price AS price,
    med.brand,
    med.top,
    p.name as padecimiento
  FROM inventory_movements mov
  JOIN clients c ON mov.client_id = c.client_id
  JOIN medications med ON mov.sku = med.sku
  LEFT JOIN cabinet_inventory inv
    ON mov.client_id = inv.client_id AND mov.sku = inv.sku
  LEFT JOIN medication_conditions mp ON mov.sku = mp.sku
  LEFT JOIN conditions p ON mp.condition_id = p.condition_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_brand_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(brand character varying, valor numeric, piezas integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  )
  SELECT m.brand,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor,
         SUM(mi.quantity)::int AS piezas
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY m.brand
  ORDER BY valor DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_conversion_details(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(m_type text, client_id character varying, client_name character varying, sku character varying, product character varying, fecha_botiquin date, fecha_primera_odv date, dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric, valor_botiquin numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT b.m_type::text, b.client_id, b.client_name, b.sku, b.product,
    b.first_event_date, odv_first.first_odv, (odv_first.first_odv - b.first_event_date)::int,
    b.num_transacciones_odv, b.cantidad_odv::bigint, b.revenue_odv, b.revenue_botiquin
  FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
  JOIN LATERAL (
    SELECT MIN(v.date) AS first_odv FROM odv_sales v
    WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date >= b.first_event_date
      AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
  ) odv_first ON true
  WHERE b.m_type IN ('M2', 'M3')
  ORDER BY b.revenue_odv DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_conversion_metrics(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric, valor_botiquin numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date))
  SELECT (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
         (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
         COALESCE((SELECT SUM(revenue_odv) FROM base WHERE m_type = 'M2'), 0)::numeric,
         COALESCE((SELECT SUM(revenue_botiquin) FROM base WHERE m_type = 'M2'), 0)::numeric;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_current_cutoff_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  -- Identify clients whose current cycle was voided (completed then cancelled)
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  -- Rank all completed visits per active doctor using corte_number
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.client_id,
      v.completed_at::date AS fecha_visita,
      v.corte_number,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  ),
  -- All active clients for pending detection
  all_active_clients AS (
    SELECT c.client_id, c.client_name
    FROM clients c
    WHERE c.active = TRUE
      AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ),
  -- Padecimiento dedup (1:1 per sku)
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  -- SKUs passing brand + padecimiento filters
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  -- Current corte movements via saga_transactions from current_visits
  current_mov AS (
    SELECT mov.client_id, ac.client_name, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.client_id = mov.client_id
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Previous corte movements via saga_transactions from prev_visits
  prev_mov AS (
    SELECT mov.client_id, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM prev_visits pv
    JOIN saga_transactions st ON st.visit_id = pv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI aggregation (current corte)
  kpi_stats AS (
    SELECT
      COALESCE(COUNT(DISTINCT mov.client_id), 0)::int AS total_medicos_visitados,
      COALESCE(COUNT(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.client_id END), 0)::int AS medicos_con_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM current_mov mov
  ),
  -- Previous corte totals for % change (consistent filters)
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM prev_mov mov
  ),
  -- Previous per-medico venta (for VS Anterior column)
  prev_medico_stats AS (
    SELECT mov.client_id,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_venta
    FROM prev_mov mov
    GROUP BY mov.client_id
  ),
  -- Visited doctors with movement data
  visited_medico_rows AS (
    SELECT
      mov.client_id,
      mov.client_name,
      cv.fecha_visita::text AS fecha_visita,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS valor_recoleccion,
      STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', ') AS skus_vendidos,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0 AS tiene_venta,
      pms.valor_venta AS valor_venta_anterior,
      CASE
        WHEN pms.valor_venta IS NOT NULL AND pms.valor_venta > 0
          THEN ROUND(((COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0) - pms.valor_venta) / pms.valor_venta * 100)::numeric, 1)
        WHEN (pms.valor_venta IS NULL OR pms.valor_venta = 0)
          AND COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0
          THEN 100.0
        ELSE NULL
      END AS porcentaje_cambio
    FROM current_mov mov
    JOIN current_visits cv ON cv.client_id = mov.client_id
    LEFT JOIN prev_medico_stats pms ON mov.client_id = pms.client_id
    GROUP BY mov.client_id, mov.client_name, cv.fecha_visita, pms.valor_venta
  ),
  -- Pending doctors: active clients without a current completed visit
  pending_medico_rows AS (
    SELECT
      ac.client_id,
      ac.client_name,
      NULL::text AS fecha_visita,
      0::int AS piezas_venta,
      0::int AS piezas_creacion,
      0::int AS piezas_recoleccion,
      0::numeric AS valor_venta,
      0::numeric AS valor_creacion,
      0::numeric AS valor_recoleccion,
      NULL::text AS skus_vendidos,
      false AS tiene_venta,
      NULL::numeric AS valor_venta_anterior,
      NULL::numeric AS porcentaje_cambio
    FROM all_active_clients ac
    WHERE NOT EXISTS (SELECT 1 FROM current_visits cv WHERE cv.client_id = ac.client_id)
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'fecha_inicio', (SELECT MIN(cv.fecha_visita) FROM current_visits cv),
      'fecha_fin', (SELECT MAX(cv.fecha_visita) FROM current_visits cv),
      'dias_corte', COALESCE((SELECT MAX(cv.fecha_visita) - MIN(cv.fecha_visita) + 1 FROM current_visits cv), 0),
      'total_medicos_visitados', k.total_medicos_visitados,
      'medicos_con_venta', k.medicos_con_venta,
      'medicos_sin_venta', k.total_medicos_visitados - k.medicos_con_venta,
      'piezas_venta', k.piezas_venta,
      'valor_venta', k.valor_venta,
      'piezas_creacion', k.piezas_creacion,
      'valor_creacion', k.valor_creacion,
      'piezas_recoleccion', k.piezas_recoleccion,
      'valor_recoleccion', k.valor_recoleccion,
      'porcentaje_cambio_venta',
        CASE WHEN p.valor_venta > 0
          THEN ROUND(((k.valor_venta - p.valor_venta) / p.valor_venta * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_creacion',
        CASE WHEN p.valor_creacion > 0
          THEN ROUND(((k.valor_creacion - p.valor_creacion) / p.valor_creacion * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_recoleccion',
        CASE WHEN p.valor_recoleccion > 0
          THEN ROUND(((k.valor_recoleccion - p.valor_recoleccion) / p.valor_recoleccion * 100)::numeric, 1)
          ELSE NULL END
    ),
    'medicos', COALESCE(
      (SELECT json_agg(row_to_json(sub) ORDER BY sub.fecha_visita IS NULL ASC, sub.valor_venta DESC, sub.client_name)
       FROM (
         SELECT * FROM visited_medico_rows
         UNION ALL
         SELECT * FROM pending_medico_rows
       ) sub),
      '[]'::json)
  ) INTO v_result
  FROM kpi_stats k
  CROSS JOIN prev_stats p;

  RETURN v_result;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_current_cutoff_range()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH latest_per_client AS (
    SELECT DISTINCT ON (v.client_id)
      v.created_at::date AS created_date,
      v.completed_at::date AS fecha_completado
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
    ORDER BY v.client_id, v.completed_at DESC
  )
  SELECT
    MIN(lpc.created_date)  AS fecha_inicio,
    MAX(lpc.fecha_completado) AS fecha_fin,
    COALESCE(MAX(lpc.fecha_completado) - MIN(lpc.created_date) + 1, 0)::int AS dias_corte
  FROM latest_per_client lpc;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_previous_cutoff_stats()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, client_id character varying, client_name character varying, valor_venta numeric, piezas_venta integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE
  v_corte_actual_inicio date;
  v_prev_inicio date;
  v_prev_fin date;
BEGIN
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM analytics.get_current_cutoff_range() r;

  SELECT MIN(sq.created_date), MAX(sq.fecha_completado)
  INTO v_prev_inicio, v_prev_fin
  FROM (
    SELECT DISTINCT ON (v.client_id)
      v.created_at::date AS created_date,
      v.completed_at::date AS fecha_completado
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at::date < v_corte_actual_inicio
    ORDER BY v.client_id, v.completed_at DESC
  ) sq;

  IF v_prev_inicio IS NULL THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT
    v_prev_inicio,
    v_prev_fin,
    c.client_id,
    c.client_name,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int
  FROM inventory_movements mov
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON mov.client_id = c.client_id
  WHERE mov.movement_date::date BETWEEN v_prev_inicio AND v_prev_fin
  GROUP BY c.client_id, c.client_name;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_available_filters()
 RETURNS TABLE(marcas character varying[], medicos jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  SELECT
    ARRAY_AGG(DISTINCT med.brand)::varchar[] as marcas,
    jsonb_agg(DISTINCT jsonb_build_object('id', c.client_id, 'name', c.client_name)) as medicos
  FROM inventory_movements mov
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON mov.client_id = c.client_id
  WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_historical_cutoff_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM inventory_movements mov
    WHERE mov.type = 'SALE'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM inventory_movements mov
    WHERE mov.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.available_quantity * COALESCE(inv.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_unicos
    FROM cabinet_inventory inv
    JOIN medications med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE inv.available_quantity > 0
      AND (p_doctors IS NULL OR inv.client_id = ANY(p_doctors))
      AND (p_brands IS NULL OR med.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM inventory_movements mov
    WHERE mov.type = 'COLLECTION'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.client_id,
      MIN(mov.movement_date::date) AS fecha_visita
    FROM inventory_movements mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.type = 'SALE'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mov.id_saga_transaction, mov.client_id
  ),
  visita_rows AS (
    SELECT
      c.client_id,
      c.client_name,
      vb.fecha_visita::text AS fecha_visita,
      COUNT(DISTINCT mov.sku)::int AS skus_unicos,
      COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor_venta,
      COALESCE(SUM(mov.quantity), 0)::int AS piezas_venta
    FROM visitas_base vb
    JOIN inventory_movements mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN clients c ON vb.client_id = c.client_id
    WHERE mov.type = 'SALE'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR vb.fecha_visita >= p_start_date)
      AND (p_end_date IS NULL OR vb.fecha_visita <= p_end_date)
    GROUP BY c.client_id, c.client_name, vb.fecha_visita
    ORDER BY vb.fecha_visita ASC, c.client_name
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'valor_venta_m1', (SELECT valor FROM kpi_venta_m1),
      'skus_venta_m1', (SELECT skus_unicos FROM kpi_venta_m1),
      'valor_creacion', (SELECT valor FROM kpi_creacion),
      'skus_creacion', (SELECT skus_unicos FROM kpi_creacion),
      'stock_activo', (SELECT valor FROM kpi_stock),
      'skus_stock', (SELECT skus_unicos FROM kpi_stock),
      'valor_recoleccion', (SELECT valor FROM kpi_recoleccion),
      'skus_recoleccion', (SELECT skus_unicos FROM kpi_recoleccion)
    ),
    'visits', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_logistics_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(nombre_asesor text, client_name character varying, client_id character varying, fecha_visita text, sku character varying, product character varying, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_status text, odv_botiquin text, odv_venta text, recoleccion_id uuid, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.client_id,
      v.user_id,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.type = 'VISIT_CUTOFF'
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  -- Find the most recent completed visit (any type) with CREACION movements,
  -- completed before the current corte visit, per client.
  -- Covers both VISITA_LEVANTAMIENTO_INICIAL (first corte) and previous VISITA_CORTE.
  prev_placement_visits AS (
    SELECT DISTINCT ON (cv.client_id)
      cv.client_id,
      v.visit_id
    FROM current_visits cv
    JOIN visits v_cur ON v_cur.visit_id = cv.visit_id
    JOIN visits v ON v.client_id = cv.client_id
      AND v.visit_id != cv.visit_id
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at < v_cur.completed_at
    WHERE EXISTS (
      SELECT 1 FROM saga_transactions st
      JOIN inventory_movements mi ON mi.id_saga_transaction = st.id
      WHERE st.visit_id = v.visit_id
        AND mi.type = 'PLACEMENT'
        AND mi.client_id = cv.client_id
    )
    ORDER BY cv.client_id, v.completed_at DESC
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  )
  SELECT
    u.name::text                                                          AS nombre_asesor,
    c.client_name,
    mov.client_id,
    TO_CHAR(cv.fecha_visita, 'YYYY-MM-DD')                                 AS fecha_visita,
    mov.sku,
    med.product,
    (SELECT COALESCE(SUM(m_cre.quantity), 0)
     FROM inventory_movements m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = ppv.visit_id
       AND m_cre.client_id = mov.client_id
       AND m_cre.sku = mov.sku
       AND m_cre.type = 'PLACEMENT')::int                                    AS cantidad_colocada,
    CASE WHEN mov.type = 'SALE'       THEN mov.quantity ELSE 0 END        AS qty_venta,
    CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END        AS qty_recoleccion,
    mov.quantity                                                           AS total_corte,
    mov.type::text                                                         AS destino,
    st.status::text                                                        AS saga_status,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_transaction
       FROM inventory_movements m_cre
       WHERE m_cre.client_id = mov.client_id
         AND m_cre.sku = mov.sku
         AND m_cre.type = 'PLACEMENT'
         AND m_cre.movement_date <= mov.movement_date
         AND EXISTS (
           SELECT 1 FROM saga_zoho_links szl_chk
           WHERE szl_chk.id_saga_transaction = m_cre.id_saga_transaction
             AND szl_chk.type = 'CABINET'
             AND szl_chk.zoho_id IS NOT NULL
         )
       ORDER BY m_cre.movement_date DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = latest_cre.id_saga_transaction
       AND szl.type = 'CABINET'
       AND szl.zoho_id IS NOT NULL
       AND EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       ))                                                                   AS odv_botiquin,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM saga_transactions st_mov
     JOIN saga_transactions st_ven ON st_ven.visit_id = st_mov.visit_id
       AND st_ven.type = 'SALE'
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = st_ven.id
       AND szl.type = 'SALE'
       AND szl.zoho_id IS NOT NULL
     WHERE st_mov.id = mov.id_saga_transaction
       AND (szl.items IS NULL OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       )))                                                                  AS odv_venta,
    rcl.recoleccion_id,
    rcl.status::text                                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM collection_evidence re
     WHERE re.recoleccion_id = rcl.recoleccion_id)                         AS evidencia_paths,
    (SELECT rf.storage_path
     FROM collection_signatures rf
     WHERE rf.recoleccion_id = rcl.recoleccion_id
     LIMIT 1)                                                              AS firma_path,
    rcl.cedis_observations                                                AS observaciones,
    rcl.cedis_responsible_name                                           AS quien_recibio
  FROM current_visits cv
  LEFT JOIN prev_placement_visits ppv ON ppv.client_id = cv.client_id
  JOIN saga_transactions st ON st.visit_id = cv.visit_id
  JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
  JOIN clients c        ON mov.client_id = c.client_id
  JOIN medications med  ON mov.sku = med.sku
  LEFT JOIN users u   ON cv.user_id = u.user_id
  LEFT JOIN collections rcl ON cv.visit_id = rcl.visit_id AND mov.client_id = rcl.client_id
  WHERE mov.type IN ('SALE', 'COLLECTION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY c.client_name, mov.sku;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_logistics_detail()
 RETURNS TABLE(nombre_asesor text, client_name text, client_id text, fecha_visita date, sku text, product text, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_status text, odv_botiquin text, odv_venta text, recoleccion_id text, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH
  latest_visits AS (
    SELECT DISTINCT ON (v.client_id)
      v.visit_id,
      v.client_id,
      v.user_id,
      v.created_at::date AS fecha_visita
    FROM visits v
    JOIN clients cl ON cl.client_id = v.client_id AND cl.active = TRUE
    WHERE v.created_at::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND v.status NOT IN ('CANCELLED')
    ORDER BY v.client_id, v.created_at DESC
  ),
  visit_sagas AS (
    SELECT
      lv.visit_id,
      lv.client_id,
      st.id AS saga_id,
      st.type AS saga_type,
      st.status AS saga_status
    FROM latest_visits lv
    JOIN saga_transactions st ON st.visit_id = lv.visit_id
  ),
  creaciones AS (
    SELECT
      vs.client_id,
      mi.sku,
      SUM(mi.quantity)::int AS cantidad_colocada,
      vs.saga_id,
      vs.saga_status
    FROM visit_sagas vs
    JOIN inventory_movements mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'POST_CUTOFF_PLACEMENT'
      AND mi.type = 'PLACEMENT'
    GROUP BY vs.client_id, mi.sku, vs.saga_id, vs.saga_status
  ),
  ventas AS (
    SELECT
      vs.client_id,
      mi.sku,
      SUM(mi.quantity)::int AS qty_venta,
      vs.saga_id
    FROM visit_sagas vs
    JOIN inventory_movements mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'SALE'
      AND mi.type = 'SALE'
    GROUP BY vs.client_id, mi.sku, vs.saga_id
  ),
  recol AS (
    SELECT
      r.client_id,
      r.recoleccion_id,
      r.status AS recoleccion_estado,
      r.cedis_observations,
      r.cedis_responsible_name
    FROM latest_visits lv
    JOIN collections r ON r.visit_id = lv.visit_id
  ),
  recol_items AS (
    SELECT
      rec.client_id,
      ri.sku,
      SUM(ri.quantity)::int AS qty_recoleccion,
      rec.recoleccion_id,
      rec.recoleccion_estado,
      rec.cedis_observations,
      rec.cedis_responsible_name
    FROM recol rec
    JOIN collection_items ri ON ri.recoleccion_id = rec.recoleccion_id
    GROUP BY rec.client_id, ri.sku, rec.recoleccion_id, rec.recoleccion_estado,
             rec.cedis_observations, rec.cedis_responsible_name
  ),
  zoho_botiquin AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'POST_CUTOFF_PLACEMENT' AND szl.type = 'CABINET'
    GROUP BY vs.saga_id
  ),
  zoho_venta AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'SALE' AND szl.type = 'SALE'
    GROUP BY vs.saga_id
  ),
  evidencias AS (
    SELECT re.recoleccion_id, array_agg(re.storage_path) AS paths
    FROM collection_evidence re
    GROUP BY re.recoleccion_id
  ),
  firmas AS (
    SELECT rf.recoleccion_id, rf.storage_path
    FROM collection_signatures rf
  ),
  combined AS (
    SELECT
      c.client_id,
      c.sku,
      c.cantidad_colocada,
      c.saga_id AS saga_creacion,
      c.saga_status,
      COALESCE(v.qty_venta, 0) AS qty_venta,
      v.saga_id AS saga_venta,
      COALESCE(ri.qty_recoleccion, 0) AS qty_recoleccion,
      ri.recoleccion_id,
      ri.recoleccion_estado,
      ri.cedis_observations,
      ri.cedis_responsible_name
    FROM creaciones c
    LEFT JOIN ventas v ON v.client_id = c.client_id AND v.sku = c.sku
    LEFT JOIN recol_items ri ON ri.client_id = c.client_id AND ri.sku = c.sku
  )
  SELECT
    u.name::text                                    AS nombre_asesor,
    cl.client_name::text                           AS client_name,
    cl.client_id::text                               AS client_id,
    lv.fecha_visita                                   AS fecha_visita,
    cb.sku::text                                      AS sku,
    med.product::text                                AS product,
    cb.cantidad_colocada                              AS cantidad_colocada,
    cb.qty_venta                                      AS qty_venta,
    cb.qty_recoleccion                                AS qty_recoleccion,
    cb.qty_venta + cb.qty_recoleccion                 AS total_corte,
    CASE
      WHEN cb.qty_venta > 0 AND cb.qty_recoleccion > 0 THEN 'VENTA+RECOLECCION'
      WHEN cb.qty_venta > 0 THEN 'SALE'
      WHEN cb.qty_recoleccion > 0 THEN 'COLLECTION'
      ELSE 'PENDING'
    END                                               AS destino,
    cb.saga_status::text                              AS saga_status,
    zb.odv                                            AS odv_botiquin,
    zv.odv                                            AS odv_venta,
    cb.recoleccion_id::text                           AS recoleccion_id,
    cb.recoleccion_estado::text                       AS recoleccion_estado,
    ev.paths                                          AS evidencia_paths,
    fi.storage_path                                   AS firma_path,
    cb.cedis_observations::text                      AS observaciones,
    cb.cedis_responsible_name::text                 AS quien_recibio
  FROM combined cb
  JOIN latest_visits lv ON lv.client_id = cb.client_id
  JOIN clients cl ON cl.client_id = cb.client_id
  JOIN medications med ON med.sku = cb.sku
  LEFT JOIN users u ON u.user_id = lv.user_id
  LEFT JOIN zoho_botiquin zb ON zb.saga_id = cb.saga_creacion
  LEFT JOIN zoho_venta zv ON zv.saga_id = cb.saga_venta
  LEFT JOIN evidencias ev ON ev.recoleccion_id = cb.recoleccion_id
  LEFT JOIN firmas fi ON fi.recoleccion_id = cb.recoleccion_id
  ORDER BY cl.client_name, cb.sku;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_skus_value_per_visit(p_client_id character varying DEFAULT NULL::character varying, p_brand character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, brand character varying)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  SELECT
    c.client_id,
    c.client_name,
    mov.movement_date::date as fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0) as valor_venta,
    med.brand
  FROM inventory_movements mov
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON mov.client_id = c.client_id
  WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.type = 'SALE'
    AND (p_client_id IS NULL OR c.client_id = p_client_id)
    AND (p_brand IS NULL OR med.brand = p_brand)
  GROUP BY c.client_id, c.client_name, mov.movement_date::date, med.brand
  ORDER BY valor_venta DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_general_stats()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH medicos_visitados AS (
    SELECT DISTINCT mov.client_id
    FROM inventory_movements mov
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_con_venta AS (
    SELECT DISTINCT mov.client_id
    FROM inventory_movements mov
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.type = 'SALE'
  ),
  stats AS (
    SELECT
      COUNT(*)::int as total_mov,
      SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END)::int as pz_venta,
      SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END)::int as pz_creacion,
      SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END)::int as pz_recoleccion,
      SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as val_venta,
      SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as val_creacion,
      SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as val_recoleccion
    FROM inventory_movements mov
    JOIN medications med ON mov.sku = med.sku
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    (v_fecha_fin - v_fecha_inicio + 1)::int,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta)
  FROM stats s;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_general_stats_with_comparison()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_dias_corte int;
  v_prev_inicio date;
  v_prev_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin, r.dias_corte
  INTO v_fecha_inicio, v_fecha_fin, v_dias_corte
  FROM analytics.get_current_cutoff_range() r;

  SELECT MIN(sq.created_date), MAX(sq.fecha_completado)
  INTO v_prev_inicio, v_prev_fin
  FROM (
    SELECT DISTINCT ON (v.client_id)
      v.created_at::date AS created_date,
      v.completed_at::date AS fecha_completado
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at::date < v_fecha_inicio
    ORDER BY v.client_id, v.completed_at DESC
  ) sq;

  IF v_prev_inicio IS NOT NULL THEN
    SELECT
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_prev_inicio AND v_prev_fin;

    SELECT COUNT(DISTINCT mov.client_id)
    INTO v_ant_medicos_con_venta
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_prev_inicio AND v_prev_fin
      AND mov.type = 'SALE';
  END IF;

  RETURN QUERY
  WITH
  current_movements AS (
    SELECT mov.*
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_visitados AS (
    SELECT DISTINCT cm.client_id FROM current_movements cm
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT cm.client_id FROM current_movements cm WHERE cm.type = 'SALE'
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int AS total_mov,
      SUM(CASE WHEN cm.type = 'SALE' THEN cm.quantity ELSE 0 END)::int AS pz_venta,
      SUM(CASE WHEN cm.type = 'PLACEMENT' THEN cm.quantity ELSE 0 END)::int AS pz_creacion,
      SUM(CASE WHEN cm.type = 'COLLECTION' THEN cm.quantity ELSE 0 END)::int AS pz_recoleccion,
      SUM(CASE WHEN cm.type = 'SALE' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_venta,
      SUM(CASE WHEN cm.type = 'PLACEMENT' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_creacion,
      SUM(CASE WHEN cm.type = 'COLLECTION' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_recoleccion
    FROM current_movements cm
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    v_dias_corte,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    v_ant_val_venta,
    v_ant_val_creacion,
    v_ant_val_recoleccion,
    CASE WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
      THEN v_ant_val_venta / v_ant_medicos_con_venta
      ELSE NULL
    END,
    CASE WHEN v_ant_val_venta IS NOT NULL AND v_ant_val_venta > 0
      THEN ROUND(((COALESCE(s.val_venta, 0) - v_ant_val_venta) / v_ant_val_venta * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_creacion IS NOT NULL AND v_ant_val_creacion > 0
      THEN ROUND(((COALESCE(s.val_creacion, 0) - v_ant_val_creacion) / v_ant_val_creacion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_recoleccion IS NOT NULL AND v_ant_val_recoleccion > 0
      THEN ROUND(((COALESCE(s.val_recoleccion, 0) - v_ant_val_recoleccion) / v_ant_val_recoleccion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE
      WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
           AND (SELECT COUNT(*)::int FROM medicos_con_venta_actual) > 0
           AND v_ant_val_venta > 0 THEN
        ROUND((
          (COALESCE(s.val_venta, 0) / (SELECT COUNT(*)::int FROM medicos_con_venta_actual)) -
          (v_ant_val_venta / v_ant_medicos_con_venta)
        ) / (v_ant_val_venta / v_ant_medicos_con_venta) * 100, 1)
      ELSE NULL
    END
  FROM stats_actual s;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_stats_by_doctor()
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, skus_creados text, skus_recolectados text, tiene_venta boolean)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH visitas_en_corte AS (
    SELECT DISTINCT
      mov.client_id,
      mov.id_saga_transaction,
      MIN(mov.movement_date::date) as fecha_saga
    FROM inventory_movements mov
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.id_saga_transaction IS NOT NULL
    GROUP BY mov.client_id, mov.id_saga_transaction
  )
  SELECT
    c.client_id,
    c.client_name,
    MAX(v.fecha_saga) as fecha_visita,
    SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END)::int,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'PLACEMENT' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'COLLECTION' THEN mov.sku END, ', '),
    SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END) > 0
  FROM visitas_en_corte v
  JOIN inventory_movements mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON v.client_id = c.client_id
  GROUP BY c.client_id, c.client_name
  ORDER BY SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) DESC,
           c.client_name;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cutoff_stats_by_doctor_with_comparison()
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, tiene_venta boolean, valor_venta_anterior numeric, porcentaje_cambio numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH corte_actual AS (
    SELECT * FROM analytics.get_cutoff_stats_by_doctor()
  ),
  corte_anterior AS (
    SELECT * FROM analytics.get_previous_cutoff_stats()
  )
  SELECT
    ca.client_id,
    ca.client_name,
    ca.fecha_visita,
    ca.piezas_venta,
    ca.piezas_creacion,
    ca.piezas_recoleccion,
    ca.valor_venta,
    ca.valor_creacion,
    ca.valor_recoleccion,
    ca.skus_vendidos,
    ca.tiene_venta,
    COALESCE(cp.valor_venta, 0) as valor_venta_anterior,
    CASE
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta > 0 THEN 100.00
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta = 0 THEN 0.00
      ELSE ROUND(((ca.valor_venta - COALESCE(cp.valor_venta, 0)) / cp.valor_venta * 100), 1)
    END as porcentaje_cambio
  FROM corte_actual ca
  LEFT JOIN corte_anterior cp ON ca.client_id = cp.client_id
  ORDER BY ca.valor_venta DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_crosssell_significance()
 RETURNS TABLE(exposed_total integer, exposed_with_crosssell integer, exposed_conversion_pct numeric, unexposed_total integer, unexposed_with_crosssell integer, unexposed_conversion_pct numeric, chi_squared numeric, significancia text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_a int;
  v_b int;
  v_c int;
  v_d int;
  v_n int;
  v_chi numeric;
BEGIN
  WITH exposure AS (
    SELECT mi.client_id, mp.condition_id,
           MIN(mi.movement_date::date) AS first_exposure
    FROM inventory_movements mi
    JOIN medication_conditions mp ON mp.sku = mi.sku
    WHERE mi.type = 'PLACEMENT'
    GROUP BY mi.client_id, mp.condition_id
  ),
  all_combos AS (
    SELECT d.client_id, p.condition_id
    FROM (SELECT DISTINCT client_id FROM odv_sales) d
    CROSS JOIN (SELECT DISTINCT condition_id FROM medication_conditions) p
  ),
  analysis AS (
    SELECT ac.client_id, ac.condition_id,
           e.first_exposure IS NOT NULL AS is_exposed,
           EXISTS (
             SELECT 1 FROM odv_sales v
             JOIN medication_conditions mp ON mp.sku = v.sku
               AND mp.condition_id = ac.condition_id
             WHERE v.client_id = ac.client_id
               AND e.first_exposure IS NOT NULL
               AND v.date > e.first_exposure
               AND NOT EXISTS (
                 SELECT 1 FROM inventory_movements mi2
                 WHERE mi2.client_id = ac.client_id AND mi2.sku = v.sku
               )
               AND NOT EXISTS (
                 SELECT 1 FROM odv_sales v2
                 WHERE v2.client_id = ac.client_id AND v2.sku = v.sku
                   AND v2.date <= e.first_exposure
               )
               AND v.odv_id NOT IN (
                 SELECT szl.zoho_id FROM saga_zoho_links szl
                 WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL
               )
           ) AS has_cross_sell
    FROM all_combos ac
    LEFT JOIN exposure e ON e.client_id = ac.client_id
      AND e.condition_id = ac.condition_id
  )
  SELECT
    COUNT(*) FILTER (WHERE is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE is_exposed AND NOT has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND NOT has_cross_sell)
  INTO v_a, v_b, v_c, v_d
  FROM analysis;

  v_n := v_a + v_b + v_c + v_d;

  IF (v_a + v_b) > 0 AND (v_c + v_d) > 0 AND (v_a + v_c) > 0 AND (v_b + v_d) > 0 THEN
    v_chi := v_n::numeric
      * POWER(GREATEST(ABS(v_a::numeric * v_d - v_b::numeric * v_c) - v_n::numeric / 2, 0), 2)
      / ((v_a + v_b)::numeric * (v_c + v_d) * (v_a + v_c) * (v_b + v_d));
  ELSE
    v_chi := 0;
  END IF;

  RETURN QUERY SELECT
    (v_a + v_b)::int,
    v_a,
    CASE WHEN (v_a + v_b) > 0
      THEN ROUND(v_a::numeric / (v_a + v_b) * 100, 1)
      ELSE 0::numeric END,
    (v_c + v_d)::int,
    v_c,
    CASE WHEN (v_c + v_d) > 0
      THEN ROUND(v_c::numeric / (v_c + v_d) * 100, 1)
      ELSE 0::numeric END,
    ROUND(v_chi, 2),
    CASE
      WHEN v_chi > 10.83 THEN 'ALTA (p < 0.001)'
      WHEN v_chi > 6.64 THEN 'MEDIA (p < 0.01)'
      WHEN v_chi > 3.84 THEN 'BAJA (p < 0.05)'
      ELSE 'NO SIGNIFICATIVA'
    END;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_dashboard_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
WITH
sku_padecimiento AS (
  SELECT DISTINCT ON (mp.sku)
    mp.sku, p.name AS padecimiento
  FROM medication_conditions mp
  JOIN conditions p ON p.condition_id = mp.condition_id
  ORDER BY mp.sku, p.condition_id
),
filtered_skus AS (
  SELECT m.sku
  FROM medications m
  LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
  WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
    AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
),
saga_odv_ids AS (
  SELECT DISTINCT szl.zoho_id
  FROM saga_zoho_links szl
  WHERE szl.type = 'SALE'
    AND szl.zoho_id IS NOT NULL
),
m1_pairs AS (
  SELECT mi.client_id, mi.sku,
         MIN(mi.movement_date::date) AS first_venta,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS revenue_botiquin
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id, mi.sku
),
m2_agg AS (
  SELECT mp.client_id, mp.sku,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN odv_sales v ON v.client_id = mp.client_id
                    AND v.sku = mp.sku
                    AND v.date >= mp.first_venta
  WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
  GROUP BY mp.client_id, mp.sku
  HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
),
m3_candidates AS (
  SELECT mi.client_id, mi.sku,
         MIN(mi.movement_date::date) AS first_creacion
  FROM inventory_movements mi
  WHERE mi.type = 'PLACEMENT'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    AND NOT EXISTS (
      SELECT 1 FROM inventory_movements mi2
      WHERE mi2.client_id = mi.client_id
        AND mi2.sku = mi.sku
        AND mi2.type = 'SALE'
    )
  GROUP BY mi.client_id, mi.sku
),
m3_agg AS (
  SELECT mc.client_id, mc.sku, mc.first_creacion,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m3_candidates mc
  JOIN odv_sales v ON v.client_id = mc.client_id
                    AND v.sku = mc.sku
                    AND v.date >= mc.first_creacion
  WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    AND NOT EXISTS (
      SELECT 1 FROM odv_sales v2
      WHERE v2.client_id = mc.client_id
        AND v2.sku = mc.sku
        AND v2.date <= mc.first_creacion
    )
  GROUP BY mc.client_id, mc.sku, mc.first_creacion
  HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
),
clasificacion AS (
  SELECT
    mp.client_id, c.client_name, mp.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS') AS padecimiento,
    m.brand, m.top AS es_top,
    'M1'::text AS m_type,
    mp.first_venta AS first_event_date,
    mp.revenue_botiquin,
    0::numeric AS revenue_odv,
    0::numeric AS cantidad_odv,
    0::bigint AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN clients c ON c.client_id = mp.client_id
  JOIN medications m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku

  UNION ALL

  SELECT
    m2.client_id, c.client_name, m2.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.brand, m.top,
    'M2'::text,
    mp.first_venta,
    mp.revenue_botiquin,
    m2.revenue_odv,
    m2.cantidad_odv,
    m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id
  JOIN medications m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku

  UNION ALL

  SELECT
    m3.client_id, c.client_name, m3.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.brand, m.top,
    'M3'::text,
    m3.first_creacion,
    0::numeric,
    m3.revenue_odv,
    m3.cantidad_odv,
    m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clients c ON c.client_id = m3.client_id
  JOIN medications m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku
),
clasificacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT client_id, client_name, sku, product, padecimiento,
           brand, es_top, m_type, first_event_date,
           revenue_botiquin, revenue_odv, cantidad_odv, num_transacciones_odv
    FROM clasificacion
  ) r
),
impacto_m1 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev
  FROM clasificacion WHERE m_type = 'M1'
),
impacto_m2 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M2'
),
impacto_m3 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M3'
),
total_odv AS (
  SELECT COALESCE(SUM(v.quantity * v.price), 0) AS rev
  FROM odv_sales v
  WHERE (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR v.date >= p_start_date)
    AND (p_end_date IS NULL OR v.date <= p_end_date)
),
impacto_json AS (
  SELECT row_to_json(r) AS val
  FROM (
    SELECT m1.cnt AS adopciones, m1.rev AS revenue_adopciones,
           m2.cnt AS conversiones, m2.rev AS revenue_conversiones,
           m3.cnt AS exposiciones, m3.rev AS revenue_exposiciones,
           0::int AS crosssell_pares, 0::numeric AS revenue_crosssell,
           (m1.rev + m2.rev + m3.rev) AS revenue_total_impacto,
           t.rev AS revenue_total_odv,
           CASE WHEN t.rev > 0
             THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1)
             ELSE 0 END AS porcentaje_impacto
    FROM impacto_m1 m1, impacto_m2 m2, impacto_m3 m3, total_odv t
  ) r
),
movements AS (
  SELECT
    mi.client_id, mi.sku,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::bigint           AS venta_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0)       AS venta_valor,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::bigint         AS creacion_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'PLACEMENT'), 0)    AS creacion_valor,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::bigint      AS recoleccion_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'COLLECTION'), 0) AS recoleccion_valor
  FROM inventory_movements mi
  JOIN medications med ON med.sku = mi.sku
  WHERE (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id, mi.sku
),
m2_counts AS (
  SELECT cb.client_id, cb.sku,
    COUNT(*)::bigint AS conversiones_m2,
    COALESCE(SUM(cb.revenue_odv), 0) AS revenue_m2
  FROM clasificacion cb
  WHERE cb.m_type = 'M2'
  GROUP BY cb.client_id, cb.sku
),
market_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      mv.client_id, mv.sku, med.product, med.brand,
      COALESCE(sp.padecimiento, 'OTROS') AS padecimiento, med.top AS es_top,
      mv.venta_pz, mv.venta_valor,
      mv.creacion_pz, mv.creacion_valor,
      mv.recoleccion_pz, mv.recoleccion_valor,
      COALESCE(ib.available_quantity, 0)::bigint AS stock_activo_pz,
      COALESCE(m2c.conversiones_m2, 0)::bigint AS conversiones_m2,
      COALESCE(m2c.revenue_m2, 0)::numeric AS revenue_m2
    FROM movements mv
    JOIN medications med ON med.sku = mv.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
    LEFT JOIN m2_counts m2c ON m2c.client_id = mv.client_id AND m2c.sku = mv.sku
    LEFT JOIN cabinet_inventory ib ON ib.client_id = mv.client_id AND ib.sku = mv.sku
  ) r
),
conversion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r."valorGenerado" DESC), '[]'::json) AS val
  FROM (
    SELECT
      b.m_type AS "mType",
      b.client_id AS "idCliente",
      b.client_name AS "nombreCliente",
      b.sku,
      b.product,
      b.first_event_date AS "fechaBotiquin",
      odv_first.first_odv AS "fechaPrimeraOdv",
      (odv_first.first_odv - b.first_event_date)::int AS "diasConversion",
      b.num_transacciones_odv AS "numVentasOdv",
      b.cantidad_odv::bigint AS "totalPiezas",
      b.revenue_odv AS "valorGenerado",
      b.revenue_botiquin AS "valorBotiquin"
    FROM clasificacion b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_odv
      FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku
        AND v.date >= b.first_event_date
        AND v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    ) odv_first ON true
    WHERE b.m_type IN ('M2', 'M3')
  ) r
),
m1_odv_ids AS (
  SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
  FROM saga_zoho_links szl
  JOIN saga_transactions st ON szl.id_saga_transaction = st.id
  WHERE szl.type = 'SALE'
    AND szl.zoho_id IS NOT NULL
),
m1_impacto AS (
  SELECT mi.client_id,
    SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
    SUM(mi.quantity) AS m1_piezas,
    COUNT(DISTINCT mi.sku) AS m1_skus
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id
),
first_venta AS (
  SELECT client_id, sku, MIN(movement_date::date) AS first_venta
  FROM inventory_movements
  WHERE type = 'SALE'
  GROUP BY client_id, sku
),
first_creacion AS (
  SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
  FROM inventory_movements mi
  WHERE mi.type = 'PLACEMENT'
    AND NOT EXISTS (
      SELECT 1 FROM inventory_movements mi2
      WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
    )
  GROUP BY mi.client_id, mi.sku
),
prior_odv AS (
  SELECT DISTINCT v.client_id, v.sku
  FROM odv_sales v
  JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
  WHERE v.date <= fc.first_creacion
),
categorized AS (
  SELECT
    v.client_id,
    v.sku,
    v.date,
    v.quantity,
    v.quantity * v.price AS line_total,
    CASE
      WHEN m1o.odv_id IS NOT NULL THEN 'M1'
      WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
      WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
      ELSE 'UNLINKED'
    END AS categoria
  FROM odv_sales v
  LEFT JOIN m1_odv_ids m1o ON v.odv_id = m1o.odv_id AND v.client_id = m1o.client_id
  LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
  LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
  LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
  WHERE v.price > 0
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR v.date >= p_start_date)
    AND (p_end_date IS NULL OR v.date <= p_end_date)
),
fact_totals AS (
  SELECT
    client_id,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M1'), 0) AS m1_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M2'), 0) AS m2_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M3'), 0) AS m3_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'UNLINKED'), 0) AS unlinked_total,
    COALESCE(SUM(line_total), 0) AS grand_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_valor,
    COALESCE(SUM(quantity) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_piezas,
    COUNT(DISTINCT sku) FILTER (WHERE categoria IN ('M2','M3')) AS m2m3_skus
  FROM categorized
  GROUP BY client_id
),
facturacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.diff DESC), '[]'::json) AS val
  FROM (
    SELECT
      c.client_id,
      c.client_name,
      c.current_tier,
      c.tier AS rango_anterior,
      c.active,
      COALESCE(c.avg_billing, 0)::numeric AS baseline,
      COALESCE(c.current_billing, 0)::numeric AS current_billing,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m1_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m1,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m2_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m2,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m3_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m3,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.unlinked_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_unlinked,
      CASE WHEN COALESCE(c.avg_billing, 0) > 0
        THEN ROUND(((COALESCE(c.current_billing, 0) - c.avg_billing) / c.avg_billing * 100)::numeric, 1)
        ELSE NULL END AS pct_crecimiento,
      CASE WHEN t.grand_total > 0
        THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
        ELSE 0 END AS pct_vinculado,
      (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
      (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
      (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados,
      (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) AS diff
    FROM clients c
    LEFT JOIN fact_totals t ON c.client_id = t.client_id
    LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
    WHERE c.current_tier IS NOT NULL
      AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ) r
),
sankey_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      b.client_id, b.client_name, b.sku, b.product,
      b.m_type AS categoria,
      b.revenue_odv AS valor_odv,
      b.cantidad_odv,
      b.num_transacciones_odv AS num_transacciones
    FROM clasificacion b
    WHERE b.m_type IN ('M2', 'M3')
  ) r
)

SELECT json_build_object(
  'clasificacionBase', c.val,
  'impactoResumen', i.val,
  'marketAnalysis', m.val,
  'conversionDetails', cv.val,
  'facturacionComposicion', f.val,
  'sankeyFlows', s.val
)
FROM clasificacion_json c, impacto_json i, market_json m,
     conversion_json cv, facturacion_json f, sankey_json s;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filtros json;
  v_stats json;
  v_progress json;
BEGIN
  SELECT row_to_json(f) INTO v_filtros
  FROM (
    SELECT
      (SELECT ARRAY_AGG(DISTINCT m.brand ORDER BY m.brand)
       FROM medications m WHERE m.brand IS NOT NULL) AS marcas,
      (SELECT jsonb_agg(jsonb_build_object('id', c.client_id, 'name', c.client_name) ORDER BY c.client_name)
       FROM clients c WHERE c.active = true) AS medicos,
      (SELECT ARRAY_AGG(DISTINCT p.name ORDER BY p.name)
       FROM conditions p) AS conditions,
      (SELECT MIN(movement_date)::date
       FROM inventory_movements WHERE type = 'PLACEMENT') AS "fechaPrimerLevantamiento"
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
    FROM analytics.get_cutoff_general_stats_with_comparison() r
    LIMIT 1
  ) s;

  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_completados AS (
    SELECT v.client_id,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
  )
  SELECT json_build_object(
    'completaron', (SELECT COUNT(DISTINCT client_id) FROM ranked_completados WHERE rn = 1),
    'pendientes', (SELECT COUNT(*) FROM clients WHERE active = TRUE)
      - (SELECT COUNT(DISTINCT client_id) FROM ranked_completados WHERE rn = 1)
      - (SELECT COUNT(*) FROM voided_clients),
    'cancelados', (SELECT COUNT(*) FROM voided_clients),
    'total', (SELECT COUNT(*) FROM clients WHERE active = TRUE)
  ) INTO v_progress;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats,
    'corteProgress', v_progress
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_billing_composition(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, rango_anterior character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
  ),
  m1_impacto AS (
    SELECT mi.client_id,
      SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
      SUM(mi.quantity) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id
  ),
  first_venta AS (
    SELECT client_id, sku, MIN(movement_date::date) AS first_venta
    FROM inventory_movements
    WHERE type = 'SALE'
    GROUP BY client_id, sku
  ),
  first_creacion AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND NOT EXISTS (
        SELECT 1 FROM inventory_movements mi2
        WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
      )
    GROUP BY mi.client_id, mi.sku
  ),
  prior_odv AS (
    SELECT DISTINCT v.client_id, v.sku
    FROM odv_sales v
    JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    WHERE v.date <= fc.first_creacion
  ),
  categorized AS (
    SELECT
      v.client_id,
      v.sku,
      v.date,
      v.quantity,
      v.quantity * v.price AS line_total,
      CASE
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM odv_sales v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.client_id = m1.client_id
    LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
    WHERE v.price > 0
      AND v.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR v.date >= p_start_date)
      AND (p_end_date IS NULL OR v.date <= p_end_date)
  ),
  totals AS (
    SELECT
      client_id,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M1'), 0) AS m1_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M2'), 0) AS m2_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M3'), 0) AS m3_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'UNLINKED'), 0) AS unlinked_total,
      COALESCE(SUM(line_total), 0) AS grand_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_valor,
      COALESCE(SUM(quantity) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_piezas,
      COUNT(DISTINCT sku) FILTER (WHERE categoria IN ('M2','M3')) AS m2m3_skus
    FROM categorized
    GROUP BY client_id
  )
  SELECT
    c.client_id,
    c.client_name,
    c.current_tier,
    c.tier AS rango_anterior,
    c.active,
    COALESCE(c.avg_billing, 0)::numeric AS baseline,
    COALESCE(c.current_billing, 0)::numeric AS current_billing,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m1_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m1,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m2_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m2,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m3_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m3,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.unlinked_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_unlinked,
    CASE WHEN COALESCE(c.avg_billing, 0) > 0
      THEN ROUND(((COALESCE(c.current_billing, 0) - c.avg_billing) / c.avg_billing * 100)::numeric, 1)
      ELSE NULL END AS pct_crecimiento,
    CASE WHEN t.grand_total > 0
      THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
      ELSE 0 END AS pct_vinculado,
    (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
    (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clients c
  LEFT JOIN totals t ON c.client_id = t.client_id
  LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
  WHERE c.current_tier IS NOT NULL
    AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ORDER BY (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_billing_composition_legacy()
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  WITH
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
  ),
  m1_impacto AS (
    SELECT mi.client_id,
      SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
      SUM(mi.quantity) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM inventory_movements mi
    WHERE mi.type = 'SALE'
    GROUP BY mi.client_id
  ),
  first_venta AS (
    SELECT client_id, sku, MIN(movement_date::date) AS first_venta
    FROM inventory_movements
    WHERE type = 'SALE'
    GROUP BY client_id, sku
  ),
  first_creacion AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND NOT EXISTS (
        SELECT 1 FROM inventory_movements mi2
        WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
      )
    GROUP BY mi.client_id, mi.sku
  ),
  prior_odv AS (
    SELECT DISTINCT v.client_id, v.sku
    FROM odv_sales v
    JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    WHERE v.date <= fc.first_creacion
  ),
  categorized AS (
    SELECT
      v.client_id,
      v.sku,
      v.date,
      v.quantity,
      v.quantity * v.price AS line_total,
      CASE
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM odv_sales v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.client_id = m1.client_id
    LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
    WHERE v.price > 0
  ),
  totals AS (
    SELECT
      client_id,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M1'), 0) AS m1_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M2'), 0) AS m2_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M3'), 0) AS m3_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria = 'UNLINKED'), 0) AS unlinked_total,
      COALESCE(SUM(line_total), 0) AS grand_total,
      COALESCE(SUM(line_total) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_valor,
      COALESCE(SUM(quantity) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_piezas,
      COUNT(DISTINCT sku) FILTER (WHERE categoria IN ('M2','M3')) AS m2m3_skus
    FROM categorized
    GROUP BY client_id
  )
  SELECT
    c.client_id,
    c.client_name,
    c.current_tier,
    c.active,
    COALESCE(c.avg_billing, 0)::numeric AS baseline,
    COALESCE(c.current_billing, 0)::numeric AS current_billing,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m1_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m1,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m2_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m2,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.m3_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_m3,
    CASE WHEN t.grand_total > 0
      THEN ROUND((COALESCE(c.current_billing, 0) * t.unlinked_total / t.grand_total)::numeric, 2)
      ELSE 0 END AS current_unlinked,
    CASE WHEN COALESCE(c.avg_billing, 0) > 0
      THEN ROUND(((COALESCE(c.current_billing, 0) - c.avg_billing) / c.avg_billing * 100)::numeric, 1)
      ELSE NULL END AS pct_crecimiento,
    CASE WHEN t.grand_total > 0
      THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
      ELSE 0 END AS pct_vinculado,
    (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
    (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clients c
  LEFT JOIN totals t ON c.client_id = t.client_id
  LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
  WHERE c.current_tier IS NOT NULL
  ORDER BY (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_available_filters()
 RETURNS TABLE(marcas character varying[], medicos jsonb, conditions character varying[], fecha_primer_levantamiento date)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.brand ORDER BY m.brand)
     FROM medications m WHERE m.brand IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.client_id, 'name', c.client_name) ORDER BY c.client_name)
     FROM clients c WHERE c.active = true),
    (SELECT ARRAY_AGG(DISTINCT p.name ORDER BY p.name)
     FROM conditions p)::varchar[],
    (SELECT MIN(movement_date)::date
     FROM inventory_movements WHERE type = 'PLACEMENT');
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_historical_conversions_evolution(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_grouping text DEFAULT 'day'::text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(fecha_grupo date, fecha_label text, pares_total integer, pares_botiquin integer, pares_directo integer, valor_total numeric, valor_botiquin numeric, valor_directo numeric, num_transacciones integer, num_clientes integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  botiquin_linked AS (
    SELECT DISTINCT b.client_id, b.sku, b.first_event_date
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    WHERE b.m_type IN ('M1', 'M2', 'M3')
      AND (p_doctors IS NULL OR b.client_id = ANY(p_doctors))
      AND b.sku IN (SELECT fs.sku FROM filtered_skus fs)
  ),
  ventas_clasificadas AS (
    SELECT
      v.client_id, v.sku, v.date, v.quantity, v.price,
      (v.quantity * COALESCE(v.price, 0)) as valor_venta,
      CASE
        WHEN bl.client_id IS NOT NULL
             AND v.date >= bl.first_event_date THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_grouping = 'week' THEN date_trunc('week', v.date)::DATE
        ELSE v.date::DATE
      END as fecha_agrupada
    FROM odv_sales v
    LEFT JOIN botiquin_linked bl ON v.client_id = bl.client_id AND v.sku = bl.sku
    WHERE (p_start_date IS NULL OR v.date >= p_start_date)
      AND (p_end_date IS NULL OR v.date <= p_end_date)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
      AND v.sku IN (SELECT fs.sku FROM filtered_skus fs)
  )
  SELECT
    vc.fecha_agrupada as fecha_grupo,
    CASE
      WHEN p_grouping = 'week' THEN 'Sem ' || to_char(vc.fecha_agrupada, 'DD/MM')
      ELSE to_char(vc.fecha_agrupada, 'DD Mon')
    END as fecha_label,
    COUNT(DISTINCT vc.client_id || '-' || vc.sku)::INT as pares_total,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.client_id || '-' || vc.sku END)::INT as pares_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.client_id || '-' || vc.sku END)::INT as pares_directo,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC as valor_total,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_botiquin,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_directo,
    COUNT(*)::INT as num_transacciones,
    COUNT(DISTINCT vc.client_id)::INT as num_clientes
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_historical_skus_value_per_visit(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_client_id character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, piezas_venta integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH visits AS (
    SELECT
      mov.id_saga_transaction,
      mov.client_id,
      MIN(mov.movement_date::date) as fecha_visita
    FROM inventory_movements mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.type = 'SALE'
    GROUP BY mov.id_saga_transaction, mov.client_id
  )
  SELECT
    c.client_id,
    c.client_name,
    v.fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0) as valor_venta,
    SUM(mov.quantity)::int as piezas_venta
  FROM visits v
  JOIN inventory_movements mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON v.client_id = c.client_id
  WHERE mov.type = 'SALE'
    AND (p_start_date IS NULL OR v.fecha_visita >= p_start_date)
    AND (p_end_date IS NULL OR v.fecha_visita <= p_end_date)
    AND (p_client_id IS NULL OR v.client_id = p_client_id)
  GROUP BY c.client_id, c.client_name, v.fecha_visita
  ORDER BY v.fecha_visita ASC, c.client_name;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_cabinet_impact_summary(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(adopciones integer, revenue_adopciones numeric, conversiones integer, revenue_conversiones numeric, exposiciones integer, revenue_exposiciones numeric, crosssell_pares integer, revenue_crosssell numeric, revenue_total_impacto numeric, revenue_total_odv numeric, porcentaje_impacto numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento FROM medication_conditions mp JOIN conditions p ON p.condition_id = mp.condition_id ORDER BY mp.sku, p.condition_id),
  filtered_skus AS (SELECT m.sku FROM medications m LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku WHERE (p_brands IS NULL OR m.brand = ANY(p_brands)) AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))),
  base AS (SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date)),
  m1 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev FROM base WHERE m_type = 'M1'),
  m2 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M2'),
  m3 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev FROM base WHERE m_type = 'M3'),
  total_odv AS (SELECT COALESCE(SUM(v.quantity * v.price), 0) AS rev FROM odv_sales v WHERE (p_doctors IS NULL OR v.client_id = ANY(p_doctors)) AND v.sku IN (SELECT sku FROM filtered_skus) AND (p_start_date IS NULL OR v.date >= p_start_date) AND (p_end_date IS NULL OR v.date <= p_end_date))
  SELECT m1.cnt, m1.rev, m2.cnt, m2.rev, m3.cnt, m3.rev, 0::int, 0::numeric,
    (m1.rev + m2.rev + m3.rev), t.rev,
    CASE WHEN t.rev > 0 THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1) ELSE 0 END
  FROM m1, m2, m3, total_odv t;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_impact_detail(p_metric text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, quantity integer, price numeric, valor numeric, date date, detalle text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_metric = 'M1' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           CASE WHEN avg_pu.avg_precio > 0
             THEN ROUND(b.revenue_botiquin / avg_pu.avg_precio)::int
             ELSE 0
           END AS quantity,
           COALESCE(avg_pu.avg_precio, 0) AS price,
           b.revenue_botiquin AS valor,
           b.first_event_date AS date,
           'Adopción en botiquín'::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    LEFT JOIN LATERAL (
      SELECT CASE WHEN SUM(mi.quantity) > 0
        THEN SUM(mi.quantity * COALESCE(mi.unit_price, 0)) / SUM(mi.quantity)
        ELSE 0 END AS avg_precio
      FROM inventory_movements mi
      WHERE mi.client_id = b.client_id AND mi.sku = b.sku AND mi.type = 'SALE'
    ) avg_pu ON true
    WHERE b.m_type = 'M1'
    ORDER BY b.revenue_botiquin DESC;

  ELSIF p_metric = 'M2' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           b.cantidad_odv::int AS quantity,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS price,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS date,
           ('ODV después de botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_fecha FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date > b.first_event_date
        AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
    ) odv_first ON true
    WHERE b.m_type = 'M2'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metric = 'M3' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           b.cantidad_odv::int AS quantity,
           ROUND(b.revenue_odv / NULLIF(b.cantidad_odv, 0), 2) AS price,
           b.revenue_odv AS valor,
           odv_first.first_fecha AS date,
           ('Exposición post-botiquín (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_fecha FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date > b.first_event_date
        AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
    ) odv_first ON true
    WHERE b.m_type = 'M3'
    ORDER BY b.revenue_odv DESC;

  ELSIF p_metric = 'M4' THEN
    RETURN;

  ELSE
    RAISE EXCEPTION 'Métrica inválida: %. Use M1, M2, M3 o M4.', p_metric;
  END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_market_analysis(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, sku character varying, product character varying, brand character varying, padecimiento character varying, es_top boolean, venta_pz bigint, venta_valor numeric, creacion_pz bigint, creacion_valor numeric, recoleccion_pz bigint, recoleccion_valor numeric, stock_activo_pz bigint, conversiones_m2 bigint, revenue_m2 numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku)
      mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  movements AS (
    SELECT
      mi.client_id, mi.sku,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::bigint             AS venta_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0)         AS venta_valor,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::bigint           AS creacion_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'PLACEMENT'), 0)      AS creacion_valor,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::bigint        AS recoleccion_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'COLLECTION'), 0)   AS recoleccion_valor
    FROM inventory_movements mi
    JOIN medications med ON med.sku = mi.sku
    WHERE (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id, mi.sku
  ),
  m2_counts AS (
    SELECT cb.client_id, cb.sku,
      COUNT(*)::bigint AS conversiones_m2,
      COALESCE(SUM(cb.revenue_odv), 0) AS revenue_m2
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) cb
    WHERE cb.m_type = 'M2'
    GROUP BY cb.client_id, cb.sku
  )
  SELECT
    mv.client_id::varchar, mv.sku::varchar, med.product::varchar, med.brand::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, med.top AS es_top,
    mv.venta_pz, mv.venta_valor,
    mv.creacion_pz, mv.creacion_valor,
    mv.recoleccion_pz, mv.recoleccion_valor,
    COALESCE(ib.available_quantity, 0)::bigint AS stock_activo_pz,
    COALESCE(m2.conversiones_m2, 0)::bigint,
    COALESCE(m2.revenue_m2, 0)::numeric
  FROM movements mv
  JOIN medications med ON med.sku = mv.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
  LEFT JOIN m2_counts m2 ON m2.client_id = mv.client_id AND m2.sku = mv.sku
  LEFT JOIN cabinet_inventory ib ON ib.client_id = mv.client_id AND ib.sku = mv.sku;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_opportunity_matrix(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, venta integer, recoleccion integer, valor numeric, converted_qty integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  botiquin_by_pad AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::int AS venta,
           COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::int AS recoleccion,
           COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0) AS valor
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.type IN ('SALE', 'COLLECTION')
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ),
  converted AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(v.quantity), 0)::int AS converted_qty
    FROM (
      SELECT mi.client_id, mi.sku,
             MIN(mi.movement_date::date) AS first_venta
      FROM inventory_movements mi
      WHERE mi.type = 'SALE'
        AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
        AND mi.sku IN (SELECT sku FROM filtered_skus)
      GROUP BY mi.client_id, mi.sku
    ) bv
    JOIN odv_sales v ON v.client_id = bv.client_id AND v.sku = bv.sku AND v.date > bv.first_venta
    LEFT JOIN sku_padecimiento sp ON sp.sku = bv.sku
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  )
  SELECT bp.padecimiento, bp.venta, bp.recoleccion, bp.valor,
         COALESCE(cv.converted_qty, 0)::int AS converted_qty
  FROM botiquin_by_pad bp
  LEFT JOIN converted cv ON cv.padecimiento = bp.padecimiento
  ORDER BY bp.valor DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_condition_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, valor numeric, piezas integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  )
  SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor,
         SUM(mi.quantity)::int AS piezas
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ORDER BY valor DESC;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_product_interest(p_limit integer DEFAULT 15, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(product character varying, venta integer, creacion integer, recoleccion integer, stock_activo integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  )
  SELECT
    CASE WHEN LENGTH(m.product) > 20 THEN LEFT(m.product, 20) || '...' ELSE m.product END AS product,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::int AS venta,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::int AS creacion,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::int AS recoleccion,
    GREATEST(0,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)
    )::int AS stock_activo
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type IN ('SALE', 'PLACEMENT', 'COLLECTION')
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY m.product
  ORDER BY (
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0) +
    GREATEST(0,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)
    )
  ) DESC
  LIMIT p_limit;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_active_collection()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE
  result json;
BEGIN
  WITH borrador_items AS (
    SELECT
      st.client_id,
      (item->>'sku')::text as sku,
      (item->>'quantity')::int as quantity
    FROM saga_transactions st,
    jsonb_array_elements(st.items) as item
    WHERE st.type = 'COLLECTION'
      AND st.status = 'DRAFT'
  )
  SELECT json_build_object(
    'total_piezas', COALESCE(SUM(bi.quantity), 0)::bigint,
    'valor_total', COALESCE(SUM(bi.quantity * COALESCE(inv.unit_price, 0)), 0),
    'num_clientes', COALESCE(COUNT(DISTINCT bi.client_id), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN clients c ON bi.client_id = c.client_id
  LEFT JOIN cabinet_inventory inv ON bi.client_id = inv.client_id AND bi.sku = inv.sku
  WHERE c.active = TRUE;

  RETURN COALESCE(result, json_build_object('total_piezas', 0, 'valor_total', 0, 'num_clientes', 0));
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_recurring_data()
 RETURNS TABLE(client_id character varying, sku character varying, date date, quantity integer, price numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT v.client_id, v.sku, v.date, v.quantity, v.price
  FROM odv_sales v;
END;
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_sankey_conversion_flows(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku text, product text, categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT b.client_id::varchar, b.client_name::varchar, b.sku::text, b.product::text,
    b.m_type::text AS categoria, b.revenue_odv AS valor_odv, b.cantidad_odv, b.num_transacciones_odv AS num_transacciones
  FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
  WHERE b.m_type IN ('M2', 'M3');
$function$
;

CREATE OR REPLACE FUNCTION analytics.get_yoy_padecimiento(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, anio integer, valor numeric, crecimiento numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  yearly AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           EXTRACT(YEAR FROM mi.movement_date)::int AS anio,
           SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS'), EXTRACT(YEAR FROM mi.movement_date)::int
  ),
  with_prev AS (
    SELECT y.padecimiento, y.anio, y.valor,
           LAG(y.valor) OVER (PARTITION BY y.padecimiento ORDER BY y.anio) AS prev_valor
    FROM yearly y
  )
  SELECT wp.padecimiento, wp.anio, wp.valor,
         CASE WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor > 0
              THEN ROUND(((wp.valor - wp.prev_valor) / wp.prev_valor) * 100)
              WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor = 0 AND wp.valor > 0
              THEN 100::numeric
              ELSE NULL
         END AS crecimiento
  FROM with_prev wp
  ORDER BY wp.padecimiento, wp.anio;
END;
$function$
;



-- ============================================================================
-- Chatbot Schema Functions
-- ============================================================================

CREATE OR REPLACE FUNCTION chatbot.check_and_increment_usage(p_user_id character varying, p_role character varying)
 RETURNS TABLE(allowed boolean, queries_used integer, queries_limit integer, remaining integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  IF p_role = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_role = 'ADMIN' THEN
    v_limit := 10;
  ELSE
    v_limit := 5;
  END IF;

  INSERT INTO chatbot.usage_limits (user_id, date, queries_used, queries_limit)
  VALUES (p_user_id, CURRENT_DATE, 0, v_limit)
  ON CONFLICT (user_id, date) DO UPDATE
  SET updated_at = now();

  SELECT ul.queries_used INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.user_id = p_user_id
    AND ul.date = CURRENT_DATE;

  IF v_used >= v_limit THEN
    RETURN QUERY SELECT false::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, 0::INTEGER;
    RETURN;
  END IF;

  UPDATE chatbot.usage_limits
  SET queries_used = chatbot.usage_limits.queries_used + 1,
      updated_at = now()
  WHERE user_id = p_user_id
    AND date = CURRENT_DATE;

  v_used := v_used + 1;
  RETURN QUERY SELECT true::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.classification_by_client(p_client_id character varying)
 RETURNS TABLE(sku character varying, clasificacion text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT cb.sku, cb.m_type as clasificacion
  FROM analytics.clasificacion_base() cb
  WHERE cb.client_id = p_client_id;
$function$;

CREATE OR REPLACE FUNCTION chatbot.fn_mark_embedding_stale()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  DELETE FROM chatbot.medication_embeddings WHERE sku = NEW.sku;
  RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.fuzzy_search_clients(p_search text, p_user_id character varying DEFAULT NULL::character varying, p_limit integer DEFAULT 5)
 RETURNS TABLE(client_id character varying, name text, similarity real)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT c.client_id, c.client_name::TEXT as name,
    extensions.similarity(unaccent(lower(c.client_name)), unaccent(lower(p_search))) as similarity
  FROM clients c
  WHERE (p_user_id IS NULL OR c.user_id = p_user_id)
    AND extensions.similarity(unaccent(lower(c.client_name)), unaccent(lower(p_search))) > 0.2
  ORDER BY extensions.similarity(unaccent(lower(c.client_name)), unaccent(lower(p_search))) DESC
  LIMIT p_limit;
$function$;

CREATE OR REPLACE FUNCTION chatbot.fuzzy_search_medications(p_search text, p_limit integer DEFAULT 5)
 RETURNS TABLE(sku character varying, description text, brand character varying, similarity real)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT m.sku, m.description, m.brand,
    GREATEST(
      extensions.similarity(unaccent(lower(m.description)), unaccent(lower(p_search))),
      extensions.similarity(unaccent(lower(m.sku)), unaccent(lower(p_search)))
    ) as similarity
  FROM medications m
  WHERE extensions.similarity(unaccent(lower(m.description)), unaccent(lower(p_search))) > 0.15
     OR extensions.similarity(unaccent(lower(m.sku)), unaccent(lower(p_search))) > 0.3
  ORDER BY similarity DESC
  LIMIT p_limit;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_doctor_inventory(p_client_id character varying, p_user_id character varying, p_is_admin boolean DEFAULT false)
 RETURNS TABLE(sku character varying, description text, brand character varying, content character varying, available_quantity integer, price numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF NOT p_is_admin THEN
    IF NOT EXISTS (
      SELECT 1 FROM clients c
      WHERE c.client_id = p_client_id AND c.user_id = p_user_id
    ) THEN
      RETURN;
    END IF;
  END IF;

  RETURN QUERY
  SELECT
    m.sku,
    m.description,
    m.brand,
    m.content,
    ib.available_quantity,
    m.price
  FROM cabinet_inventory ib
  JOIN medications m ON m.sku = ib.sku
  WHERE ib.client_id = p_client_id
    AND ib.available_quantity > 0
  ORDER BY m.brand, m.sku;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_doctor_movements(p_client_id character varying, p_user_id character varying, p_is_admin boolean DEFAULT false, p_source text DEFAULT 'ambos'::text, p_limit_count integer DEFAULT 30)
 RETURNS TABLE(fuente text, sku character varying, description text, brand character varying, type text, quantity integer, price numeric, date timestamp with time zone)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF NOT p_is_admin THEN
    IF NOT EXISTS (
      SELECT 1 FROM clients c
      WHERE c.client_id = p_client_id AND c.user_id = p_user_id
    ) THEN
      RETURN;
    END IF;
  END IF;

  RETURN QUERY
  SELECT * FROM (
    SELECT
      'botiquin'::TEXT AS f_fuente,
      mi.sku::VARCHAR AS f_sku,
      m.description::TEXT AS f_desc,
      m.brand::VARCHAR AS f_marca,
      mi.type::TEXT AS f_tipo,
      mi.quantity AS f_cant,
      mi.unit_price AS f_precio,
      mi.movement_date AS f_fecha
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    WHERE mi.client_id = p_client_id
      AND (p_source = 'ambos' OR p_source = 'botiquin')

    UNION ALL

    SELECT
      'odv'::TEXT,
      vo.sku::VARCHAR,
      m.description::TEXT,
      m.brand::VARCHAR,
      'SALE_ODV'::TEXT,
      vo.quantity,
      vo.price,
      vo.date::TIMESTAMPTZ
    FROM odv_sales vo
    JOIN medications m ON m.sku = vo.sku
    WHERE vo.client_id = p_client_id
      AND (p_source = 'ambos' OR p_source = 'odv')
  ) sub
  ORDER BY 8 DESC
  LIMIT LEAST(p_limit_count, 100);
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_medication_prices(p_search_term text DEFAULT NULL::text, p_brand_filter character varying DEFAULT NULL::character varying)
 RETURNS TABLE(sku character varying, description text, brand character varying, content character varying, price numeric, last_updated timestamp with time zone)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_search_term IS NOT NULL THEN
    RETURN QUERY
    SELECT
      m.sku,
      m.description,
      m.brand,
      m.content,
      m.price,
      m.last_updated
    FROM medications m
    WHERE (p_brand_filter IS NULL OR m.brand ILIKE p_brand_filter)
      AND (
        extensions.similarity(unaccent(lower(m.description)), unaccent(lower(p_search_term))) > 0.15
        OR extensions.similarity(m.sku, upper(p_search_term)) > 0.3
        OR m.sku ILIKE '%' || p_search_term || '%'
        OR m.description ILIKE '%' || p_search_term || '%'
      )
    ORDER BY GREATEST(
      extensions.similarity(unaccent(lower(m.description)), unaccent(lower(p_search_term))),
      extensions.similarity(m.sku, upper(p_search_term))
    ) DESC
    LIMIT 10;
  ELSE
    RETURN QUERY
    SELECT
      m.sku,
      m.description,
      m.brand,
      m.content,
      m.price,
      m.last_updated
    FROM medications m
    WHERE (p_brand_filter IS NULL OR m.brand ILIKE p_brand_filter)
    ORDER BY m.brand, m.sku
    LIMIT 50;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_complete_sales_ranking(p_limit_count integer DEFAULT 20)
 RETURNS TABLE(sku character varying, description text, brand character varying, piezas_botiquin integer, piezas_conversion integer, piezas_exposicion integer, piezas_totales integer, ventas_botiquin numeric, ventas_conversion numeric, ventas_exposicion numeric, ventas_totales numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH impacts AS (
    SELECT d.sku, d.quantity, d.valor, 'M1'::text AS type
    FROM analytics.get_impact_detail('M1') d
    UNION ALL
    SELECT d.sku, d.quantity, d.valor, 'M2'::text
    FROM analytics.get_impact_detail('M2') d
    UNION ALL
    SELECT d.sku, d.quantity, d.valor, 'M3'::text
    FROM analytics.get_impact_detail('M3') d
  )
  SELECT
    i.sku::VARCHAR,
    m.description::TEXT,
    m.brand::VARCHAR,
    SUM(CASE WHEN i.type = 'M1' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_botiquin,
    SUM(CASE WHEN i.type = 'M2' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_conversion,
    SUM(CASE WHEN i.type = 'M3' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_exposicion,
    SUM(i.quantity)::INTEGER AS piezas_totales,
    ROUND(SUM(CASE WHEN i.type = 'M1' THEN i.valor ELSE 0 END), 2) AS ventas_botiquin,
    ROUND(SUM(CASE WHEN i.type = 'M2' THEN i.valor ELSE 0 END), 2) AS ventas_conversion,
    ROUND(SUM(CASE WHEN i.type = 'M3' THEN i.valor ELSE 0 END), 2) AS ventas_exposicion,
    ROUND(SUM(i.valor), 2) AS ventas_totales
  FROM impacts i
  JOIN medications m ON m.sku = i.sku
  GROUP BY i.sku, m.description, m.brand
  ORDER BY SUM(i.valor) DESC
  LIMIT p_limit_count;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_user_collections(p_user_id character varying, p_client_id character varying DEFAULT NULL::character varying, p_limit integer DEFAULT 20, p_is_admin boolean DEFAULT false)
 RETURNS TABLE(recoleccion_id uuid, client_id character varying, client_name character varying, status text, created_at timestamp with time zone, delivered_at timestamp with time zone, cedis_observations text, items json)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT r.recoleccion_id, r.client_id, c.client_name,
    r.status, r.created_at, r.delivered_at,
    r.cedis_observations,
    (SELECT COALESCE(json_agg(json_build_object(
      'sku', ri.sku, 'quantity', ri.quantity,
      'product', m.description
    )), '[]'::json)
    FROM collection_items ri
    LEFT JOIN medications m ON m.sku = ri.sku
    WHERE ri.recoleccion_id = r.recoleccion_id) as items
  FROM collections r
  JOIN clients c ON c.client_id = r.client_id
  WHERE (p_is_admin OR r.user_id = p_user_id)
    AND (p_client_id IS NULL OR r.client_id = p_client_id)
  ORDER BY r.created_at DESC
  LIMIT p_limit;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_remaining_queries(p_user_id character varying, p_role character varying)
 RETURNS TABLE(queries_used integer, queries_limit integer, remaining integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  IF p_role = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_role = 'ADMIN' THEN
    v_limit := 10;
  ELSE
    v_limit := 5;
  END IF;

  SELECT COALESCE(ul.queries_used, 0)
  INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.user_id = p_user_id
    AND ul.date = CURRENT_DATE;

  IF NOT FOUND THEN
    v_used := 0;
  END IF;

  RETURN QUERY SELECT v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_complete_brand_performance()
 RETURNS TABLE(brand character varying, piezas_botiquin integer, piezas_conversion integer, piezas_exposicion integer, piezas_totales integer, ventas_botiquin numeric, ventas_conversion numeric, ventas_exposicion numeric, ventas_totales numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH impacts AS (
    SELECT d.sku, d.quantity, d.valor, 'M1'::text AS type
    FROM analytics.get_impact_detail('M1') d
    UNION ALL
    SELECT d.sku, d.quantity, d.valor, 'M2'::text
    FROM analytics.get_impact_detail('M2') d
    UNION ALL
    SELECT d.sku, d.quantity, d.valor, 'M3'::text
    FROM analytics.get_impact_detail('M3') d
  )
  SELECT
    m.brand::VARCHAR,
    SUM(CASE WHEN i.type = 'M1' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_botiquin,
    SUM(CASE WHEN i.type = 'M2' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_conversion,
    SUM(CASE WHEN i.type = 'M3' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_exposicion,
    SUM(i.quantity)::INTEGER AS piezas_totales,
    ROUND(SUM(CASE WHEN i.type = 'M1' THEN i.valor ELSE 0 END), 2) AS ventas_botiquin,
    ROUND(SUM(CASE WHEN i.type = 'M2' THEN i.valor ELSE 0 END), 2) AS ventas_conversion,
    ROUND(SUM(CASE WHEN i.type = 'M3' THEN i.valor ELSE 0 END), 2) AS ventas_exposicion,
    ROUND(SUM(i.valor), 2) AS ventas_totales
  FROM impacts i
  JOIN medications m ON m.sku = i.sku
  GROUP BY m.brand
  ORDER BY SUM(i.valor) DESC;
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.get_user_odv_sales(p_user_id character varying, p_is_admin boolean DEFAULT false, p_sku_filter character varying DEFAULT NULL::character varying, p_limit_count integer DEFAULT 50)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, description text, brand character varying, quantity integer, price numeric, date date)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    c.client_id,
    c.client_name,
    vo.sku,
    m.description,
    m.brand,
    vo.quantity,
    vo.price,
    vo.date
  FROM odv_sales vo
  JOIN clients c ON c.client_id = vo.client_id
  JOIN medications m ON m.sku = vo.sku
  WHERE (p_is_admin OR c.user_id = p_user_id)
    AND (p_sku_filter IS NULL OR vo.sku = p_sku_filter)
  ORDER BY vo.date DESC
  LIMIT LEAST(p_limit_count, 200);
END;
$function$;

CREATE OR REPLACE FUNCTION chatbot.match_data_sheets(query_embedding vector, match_threshold double precision DEFAULT 0.70, match_count integer DEFAULT 5)
 RETURNS TABLE(sku character varying, content text, chunk_index integer, similarity double precision)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT fc.sku, fc.content, fc.chunk_index,
    1 - (fc.embedding <=> query_embedding) AS similarity
  FROM chatbot.data_sheet_chunks fc
  WHERE 1 - (fc.embedding <=> query_embedding) > match_threshold
  ORDER BY fc.embedding <=> query_embedding ASC
  LIMIT LEAST(match_count, 20);
$function$;

CREATE OR REPLACE FUNCTION chatbot.match_medications(query_embedding vector, match_threshold double precision DEFAULT 0.65, match_count integer DEFAULT 10)
 RETURNS TABLE(sku character varying, brand character varying, description text, content character varying, price numeric, conditions text, similarity double precision)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  SELECT m.sku, m.brand, m.description, m.content, m.price,
    string_agg(DISTINCT p.name, ', ') as conditions,
    1 - (me.embedding <=> query_embedding) AS similarity
  FROM chatbot.medication_embeddings me
  JOIN medications m ON m.sku = me.sku
  LEFT JOIN medication_conditions mp ON mp.sku = m.sku
  LEFT JOIN conditions p ON p.condition_id = mp.condition_id
  WHERE 1 - (me.embedding <=> query_embedding) > match_threshold
  GROUP BY m.sku, m.brand, m.description, m.content, m.price, me.embedding, query_embedding
  ORDER BY me.embedding <=> query_embedding ASC
  LIMIT LEAST(match_count, 50);
$function$;

CREATE OR REPLACE FUNCTION chatbot.rollback_usage(p_user_id character varying)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  UPDATE chatbot.usage_limits
  SET queries_used = GREATEST(queries_used - 1, 0),
      updated_at = now()
  WHERE user_id = p_user_id
    AND date = CURRENT_DATE;
END;
$function$;

