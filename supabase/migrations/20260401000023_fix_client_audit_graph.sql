-- Fix get_client_audit graph structure to match frontend expectations.
-- Migration 014 oversimplified the graph edges when removing saga tables:
--   - Edges were PLACEMENT/SALE/COLLECTION from visit→sku (no sku_ prefix)
--   - Frontend expects aggregate edges (visit↔odv, visit→sink, visit→visit)
--     plus sku_-prefixed edges for the detail layer
--   - Sale ODV node IDs need 'odv-vta-' prefix (were using 'odv-')

CREATE OR REPLACE FUNCTION analytics.get_client_audit(p_client varchar)
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
    RETURN json_build_object('error', 'Client not found');
  END IF;

  -- _av: All completed visits with movements
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    COALESCE(v.completed_at, v.created_at)::date as visit_date,
    COALESCE(v.type::text, 'UNKNOWN') as visit_type,
    ROW_NUMBER() OVER (ORDER BY v.corte_number, v.created_at, v.visit_id) as visit_num
  FROM visits v
  WHERE v.client_id = p_client
    AND v.status = 'COMPLETED'
    AND EXISTS (SELECT 1 FROM inventory_movements im WHERE im.visit_id = v.visit_id AND im.client_id = p_client);

  -- 2. Visits with tasks, movements, anomalies
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.visit_date, 'YYYY-MM-DD'),
      'visit_type', av.visit_type,
      'tasks', (
        SELECT COALESCE(json_agg(json_build_object(
          'task_type', vt.task_type::text,
          'status', vt.status::text,
          'transaction_type', vt.transaction_type::text,
          'completed_at', vt.completed_at
        ) ORDER BY vt.step_order), '[]'::json)
        FROM visit_tasks vt
        WHERE vt.visit_id = av.visit_id
      ),
      'odvs', (
        SELECT COALESCE(json_agg(json_build_object(
          'odv_id', cso.odv_id,
          'type', cso.odv_type::text
        )), '[]'::json)
        FROM cabinet_sale_odv_ids cso
        WHERE cso.visit_id = av.visit_id
      ),
      'movements', (
        SELECT COALESCE(json_agg(
          json_build_object(
            'mov_id', m.id,
            'sku', m.sku,
            'product', med.product,
            'type', m.type::text,
            'quantity', m.quantity,
            'date', TO_CHAR(m.movement_date, 'YYYY-MM-DD')
          ) ORDER BY m.sku, m.type
        ), '[]'::json)
        FROM inventory_movements m
        JOIN medications med ON m.sku = med.sku
        WHERE m.visit_id = av.visit_id
          AND m.client_id = p_client
      ),
      'inventory_pieces', (
        SELECT COALESCE(SUM(
          CASE m.type
            WHEN 'PLACEMENT' THEN m.quantity
            WHEN 'SALE' THEN -m.quantity
            WHEN 'COLLECTION' THEN -m.quantity
            ELSE 0
          END
        ), 0)
        FROM inventory_movements m
        JOIN _av av2 ON m.visit_id = av2.visit_id
        WHERE m.client_id = p_client
          AND av2.visit_date <= av.visit_date
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
      ),
      'inventory_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM inventory_movements m
          JOIN _av av2 ON m.visit_id = av2.visit_id
          WHERE m.client_id = p_client
            AND av2.visit_date <= av.visit_date
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
      'anomalies', (
        SELECT COALESCE(json_agg(va.msg), '[]'::json)
        FROM (
          SELECT 'DUPLICATE_MOVEMENT: ' || m.sku || ' ' || m.type::text
                 || ' appears ' || COUNT(*) || ' times' as msg
          FROM inventory_movements m
          WHERE m.visit_id = av.visit_id
            AND m.client_id = p_client
          GROUP BY m.sku, m.type
          HAVING COUNT(*) > 1

          UNION ALL

          SELECT 'SALE_WITHOUT_PLACEMENT: ' || m.sku || ' ' || m.type::text
                 || ' without prior PLACEMENT in cabinet' as msg
          FROM inventory_movements m
          WHERE m.visit_id = av.visit_id
            AND m.client_id = p_client
            AND m.type IN ('SALE', 'COLLECTION')
            AND NOT EXISTS (
              SELECT 1
              FROM inventory_movements m2
              JOIN _av av2 ON m2.visit_id = av2.visit_id
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
      'events', sub.eventos,
      'current_status', CASE
        WHEN sub.last_tipo = 'COLLECTION' THEN 'COLLECTED'
        WHEN sub.last_tipo = 'SALE' THEN 'SOLD'
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
            'quantity', m.quantity
          ) ORDER BY av.visit_num, m.type
        ) as eventos,
        (
          SELECT m2.type::text
          FROM inventory_movements m2
          JOIN _av av2 ON m2.visit_id = av2.visit_id
          WHERE m2.client_id = p_client AND m2.sku = m.sku
            AND m2.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
          ORDER BY av2.visit_num DESC, m2.movement_date DESC
          LIMIT 1
        ) as last_tipo
      FROM inventory_movements m
      JOIN medications med ON m.sku = med.sku
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
      GROUP BY m.sku
    ) sub
  ) ciclo_rows;

  -- 4. Graph Nodes (fixed: separate CABINET and SALE ODV prefixes)
  SELECT COALESCE(json_agg(n ORDER BY n->>'id'), '[]'::json)
  INTO v_grafo_nodos
  FROM (
    -- Visit nodes
    SELECT json_build_object(
      'id', 'v' || av.visit_num,
      'type', 'visit',
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.visit_date, 'YYYY-MM-DD'),
      'label', 'V' || av.visit_num || ' ' || TO_CHAR(av.visit_date, 'Mon DD'),
      'visit_type', av.visit_type
    ) as n
    FROM _av av

    UNION ALL

    -- ODV Cabinet nodes
    SELECT json_build_object(
      'id', 'odv-' || cso.odv_id || '-v' || av.visit_num,
      'type', 'odv',
      'label', cso.odv_id,
      'visit_num', av.visit_num,
      'pieces', (SELECT COALESCE(SUM(im.quantity), 0)
                 FROM inventory_movements im
                 WHERE im.visit_id = av.visit_id AND im.client_id = p_client AND im.type = 'PLACEMENT'),
      'skus_count', (SELECT COUNT(DISTINCT im.sku)
                     FROM inventory_movements im
                     WHERE im.visit_id = av.visit_id AND im.client_id = p_client AND im.type = 'PLACEMENT')
    ) as n
    FROM cabinet_sale_odv_ids cso
    JOIN _av av ON cso.visit_id = av.visit_id
    WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'CABINET'

    UNION ALL

    -- ODV Sale nodes (use odv-vta- prefix to match ODV_SALE edge targets)
    SELECT json_build_object(
      'id', 'odv-vta-' || cso.odv_id || '-v' || av.visit_num,
      'type', 'odv_sale',
      'label', cso.odv_id,
      'visit_num', av.visit_num,
      'pieces', (SELECT COALESCE(SUM(im.quantity), 0)
                 FROM inventory_movements im
                 WHERE im.visit_id = av.visit_id AND im.client_id = p_client AND im.type = 'SALE'),
      'skus_count', (SELECT COUNT(DISTINCT im.sku)
                     FROM inventory_movements im
                     WHERE im.visit_id = av.visit_id AND im.client_id = p_client AND im.type = 'SALE')
    ) as n
    FROM cabinet_sale_odv_ids cso
    JOIN _av av ON cso.visit_id = av.visit_id
    WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'SALE'

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
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
  ) all_nodes;

  -- 5. Graph Edges (fixed: aggregate layer + sku_ detail layer)
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- ═══ AGGREGATE LAYER (shown by default) ═══

    -- PLACEMENT aggregate: ODV Cabinet → Visit
    SELECT json_build_object(
      'source', 'odv-' || cso.odv_id || '-v' || av.visit_num,
      'target', 'v' || av.visit_num,
      'type', 'PLACEMENT',
      'label', agg.sku_count || ' SKUs',
      'skus_count', agg.sku_count,
      'pieces', agg.total_pieces
    ) as e
    FROM cabinet_sale_odv_ids cso
    JOIN _av av ON cso.visit_id = av.visit_id
    CROSS JOIN LATERAL (
      SELECT COUNT(DISTINCT im.sku) as sku_count,
             COALESCE(SUM(im.quantity), 0) as total_pieces
      FROM inventory_movements im
      WHERE im.visit_id = av.visit_id
        AND im.client_id = p_client
        AND im.type = 'PLACEMENT'
    ) agg
    WHERE cso.odv_id IS NOT NULL
      AND cso.odv_type = 'CABINET'
      AND agg.total_pieces > 0

    UNION ALL

    -- HOLDING aggregate: Visit → Next Visit (running inventory balance)
    SELECT json_build_object(
      'source', 'v' || r.visit_num,
      'target', 'v' || (r.visit_num + 1),
      'type', 'HOLDING',
      'label', r.running_pieces || ' pzs',
      'pieces', r.running_pieces
    ) as e
    FROM (
      SELECT
        av.visit_num,
        (SELECT COALESCE(SUM(
          CASE im.type
            WHEN 'PLACEMENT' THEN im.quantity
            WHEN 'SALE' THEN -im.quantity
            WHEN 'COLLECTION' THEN -im.quantity
            ELSE 0
          END
        ), 0)
        FROM inventory_movements im
        JOIN _av av2 ON im.visit_id = av2.visit_id
        WHERE im.client_id = p_client
          AND av2.visit_num <= av.visit_num
          AND im.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
        ) as running_pieces
      FROM _av av
      WHERE EXISTS (SELECT 1 FROM _av av2 WHERE av2.visit_num = av.visit_num + 1)
    ) r
    WHERE r.running_pieces > 0

    UNION ALL

    -- COLLECTION aggregate: Visit → REC sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'rec-v' || av.visit_num,
      'type', 'COLLECTION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'COLLECTION'
    GROUP BY av.visit_num

    UNION ALL

    -- SALE aggregate: Visit → VTA sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'vta-v' || av.visit_num,
      'type', 'SALE',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'SALE'
    GROUP BY av.visit_num

    UNION ALL

    -- ODV_SALE: VTA sink → ODV Sale node
    SELECT json_build_object(
      'source', 'vta-v' || av.visit_num,
      'target', 'odv-vta-' || cso.odv_id || '-v' || av.visit_num,
      'type', 'ODV_SALE',
      'label', agg.sku_count || ' SKUs',
      'skus_count', agg.sku_count,
      'pieces', agg.total_pieces
    ) as e
    FROM cabinet_sale_odv_ids cso
    JOIN _av av ON cso.visit_id = av.visit_id
    CROSS JOIN LATERAL (
      SELECT COUNT(DISTINCT im.sku) as sku_count,
             COALESCE(SUM(im.quantity), 0) as total_pieces
      FROM inventory_movements im
      WHERE im.visit_id = av.visit_id
        AND im.client_id = p_client
        AND im.type = 'SALE'
    ) agg
    WHERE cso.odv_id IS NOT NULL
      AND cso.odv_type = 'SALE'
      AND agg.total_pieces > 0

    UNION ALL

    -- ═══ SKU DETAIL LAYER (shown when showSkuNodes toggled on) ═══

    -- sku_placement: SKU → Visit (direction: sku is source for placement)
    SELECT json_build_object(
      'source', 'sku-' || m.sku,
      'target', 'v' || av.visit_num,
      'type', 'sku_placement',
      'label', 'PLA(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'PLACEMENT'
    GROUP BY m.sku, av.visit_num

    UNION ALL

    -- sku_sale: Visit → SKU
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'sku-' || m.sku,
      'type', 'sku_sale',
      'label', 'SAL(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'SALE'
    GROUP BY m.sku, av.visit_num

    UNION ALL

    -- sku_collection: Visit → SKU
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'sku-' || m.sku,
      'type', 'sku_collection',
      'label', 'COL(' || SUM(m.quantity) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'quantity', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'COLLECTION'
    GROUP BY m.sku, av.visit_num
  ) all_edges;

  -- 6. Count anomalies
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
    SELECT 1
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
    GROUP BY av.visit_id, m.sku, m.type
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT 1
    FROM inventory_movements m
    JOIN _av av ON m.visit_id = av.visit_id
    WHERE m.client_id = p_client
      AND m.type IN ('SALE', 'COLLECTION')
      AND NOT EXISTS (
        SELECT 1
        FROM inventory_movements m2
        JOIN _av av2 ON m2.visit_id = av2.visit_id
        WHERE m2.client_id = p_client
          AND m2.sku = m.sku
          AND m2.type = 'PLACEMENT'
          AND av2.visit_num <= av.visit_num
      )
    GROUP BY m.sku
  ) anomalies;

  -- 7. Summary
  SELECT json_build_object(
    'total_visits', (SELECT COUNT(*) FROM _av),
    'total_historical_skus', (
      SELECT COUNT(DISTINCT m.sku)
      FROM inventory_movements m
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
    ),
    'current_inventory_pieces', (
      SELECT COALESCE(SUM(
        CASE m.type
          WHEN 'PLACEMENT' THEN m.quantity
          WHEN 'SALE' THEN -m.quantity
          WHEN 'COLLECTION' THEN -m.quantity
          ELSE 0
        END
      ), 0)
      FROM inventory_movements m
      JOIN _av av ON m.visit_id = av.visit_id
      WHERE m.client_id = p_client
        AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
    ),
    'current_inventory_skus', (
      SELECT COUNT(*) FROM (
        SELECT m.sku
        FROM inventory_movements m
        JOIN _av av ON m.visit_id = av.visit_id
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
    'total_anomalies', v_anomalias_count,
    'all_cabinet_odvs', (
      SELECT COALESCE(json_agg(DISTINCT cso.odv_id ORDER BY cso.odv_id), '[]'::json)
      FROM cabinet_sale_odv_ids cso
      JOIN _av av ON cso.visit_id = av.visit_id
      WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'CABINET'
    ),
    'all_sale_odvs', (
      SELECT COALESCE(json_agg(DISTINCT cso.odv_id ORDER BY cso.odv_id), '[]'::json)
      FROM cabinet_sale_odv_ids cso
      JOIN _av av ON cso.visit_id = av.visit_id
      WHERE cso.odv_id IS NOT NULL AND cso.odv_type = 'SALE'
    )
  ) INTO v_resumen;

  -- 8. Return combined result
  RETURN json_build_object(
    'client', v_cliente,
    'visits', COALESCE(v_visitas, '[]'::json),
    'sku_lifecycle', COALESCE(v_ciclo, '[]'::json),
    'graph', json_build_object(
      'nodes', COALESCE(v_grafo_nodos, '[]'::json),
      'edges', COALESCE(v_grafo_aristas, '[]'::json)
    ),
    'summary', v_resumen
  );
END;
$function$;

-- Recreate public wrapper
CREATE OR REPLACE FUNCTION public.get_client_audit(p_client varchar)
RETURNS json
LANGUAGE sql
SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT analytics.get_client_audit(p_client);
$$;
