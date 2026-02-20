-- Migration: Create get_auditoria_cliente RPC
--
-- Returns full lifecycle audit for a single client as JSON:
--   cliente, visitas (with sagas/movements/anomalies), ciclo_vida_skus,
--   grafo (nodos + aristas including SKU nodes), resumen
--
-- Anomalies detected:
--   PERMANENCIA_DUPLICADA, MOVIMIENTO_DUPLICADO, ODV_MISSING,
--   SKU_SIN_CREACION, SYNC_PENDING

CREATE OR REPLACE FUNCTION analytics.get_auditoria_cliente(p_cliente varchar)
RETURNS json
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_cliente json;
  v_visitas json;
  v_ciclo json;
  v_grafo_nodos json;
  v_grafo_aristas json;
  v_resumen json;
  v_anomalias_count int := 0;
BEGIN
  -- ── 1. Client Info ──
  SELECT json_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)
  INTO v_cliente
  FROM clientes c
  WHERE c.id_cliente = p_cliente;

  IF v_cliente IS NULL THEN
    RETURN json_build_object('error', 'Cliente no encontrado');
  END IF;

  -- ── Temp: ordered visits for this client ──
  -- Derive fecha from movimientos (visitas table has no fecha_visita column)
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    MIN(m.fecha_movimiento)::date as fecha_visita,
    COALESCE(v.tipo::text, 'DESCONOCIDO') as tipo_visita,
    ROW_NUMBER() OVER (ORDER BY MIN(m.fecha_movimiento), v.visit_id) as visit_num
  FROM visitas v
  JOIN saga_transactions st ON st.visit_id = v.visit_id
  JOIN movimientos_inventario m ON m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
  GROUP BY v.visit_id, v.tipo;

  -- ── 2. Visitas with sagas, movements, anomalies ──
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'fecha', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'visita_tipo', av.tipo_visita,
      'sagas', (
        SELECT COALESCE(json_agg(sr ORDER BY sr->>'saga_tipo'), '[]'::json)
        FROM (
          SELECT json_build_object(
            'saga_tipo', st.tipo::text,
            'saga_estado', st.estado::text,
            'odv_botiquin', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.tipo = 'BOTIQUIN'
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
                  'producto', med.producto,
                  'tipo', m.tipo::text,
                  'cantidad', m.cantidad,
                  'fecha', TO_CHAR(m.fecha_movimiento, 'YYYY-MM-DD')
                ) ORDER BY m.sku, m.tipo
              ), '[]'::json)
              FROM movimientos_inventario m
              JOIN medicamentos med ON m.sku = med.sku
              WHERE m.id_saga_transaction = st.id
                AND m.id_cliente = p_cliente
            ),
            'anomalias', (
              SELECT COALESCE(json_agg(d.msg), '[]'::json)
              FROM (
                -- MOVIMIENTO_DUPLICADO
                SELECT 'MOVIMIENTO_DUPLICADO: ' || m.sku || ' ' || m.tipo::text
                       || ' aparece ' || COUNT(*) || ' veces' as msg
                FROM movimientos_inventario m
                WHERE m.id_saga_transaction = st.id
                  AND m.id_cliente = p_cliente
                GROUP BY m.sku, m.tipo
                HAVING COUNT(*) > 1
              ) d
            )
          ) as sr
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )
        ) saga_sub
      ),
      'inventario_piezas', (
        SELECT COALESCE(SUM(
          CASE m.tipo
            WHEN 'CREACION' THEN m.cantidad
            WHEN 'VENTA' THEN -m.cantidad
            WHEN 'RECOLECCION' THEN -m.cantidad
            ELSE 0
          END
        ), 0)
        FROM movimientos_inventario m
        JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m.id_cliente = p_cliente
          AND av2.fecha_visita <= av.fecha_visita
          AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
      ),
      'inventario_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM movimientos_inventario m
          JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m.id_cliente = p_cliente
            AND av2.fecha_visita <= av.fecha_visita
            AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
          GROUP BY m.sku
          HAVING SUM(
            CASE m.tipo
              WHEN 'CREACION' THEN m.cantidad
              WHEN 'VENTA' THEN -m.cantidad
              WHEN 'RECOLECCION' THEN -m.cantidad
              ELSE 0
            END
          ) > 0
        ) sc
      ),
      'anomalias', (
        SELECT COALESCE(json_agg(va.msg), '[]'::json)
        FROM (
          -- PERMANENCIA_DUPLICADA
          SELECT 'PERMANENCIA_DUPLICADA: ' || COUNT(*)
                 || ' sagas PERMANENCIA' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.tipo = 'PERMANENCIA'
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )
          HAVING COUNT(*) > 1

          UNION ALL

          -- ODV_MISSING: CREACION saga without BOTIQUIN ODV
          SELECT 'ODV_MISSING: saga ' || st.tipo::text || ' sin ODV BOTIQUIN' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.tipo IN ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION', 'LEV_POST_CORTE')
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id
                AND m.id_cliente = p_cliente
                AND m.tipo = 'CREACION'
            )
            AND NOT EXISTS (
              SELECT 1 FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.tipo = 'BOTIQUIN'
                AND szl.zoho_id IS NOT NULL
            )

          UNION ALL

          -- SYNC_PENDING
          SELECT 'SYNC_PENDING: ODV ' || szl.zoho_id
                 || ' status=' || COALESCE(szl.zoho_sync_status, 'null') as msg
          FROM saga_zoho_links szl
          JOIN saga_transactions st ON szl.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND szl.zoho_id IS NOT NULL
            AND COALESCE(szl.zoho_sync_status, '') != 'synced'
            AND EXISTS (
              SELECT 1 FROM movimientos_inventario m
              WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
            )
        ) va
      )
    ) as vr
    FROM _av av
  ) visit_rows;

  -- ── 3. SKU Lifecycle ──
  SELECT COALESCE(json_agg(sr ORDER BY sr->>'sku'), '[]'::json)
  INTO v_ciclo
  FROM (
    SELECT json_build_object(
      'sku', sub.sku,
      'producto', sub.producto,
      'eventos', sub.eventos,
      'estado_actual', CASE
        WHEN sub.last_tipo = 'RECOLECCION' THEN 'RECOLECTADO'
        WHEN sub.last_tipo = 'VENTA' THEN 'VENDIDO'
        ELSE 'ACTIVO'
      END
    ) as sr
    FROM (
      SELECT
        m.sku,
        MAX(med.producto) as producto,
        json_agg(
          json_build_object(
            'visit_num', av.visit_num,
            'fecha', TO_CHAR(m.fecha_movimiento, 'YYYY-MM-DD'),
            'tipo', m.tipo::text,
            'cantidad', m.cantidad,
            'odv', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = m.id_saga_transaction
                AND szl.tipo = 'BOTIQUIN'
                AND szl.zoho_id IS NOT NULL
            )
          ) ORDER BY av.visit_num, m.tipo
        ) as eventos,
        -- Determine current state from last event
        (
          SELECT m2.tipo::text
          FROM movimientos_inventario m2
          JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
          WHERE m2.id_cliente = p_cliente AND m2.sku = m.sku
            AND m2.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
          ORDER BY av2.visit_num DESC, m2.fecha_movimiento DESC
          LIMIT 1
        ) as last_tipo
      FROM movimientos_inventario m
      JOIN medicamentos med ON m.sku = med.sku
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
      GROUP BY m.sku
    ) sub
  ) ciclo_rows;

  -- ── 4. Graph Nodes ──
  SELECT COALESCE(json_agg(n ORDER BY n->>'id'), '[]'::json)
  INTO v_grafo_nodos
  FROM (
    -- Visit nodes
    SELECT json_build_object(
      'id', 'v' || av.visit_num,
      'tipo', 'visita',
      'visit_num', av.visit_num,
      'fecha', TO_CHAR(av.fecha_visita, 'YYYY-MM-DD'),
      'label', 'V' || av.visit_num || ' ' || TO_CHAR(av.fecha_visita, 'Mon DD'),
      'visita_tipo', av.tipo_visita
    ) as n
    FROM _av av

    UNION ALL

    -- ODV nodes (deduplicated)
    SELECT DISTINCT ON (szl.zoho_id)
      json_build_object(
        'id', 'odv-' || szl.zoho_id,
        'tipo', 'odv',
        'label', szl.zoho_id,
        'visit_num', av.visit_num
      ) as n
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE szl.zoho_id IS NOT NULL
      AND szl.tipo = 'BOTIQUIN'
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )

    UNION ALL

    -- SKU nodes (deduplicated)
    SELECT DISTINCT ON (m.sku)
      json_build_object(
        'id', 'sku-' || m.sku,
        'tipo', 'sku',
        'label', m.sku,
        'producto', med.producto
      ) as n
    FROM movimientos_inventario m
    JOIN medicamentos med ON m.sku = med.sku
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
  ) all_nodes;

  -- ── 5. Graph Edges ──
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- CREACION: ODV → Visit (aggregate)
    SELECT json_build_object(
      'source', 'odv-' || szl.zoho_id,
      'target', 'v' || av.visit_num,
      'tipo', 'CREACION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = st.id
      AND szl.tipo = 'BOTIQUIN' AND szl.zoho_id IS NOT NULL
    WHERE m.id_cliente = p_cliente AND m.tipo = 'CREACION'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- PERMANENCIA: Visit → Next Visit (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'v' || (av.visit_num + 1),
      'tipo', 'PERMANENCIA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'PERMANENCIA'
      AND EXISTS (SELECT 1 FROM _av av2 WHERE av2.visit_num = av.visit_num + 1)
    GROUP BY av.visit_num

    UNION ALL

    -- RECOLECCION: Visit → sink (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'rec-v' || av.visit_num,
      'tipo', 'RECOLECCION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'RECOLECCION'
    GROUP BY av.visit_num

    UNION ALL

    -- VENTA: Visit → sink (aggregate)
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'vta-v' || av.visit_num,
      'tipo', 'VENTA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente AND m.tipo = 'VENTA'
    GROUP BY av.visit_num

    UNION ALL

    -- SKU-level: each SKU's movement at each visit
    -- Note: enum types need explicit ::text cast for LOWER() and SUBSTR()
    SELECT json_build_object(
      'source', CASE
        WHEN m.tipo = 'CREACION' THEN 'sku-' || m.sku
        ELSE 'v' || av.visit_num
      END,
      'target', CASE
        WHEN m.tipo = 'CREACION' THEN 'v' || av.visit_num
        ELSE 'sku-' || m.sku
      END,
      'tipo', 'sku_' || LOWER(m.tipo::text),
      'label', SUBSTR(m.tipo::text, 1, 3) || '(' || SUM(m.cantidad) || ')',
      'sku', m.sku,
      'visit_num', av.visit_num,
      'cantidad', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
    GROUP BY m.sku, m.tipo, av.visit_num
  ) all_edges;

  -- ── 6. Count anomalies ──
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
    -- PERMANENCIA_DUPLICADA
    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.tipo = 'PERMANENCIA'
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )
    GROUP BY av.visit_id
    HAVING COUNT(*) > 1

    UNION ALL

    -- MOVIMIENTO_DUPLICADO
    SELECT 1
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
    GROUP BY st.id, m.sku, m.tipo
    HAVING COUNT(*) > 1

    UNION ALL

    -- ODV_MISSING
    SELECT 1
    FROM saga_transactions st
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE st.tipo IN ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION', 'LEV_POST_CORTE')
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id
          AND m.id_cliente = p_cliente
          AND m.tipo = 'CREACION'
      )
      AND NOT EXISTS (
        SELECT 1 FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = st.id
          AND szl.tipo = 'BOTIQUIN'
          AND szl.zoho_id IS NOT NULL
      )

    UNION ALL

    -- SKU_SIN_CREACION
    SELECT 1
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND m.tipo IN ('VENTA', 'RECOLECCION')
      AND NOT EXISTS (
        SELECT 1
        FROM movimientos_inventario m2
        JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m2.id_cliente = p_cliente
          AND m2.sku = m.sku
          AND m2.tipo = 'CREACION'
          AND av2.visit_num < av.visit_num
      )
    GROUP BY m.sku

    UNION ALL

    -- SYNC_PENDING
    SELECT 1
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE szl.zoho_id IS NOT NULL
      AND COALESCE(szl.zoho_sync_status, '') != 'synced'
      AND EXISTS (
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )
  ) anomalies;

  -- ── 7. Summary ──
  SELECT json_build_object(
    'total_visitas', (SELECT COUNT(*) FROM _av),
    'total_skus_historico', (
      SELECT COUNT(DISTINCT m.sku)
      FROM movimientos_inventario m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
    ),
    'inventario_actual_piezas', (
      SELECT COALESCE(SUM(
        CASE m.tipo
          WHEN 'CREACION' THEN m.cantidad
          WHEN 'VENTA' THEN -m.cantidad
          WHEN 'RECOLECCION' THEN -m.cantidad
          ELSE 0
        END
      ), 0)
      FROM movimientos_inventario m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.id_cliente = p_cliente
        AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
    ),
    'inventario_actual_skus', (
      SELECT COUNT(*) FROM (
        SELECT m.sku
        FROM movimientos_inventario m
        JOIN saga_transactions st ON m.id_saga_transaction = st.id
        JOIN _av av ON st.visit_id = av.visit_id
        WHERE m.id_cliente = p_cliente
          AND m.tipo IN ('CREACION', 'VENTA', 'RECOLECCION')
        GROUP BY m.sku
        HAVING SUM(
          CASE m.tipo
            WHEN 'CREACION' THEN m.cantidad
            WHEN 'VENTA' THEN -m.cantidad
            WHEN 'RECOLECCION' THEN -m.cantidad
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
      WHERE szl.zoho_id IS NOT NULL AND szl.tipo = 'BOTIQUIN'
        AND EXISTS (
          SELECT 1 FROM movimientos_inventario m
          WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
        )
    )
  ) INTO v_resumen;

  -- ── 8. Return combined result ──
  RETURN json_build_object(
    'cliente', v_cliente,
    'visitas', COALESCE(v_visitas, '[]'::json),
    'ciclo_vida_skus', COALESCE(v_ciclo, '[]'::json),
    'grafo', json_build_object(
      'nodos', COALESCE(v_grafo_nodos, '[]'::json),
      'aristas', COALESCE(v_grafo_aristas, '[]'::json)
    ),
    'resumen', v_resumen
  );
END;
$$;

-- ============================================================
-- Public wrapper for PostgREST access (VOLATILE — inner fn uses temp tables)
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_auditoria_cliente(p_cliente varchar)
RETURNS json
LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path = public AS $$
  SELECT analytics.get_auditoria_cliente(p_cliente);
$$;

NOTIFY pgrst, 'reload schema';
