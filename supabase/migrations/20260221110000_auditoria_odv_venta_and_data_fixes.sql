-- Migration: Fix auditoria base data + add ODV Venta nodes to get_auditoria_cliente
-- Issues addressed:
--   0a. Backfill 31 missing id_saga_zoho_link for MEXBR172 (DEV only, PROD already done)
--   0b. Fix Jaime Oscar (MEXJG20850) fecha_movimiento: 2025-11-01 → 2025-11-14
--   0c. Fix ERICKA ROSERMOND (MEXER156) fecha_movimiento mismatches
--   1.  RPC: fix _av temp table (use completed_at, include movement-less visits)
--   2.  RPC: add ODV Venta nodes + edges + todas_odv_venta in resumen

-- ═══════════════════════════════════════════════════════════════
-- 0a. Backfill id_saga_zoho_link for MEXBR172 (safe: WHERE ... IS NULL)
-- ═══════════════════════════════════════════════════════════════

-- VENTA movements → link 41 (DCOdV-35158)
UPDATE movimientos_inventario SET id_saga_zoho_link = 41
WHERE id IN (2059,2060,2061,2062,2063,2065,2066) AND id_saga_zoho_link IS NULL;

-- VENTA P081 → link 199 (DCOdV-35423)
UPDATE movimientos_inventario SET id_saga_zoho_link = 199
WHERE id = 2058 AND id_saga_zoho_link IS NULL;

-- VENTA Y810+P632 → link 56 (DCOdV-36315)
UPDATE movimientos_inventario SET id_saga_zoho_link = 56
WHERE id IN (2070,2071) AND id_saga_zoho_link IS NULL;

-- RECOLECCION movements → link 231 (REC-MEXBR172-2025-11-28)
UPDATE movimientos_inventario SET id_saga_zoho_link = 231
WHERE id IN (2064,2067,2068,2069,2697,2698,2699) AND id_saga_zoho_link IS NULL;

-- CREACION movements → link 54 (DCOdV-35428)
UPDATE movimientos_inventario SET id_saga_zoho_link = 54
WHERE id IN (2074,2076,2077,2078,2079,2080) AND id_saga_zoho_link IS NULL;

-- CREACION movements → link 38 (DCOdV-35155)
UPDATE movimientos_inventario SET id_saga_zoho_link = 38
WHERE id IN (2081,2082,2083,2084,2085) AND id_saga_zoho_link IS NULL;

-- CREACION movements → link 55 (DCOdV-36318)
UPDATE movimientos_inventario SET id_saga_zoho_link = 55
WHERE id IN (2737,2738,2739) AND id_saga_zoho_link IS NULL;


-- ═══════════════════════════════════════════════════════════════
-- 0b. Fix Jaime Oscar (MEXJG20850) — 2 CREACION movements
--     fecha_movimiento: 2025-11-01 → 2025-11-14 (matches saga/visita date)
-- ═══════════════════════════════════════════════════════════════

UPDATE movimientos_inventario
SET fecha_movimiento = '2025-11-14'::date
WHERE id IN (1984, 1985)
  AND fecha_movimiento = '2025-11-01'::date;


-- ═══════════════════════════════════════════════════════════════
-- 0c. Fix ERICKA ROSERMOND (MEXER156) — 14 movements
--     8 VENTA: 2025-11-27 → 2025-11-28 (saga created Nov 28)
--     6 RECOLECCION: 2025-11-27 → 2025-12-03 (saga created Dec 3)
-- ═══════════════════════════════════════════════════════════════

UPDATE movimientos_inventario
SET fecha_movimiento = '2025-11-28'::date
WHERE id IN (2152,2153,2154,2155,2156,2157,2158,2159)
  AND fecha_movimiento = '2025-11-27'::date;

UPDATE movimientos_inventario
SET fecha_movimiento = '2025-12-03'::date
WHERE id IN (2160,2161,2162,2163,2164,2165)
  AND fecha_movimiento = '2025-11-27'::date;


-- ═══════════════════════════════════════════════════════════════
-- 1. Rewrite analytics.get_auditoria_cliente
--    - Fix _av: use completed_at, include movement-less visits
--    - Add ODV Venta nodes + edges
--    - Add todas_odv_venta to resumen
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION analytics.get_auditoria_cliente(p_cliente varchar)
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
  SELECT json_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)
  INTO v_cliente
  FROM clientes c
  WHERE c.id_cliente = p_cliente;

  IF v_cliente IS NULL THEN
    RETURN json_build_object('error', 'Cliente no encontrado');
  END IF;

  -- ── _av: All completed visits with sagas (even if 0 movements) ──
  DROP TABLE IF EXISTS _av;
  CREATE TEMP TABLE _av ON COMMIT DROP AS
  SELECT
    v.visit_id,
    COALESCE(v.completed_at, v.created_at)::date as fecha_visita,
    COALESCE(v.tipo::text, 'DESCONOCIDO') as tipo_visita,
    ROW_NUMBER() OVER (ORDER BY v.corte_number, v.created_at, v.visit_id) as visit_num
  FROM visitas v
  WHERE v.id_cliente = p_cliente
    AND v.estado = 'COMPLETADO'
    AND EXISTS (SELECT 1 FROM saga_transactions st WHERE st.visit_id = v.visit_id);

  -- 2. Visitas with sagas, movements, anomalies
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
            'odv_venta', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.tipo = 'VENTA'
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
                  'fecha', TO_CHAR(m.fecha_movimiento, 'YYYY-MM-DD'),
                  'zoho_link_id', m.id_saga_zoho_link,
                  'odv', (SELECT szl2.zoho_id FROM saga_zoho_links szl2 WHERE szl2.id = m.id_saga_zoho_link)
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

          UNION ALL

          SELECT 'VTA_SIN_CREACION: ' || m.sku || ' ' || m.tipo::text
                 || ' sin CREACION previa en botiquin' as msg
          FROM movimientos_inventario m
          JOIN saga_transactions st ON m.id_saga_transaction = st.id
          WHERE st.visit_id = av.visit_id
            AND m.id_cliente = p_cliente
            AND m.tipo IN ('VENTA', 'RECOLECCION')
            AND NOT EXISTS (
              SELECT 1
              FROM movimientos_inventario m2
              JOIN saga_transactions st2 ON m2.id_saga_transaction = st2.id
              JOIN _av av2 ON st2.visit_id = av2.visit_id
              WHERE m2.id_cliente = p_cliente
                AND m2.sku = m.sku
                AND m2.tipo = 'CREACION'
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
            'odv', (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.id = m.id_saga_zoho_link)
          ) ORDER BY av.visit_num, m.tipo
        ) as eventos,
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

  -- 4. Graph Nodes
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

    -- ODV Botiquin nodes
    SELECT json_build_object(
      'id', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'tipo', 'odv',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'piezas', SUM(m.cantidad),
      'skus_count', COUNT(DISTINCT m.sku)
    ) as n
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND szl.tipo = 'BOTIQUIN'
      AND szl.zoho_id IS NOT NULL
      AND m.tipo = 'CREACION'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- ODV Venta nodes (NEW)
    SELECT json_build_object(
      'id', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'tipo', 'odv_venta',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'piezas', SUM(m.cantidad),
      'skus_count', COUNT(DISTINCT m.sku)
    ) as n
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
      AND m.tipo = 'VENTA'
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- SKU nodes
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

  -- 5. Graph Edges
  SELECT COALESCE(json_agg(e), '[]'::json)
  INTO v_grafo_aristas
  FROM (
    -- CREACION: ODV Botiquin → Visit
    SELECT json_build_object(
      'source', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'target', 'v' || av.visit_num,
      'tipo', 'CREACION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND m.tipo = 'CREACION'
      AND szl.tipo = 'BOTIQUIN'
      AND szl.zoho_id IS NOT NULL
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- PERMANENCIA: Visit → Next Visit
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

    -- RECOLECCION: Visit → REC sink
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

    -- VENTA: Visit → VTA sink
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

    -- ODV_VENTA: VTA sink → ODV Venta node (NEW)
    SELECT json_build_object(
      'source', 'vta-v' || av.visit_num,
      'target', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'tipo', 'ODV_VENTA',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'piezas', SUM(m.cantidad)
    ) as e
    FROM movimientos_inventario m
    JOIN saga_zoho_links szl ON szl.id = m.id_saga_zoho_link
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
      AND m.tipo = 'VENTA'
      AND szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
    GROUP BY szl.zoho_id, av.visit_num

    UNION ALL

    -- SKU-level edges
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

  -- 6. Count anomalies
  SELECT COUNT(*) INTO v_anomalias_count
  FROM (
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

    SELECT 1
    FROM movimientos_inventario m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.id_cliente = p_cliente
    GROUP BY st.id, m.sku, m.tipo
    HAVING COUNT(*) > 1

    UNION ALL

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
        SELECT 1 FROM movimientos_inventario m
        WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
      )
  ) anomalies;

  -- 7. Summary
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
    ),
    'todas_odv_venta', (
      SELECT COALESCE(json_agg(DISTINCT szl.zoho_id ORDER BY szl.zoho_id), '[]'::json)
      FROM saga_zoho_links szl
      JOIN saga_transactions st ON szl.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE szl.zoho_id IS NOT NULL AND szl.tipo = 'VENTA'
        AND EXISTS (
          SELECT 1 FROM movimientos_inventario m
          WHERE m.id_saga_transaction = st.id AND m.id_cliente = p_cliente
        )
    )
  ) INTO v_resumen;

  -- 8. Return combined result
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
$function$;


-- ═══════════════════════════════════════════════════════════════
-- Recreate public wrapper (idempotent)
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION public.get_auditoria_cliente(p_cliente varchar)
RETURNS json
LANGUAGE sql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
  SELECT analytics.get_auditoria_cliente(p_cliente);
$function$;

GRANT EXECUTE ON FUNCTION analytics.get_auditoria_cliente(varchar) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_auditoria_cliente(varchar) TO authenticated, anon;

NOTIFY pgrst, 'reload schema';
