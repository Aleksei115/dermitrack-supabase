-- ============================================================================
-- PHASE 7: Fix JSON-returning RPCs to return English keys
-- ============================================================================
-- These functions return json (not TABLE), so their keys are hardcoded in
-- json_build_object() calls. Phase 5 missed renaming these internal keys.
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. analytics.get_dashboard_static()
-- ---------------------------------------------------------------------------
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
       FROM medications m WHERE m.brand IS NOT NULL) AS brands,
      (SELECT jsonb_agg(jsonb_build_object('id', c.client_id, 'name', c.client_name) ORDER BY c.client_name)
       FROM clients c WHERE c.active = true) AS doctors,
      (SELECT ARRAY_AGG(DISTINCT p.name ORDER BY p.name)
       FROM conditions p) AS conditions,
      (SELECT MIN(movement_date)::date
       FROM inventory_movements WHERE type = 'PLACEMENT') AS "firstPlacementDate"
  ) f;

  SELECT row_to_json(s) INTO v_stats
  FROM (
    SELECT
      r.fecha_inicio AS "startDate",
      r.fecha_fin AS "endDate",
      r.dias_corte AS "cutoffDays",
      r.total_medicos_visitados AS "totalDoctorsVisited",
      r.total_movimientos AS "totalMovements",
      r.piezas_venta AS "salePieces",
      r.piezas_creacion AS "placementPieces",
      r.piezas_recoleccion AS "collectionPieces",
      r.valor_venta AS "saleValue",
      r.valor_creacion AS "placementValue",
      r.valor_recoleccion AS "collectionValue",
      r.medicos_con_venta AS "doctorsWithSales",
      r.medicos_sin_venta AS "doctorsWithoutSales",
      r.valor_venta_anterior AS "previousSaleValue",
      r.valor_creacion_anterior AS "previousPlacementValue",
      r.valor_recoleccion_anterior AS "previousCollectionValue",
      r.promedio_por_medico_anterior AS "previousAvgPerDoctor",
      r.porcentaje_cambio_venta AS "saleChangePct",
      r.porcentaje_cambio_creacion AS "placementChangePct",
      r.porcentaje_cambio_recoleccion AS "collectionChangePct",
      r.porcentaje_cambio_promedio AS "avgChangePct"
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
    'cutoffFilters', v_filtros,
    'cutoffStatsGeneral', v_stats,
    'corteProgress', v_progress
  );
END;
$function$;

-- ---------------------------------------------------------------------------
-- 2. analytics.get_active_collection()
-- ---------------------------------------------------------------------------
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
    'total_pieces', COALESCE(SUM(bi.quantity), 0)::bigint,
    'total_value', COALESCE(SUM(bi.quantity * COALESCE(inv.unit_price, 0)), 0),
    'client_count', COALESCE(COUNT(DISTINCT bi.client_id), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN clients c ON bi.client_id = c.client_id
  LEFT JOIN cabinet_inventory inv ON bi.client_id = inv.client_id AND bi.sku = inv.sku
  WHERE c.active = TRUE;

  RETURN COALESCE(result, json_build_object('total_pieces', 0, 'total_value', 0, 'client_count', 0));
END;
$function$;

-- ---------------------------------------------------------------------------
-- 3. analytics.get_current_cutoff_data()
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_current_cutoff_data(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[]
)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result json;
BEGIN
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
      v.completed_at::date AS visit_date,
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
  all_active_clients AS (
    SELECT c.client_id, c.client_name
    FROM clients c
    WHERE c.active = TRUE
      AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
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
  ),
  current_mov AS (
    SELECT mov.client_id, ac.client_name, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.client_id = mov.client_id
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  prev_mov AS (
    SELECT mov.client_id, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM prev_visits pv
    JOIN saga_transactions st ON st.visit_id = pv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stats AS (
    SELECT
      COALESCE(COUNT(DISTINCT mov.client_id), 0)::int AS total_doctors_visited,
      COALESCE(COUNT(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.client_id END), 0)::int AS doctors_with_sales,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS sale_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS placement_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS collection_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value
    FROM current_mov mov
  ),
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value
    FROM prev_mov mov
  ),
  prev_medico_stats AS (
    SELECT mov.client_id,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value
    FROM prev_mov mov
    GROUP BY mov.client_id
  ),
  visited_medico_rows AS (
    SELECT
      mov.client_id,
      mov.client_name,
      cv.visit_date::text AS visit_date,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS sale_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS placement_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS collection_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value,
      STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', ') AS sold_skus,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0 AS has_sale,
      pms.sale_value AS previous_sale_value,
      CASE
        WHEN pms.sale_value IS NOT NULL AND pms.sale_value > 0
          THEN ROUND(((COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0) - pms.sale_value) / pms.sale_value * 100)::numeric, 1)
        WHEN (pms.sale_value IS NULL OR pms.sale_value = 0)
          AND COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0
          THEN 100.0
        ELSE NULL
      END AS change_pct
    FROM current_mov mov
    JOIN current_visits cv ON cv.client_id = mov.client_id
    LEFT JOIN prev_medico_stats pms ON mov.client_id = pms.client_id
    GROUP BY mov.client_id, mov.client_name, cv.visit_date, pms.sale_value
  ),
  pending_medico_rows AS (
    SELECT
      ac.client_id,
      ac.client_name,
      NULL::text AS visit_date,
      0::int AS sale_pieces,
      0::int AS placement_pieces,
      0::int AS collection_pieces,
      0::numeric AS sale_value,
      0::numeric AS placement_value,
      0::numeric AS collection_value,
      NULL::text AS sold_skus,
      false AS has_sale,
      NULL::numeric AS previous_sale_value,
      NULL::numeric AS change_pct
    FROM all_active_clients ac
    WHERE NOT EXISTS (SELECT 1 FROM current_visits cv WHERE cv.client_id = ac.client_id)
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'start_date', (SELECT MIN(cv.visit_date) FROM current_visits cv),
      'end_date', (SELECT MAX(cv.visit_date) FROM current_visits cv),
      'cutoff_days', COALESCE((SELECT MAX(cv.visit_date) - MIN(cv.visit_date) + 1 FROM current_visits cv), 0),
      'total_doctors_visited', k.total_doctors_visited,
      'doctors_with_sales', k.doctors_with_sales,
      'doctors_without_sales', k.total_doctors_visited - k.doctors_with_sales,
      'sale_pieces', k.sale_pieces,
      'sale_value', k.sale_value,
      'placement_pieces', k.placement_pieces,
      'placement_value', k.placement_value,
      'collection_pieces', k.collection_pieces,
      'collection_value', k.collection_value,
      'sale_change_pct',
        CASE WHEN p.sale_value > 0
          THEN ROUND(((k.sale_value - p.sale_value) / p.sale_value * 100)::numeric, 1)
          ELSE NULL END,
      'placement_change_pct',
        CASE WHEN p.placement_value > 0
          THEN ROUND(((k.placement_value - p.placement_value) / p.placement_value * 100)::numeric, 1)
          ELSE NULL END,
      'collection_change_pct',
        CASE WHEN p.collection_value > 0
          THEN ROUND(((k.collection_value - p.collection_value) / p.collection_value * 100)::numeric, 1)
          ELSE NULL END
    ),
    'doctors', COALESCE(
      (SELECT json_agg(row_to_json(sub) ORDER BY sub.visit_date IS NULL ASC, sub.sale_value DESC, sub.client_name)
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
$function$;

-- ---------------------------------------------------------------------------
-- 4. analytics.get_historical_cutoff_data()
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_historical_cutoff_data(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
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
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'SALE'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.available_quantity * COALESCE(inv.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_count
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
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'COLLECTION'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.client_id,
      MIN(mov.movement_date::date) AS visit_date
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
      vb.visit_date::text AS visit_date,
      COUNT(DISTINCT mov.sku)::int AS unique_skus,
      COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS sale_value,
      COALESCE(SUM(mov.quantity), 0)::int AS sale_pieces
    FROM visitas_base vb
    JOIN inventory_movements mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN clients c ON vb.client_id = c.client_id
    WHERE mov.type = 'SALE'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR vb.visit_date >= p_start_date)
      AND (p_end_date IS NULL OR vb.visit_date <= p_end_date)
    GROUP BY c.client_id, c.client_name, vb.visit_date
    ORDER BY vb.visit_date ASC, c.client_name
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'sale_value_m1', (SELECT valor FROM kpi_venta_m1),
      'sale_skus_m1', (SELECT skus_count FROM kpi_venta_m1),
      'placement_value', (SELECT valor FROM kpi_creacion),
      'placement_skus', (SELECT skus_count FROM kpi_creacion),
      'active_stock', (SELECT valor FROM kpi_stock),
      'stock_skus', (SELECT skus_count FROM kpi_stock),
      'collection_value', (SELECT valor FROM kpi_recoleccion),
      'collection_skus', (SELECT skus_count FROM kpi_recoleccion)
    ),
    'visits', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 5. analytics.get_dashboard_data()
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_dashboard_data(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
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
-- ── JSON builders with ENGLISH keys ──
clasificacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT client_id, client_name, sku, product,
           padecimiento AS condition,
           brand, es_top AS is_top, m_type, first_event_date,
           revenue_botiquin AS cabinet_revenue,
           revenue_odv AS odv_revenue,
           cantidad_odv AS odv_quantity,
           num_transacciones_odv AS odv_transaction_count
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
    SELECT m1.cnt AS adoptions, m1.rev AS revenue_adoptions,
           m2.cnt AS conversions, m2.rev AS revenue_conversions,
           m3.cnt AS exposures, m3.rev AS revenue_exposures,
           0::int AS crosssell_pairs, 0::numeric AS revenue_crosssell,
           (m1.rev + m2.rev + m3.rev) AS total_impact_revenue,
           t.rev AS total_odv_revenue,
           CASE WHEN t.rev > 0
             THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1)
             ELSE 0 END AS impact_percentage
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
      COALESCE(sp.padecimiento, 'OTROS') AS condition, med.top AS is_top,
      mv.venta_pz AS sale_pieces, mv.venta_valor AS sale_value,
      mv.creacion_pz AS placement_pieces, mv.creacion_valor AS placement_value,
      mv.recoleccion_pz AS collection_pieces, mv.recoleccion_valor AS collection_value,
      COALESCE(ib.available_quantity, 0)::bigint AS active_stock_pieces,
      COALESCE(m2c.conversiones_m2, 0)::bigint AS m2_conversions,
      COALESCE(m2c.revenue_m2, 0)::numeric AS m2_revenue
    FROM movements mv
    JOIN medications med ON med.sku = mv.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
    LEFT JOIN m2_counts m2c ON m2c.client_id = mv.client_id AND m2c.sku = mv.sku
    LEFT JOIN cabinet_inventory ib ON ib.client_id = mv.client_id AND ib.sku = mv.sku
  ) r
),
conversion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r."generatedValue" DESC), '[]'::json) AS val
  FROM (
    SELECT
      b.m_type AS "mType",
      b.client_id AS "clientId",
      b.client_name AS "clientName",
      b.sku,
      b.product,
      b.first_event_date AS "cabinetDate",
      odv_first.first_odv AS "firstOdvDate",
      (odv_first.first_odv - b.first_event_date)::int AS "conversionDays",
      b.num_transacciones_odv AS "odvSaleCount",
      b.cantidad_odv::bigint AS "totalPieces",
      b.revenue_odv AS "generatedValue",
      b.revenue_botiquin AS "cabinetValue"
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
      c.tier AS previous_tier,
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
        ELSE NULL END AS growth_pct,
      CASE WHEN t.grand_total > 0
        THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
        ELSE 0 END AS linked_pct,
      (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS linked_value,
      (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS linked_pieces,
      (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS linked_skus,
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
      b.m_type AS category,
      b.revenue_odv AS odv_value,
      b.cantidad_odv AS odv_quantity,
      b.num_transacciones_odv AS transaction_count
    FROM clasificacion b
    WHERE b.m_type IN ('M2', 'M3')
  ) r
)

SELECT json_build_object(
  'classificationBase', c.val,
  'impactSummary', i.val,
  'marketAnalysis', m.val,
  'conversionDetails', cv.val,
  'billingComposition', f.val,
  'sankeyFlows', s.val
)
FROM clasificacion_json c, impacto_json i, market_json m,
     conversion_json cv, facturacion_json f, sankey_json s;
$function$;

-- ---------------------------------------------------------------------------
-- 6. analytics.get_client_audit()
-- ---------------------------------------------------------------------------
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
    RETURN json_build_object('error', 'Client not found');
  END IF;

  -- ── _av: All completed visits with sagas (even if 0 movements) ──
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
    AND EXISTS (SELECT 1 FROM saga_transactions st WHERE st.visit_id = v.visit_id);

  -- 2. Visits with sagas, movements, anomalies
  SELECT COALESCE(json_agg(vr ORDER BY (vr->>'visit_num')::int), '[]'::json)
  INTO v_visitas
  FROM (
    SELECT json_build_object(
      'visit_num', av.visit_num,
      'date', TO_CHAR(av.visit_date, 'YYYY-MM-DD'),
      'visit_type', av.visit_type,
      'sagas', (
        SELECT COALESCE(json_agg(sr ORDER BY sr->>'saga_type'), '[]'::json)
        FROM (
          SELECT json_build_object(
            'saga_type', st.type::text,
            'saga_status', st.status::text,
            'odv_cabinet', (
              SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ')
              FROM saga_zoho_links szl
              WHERE szl.id_saga_transaction = st.id
                AND szl.type = 'CABINET'
                AND szl.zoho_id IS NOT NULL
            ),
            'odv_sale', (
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
            'movements', (
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
            'anomalies', (
              SELECT COALESCE(json_agg(d.msg), '[]'::json)
              FROM (
                SELECT 'DUPLICATE_MOVEMENT: ' || m.sku || ' ' || m.type::text
                       || ' appears ' || COUNT(*) || ' times' as msg
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
        JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
        JOIN _av av2 ON st2.visit_id = av2.visit_id
        WHERE m.client_id = p_client
          AND av2.visit_date <= av.visit_date
          AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
      ),
      'inventory_skus', (
        SELECT COUNT(*) FROM (
          SELECT m.sku
          FROM inventory_movements m
          JOIN saga_transactions st2 ON m.id_saga_transaction = st2.id
          JOIN _av av2 ON st2.visit_id = av2.visit_id
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
          SELECT 'DUPLICATE_HOLDING: ' || COUNT(*)
                 || ' HOLDING sagas' as msg
          FROM saga_transactions st
          WHERE st.visit_id = av.visit_id
            AND st.type = 'HOLDING'
            AND EXISTS (
              SELECT 1 FROM inventory_movements m
              WHERE m.id_saga_transaction = st.id AND m.client_id = p_client
            )
          HAVING COUNT(*) > 1

          UNION ALL

          SELECT 'ODV_MISSING: saga ' || st.type::text || ' missing CABINET ODV' as msg
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

          SELECT 'SALE_WITHOUT_PLACEMENT: ' || m.sku || ' ' || m.type::text
                 || ' without prior PLACEMENT in cabinet' as msg
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
      'id', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'odv',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'pieces', SUM(m.quantity),
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

    -- ODV Sale nodes
    SELECT json_build_object(
      'id', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'odv_sale',
      'label', szl.zoho_id,
      'visit_num', av.visit_num,
      'pieces', SUM(m.quantity),
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
    -- PLACEMENT: ODV Cabinet → Visit
    SELECT json_build_object(
      'source', 'odv-' || szl.zoho_id || '-v' || av.visit_num,
      'target', 'v' || av.visit_num,
      'type', 'PLACEMENT',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
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

    -- HOLDING: Visit → Next Visit
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'v' || (av.visit_num + 1),
      'type', 'HOLDING',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'HOLDING'
      AND EXISTS (SELECT 1 FROM _av av2 WHERE av2.visit_num = av.visit_num + 1)
    GROUP BY av.visit_num

    UNION ALL

    -- COLLECTION: Visit → REC sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'rec-v' || av.visit_num,
      'type', 'COLLECTION',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'COLLECTION'
    GROUP BY av.visit_num

    UNION ALL

    -- SALE: Visit → VTA sink
    SELECT json_build_object(
      'source', 'v' || av.visit_num,
      'target', 'vta-v' || av.visit_num,
      'type', 'SALE',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
    ) as e
    FROM inventory_movements m
    JOIN saga_transactions st ON m.id_saga_transaction = st.id
    JOIN _av av ON st.visit_id = av.visit_id
    WHERE m.client_id = p_client AND m.type = 'SALE'
    GROUP BY av.visit_num

    UNION ALL

    -- ODV_SALE: VTA sink → ODV Sale node
    SELECT json_build_object(
      'source', 'vta-v' || av.visit_num,
      'target', 'odv-vta-' || szl.zoho_id || '-v' || av.visit_num,
      'type', 'ODV_SALE',
      'label', COUNT(DISTINCT m.sku) || ' SKUs',
      'skus_count', COUNT(DISTINCT m.sku),
      'pieces', SUM(m.quantity)
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
    'total_visits', (SELECT COUNT(*) FROM _av),
    'total_historical_skus', (
      SELECT COUNT(DISTINCT m.sku)
      FROM inventory_movements m
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
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
      JOIN saga_transactions st ON m.id_saga_transaction = st.id
      JOIN _av av ON st.visit_id = av.visit_id
      WHERE m.client_id = p_client
        AND m.type IN ('PLACEMENT', 'SALE', 'COLLECTION')
    ),
    'current_inventory_skus', (
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
    'total_anomalies', v_anomalias_count,
    'all_cabinet_odvs', (
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
    'all_sale_odvs', (
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

COMMIT;
