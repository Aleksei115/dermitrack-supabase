-- ============================================================================
-- Migration 10: Update analytics RPCs — saga → visit_id + cabinet_sale_odv_ids
-- Fase 3: Create cabinet_sale_odv_ids, rewrite clasificacion_base and billing
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- cabinet_sale_odv_ids: Replaces saga_zoho_links for M1/M2 deduplication
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE cabinet_sale_odv_ids (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  odv_id text NOT NULL,
  client_id character varying NOT NULL REFERENCES clients(client_id),
  visit_id uuid REFERENCES visits(visit_id),
  odv_type text NOT NULL CHECK (odv_type IN ('SALE', 'CABINET')),
  created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE cabinet_sale_odv_ids IS
'Maps Zoho ODV IDs to cabinet-linked visits. Replaces saga_zoho_links for M1/M2 deduplication in clasificacion_base. Populated by rpc_link_odv().';

COMMENT ON COLUMN cabinet_sale_odv_ids.odv_id IS 'Zoho ODV ID (e.g., DCOdV-12345). Used to exclude from M2/M3 counts.';
COMMENT ON COLUMN cabinet_sale_odv_ids.client_id IS 'FK to clients. Needed for joining against odv_sales.client_id.';
COMMENT ON COLUMN cabinet_sale_odv_ids.visit_id IS 'FK to visits. The visit that linked this ODV via rpc_link_odv().';
COMMENT ON COLUMN cabinet_sale_odv_ids.odv_type IS 'SALE: linked to cutoff SALE movements. CABINET: linked to PLACEMENT movements.';

-- Unique constraint: one ODV per client per type
CREATE UNIQUE INDEX idx_cabinet_sale_odv_unique ON cabinet_sale_odv_ids (odv_id, client_id, odv_type);
CREATE INDEX idx_cabinet_sale_odv_client ON cabinet_sale_odv_ids (client_id);
CREATE INDEX idx_cabinet_sale_odv_visit ON cabinet_sale_odv_ids (visit_id);

-- RLS
ALTER TABLE cabinet_sale_odv_ids ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON cabinet_sale_odv_ids
  FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "authenticated_read" ON cabinet_sale_odv_ids
  FOR SELECT TO authenticated USING (true);

-- ═══════════════════════════════════════════════════════════════════════════
-- Backfill cabinet_sale_odv_ids from saga_zoho_links
-- ═══════════════════════════════════════════════════════════════════════════

INSERT INTO cabinet_sale_odv_ids (odv_id, client_id, visit_id, odv_type, created_at)
SELECT DISTINCT
  szl.zoho_id,
  st.client_id,
  st.visit_id,
  CASE szl.type
    WHEN 'SALE' THEN 'SALE'
    WHEN 'CABINET' THEN 'CABINET'
    ELSE 'SALE'  -- default
  END,
  szl.created_at
FROM saga_zoho_links szl
JOIN saga_transactions st ON szl.id_saga_transaction = st.id
WHERE szl.zoho_id IS NOT NULL
  AND szl.type IN ('SALE', 'CABINET')
ON CONFLICT DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════
-- Update rpc_link_odv to also write to cabinet_sale_odv_ids
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_link_odv(
  p_visit_id uuid,
  p_odv_id text,
  p_odv_type text,          -- 'SALE' or 'CABINET'
  p_items jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_task_type visit_task_type;
  v_movement_type cabinet_movement_type;
  v_normalized_odv text;
  v_updated_count integer;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Validate and normalize ODV ID
  v_normalized_odv := TRIM(p_odv_id);
  IF v_normalized_odv ~ '^\d{1,5}$' THEN
    v_normalized_odv := 'DCOdV-' || v_normalized_odv;
  END IF;

  IF v_normalized_odv !~ '^DCOdV-[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Invalid ODV ID format: %. Expected DCOdV-NNNNN', p_odv_id;
  END IF;

  -- Map ODV type
  CASE p_odv_type
    WHEN 'SALE' THEN
      v_task_type := 'SALE_ODV';
      v_movement_type := 'SALE';
    WHEN 'CABINET' THEN
      v_task_type := 'ODV_CABINET';
      v_movement_type := 'PLACEMENT';
    ELSE
      RAISE EXCEPTION 'Invalid odv_type: %. Must be SALE or CABINET', p_odv_type;
  END CASE;

  -- Idempotency
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = v_task_type
      AND status IN ('COMPLETED', 'SKIPPED')
  ) THEN
    RETURN jsonb_build_object('success', true, 'already_completed', true);
  END IF;

  -- Mark movements as validated
  UPDATE inventory_movements
  SET validated = true
  WHERE visit_id = p_visit_id
    AND type = v_movement_type
    AND validated = false;

  GET DIAGNOSTICS v_updated_count = ROW_COUNT;

  -- Record ODV link in cabinet_sale_odv_ids
  INSERT INTO cabinet_sale_odv_ids (odv_id, client_id, visit_id, odv_type)
  VALUES (v_normalized_odv, v_visit.client_id, p_visit_id, p_odv_type)
  ON CONFLICT (odv_id, client_id, odv_type) DO NOTHING;

  -- Mark validation task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'odv_id', v_normalized_odv,
        'odv_type', p_odv_type,
        'movements_validated', v_updated_count,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = v_task_type;

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'odv_id', v_normalized_odv,
    'movements_validated', v_updated_count
  );
END;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- Rewrite clasificacion_base: saga_zoho_links → cabinet_sale_odv_ids
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION analytics.clasificacion_base(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(
  client_id character varying,
  client_name character varying,
  sku character varying,
  product character varying,
  condition character varying,
  brand character varying,
  is_top boolean,
  m_type text,
  first_event_date date,
  cabinet_revenue numeric,
  odv_revenue numeric,
  odv_quantity numeric,
  odv_transaction_count bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'analytics', 'public'
AS $function$
  WITH
  -- CHANGE: saga_zoho_links → cabinet_sale_odv_ids
  sale_odv_ids AS (
    SELECT DISTINCT csoi.odv_id AS zoho_id
    FROM cabinet_sale_odv_ids csoi
    WHERE csoi.odv_type = 'SALE'
  ),
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
    JOIN odv_sales v ON v.client_id = mp.client_id AND v.sku = mp.sku
      AND v.date >= mp.first_venta
    -- CHANGE: saga_odv_ids → sale_odv_ids
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM sale_odv_ids)
    GROUP BY mp.client_id, mp.sku
    HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  ),
  m3_candidates AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
      AND NOT EXISTS (
        SELECT 1 FROM inventory_movements mi2
        WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
      )
    GROUP BY mi.client_id, mi.sku
  ),
  m3_agg AS (
    SELECT mc.client_id, mc.sku, mc.first_creacion,
           COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc
    JOIN odv_sales v ON v.client_id = mc.client_id AND v.sku = mc.sku
      AND v.date >= mc.first_creacion
    -- CHANGE: saga_odv_ids → sale_odv_ids
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM sale_odv_ids)
      AND NOT EXISTS (
        SELECT 1 FROM odv_sales v2
        WHERE v2.client_id = mc.client_id AND v2.sku = mc.sku AND v2.date <= mc.first_creacion
      )
    GROUP BY mc.client_id, mc.sku, mc.first_creacion
    HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  )
  -- M1 rows
  SELECT mp.client_id::varchar, c.client_name::varchar, mp.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M1'::text,
    mp.first_venta,
    mp.revenue_botiquin, 0::numeric, 0::numeric, 0::bigint
  FROM m1_pairs mp
  JOIN clients c ON c.client_id = mp.client_id
  JOIN medications m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku
  UNION ALL
  -- M2 rows
  SELECT m2.client_id::varchar, c.client_name::varchar, m2.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M2'::text,
    mp.first_venta,
    mp.revenue_botiquin, m2.revenue_odv, m2.cantidad_odv, m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id
  JOIN medications m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku
  UNION ALL
  -- M3 rows
  SELECT m3.client_id::varchar, c.client_name::varchar, m3.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M3'::text,
    m3.first_creacion,
    0::numeric, m3.revenue_odv, m3.cantidad_odv, m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clients c ON c.client_id = m3.client_id
  JOIN medications m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$function$;

-- Update public wrapper
CREATE OR REPLACE FUNCTION public.clasificacion_base(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(
  client_id character varying, client_name character varying,
  sku character varying, product character varying,
  condition character varying, brand character varying,
  is_top boolean, m_type text, first_event_date date,
  cabinet_revenue numeric, odv_revenue numeric,
  odv_quantity numeric, odv_transaction_count bigint
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$$;

COMMENT ON FUNCTION analytics.clasificacion_base(character varying[], character varying[], character varying[], date, date) IS
'SINGLE SOURCE OF TRUTH for M1/M2/M3 classification. Uses cabinet_sale_odv_ids (replacing saga_zoho_links) for M1/M2 deduplication. Maps every (client, SKU) to M1 (cabinet sale), M2 (cabinet→ODV conversion), M3 (exposure→ODV).';

-- ═══════════════════════════════════════════════════════════════════════════
-- Rewrite get_billing_composition: saga joins → cabinet_sale_odv_ids
-- ═══════════════════════════════════════════════════════════════════════════
-- Note: We only replace the m1_odv_ids CTE. The rest of the function is unchanged.

-- The full function is large, so we use a targeted approach:
-- Drop and recreate with cabinet_sale_odv_ids instead of saga_zoho_links+saga_transactions

CREATE OR REPLACE FUNCTION analytics.get_billing_composition(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(
  client_id character varying, client_name character varying,
  current_tier character varying, previous_tier character varying,
  active boolean, baseline numeric, current_billing numeric,
  current_m1 numeric, current_m2 numeric, current_m3 numeric,
  current_unlinked numeric, growth_pct numeric,
  linked_pct numeric, linked_value numeric,
  linked_pieces bigint, linked_skus bigint
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'analytics', 'public'
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
  -- CHANGE: saga_zoho_links + saga_transactions → cabinet_sale_odv_ids
  m1_odv_ids AS (
    SELECT DISTINCT csoi.odv_id, csoi.client_id
    FROM cabinet_sale_odv_ids csoi
    WHERE csoi.odv_type = 'SALE'
  ),
  m1_impacto AS (
    SELECT mi.client_id,
      SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
      SUM(mi.quantity) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM inventory_movements mi
    WHERE mi.type = 'SALE'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id
  ),
  first_venta AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_venta
    FROM inventory_movements mi
    WHERE mi.type = 'SALE'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mi.client_id, mi.sku
  ),
  first_creacion AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND NOT EXISTS (
        SELECT 1 FROM inventory_movements mi2
        WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
      )
    GROUP BY mi.client_id, mi.sku
  ),
  prior_odv AS (
    SELECT DISTINCT v.client_id, v.sku
    FROM odv_sales v
    JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku AND v.date <= fc.first_creacion
  ),
  categorized AS (
    SELECT v.client_id, v.sku, v.date, v.quantity,
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
    SELECT cat.client_id,
      SUM(CASE WHEN cat.categoria = 'M1' THEN cat.line_total ELSE 0 END) AS m1_total,
      SUM(CASE WHEN cat.categoria = 'M2' THEN cat.line_total ELSE 0 END) AS m2_total,
      SUM(CASE WHEN cat.categoria = 'M3' THEN cat.line_total ELSE 0 END) AS m3_total,
      SUM(CASE WHEN cat.categoria = 'UNLINKED' THEN cat.line_total ELSE 0 END) AS unlinked_total
    FROM categorized cat
    GROUP BY cat.client_id
  )
  SELECT
    c.client_id::varchar, c.client_name::varchar,
    c.current_tier::varchar AS current_tier,
    c.tier::varchar AS previous_tier,
    (c.status = 'ACTIVE')::boolean AS active,
    COALESCE(c.avg_billing, 0)::numeric AS baseline,
    COALESCE(c.current_billing, 0)::numeric AS current_billing,
    COALESCE(m1i.m1_valor, 0)::numeric AS current_m1,
    COALESCE(t.m2_total, 0)::numeric AS current_m2,
    COALESCE(t.m3_total, 0)::numeric AS current_m3,
    COALESCE(t.unlinked_total, 0)::numeric AS current_unlinked,
    CASE WHEN COALESCE(c.avg_billing, 0) > 0
      THEN ((COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) / c.avg_billing * 100)
      ELSE 0
    END::numeric AS growth_pct,
    CASE WHEN COALESCE(c.current_billing, 0) > 0
      THEN (COALESCE(m1i.m1_valor, 0) / c.current_billing * 100)
      ELSE 0
    END::numeric AS linked_pct,
    COALESCE(m1i.m1_valor, 0)::numeric AS linked_value,
    COALESCE(m1i.m1_piezas, 0)::bigint AS linked_pieces,
    COALESCE(m1i.m1_skus, 0)::bigint AS linked_skus
  FROM clients c
  LEFT JOIN totals t ON c.client_id = t.client_id
  LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
  WHERE c.tier IS NOT NULL
    AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ORDER BY (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) DESC;
$function$;

COMMENT ON FUNCTION analytics.get_billing_composition(character varying[], character varying[], character varying[], date, date) IS
'Per-client billing composition with M1/M2/M3/UNLINKED breakdown. Uses cabinet_sale_odv_ids for M1 ODV identification (replacing saga_zoho_links).';

-- ═══════════════════════════════════════════════════════════════════════════
-- Update get_active_collection to support IN_TRANSIT status
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION analytics.get_active_collection()
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'analytics', 'public'
AS $$
  SELECT json_build_object(
    'pending_count', (SELECT COUNT(*) FROM collections WHERE status = 'PENDIENTE'),
    'in_transit_count', (SELECT COUNT(*) FROM collections WHERE status = 'IN_TRANSIT'),
    'delivered_count', (SELECT COUNT(*) FROM collections WHERE status = 'ENTREGADA'
                        AND delivered_at > now() - interval '30 days'),
    'pending_items', (
      SELECT COALESCE(SUM(ci.quantity), 0) FROM collection_items ci
      JOIN collections c ON ci.collection_id = c.collection_id
      WHERE c.status = 'PENDIENTE'
    ),
    'in_transit_items', (
      SELECT COALESCE(SUM(ci.quantity), 0) FROM collection_items ci
      JOIN collections c ON ci.collection_id = c.collection_id
      WHERE c.status = 'IN_TRANSIT'
    )
  );
$$;

-- Update public wrapper
CREATE OR REPLACE FUNCTION public.get_active_collection()
RETURNS json
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT analytics.get_active_collection();
$$;

COMMENT ON FUNCTION analytics.get_active_collection() IS
'Active collection metrics: pending, in-transit, and recently delivered counts with item totals. Supports IN_TRANSIT status.';

-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

GRANT SELECT ON cabinet_sale_odv_ids TO authenticated, anon;
GRANT INSERT ON cabinet_sale_odv_ids TO authenticated;

NOTIFY pgrst, 'reload schema';
