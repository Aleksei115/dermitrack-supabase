-- ============================================================================
-- Migration 12: Semantic Layer — Bronze/Silver/Gold views for AI queries
-- Fase 4: AI-optimized views in metadata schema
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- BRONZE: Clean data with human-readable names, no business logic
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW metadata.bronze_inventory_movements AS
SELECT
  im.id,
  im.visit_id,
  im.client_id,
  c.client_name,
  c.tier AS client_tier,
  im.sku,
  m.product,
  m.brand,
  im.type AS movement_type,
  im.quantity,
  im.unit_price,
  (im.quantity * COALESCE(im.unit_price, 0))::numeric AS total_value,
  im.quantity_before,
  im.quantity_after,
  im.validated,
  im.movement_date,
  im.movement_date::date AS movement_day
FROM inventory_movements im
JOIN clients c ON im.client_id = c.client_id
JOIN medications m ON im.sku = m.sku;

COMMENT ON VIEW metadata.bronze_inventory_movements IS
'Human-readable inventory movements with client name, product name, brand, tier. Use for AI queries: "show movements for doctor X", "what was sold last week".';

CREATE OR REPLACE VIEW metadata.bronze_visits AS
SELECT
  v.visit_id,
  v.client_id,
  c.client_name,
  v.user_id,
  u.name AS rep_name,
  v.type AS visit_type,
  v.status AS visit_status,
  v.saga_status,
  v.corte_number,
  v.created_at,
  v.completed_at,
  (v.completed_at - v.created_at) AS duration
FROM visits v
JOIN clients c ON v.client_id = c.client_id
JOIN users u ON v.user_id::text = u.user_id::text;

COMMENT ON VIEW metadata.bronze_visits IS
'Visits with client and rep names. Use for AI queries: "visits for doctor X", "how many visits did rep Y do this month".';

CREATE OR REPLACE VIEW metadata.bronze_collections AS
SELECT
  col.collection_id,
  col.visit_id,
  col.client_id,
  c.client_name,
  col.user_id,
  u.name AS rep_name,
  col.status,
  col.transit_started_at,
  col.delivered_at,
  col.cedis_responsible_name,
  COALESCE(items.item_count, 0) AS item_count,
  COALESCE(items.total_quantity, 0) AS total_quantity
FROM collections col
JOIN clients c ON col.client_id = c.client_id
JOIN users u ON col.user_id::text = u.user_id::text
LEFT JOIN LATERAL (
  SELECT COUNT(*) AS item_count, SUM(quantity) AS total_quantity
  FROM collection_items ci WHERE ci.collection_id = col.collection_id
) items ON true;

COMMENT ON VIEW metadata.bronze_collections IS
'Collections with status, rep name, and item counts. Use for AI queries: "pending collections", "collections for doctor X".';

-- ═══════════════════════════════════════════════════════════════════════════
-- SILVER: Business logic applied, curated datasets
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW metadata.silver_cabinet_status AS
SELECT
  ci.client_id,
  c.client_name,
  c.tier AS client_tier,
  c.status AS client_status,
  ci.sku,
  m.product,
  m.brand,
  ci.available_quantity,
  lot_agg.total_lot_quantity,
  lot_agg.lot_count,
  lot_agg.earliest_expiry,
  lot_agg.days_until_expiry,
  CASE
    WHEN lot_agg.days_until_expiry IS NULL THEN 'NO_EXPIRY'
    WHEN lot_agg.days_until_expiry <= 0 THEN 'EXPIRED'
    WHEN lot_agg.days_until_expiry <= 30 THEN 'EXPIRING_SOON'
    WHEN lot_agg.days_until_expiry <= 90 THEN 'EXPIRING_QUARTER'
    ELSE 'OK'
  END AS expiry_status
FROM cabinet_inventory ci
JOIN clients c ON ci.client_id = c.client_id
JOIN medications m ON ci.sku = m.sku
LEFT JOIN LATERAL (
  SELECT
    SUM(remaining_quantity) AS total_lot_quantity,
    COUNT(*) AS lot_count,
    MIN(expiry_date) AS earliest_expiry,
    (MIN(expiry_date) - CURRENT_DATE)::integer AS days_until_expiry
  FROM cabinet_inventory_lots
  WHERE client_id = ci.client_id AND sku = ci.sku AND status = 'active'
) lot_agg ON true;

COMMENT ON VIEW metadata.silver_cabinet_status IS
'Current cabinet stock with expiry status per (client, SKU). Use for AI queries: "which doctors have expiring products?", "show cabinet status for doctor X", "products expiring this month".';

CREATE OR REPLACE VIEW metadata.silver_visit_progress AS
SELECT
  v.visit_id,
  v.client_id,
  c.client_name,
  v.user_id,
  u.name AS rep_name,
  v.type AS visit_type,
  v.status AS visit_status,
  v.corte_number,
  v.created_at,
  task_agg.total_tasks,
  task_agg.completed_tasks,
  task_agg.pending_tasks,
  ROUND(task_agg.completed_tasks::numeric / NULLIF(task_agg.total_tasks, 0) * 100, 1) AS progress_pct,
  task_agg.current_task,
  mov_agg.total_movements,
  mov_agg.validated_movements,
  mov_agg.unvalidated_movements,
  mov_agg.total_sale_value,
  mov_agg.total_placement_value
FROM visits v
JOIN clients c ON v.client_id = c.client_id
JOIN users u ON v.user_id::text = u.user_id::text
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_tasks,
    COUNT(*) FILTER (WHERE status IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M')) AS completed_tasks,
    COUNT(*) FILTER (WHERE status IN ('PENDING', 'IN_PROGRESS')) AS pending_tasks,
    (SELECT task_type FROM visit_tasks
     WHERE visit_id = v.visit_id AND status IN ('PENDING', 'IN_PROGRESS')
     ORDER BY step_order LIMIT 1) AS current_task
  FROM visit_tasks WHERE visit_id = v.visit_id
) task_agg ON true
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_movements,
    COUNT(*) FILTER (WHERE validated = true) AS validated_movements,
    COUNT(*) FILTER (WHERE validated = false) AS unvalidated_movements,
    SUM(quantity * COALESCE(unit_price, 0)) FILTER (WHERE type = 'SALE') AS total_sale_value,
    SUM(quantity * COALESCE(unit_price, 0)) FILTER (WHERE type = 'PLACEMENT') AS total_placement_value
  FROM inventory_movements WHERE visit_id = v.visit_id
) mov_agg ON true
WHERE v.status IN ('PENDING', 'IN_PROGRESS');

COMMENT ON VIEW metadata.silver_visit_progress IS
'Active visit progress with task completion %, current task, and movement validation status. Use for AI queries: "which visits are in progress?", "what is rep X working on?".';

-- ═══════════════════════════════════════════════════════════════════════════
-- GOLD: Pre-aggregated analytics, ready for reporting
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW metadata.gold_monthly_revenue AS
SELECT
  DATE_TRUNC('month', im.movement_date)::date AS month,
  m.brand,
  SUM(im.quantity * COALESCE(im.unit_price, 0))::numeric AS m1_revenue,
  SUM(im.quantity)::bigint AS pieces_sold,
  COUNT(DISTINCT im.client_id)::integer AS active_doctors,
  COUNT(DISTINCT im.sku)::integer AS unique_skus
FROM inventory_movements im
JOIN medications m ON im.sku = m.sku
WHERE im.type = 'SALE'
GROUP BY 1, 2;

COMMENT ON VIEW metadata.gold_monthly_revenue IS
'Monthly M1 cabinet revenue by brand. Use for AI queries: "revenue this month?", "which brand sells most?", "revenue trend last 6 months?", "how does Brand X compare to Brand Y?".';

CREATE OR REPLACE VIEW metadata.gold_doctor_summary AS
SELECT
  c.client_id,
  c.client_name,
  c.tier,
  c.status AS client_status,
  c.current_billing,
  inv.cabinet_skus,
  inv.cabinet_total_units,
  inv.expiring_units,
  mov.total_m1_revenue,
  mov.total_placements,
  mov.total_collections,
  mov.last_movement_date,
  vis.total_visits,
  vis.last_visit_date
FROM clients c
LEFT JOIN LATERAL (
  SELECT
    COUNT(DISTINCT sku) AS cabinet_skus,
    SUM(available_quantity) AS cabinet_total_units,
    (SELECT COALESCE(SUM(l.remaining_quantity), 0)
     FROM cabinet_inventory_lots l
     WHERE l.client_id = c.client_id AND l.status = 'active'
       AND l.expiry_date IS NOT NULL AND l.expiry_date <= CURRENT_DATE + interval '30 days'
    ) AS expiring_units
  FROM cabinet_inventory ci WHERE ci.client_id = c.client_id
) inv ON true
LEFT JOIN LATERAL (
  SELECT
    SUM(quantity * COALESCE(unit_price, 0)) FILTER (WHERE type = 'SALE') AS total_m1_revenue,
    SUM(quantity) FILTER (WHERE type = 'PLACEMENT') AS total_placements,
    SUM(quantity) FILTER (WHERE type = 'COLLECTION') AS total_collections,
    MAX(movement_date) AS last_movement_date
  FROM inventory_movements im WHERE im.client_id = c.client_id
) mov ON true
LEFT JOIN LATERAL (
  SELECT
    COUNT(*) AS total_visits,
    MAX(created_at) AS last_visit_date
  FROM visits v WHERE v.client_id = c.client_id
) vis ON true
WHERE c.status != 'INACTIVE';

COMMENT ON VIEW metadata.gold_doctor_summary IS
'Comprehensive doctor summary: tier, billing, cabinet status, expiry alerts, revenue, visits. Use for AI queries: "tell me about doctor X", "which doctors need attention?", "top doctors by revenue".';

CREATE OR REPLACE VIEW metadata.gold_expiry_overview AS
SELECT
  c.client_id,
  c.client_name,
  l.sku,
  m.product,
  m.brand,
  l.remaining_quantity,
  l.expiry_date,
  (l.expiry_date - CURRENT_DATE)::integer AS days_until_expiry,
  CASE
    WHEN l.expiry_date <= CURRENT_DATE THEN 'EXPIRED'
    WHEN l.expiry_date <= CURRENT_DATE + interval '30 days' THEN 'CRITICAL'
    WHEN l.expiry_date <= CURRENT_DATE + interval '90 days' THEN 'WARNING'
    ELSE 'OK'
  END AS urgency
FROM cabinet_inventory_lots l
JOIN clients c ON l.client_id = c.client_id
JOIN medications m ON l.sku = m.sku
WHERE l.status = 'active'
  AND l.expiry_date IS NOT NULL
  AND l.expiry_date <= CURRENT_DATE + interval '90 days'
ORDER BY l.expiry_date ASC;

COMMENT ON VIEW metadata.gold_expiry_overview IS
'Products expiring within 90 days with urgency levels. Use for AI queries: "what is expiring soon?", "expired products", "products expiring this month".';

-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS (default privileges already set in migration 1)
-- ═══════════════════════════════════════════════════════════════════════════

GRANT SELECT ON metadata.bronze_inventory_movements TO authenticated, anon;
GRANT SELECT ON metadata.bronze_visits TO authenticated, anon;
GRANT SELECT ON metadata.bronze_collections TO authenticated, anon;
GRANT SELECT ON metadata.silver_cabinet_status TO authenticated, anon;
GRANT SELECT ON metadata.silver_visit_progress TO authenticated, anon;
GRANT SELECT ON metadata.gold_monthly_revenue TO authenticated, anon;
GRANT SELECT ON metadata.gold_doctor_summary TO authenticated, anon;
GRANT SELECT ON metadata.gold_expiry_overview TO authenticated, anon;

NOTIFY pgrst, 'reload schema';
