-- =============================================================================
-- Fix inventory_movements.type column enum type
-- =============================================================================
-- On PROD, the column uses `inventory_movement_type` (INBOUND, OUTBOUND, SALE,
-- PLACEMENT, COLLECTION, HOLDING) instead of `cabinet_movement_type` (SALE,
-- COLLECTION, HOLDING, PLACEMENT). This was changed outside of migrations at
-- some point in PROD's history.
--
-- All RPCs cast to `cabinet_movement_type`, causing:
--   ERROR: operator does not exist: inventory_movement_type = cabinet_movement_type
--
-- All existing rows use values that exist in both enums (SALE, COLLECTION,
-- HOLDING, PLACEMENT). No rows use INBOUND or OUTBOUND.
--
-- This migration changes the column to `cabinet_movement_type` to match DEV
-- and all RPC expectations. It must drop and recreate 6 dependent views.
--
-- Idempotent: if the column is already `cabinet_movement_type`, no change.
-- =============================================================================

DO $$
DECLARE
  v_current_type text;
BEGIN
  SELECT udt_name INTO v_current_type
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'inventory_movements'
    AND column_name = 'type';

  IF v_current_type = 'cabinet_movement_type' THEN
    RAISE NOTICE 'inventory_movements.type already uses cabinet_movement_type — no change needed';
    RETURN;
  END IF;

  -- Verify no rows use values outside cabinet_movement_type
  IF EXISTS (
    SELECT 1 FROM inventory_movements
    WHERE type::text NOT IN ('SALE', 'COLLECTION', 'HOLDING', 'PLACEMENT')
  ) THEN
    RAISE EXCEPTION 'Found rows with values not in cabinet_movement_type — aborting';
  END IF;

  -- ── Step 1: Drop all dependent views ──────────────────────────────────────
  DROP VIEW IF EXISTS analytics.inventory_movements;
  DROP VIEW IF EXISTS metadata.bronze_inventory_movements;
  DROP VIEW IF EXISTS metadata.gold_doctor_summary;
  DROP VIEW IF EXISTS metadata.gold_monthly_revenue;
  DROP VIEW IF EXISTS metadata.silver_visit_progress;
  DROP VIEW IF EXISTS public.v_visit_tasks_operational;

  -- ── Step 2: Change column type ────────────────────────────────────────────
  ALTER TABLE inventory_movements
    ALTER COLUMN type TYPE cabinet_movement_type
    USING type::text::cabinet_movement_type;

  -- ── Audit ─────────────────────────────────────────────────────────────────
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_after)
  VALUES (
    'inventory_movements',
    'fix-type-enum-20260411',
    'UPDATE',
    NULL,
    jsonb_build_object(
      'reason', 'Fix column type from inventory_movement_type to cabinet_movement_type',
      'previous_type', v_current_type,
      'new_type', 'cabinet_movement_type'
    )
  );

  RAISE NOTICE 'Changed inventory_movements.type from % to cabinet_movement_type', v_current_type;
END $$;

-- ── Step 3: Recreate all views ────────────────────────────────────────────
-- Definitions taken from canonical migrations (20260401000011, 20260401000014,
-- 20260401000029). PostgreSQL infers the correct enum casts from the column type.

CREATE OR REPLACE VIEW analytics.inventory_movements AS
SELECT im.*
FROM public.inventory_movements im
JOIN public.visits v ON im.visit_id = v.visit_id;

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

CREATE OR REPLACE VIEW v_visit_tasks_operational AS
SELECT
  task_id::text AS task_id,
  visit_id,
  task_type,
  status,
  required,
  created_at,
  started_at,
  completed_at,
  due_at,
  last_activity_at,
  reference_table,
  reference_id,
  metadata,
  transaction_type::text AS transaction_type,
  step_order,
  'NOT_NEEDED'::text AS compensation_status,
  '{}'::jsonb AS input_payload,
  '{}'::jsonb AS output_result,
  NULL::jsonb AS compensation_payload,
  gen_random_uuid()::text AS idempotency_key,
  0 AS retry_count,
  3 AS max_retries,
  NULL::text AS last_error,
  NULL::timestamptz AS compensation_executed_at,
  CASE
    WHEN status = 'COMPLETED' THEN 'COMPLETED'::visit_task_status
    WHEN status = 'SKIPPED_M' THEN status
    WHEN status = 'SKIPPED' THEN status
    WHEN status = 'ERROR' THEN status
    WHEN due_at IS NOT NULL AND (due_at + interval '1 day') < now()
      AND status NOT IN ('COMPLETED', 'SKIPPED_M', 'SKIPPED')
      THEN 'DELAYED'::visit_task_status
    ELSE status
  END AS operational_status,
  CASE
    WHEN task_type = 'SALE_ODV' THEN (
      SELECT string_agg(cso.odv_id, ', ' ORDER BY cso.created_at)
      FROM cabinet_sale_odv_ids cso
      WHERE cso.visit_id = vt.visit_id AND cso.odv_type = 'SALE'
    )
    WHEN task_type = 'ODV_CABINET' THEN (
      SELECT string_agg(cso.odv_id, ', ' ORDER BY cso.created_at)
      FROM cabinet_sale_odv_ids cso
      WHERE cso.visit_id = vt.visit_id AND cso.odv_type = 'CABINET'
    )
    ELSE NULL
  END AS odv_id,
  CASE
    WHEN task_type = 'SALE_ODV' THEN (
      SELECT COALESCE(SUM(im.quantity), 0)::int
      FROM inventory_movements im
      WHERE im.visit_id = vt.visit_id AND im.type = 'SALE'
    )
    WHEN task_type = 'ODV_CABINET' THEN (
      SELECT COALESCE(SUM(im.quantity), 0)::int
      FROM inventory_movements im
      WHERE im.visit_id = vt.visit_id AND im.type = 'PLACEMENT'
    )
    ELSE NULL
  END AS odv_total_pieces
FROM visit_tasks vt;

-- ── Step 4: Restore grants ──────────────────────────────────────────────────

GRANT SELECT ON analytics.inventory_movements TO authenticated, anon, service_role;
GRANT SELECT ON metadata.bronze_inventory_movements TO authenticated, anon;
GRANT SELECT ON metadata.silver_visit_progress TO authenticated, anon;
GRANT SELECT ON metadata.gold_monthly_revenue TO authenticated, anon;
GRANT SELECT ON metadata.gold_doctor_summary TO authenticated, anon;
GRANT ALL ON public.v_visit_tasks_operational TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
