-- =============================================================================
-- Migration 27: Exclude compensated visit movements from analytics
-- =============================================================================
-- Problem: When a visit is compensated (rpc_compensate_visit_v2), reverse
-- movements are created in inventory_movements but the originals remain.
-- None of the 26 analytics functions filter by workflow_status, causing
-- compensated data to inflate metrics (M1/M2/M3, balance, revenue, etc.).
--
-- Solution: Create a VIEW analytics.inventory_movements that shadows the
-- public table. Since all analytics functions use:
--   SET search_path TO 'analytics', 'public'
-- PostgreSQL resolves analytics.inventory_movements first, transparently
-- filtering out movements from compensated visits WITHOUT modifying any
-- of the 26 analytics functions.
--
-- The public.inventory_movements table is unchanged — RPCs that INSERT/UPDATE
-- (rpc_register_cutoff, rpc_compensate_visit_v2, etc.) use search_path='public'
-- and continue to write to the real table.
-- =============================================================================

BEGIN;

-- Step 1: Create the shadow view in analytics schema
CREATE OR REPLACE VIEW analytics.inventory_movements AS
SELECT im.*
FROM public.inventory_movements im
JOIN public.visits v ON im.visit_id = v.visit_id
WHERE v.workflow_status IS DISTINCT FROM 'COMPENSATED';

COMMENT ON VIEW analytics.inventory_movements IS
'Shadow view that excludes movements from compensated visits. Analytics functions resolve this view first via search_path = ''analytics'', ''public''. RPCs continue to write to public.inventory_movements.';

-- Step 2: Grant same permissions as the underlying table
GRANT SELECT ON analytics.inventory_movements TO authenticated, anon, service_role;

-- Step 3: Create index on visits.workflow_status for efficient filtering
CREATE INDEX IF NOT EXISTS idx_visits_workflow_status
  ON public.visits (workflow_status)
  WHERE workflow_status = 'COMPENSATED';

-- Step 4: Update search_path for analytics functions that currently use
-- search_path='public'. They need 'analytics' first so they resolve to
-- the shadow view instead of the public table.
-- Only the 20 functions that have search_path=public AND reference
-- inventory_movements need this change.

-- Get function signatures from pg_proc to build correct ALTER statements
ALTER FUNCTION analytics.get_available_filters()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_balance_metrics()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_brand_performance(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_client_audit(character varying)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_condition_performance(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_crosssell_significance()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_current_cutoff_data(character varying[], character varying[], character varying[])
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_cutoff_available_filters()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_cutoff_general_stats()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_cutoff_logistics_data(character varying[], character varying[], character varying[])
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_cutoff_skus_value_per_visit(character varying, character varying)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_cutoff_stats_by_doctor()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_dashboard_data(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_dashboard_static()
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_historical_skus_value_per_visit(date, date, character varying)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_impact_detail(text, character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_market_analysis(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_product_interest(integer, character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

ALTER FUNCTION analytics.get_yoy_by_condition(character varying[], character varying[], character varying[], date, date)
  SET search_path TO 'analytics', 'public';

COMMIT;

NOTIFY pgrst, 'reload schema';
