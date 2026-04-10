-- Fix: "cannot set parameter role within security-definer function"
-- Migrations 000032-000034 added SET role = 'service_role' to SECURITY DEFINER functions,
-- but this project doesn't allow SET role within SECURITY DEFINER.
-- Also, OWNER TO service_role is blocked ("permission denied for schema public").
--
-- Correct approach:
-- 1. RESET role on all affected functions (undo the broken SET role)
-- 2. Add "postgres_all" RLS policy on ALL tables with RLS enabled
--    so SECURITY DEFINER functions (running as postgres) can read/write freely.

-- ═══ Step 1: RESET role on ALL functions that had SET role added ═══

-- From migration 000007 (direct RPCs)
DO $$ BEGIN ALTER FUNCTION rpc_register_placement(uuid, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_register_cutoff(uuid, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_register_post_cutoff_placement(uuid, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_link_odv(uuid, text, text, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_start_collection_transit(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_register_collection_delivery(uuid, text, text, text, text[]) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_compensate_visit_v2(uuid, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION _consume_lots_fefo(character varying, character varying, integer, text, bigint) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_visit_summary_v2(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_expiring_items(character varying, integer) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_pending_validations(character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Legacy RPCs
DO $$ BEGIN ALTER FUNCTION rpc_try_complete_visit(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_submit_visit_report(uuid, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_start_task(uuid, visit_task_type) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_skip_sale_odv(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_skip_collection(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_create_visit(character varying, character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_create_visit(character varying, integer, character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_change_client_status(character varying, client_status, character varying, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_next_visit_type(character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_client_tiers() RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_client_status_history(character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_cutoff_items(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_cutoff_holding_items(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Admin RPCs
DO $$ BEGIN ALTER FUNCTION rpc_admin_get_all_visits(integer, integer, text, text, date, date) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_get_visit_detail(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_rollback_visit(uuid, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_force_task_status(uuid, character varying, text, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_compensate_task(uuid, character varying, text, jsonb) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_compensate_task_v2(uuid, text, text, text, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_retry_pivot(uuid, character varying) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_retry_validation(uuid, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Compensation RPCs
DO $$ BEGIN ALTER FUNCTION rpc_can_compensate_visit(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_compensate_placement(uuid, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_compensate_cutoff(uuid, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Trigger functions
DO $$ BEGIN ALTER FUNCTION audit_trigger_func() RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION fn_remove_available_sku_on_sale() RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION fn_sync_inventory_from_movements() RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- Chatbot RPCs
DO $$ BEGIN ALTER FUNCTION chatbot.get_remaining_queries(uuid) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION chatbot.send_message(uuid, text, text, boolean, text) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION chatbot.rate_message(uuid, integer) RESET role; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- ═══ Step 2: Add postgres_all RLS policy on ALL RLS-enabled tables ═══
-- This lets SECURITY DEFINER functions (running as postgres) bypass RLS.
-- Skips tables that already have a postgres_all policy (duplicate_object).

DO $$
DECLARE
  t record;
BEGIN
  FOR t IN
    SELECT c.relname AS tablename, n.nspname AS schemaname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'          -- ordinary tables only
      AND c.relrowsecurity = true  -- RLS is enabled
      AND n.nspname IN ('public', 'chatbot')
  LOOP
    BEGIN
      EXECUTE format(
        'CREATE POLICY postgres_all ON %I.%I FOR ALL TO postgres USING (true) WITH CHECK (true)',
        t.schemaname, t.tablename
      );
      RAISE NOTICE 'Created postgres_all policy on %.%', t.schemaname, t.tablename;
    EXCEPTION
      WHEN duplicate_object THEN
        RAISE NOTICE 'Policy already exists on %.%', t.schemaname, t.tablename;
    END;
  END LOOP;
END
$$;

NOTIFY pgrst, 'reload schema';
