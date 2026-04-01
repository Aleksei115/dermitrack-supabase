-- Fix ALL remaining SECURITY DEFINER functions to use service_role.
-- Each wrapped in DO block so missing functions are silently skipped.

-- ═══ Legacy RPCs (from remote_schema, used by mobile app) ═══

DO $$ BEGIN ALTER FUNCTION rpc_try_complete_visit(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_submit_visit_report(uuid, jsonb) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_start_task(uuid, visit_task_type) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_skip_sale_odv(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_skip_collection(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_create_visit(character varying, character varying) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_create_visit(character varying, integer, character varying) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_change_client_status(character varying, client_status, character varying, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_next_visit_type(character varying) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_client_tiers() SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_client_status_history(character varying) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_cutoff_items(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_get_cutoff_holding_items(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- ═══ Admin RPCs ═══

DO $$ BEGIN ALTER FUNCTION rpc_admin_get_all_visits(integer, integer, text, text, date, date) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_get_visit_detail(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_rollback_visit(uuid, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_force_task_status(uuid, character varying, text, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_compensate_task(uuid, character varying, text, jsonb) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_compensate_task_v2(uuid, text, text, text, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_retry_pivot(uuid, character varying) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_admin_retry_validation(uuid, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- ═══ Compensation RPCs ═══

DO $$ BEGIN ALTER FUNCTION rpc_can_compensate_visit(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_compensate_placement(uuid, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION rpc_compensate_cutoff(uuid, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- ═══ Trigger functions (SECURITY DEFINER that may hit RLS) ═══

DO $$ BEGIN ALTER FUNCTION audit_trigger_func() SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION fn_remove_available_sku_on_sale() SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION fn_sync_inventory_from_movements() SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- ═══ Chatbot RPCs ═══

DO $$ BEGIN ALTER FUNCTION chatbot.get_remaining_queries(uuid) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION chatbot.send_message(uuid, text, text, boolean, text) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;
DO $$ BEGIN ALTER FUNCTION chatbot.rate_message(uuid, integer) SET role = 'service_role'; EXCEPTION WHEN undefined_function THEN NULL; END $$;

NOTIFY pgrst, 'reload schema';
