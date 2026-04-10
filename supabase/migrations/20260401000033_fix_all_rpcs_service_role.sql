-- Fix: ALL write RPCs need SET role = 'service_role' to guarantee BYPASSRLS.
-- On this Supabase project, SECURITY DEFINER (postgres) alone doesn't bypass RLS.
-- ALTER FUNCTION adds the SET clause without needing to recreate the function body.

-- Direct RPCs (from migration 000007)
ALTER FUNCTION rpc_register_placement(uuid, jsonb) SET role = 'service_role';
ALTER FUNCTION rpc_register_cutoff(uuid, jsonb) SET role = 'service_role';
ALTER FUNCTION rpc_register_post_cutoff_placement(uuid, jsonb) SET role = 'service_role';
ALTER FUNCTION rpc_link_odv(uuid, text, text, jsonb) SET role = 'service_role';
ALTER FUNCTION rpc_start_collection_transit(uuid) SET role = 'service_role';
-- rpc_register_collection_delivery already fixed in migration 000032

-- Compensate (from migration 000013)
ALTER FUNCTION rpc_compensate_visit_v2(uuid, text) SET role = 'service_role';

-- Internal helper used by cutoff/collection RPCs
ALTER FUNCTION _consume_lots_fefo(character varying, character varying, integer, text, bigint) SET role = 'service_role';

-- Read-only RPCs (belt-and-suspenders for SELECT through RLS)
ALTER FUNCTION rpc_get_visit_summary_v2(uuid) SET role = 'service_role';
ALTER FUNCTION rpc_get_expiring_items(character varying, integer) SET role = 'service_role';
ALTER FUNCTION rpc_get_pending_validations(character varying) SET role = 'service_role';

NOTIFY pgrst, 'reload schema';
