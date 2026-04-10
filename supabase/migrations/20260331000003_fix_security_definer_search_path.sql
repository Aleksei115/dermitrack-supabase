-- ============================================================================
-- Migration 0D: Fix SECURITY DEFINER functions missing SET search_path
-- Fix: Prevent search_path attacks on 8 SECURITY DEFINER functions
-- ============================================================================

ALTER FUNCTION analytics.get_active_collection() SET search_path = 'analytics', 'public';
ALTER FUNCTION analytics.get_billing_composition_legacy() SET search_path = 'analytics', 'public';
ALTER FUNCTION analytics.get_cabinet_data() SET search_path = 'analytics', 'public';
ALTER FUNCTION analytics.get_cutoff_general_stats_with_comparison() SET search_path = 'analytics', 'public';
ALTER FUNCTION analytics.get_historical_cutoff_data(character varying[], character varying[], character varying[], date, date) SET search_path = 'analytics', 'public';
ALTER FUNCTION analytics.get_previous_cutoff_stats() SET search_path = 'analytics', 'public';
ALTER FUNCTION public.rpc_get_cutoff_items(uuid) SET search_path = 'public';
ALTER FUNCTION public.rpc_owner_delete_visit(uuid, text, text) SET search_path = 'public';
