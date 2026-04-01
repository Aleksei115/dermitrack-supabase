-- Fix: SECURITY DEFINER functions running as postgres cannot write to
-- cabinet_inventory_lots because the only write policy is TO service_role.
-- On newer Supabase projects, postgres may not have BYPASSRLS, so we need
-- an explicit policy.
--
-- Affected RPCs: rpc_register_placement, rpc_register_cutoff,
-- rpc_register_post_cutoff_placement, rpc_register_collection_delivery,
-- rpc_compensate_visit_v2 (all SECURITY DEFINER, run as postgres,
-- write to cabinet_inventory_lots directly or via _consume_lots_fefo).

-- Allow postgres role full access (used by SECURITY DEFINER functions)
CREATE POLICY "postgres_all" ON cabinet_inventory_lots
  FOR ALL TO postgres USING (true) WITH CHECK (true);

-- Same fix for cabinet_sale_odv_ids (future-proofing — rpc_link_odv will
-- write to it once Fase 3 is wired)
CREATE POLICY "postgres_all" ON cabinet_sale_odv_ids
  FOR ALL TO postgres USING (true) WITH CHECK (true);

NOTIFY pgrst, 'reload schema';
