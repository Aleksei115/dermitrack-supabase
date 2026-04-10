-- Fix: Add postgres RLS policies to tables that only have service_role policies.
-- Safe: skips if tables don't exist (e.g. on projects without inventario hibrido).
-- Note: Migration 000032 (SET role = 'service_role') is the primary fix;
-- this is belt-and-suspenders for when functions run as postgres directly.

DO $$ BEGIN
  CREATE POLICY "postgres_all" ON cabinet_inventory_lots
    FOR ALL TO postgres USING (true) WITH CHECK (true);
EXCEPTION WHEN undefined_table OR duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE POLICY "postgres_all" ON cabinet_sale_odv_ids
    FOR ALL TO postgres USING (true) WITH CHECK (true);
EXCEPTION WHEN undefined_table OR duplicate_object THEN NULL;
END $$;

NOTIFY pgrst, 'reload schema';
