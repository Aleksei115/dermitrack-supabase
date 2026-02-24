-- Migration: Fix collections PK column rename + view grants
-- The English schema rename (phase 3) missed renaming the PK column
-- recoleccion_id → collection_id in collections and child tables.
-- Also adds missing GRANT on v_visits_operational view.

BEGIN;

-- ============================================================
-- 1. Rename PK column in collections and related tables
-- ============================================================
-- PostgreSQL automatically updates PKs, FKs, indexes, and RLS policies
-- when columns are renamed (they track by attnum, not by name).
ALTER TABLE public.collections RENAME COLUMN recoleccion_id TO collection_id;
ALTER TABLE public.collection_items RENAME COLUMN recoleccion_id TO collection_id;
ALTER TABLE public.collection_evidence RENAME COLUMN recoleccion_id TO collection_id;
ALTER TABLE public.collection_signatures RENAME COLUMN recoleccion_id TO collection_id;

-- ============================================================
-- 2. Grant access to v_visits_operational view
-- ============================================================
-- This view was created in phase 6 but the GRANT was missed,
-- causing 403 errors when the mobile app queries it.
GRANT SELECT ON public.v_visits_operational TO authenticated;

-- ============================================================
-- 3. Update all functions that reference the old column name
-- ============================================================
-- Uses dynamic SQL to find and update all functions in public
-- and analytics schemas that still reference 'recoleccion_id'.
DO $$
DECLARE
  func_oid oid;
  func_def text;
  func_name text;
  func_schema text;
  updated_count int := 0;
BEGIN
  FOR func_oid, func_name, func_schema IN
    SELECT p.oid, p.proname, n.nspname
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname IN ('public', 'analytics', 'chatbot')
    AND p.prosrc LIKE '%recoleccion_id%'
  LOOP
    func_def := pg_get_functiondef(func_oid);
    -- Replace all occurrences of the old column name
    func_def := replace(func_def, 'recoleccion_id', 'collection_id');
    EXECUTE func_def;
    updated_count := updated_count + 1;
    RAISE NOTICE 'Updated function: %.% (% of total)', func_schema, func_name, updated_count;
  END LOOP;

  RAISE NOTICE 'Total functions updated: %', updated_count;
END $$;

-- ============================================================
-- 4. Notify PostgREST to reload schema cache
-- ============================================================
NOTIFY pgrst, 'reload schema';

COMMIT;
