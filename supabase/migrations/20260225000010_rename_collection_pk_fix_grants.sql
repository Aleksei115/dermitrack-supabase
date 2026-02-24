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
-- 3. Drop functions whose RETURNS TABLE includes the old column
--    name (CREATE OR REPLACE cannot change return types)
-- ============================================================
DROP FUNCTION IF EXISTS chatbot.get_user_collections(character varying, character varying, integer, boolean);

-- ============================================================
-- 4. Update all remaining functions that reference the old column
-- ============================================================
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
-- 5. Recreate chatbot.get_user_collections with new return type
-- ============================================================
CREATE OR REPLACE FUNCTION chatbot.get_user_collections(
  p_user_id character varying,
  p_client_id character varying DEFAULT NULL,
  p_limit integer DEFAULT 20,
  p_is_admin boolean DEFAULT false
)
RETURNS TABLE(
  collection_id uuid,
  client_id character varying,
  client_name character varying,
  status text,
  created_at timestamp with time zone,
  delivered_at timestamp with time zone,
  cedis_observations text,
  items json
)
LANGUAGE sql
STABLE SECURITY DEFINER
AS $function$
  SELECT r.collection_id, r.client_id, c.client_name,
    r.status, r.created_at, r.delivered_at,
    r.cedis_observations,
    (SELECT COALESCE(json_agg(json_build_object(
      'sku', ri.sku, 'quantity', ri.quantity,
      'product', m.description
    )), '[]'::json)
    FROM collection_items ri
    LEFT JOIN medications m ON m.sku = ri.sku
    WHERE ri.collection_id = r.collection_id) as items
  FROM collections r
  JOIN clients c ON c.client_id = r.client_id
  WHERE (p_is_admin OR r.user_id = p_user_id)
    AND (p_client_id IS NULL OR r.client_id = p_client_id)
  ORDER BY r.created_at DESC
  LIMIT p_limit;
$function$;

-- ============================================================
-- 6. Notify PostgREST to reload schema cache
-- ============================================================
NOTIFY pgrst, 'reload schema';

COMMIT;
