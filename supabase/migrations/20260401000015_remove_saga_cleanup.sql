-- Migration: Cleanup remaining saga functions missed by 20260401000014
-- The DROP statements in the previous migration used incorrect signatures.

BEGIN;

-- Drop saga-named functions with correct signatures
DROP FUNCTION IF EXISTS public.deduplicate_saga_items(jsonb);
DROP FUNCTION IF EXISTS public.regenerate_movements_from_saga(uuid);
DROP FUNCTION IF EXISTS public.rpc_confirm_saga_pivot(uuid, text, jsonb);

-- Drop legacy RPCs with correct signatures (saga references in body)
DROP FUNCTION IF EXISTS public.rpc_admin_compensate_task(uuid, varchar, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_admin_retry_pivot(uuid, varchar);
DROP FUNCTION IF EXISTS public.rpc_complete_collection(uuid, text, text, text, text[]);
DROP FUNCTION IF EXISTS public.rpc_confirm_odv_with_cotizacion(uuid, text, text, jsonb);
DROP FUNCTION IF EXISTS public.rpc_save_draft_step(uuid, text, text);
DROP FUNCTION IF EXISTS public.rpc_set_manual_botiquin_odv_id(uuid, text, text);

-- Remove saga comments from clasificacion_base
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'clasificacion_base';

  IF v_def LIKE '%saga%' THEN
    v_def := REPLACE(v_def, '-- CHANGE: saga_zoho_links → cabinet_sale_odv_ids', '');
    v_def := REPLACE(v_def, '-- CHANGE: saga_odv_ids → sale_odv_ids', '');
    EXECUTE v_def;
  END IF;
END;
$$;

-- Remove saga comments from get_billing_composition
DO $$
DECLARE
  v_def text;
BEGIN
  SELECT pg_get_functiondef(p.oid) INTO v_def
  FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
  WHERE n.nspname = 'analytics' AND p.proname = 'get_billing_composition';

  IF v_def LIKE '%saga%' THEN
    v_def := REPLACE(v_def, '-- CHANGE: saga_zoho_links + saga_transactions → cabinet_sale_odv_ids', '');
    EXECUTE v_def;
  END IF;
END;
$$;

-- Fix bronze_visits view: rename saga_status alias to workflow_status
DROP VIEW IF EXISTS metadata.bronze_visits CASCADE;
CREATE VIEW metadata.bronze_visits AS
SELECT v.visit_id,
  v.client_id,
  c.client_name,
  v.user_id,
  u.name AS rep_name,
  v.type AS visit_type,
  v.status AS visit_status,
  v.workflow_status,
  v.corte_number,
  v.created_at,
  v.completed_at,
  v.completed_at - v.created_at AS duration
FROM visits v
JOIN clients c ON v.client_id = c.client_id
JOIN users u ON v.user_id = u.user_id;

NOTIFY pgrst, 'reload schema';

COMMIT;
