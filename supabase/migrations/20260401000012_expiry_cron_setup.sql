-- ============================================================================
-- Migration 13: Expiry cron job — mark expired lots daily
-- Fase 7: Automated expiry management
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- Function to mark expired lots
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_mark_expired_lots()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_count integer;
BEGIN
  UPDATE cabinet_inventory_lots
  SET status = 'expired',
      updated_at = now()
  WHERE status = 'active'
    AND expiry_date IS NOT NULL
    AND expiry_date < CURRENT_DATE;

  GET DIAGNOSTICS v_count = ROW_COUNT;

  IF v_count > 0 THEN
    RAISE NOTICE 'Marked % lots as expired', v_count;
  END IF;

  RETURN v_count;
END;
$$;

COMMENT ON FUNCTION fn_mark_expired_lots() IS
'Daily cron function: marks active lots past their expiry_date as expired. Returns count of lots marked.';

-- ═══════════════════════════════════════════════════════════════════════════
-- Function to get expiry alerts for notification dispatch
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_get_expiry_alerts(p_days_ahead integer DEFAULT 7)
RETURNS TABLE (
  user_id character varying,
  client_id character varying,
  client_name character varying,
  sku character varying,
  product character varying,
  remaining_quantity integer,
  expiry_date date,
  days_until_expiry integer
)
LANGUAGE sql
STABLE
SET search_path = 'public'
AS $$
  SELECT
    u.user_id,
    l.client_id,
    c.client_name,
    l.sku,
    m.product,
    l.remaining_quantity,
    l.expiry_date,
    (l.expiry_date - CURRENT_DATE)::integer
  FROM cabinet_inventory_lots l
  JOIN clients c ON l.client_id = c.client_id
  JOIN medications m ON l.sku = m.sku
  JOIN users u ON c.user_id::text = u.user_id::text AND u.role = 'ADVISOR'
  WHERE l.status = 'active'
    AND l.expiry_date IS NOT NULL
    AND l.expiry_date BETWEEN CURRENT_DATE AND CURRENT_DATE + (p_days_ahead || ' days')::interval
  ORDER BY l.expiry_date ASC, u.user_id, c.client_name;
$$;

COMMENT ON FUNCTION fn_get_expiry_alerts(integer) IS
'Get products expiring within p_days_ahead days, grouped by rep. Used by expiry-notification Edge Function.';

-- ═══════════════════════════════════════════════════════════════════════════
-- pg_cron job (only works on hosted Supabase, not local)
-- For local dev, this will be a no-op
-- ═══════════════════════════════════════════════════════════════════════════

-- Note: pg_cron must be enabled in Supabase dashboard Settings > Extensions
-- This creates the cron job to run daily at 6 AM UTC
DO $$
BEGIN
  -- Check if pg_cron extension is available
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    -- Mark expired lots daily at 6 AM UTC
    PERFORM cron.schedule(
      'mark-expired-lots',
      '0 6 * * *',
      'SELECT fn_mark_expired_lots()'
    );
    RAISE NOTICE 'pg_cron job "mark-expired-lots" scheduled for 6 AM UTC daily';
  ELSE
    RAISE NOTICE 'pg_cron not available — skipping cron job creation. Enable pg_cron in Supabase dashboard.';
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
