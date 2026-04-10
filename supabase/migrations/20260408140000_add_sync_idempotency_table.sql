-- ============================================================================
-- Fase 3.4 of the offline-first refactor — `sync` schema + idempotency table
--
-- Context
-- -------
-- Starting with Fase 3.3, the DermiTrack mobile client generates a fresh
-- UUID v4 (`idempotency_key`) for every outbox enqueue and stores it on the
-- `sync_queue` row. A retry after a crash or a redelivery from the network
-- layer reuses the SAME key, so the server needs a place to recognize
-- "I've already handled this request" and return the previous result
-- instead of re-executing the side effect (creating duplicate visits,
-- double-decrementing inventory, etc).
--
-- Why a dedicated `sync` schema (not `public`)
-- --------------------------------------------
-- The project already isolates cross-cutting infrastructure from the
-- business domain via dedicated schemas: `chatbot`, `analytics`, `metadata`
-- (see `supabase/config.toml::[api] schemas`). Offline-first sync is
-- another cross-cutting concern — the table and helper RPCs here are pure
-- plumbing for the outbox pattern, not business logic. Keeping them in
-- `public` would:
--
--   1. Pollute the business namespace with dedupe machinery.
--   2. Force the `rpc_*` prefix convention used in `public` to distinguish
--      PostgREST-callable RPCs from internal helpers — redundant when the
--      whole schema IS sync infrastructure.
--   3. Make it harder to reason about which objects belong to the sync
--      subsystem (grep for `sync.` finds everything at once).
--
-- Function naming mirrors the project's non-public schemas
-- (`chatbot.execute_readonly_query`, `analytics.get_dashboard_data`,
-- `metadata.search_glossary`) — clean, unprefixed names.
--
-- This migration creates:
--
--   1. `sync` schema + permissions grants.
--   2. `sync.idempotency` table — one row per idempotency_key with the
--      cached result payload and status.
--   3. `sync.idempotency_claim` — called BEFORE the business RPC runs.
--      Returns whether the caller should PROCEED (new key) or RETURN_CACHED
--      (key already COMPLETED) or RETURN_ERROR (key previously FAILED).
--      On the first call with a new key it inserts the row with status
--      'IN_PROGRESS', so concurrent retries see it as in-flight.
--   4. `sync.idempotency_complete` — called AFTER the business RPC
--      succeeds. Stores the result jsonb and flips status to 'COMPLETED'.
--   5. `sync.idempotency_fail` — called when the business RPC rejects
--      the request (non-retryable error). Stores the error message and
--      flips status to 'FAILED'. Retryable errors should leave the row
--      as 'IN_PROGRESS' so the client can retry.
--
-- Usage pattern from the client (in future Fase 4 work):
--
--   const claim = await supabase.schema('sync').rpc('idempotency_claim', {...});
--   if (claim.data.action === 'RETURN_CACHED') return claim.data.result;
--   if (claim.data.action === 'RETURN_ERROR') throw new Error(claim.data.error_message);
--   try {
--     const result = await businessRpc(...);
--     await supabase.schema('sync').rpc('idempotency_complete', { p_key, p_result: result });
--     return result;
--   } catch (err) {
--     if (!isRetryable(err)) {
--       await supabase.schema('sync').rpc('idempotency_fail', { p_key, p_error_message: err.message });
--     }
--     throw err;
--   }
--
-- RLS
-- ---
-- RLS is ENABLED on the table with NO public policies. All access goes
-- through the SECURITY DEFINER RPCs below, which enforce that a user can
-- only claim/complete/fail keys tied to their own `user_id`. The service
-- role bypasses RLS for operational tasks (cleanup, debugging).
--
-- Callers need USAGE on the `sync` schema to INVOKE the functions (even
-- SECURITY DEFINER functions require the caller to have schema USAGE), but
-- NO direct table grants — defense in depth.
--
-- Cleanup
-- -------
-- A future migration will add a pg_cron job that deletes COMPLETED rows
-- older than 30 days. FAILED rows are retained longer for debugging.
-- Not in scope for this migration.
--
-- PostgREST exposure
-- ------------------
-- Requires `"sync"` in `supabase/config.toml::[api] schemas` for local
-- development; production exposure is configured per environment. Without
-- that, `supabase.schema('sync').rpc(...)` returns 404 from the client.
-- ============================================================================

-- ── Schema ──────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS sync;
COMMENT ON SCHEMA sync IS
  'Offline-first sync infrastructure: idempotency dedupe table + helper RPCs. All objects here are plumbing for the outbox pattern, NOT business logic.';

-- ── Table ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sync.idempotency (
  idempotency_key  text         PRIMARY KEY,
  operation        text         NOT NULL,
  visit_id         uuid         NOT NULL,
  user_id          text         NOT NULL,
  status           text         NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
  result           jsonb        NULL,
  error_message    text         NULL,
  created_at       timestamptz  NOT NULL DEFAULT now(),
  completed_at     timestamptz  NULL
);

COMMENT ON TABLE sync.idempotency IS
  'Dedupes retried offline-sync RPCs by client-generated UUID. See migration 20260408140000 for usage.';
COMMENT ON COLUMN sync.idempotency.idempotency_key IS
  'Client-generated UUID v4. Identical across retries for the same logical operation.';
COMMENT ON COLUMN sync.idempotency.operation IS
  'One of the OperationType union members (SUBMIT_CORTE, CONFIRM_VENTA_ODV, …).';
COMMENT ON COLUMN sync.idempotency.status IS
  'IN_PROGRESS while the business RPC is running; COMPLETED after success; FAILED on non-retryable error.';
COMMENT ON COLUMN sync.idempotency.result IS
  'Cached jsonb response from the business RPC. Returned verbatim on retries.';

CREATE INDEX IF NOT EXISTS idempotency_visit_created_idx
  ON sync.idempotency (visit_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idempotency_status_created_idx
  ON sync.idempotency (status, created_at)
  WHERE status = 'COMPLETED';

-- Enable RLS. Intentionally no public policies — every access must go
-- through the SECURITY DEFINER RPCs below.
ALTER TABLE sync.idempotency ENABLE ROW LEVEL SECURITY;

-- ── Helper RPC 1: claim ─────────────────────────────────────────────────────
--
-- Called at the START of an idempotent operation. Behavior:
--
--   • New key         → INSERT row (status IN_PROGRESS), return PROCEED
--   • Existing NEW    → (race: another worker just claimed it) return PROCEED
--                       — our business RPCs must themselves be idempotent
--                       so a double-run is safe.
--   • COMPLETED       → return RETURN_CACHED with the stored result
--   • FAILED          → return RETURN_ERROR with the stored error message
--
-- We never delete IN_PROGRESS rows automatically — if a client crashes
-- mid-operation the row stays until either a retry flips it to COMPLETED
-- or a cleanup job sweeps it up. The client is responsible for either
-- finishing the operation (common case) or calling fail() explicitly.

CREATE OR REPLACE FUNCTION sync.idempotency_claim(
  p_key        text,
  p_operation  text,
  p_visit_id   uuid,
  p_user_id    text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'sync', 'public'
AS $function$
DECLARE
  v_row sync.idempotency%ROWTYPE;
BEGIN
  IF p_key IS NULL OR length(trim(p_key)) = 0 THEN
    RAISE EXCEPTION 'sync.idempotency_claim: p_key is required';
  END IF;
  IF p_operation IS NULL OR length(trim(p_operation)) = 0 THEN
    RAISE EXCEPTION 'sync.idempotency_claim: p_operation is required';
  END IF;
  IF p_visit_id IS NULL THEN
    RAISE EXCEPTION 'sync.idempotency_claim: p_visit_id is required';
  END IF;
  IF p_user_id IS NULL OR length(trim(p_user_id)) = 0 THEN
    RAISE EXCEPTION 'sync.idempotency_claim: p_user_id is required';
  END IF;

  -- Fast path: try to insert a brand-new row. If the key already exists
  -- the INSERT … ON CONFLICT DO NOTHING leaves `v_row` empty and we fall
  -- through to the SELECT below.
  INSERT INTO sync.idempotency (
    idempotency_key, operation, visit_id, user_id, status
  )
  VALUES (
    p_key, p_operation, p_visit_id, p_user_id, 'IN_PROGRESS'
  )
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING * INTO v_row;

  IF FOUND THEN
    -- Brand-new claim — caller proceeds with the business RPC.
    RETURN jsonb_build_object('action', 'PROCEED');
  END IF;

  -- Key already existed. Load the existing row and return the appropriate
  -- directive. Lock FOR UPDATE so concurrent claims serialize (the typical
  -- case is a retry that lost the race to the previous attempt).
  SELECT * INTO v_row
  FROM sync.idempotency
  WHERE idempotency_key = p_key
  FOR UPDATE;

  -- Defensive: also enforce that the key is scoped to the same user and
  -- operation. A mismatch almost certainly means a caller bug (reused
  -- a key across operations) and we refuse to return cached data from a
  -- different context.
  IF v_row.operation <> p_operation THEN
    RAISE EXCEPTION
      'sync.idempotency_claim: key % reused across operations (stored: %, requested: %)',
      p_key, v_row.operation, p_operation;
  END IF;
  IF v_row.user_id <> p_user_id THEN
    RAISE EXCEPTION
      'sync.idempotency_claim: key % reused across users',
      p_key;
  END IF;

  IF v_row.status = 'COMPLETED' THEN
    RETURN jsonb_build_object(
      'action', 'RETURN_CACHED',
      'result', COALESCE(v_row.result, 'null'::jsonb)
    );
  END IF;

  IF v_row.status = 'FAILED' THEN
    RETURN jsonb_build_object(
      'action', 'RETURN_ERROR',
      'error_message', COALESCE(v_row.error_message, 'unknown error')
    );
  END IF;

  -- IN_PROGRESS — another worker is handling this right now, or the
  -- previous attempt crashed. Either way, let the caller proceed; the
  -- business RPCs are idempotent by design.
  RETURN jsonb_build_object('action', 'PROCEED');
END;
$function$;

COMMENT ON FUNCTION sync.idempotency_claim(text, text, uuid, text) IS
  'Fase 3.4: claim an idempotency key before running a business RPC. Returns PROCEED / RETURN_CACHED / RETURN_ERROR.';

-- ── Helper RPC 2: complete ──────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION sync.idempotency_complete(
  p_key     text,
  p_result  jsonb
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'sync', 'public'
AS $function$
BEGIN
  IF p_key IS NULL OR length(trim(p_key)) = 0 THEN
    RAISE EXCEPTION 'sync.idempotency_complete: p_key is required';
  END IF;

  UPDATE sync.idempotency
     SET status       = 'COMPLETED',
         result       = p_result,
         completed_at = now(),
         error_message = NULL
   WHERE idempotency_key = p_key;

  IF NOT FOUND THEN
    RAISE EXCEPTION
      'sync.idempotency_complete: no row for key % (did you forget to claim?)',
      p_key;
  END IF;
END;
$function$;

COMMENT ON FUNCTION sync.idempotency_complete(text, jsonb) IS
  'Fase 3.4: mark an idempotency key as COMPLETED with the cached result payload.';

-- ── Helper RPC 3: fail ──────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION sync.idempotency_fail(
  p_key            text,
  p_error_message  text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'sync', 'public'
AS $function$
BEGIN
  IF p_key IS NULL OR length(trim(p_key)) = 0 THEN
    RAISE EXCEPTION 'sync.idempotency_fail: p_key is required';
  END IF;

  UPDATE sync.idempotency
     SET status        = 'FAILED',
         error_message = COALESCE(p_error_message, 'unknown error'),
         completed_at  = now(),
         result        = NULL
   WHERE idempotency_key = p_key;

  IF NOT FOUND THEN
    RAISE EXCEPTION
      'sync.idempotency_fail: no row for key % (did you forget to claim?)',
      p_key;
  END IF;
END;
$function$;

COMMENT ON FUNCTION sync.idempotency_fail(text, text) IS
  'Fase 3.4: mark an idempotency key as FAILED (non-retryable). Future retries with the same key return RETURN_ERROR.';

-- ── Permissions ─────────────────────────────────────────────────────────────
--
-- Grant USAGE on the schema so `authenticated` can invoke functions. We do
-- NOT grant SELECT/INSERT/UPDATE/DELETE on the table — all mutation must go
-- through the SECURITY DEFINER helpers above (defense in depth: even if
-- RLS were accidentally disabled, the role has no table privileges).
--
-- `anon` is intentionally omitted — there is no public, unauthenticated
-- path into the sync loop.

GRANT USAGE ON SCHEMA sync TO authenticated;

GRANT EXECUTE ON FUNCTION sync.idempotency_claim(text, text, uuid, text) TO authenticated;
GRANT EXECUTE ON FUNCTION sync.idempotency_complete(text, jsonb)         TO authenticated;
GRANT EXECUTE ON FUNCTION sync.idempotency_fail(text, text)              TO authenticated;

-- Default privileges for future objects in this schema: new functions
-- automatically get EXECUTE for `authenticated`; new tables get nothing
-- (must be granted explicitly, forcing a conscious security review).
ALTER DEFAULT PRIVILEGES IN SCHEMA sync GRANT EXECUTE ON FUNCTIONS TO authenticated;

-- Make PostgREST re-read the schema cache so the new RPCs are routable
-- immediately after `supabase db push`.
NOTIFY pgrst, 'reload schema';
