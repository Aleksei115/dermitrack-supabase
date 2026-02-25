-- =============================================================================
-- CLEANUP: Remove PROD-only remnants not present in DEV baseline
-- Applied post-squash to align PROD with DEV
-- =============================================================================

-- Drop legacy Spanish-named dev helpers (only exist in PROD)
DROP SCHEMA IF EXISTS dev CASCADE;

-- Drop old chatbot.refund_usage (replaced by chatbot.rollback_usage)
DROP FUNCTION IF EXISTS chatbot.refund_usage(character varying);

-- Align zoho-auth-callback verify_jwt (PROD=true → false to match DEV + all other functions)
-- Note: This is handled by config.toml, not SQL. Included as documentation only.
