-- ============================================================================
-- Migration 0A: Add missing FK indexes
-- Fix: Missing FK indexes (HIGH impact — 10-100x faster JOINs/CASCADEs)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_admin_notifications_read_by ON admin_notifications (read_by);
CREATE INDEX IF NOT EXISTS idx_client_status_log_changed_by ON client_status_log (changed_by);
CREATE INDEX IF NOT EXISTS idx_collections_client_id ON collections (client_id);
CREATE INDEX IF NOT EXISTS idx_saga_adjustments_saga_transaction_id ON saga_adjustments (saga_transaction_id);

NOTIFY pgrst, 'reload schema';
