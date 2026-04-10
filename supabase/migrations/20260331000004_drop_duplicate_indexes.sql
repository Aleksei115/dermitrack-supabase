-- ============================================================================
-- Migration 0E: Drop duplicate indexes
-- Fix: Remove redundant indexes (wasted space + slower writes)
-- ============================================================================

-- visit_tasks: idx_visit_tasks_task_id is covered by uk_visit_tasks_task_id (UNIQUE)
DROP INDEX IF EXISTS idx_visit_tasks_task_id;

-- users: idx_users_zoho_id (partial) is covered by usuarios_id_zoho_unique (UNIQUE, full)
DROP INDEX IF EXISTS idx_users_zoho_id;

-- inventory_movements: exact duplicate
DROP INDEX IF EXISTS idx_inventory_movements_task_id;

-- notifications: idx_notifications_user is covered by idx_notifications_unread (partial, more selective)
DROP INDEX IF EXISTS idx_notifications_user;

-- user_push_tokens: idx_push_tokens_user is covered by idx_push_tokens_active (partial, more selective)
DROP INDEX IF EXISTS idx_push_tokens_user;

-- user_notification_preferences: idx_notification_prefs_user is covered by user_notification_preferences_user_id_key (UNIQUE)
DROP INDEX IF EXISTS idx_notification_prefs_user;

-- saga_zoho_links duplicates — deferring to Fase 8 (saga table drop)
-- saga_zoho_links_saga_zoho_unique is a CONSTRAINT (not just index), cannot drop independently
-- idx_saga_zoho_links_zoho_id, idx_saga_zoho_links_saga will be dropped with saga tables
