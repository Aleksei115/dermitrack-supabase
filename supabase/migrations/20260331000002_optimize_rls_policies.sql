-- ============================================================================
-- Migration 0C: Optimize RLS policies — wrap auth.uid() in (select ...)
-- Fix: auth.uid() called per row → (select auth.uid()) cached once per query
-- Affects 29 policies across 10 tables
-- ============================================================================

-- ── users (3 policies) ──────────────────────────────────────────────────────

DROP POLICY IF EXISTS "users_select" ON users;
CREATE POLICY "users_select" ON users FOR SELECT TO public
  USING (((select auth.uid()) = auth_user_id) OR is_admin());

DROP POLICY IF EXISTS "own_profile_read" ON users;
CREATE POLICY "own_profile_read" ON users FOR SELECT TO public
  USING ((select auth.uid()) = auth_user_id);

DROP POLICY IF EXISTS "own_profile_update" ON users;
CREATE POLICY "own_profile_update" ON users FOR UPDATE TO public
  USING ((select auth.uid()) = auth_user_id)
  WITH CHECK ((select auth.uid()) = auth_user_id);

-- ── admin_notifications (3 policies) ────────────────────────────────────────

DROP POLICY IF EXISTS "Admins can view notifications" ON admin_notifications;
CREATE POLICY "Admins can view notifications" ON admin_notifications FOR SELECT TO public
  USING (EXISTS (
    SELECT 1 FROM users u
    WHERE u.auth_user_id = (select auth.uid())
      AND u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
      AND (admin_notifications.for_user IS NULL OR admin_notifications.for_user::text = u.user_id::text)
  ));

DROP POLICY IF EXISTS "Admins can update notifications" ON admin_notifications;
CREATE POLICY "Admins can update notifications" ON admin_notifications FOR UPDATE TO public
  USING (EXISTS (
    SELECT 1 FROM users u
    WHERE u.auth_user_id = (select auth.uid())
      AND u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

DROP POLICY IF EXISTS "Admins can insert notifications" ON admin_notifications;
CREATE POLICY "Admins can insert notifications" ON admin_notifications FOR INSERT TO public
  WITH CHECK (EXISTS (
    SELECT 1 FROM users
    WHERE users.auth_user_id = (select auth.uid())
      AND users.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

-- ── client_status_log (1 policy — only insert uses auth.uid) ────────────────

DROP POLICY IF EXISTS "Admins can insert logs" ON client_status_log;
CREATE POLICY "Admins can insert logs" ON client_status_log FOR INSERT TO public
  WITH CHECK (EXISTS (
    SELECT 1 FROM users
    WHERE users.auth_user_id = (select auth.uid())
      AND users.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

-- ── collections (3 policies) ────────────────────────────────────────────────

DROP POLICY IF EXISTS "users_collections_select" ON collections;
CREATE POLICY "users_collections_select" ON collections FOR SELECT TO public
  USING (user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collections_insert" ON collections;
CREATE POLICY "users_collections_insert" ON collections FOR INSERT TO public
  WITH CHECK (user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collections_update" ON collections;
CREATE POLICY "users_collections_update" ON collections FOR UPDATE TO public
  USING (user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));

-- ── collection_evidence (2 policies) ────────────────────────────────────────

DROP POLICY IF EXISTS "users_collection_evidence_select" ON collection_evidence;
CREATE POLICY "users_collection_evidence_select" ON collection_evidence FOR SELECT TO public
  USING (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collection_evidence_insert" ON collection_evidence;
CREATE POLICY "users_collection_evidence_insert" ON collection_evidence FOR INSERT TO public
  WITH CHECK (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

-- ── collection_items (2 policies) ───────────────────────────────────────────

DROP POLICY IF EXISTS "users_collection_items_select" ON collection_items;
CREATE POLICY "users_collection_items_select" ON collection_items FOR SELECT TO public
  USING (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collection_items_insert" ON collection_items;
CREATE POLICY "users_collection_items_insert" ON collection_items FOR INSERT TO public
  WITH CHECK (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

-- ── collection_signatures (3 policies) ──────────────────────────────────────

DROP POLICY IF EXISTS "users_collection_signatures_select" ON collection_signatures;
CREATE POLICY "users_collection_signatures_select" ON collection_signatures FOR SELECT TO public
  USING (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collection_signatures_insert" ON collection_signatures;
CREATE POLICY "users_collection_signatures_insert" ON collection_signatures FOR INSERT TO public
  WITH CHECK (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_collection_signatures_update" ON collection_signatures;
CREATE POLICY "users_collection_signatures_update" ON collection_signatures FOR UPDATE TO public
  USING (collection_id IN (
    SELECT r.collection_id FROM collections r
    JOIN users u ON r.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

-- ── visit_reports (6 policies) ──────────────────────────────────────────────

DROP POLICY IF EXISTS "admin_visit_reports_select" ON visit_reports;
CREATE POLICY "admin_visit_reports_select" ON visit_reports FOR SELECT TO public
  USING (EXISTS (
    SELECT 1 FROM users u
    WHERE u.auth_user_id = (select auth.uid())
      AND u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

DROP POLICY IF EXISTS "admin_visit_reports_insert" ON visit_reports;
CREATE POLICY "admin_visit_reports_insert" ON visit_reports FOR INSERT TO public
  WITH CHECK (EXISTS (
    SELECT 1 FROM users u
    WHERE u.auth_user_id = (select auth.uid())
      AND u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

DROP POLICY IF EXISTS "admin_visit_reports_update" ON visit_reports;
CREATE POLICY "admin_visit_reports_update" ON visit_reports FOR UPDATE TO public
  USING (EXISTS (
    SELECT 1 FROM users u
    WHERE u.auth_user_id = (select auth.uid())
      AND u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])
  ));

DROP POLICY IF EXISTS "users_visit_reports_select" ON visit_reports;
CREATE POLICY "users_visit_reports_select" ON visit_reports FOR SELECT TO public
  USING (visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visit_reports_insert" ON visit_reports;
CREATE POLICY "users_visit_reports_insert" ON visit_reports FOR INSERT TO public
  WITH CHECK (visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visit_reports_update" ON visit_reports;
CREATE POLICY "users_visit_reports_update" ON visit_reports FOR UPDATE TO public
  USING (visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

-- ── visit_tasks (3 policies) ────────────────────────────────────────────────

DROP POLICY IF EXISTS "users_visit_tasks_select" ON visit_tasks;
CREATE POLICY "users_visit_tasks_select" ON visit_tasks FOR SELECT TO public
  USING (is_admin() OR visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visit_tasks_insert" ON visit_tasks;
CREATE POLICY "users_visit_tasks_insert" ON visit_tasks FOR INSERT TO public
  WITH CHECK (visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visit_tasks_update" ON visit_tasks;
CREATE POLICY "users_visit_tasks_update" ON visit_tasks FOR UPDATE TO public
  USING (is_admin() OR visit_id IN (
    SELECT v.visit_id FROM visits v
    JOIN users u ON v.user_id::text = u.user_id::text
    WHERE u.auth_user_id = (select auth.uid())
  ));

-- ── visits (3 policies) ─────────────────────────────────────────────────────

DROP POLICY IF EXISTS "users_visits_select" ON visits;
CREATE POLICY "users_visits_select" ON visits FOR SELECT TO public
  USING (is_admin() OR user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visits_insert" ON visits;
CREATE POLICY "users_visits_insert" ON visits FOR INSERT TO public
  WITH CHECK (user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));

DROP POLICY IF EXISTS "users_visits_update" ON visits;
CREATE POLICY "users_visits_update" ON visits FOR UPDATE TO public
  USING (is_admin() OR user_id::text IN (
    SELECT users.user_id FROM users WHERE users.auth_user_id = (select auth.uid())
  ));
