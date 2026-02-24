-- ============================================================================
-- PHASE 6: Recreate Views, Materialized Views, Triggers, Policies
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Recreate views (with new English names)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.v_clients_with_inventory AS
SELECT c.client_id,
    c.client_name,
    c.zone_id,
    c.user_id,
    c.active,
    c.status,
    c.avg_billing,
    c.total_billing,
    c.months_with_sales,
    c.tier,
    c.zoho_cabinet_client_id,
    COALESCE(inv.total_inventory, 0::bigint) AS total_inventory,
    COALESCE(inv.total_inventory, 0::bigint) > 0 AS has_active_cabinet
FROM clients c
LEFT JOIN (
    SELECT ci.client_id,
        sum(ci.available_quantity) AS total_inventory
    FROM cabinet_inventory ci
    GROUP BY ci.client_id
) inv ON c.client_id::text = inv.client_id::text
ORDER BY c.client_name;

GRANT SELECT ON public.v_clients_with_inventory TO authenticated;

CREATE OR REPLACE VIEW public.v_visits_operational AS
SELECT v.visit_id,
    v.client_id,
    v.user_id,
    u.name AS user_name,
    v.type,
    v.status,
    v.cycle_id,
    v.created_at,
    v.due_at,
    v.completed_at,
    c.client_name,
    c.zone_id,
    c.tier,
    (SELECT count(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id) AS total_tasks,
    (SELECT count(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND vt.status = 'COMPLETED'::visit_task_status) AS completed_tasks,
    CASE
        WHEN v.status = 'COMPLETED'::visit_status THEN 'COMPLETED'::text
        WHEN v.due_at < now() AND v.status <> 'COMPLETED'::visit_status THEN 'DELAYED'::text
        WHEN EXISTS (SELECT 1 FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND (vt.status = ANY (ARRAY['IN_PROGRESS'::visit_task_status, 'PENDING_SYNC'::visit_task_status, 'COMPLETED'::visit_task_status]))) THEN 'IN_PROGRESS'::text
        ELSE 'PENDING'::text
    END AS operational_status
FROM visits v
JOIN clients c ON v.client_id::text = c.client_id::text
JOIN users u ON v.user_id::text = u.user_id::text;

CREATE OR REPLACE VIEW public.v_visit_tasks_operational AS
SELECT task_id::text AS task_id,
    visit_id,
    task_type,
    status,
    required,
    created_at,
    started_at,
    completed_at,
    due_at,
    last_activity_at,
    reference_table,
    reference_id,
    metadata,
    transaction_type::text AS transaction_type,
    step_order,
    'NOT_NEEDED'::text AS compensation_status,
    '{}'::jsonb AS input_payload,
    '{}'::jsonb AS output_result,
    NULL::jsonb AS compensation_payload,
    gen_random_uuid()::text AS idempotency_key,
    0 AS retry_count,
    3 AS max_retries,
    NULL::text AS last_error,
    NULL::timestamp with time zone AS compensation_executed_at,
    CASE
        WHEN status = 'COMPLETED'::visit_task_status THEN 'COMPLETED'::visit_task_status
        WHEN status = 'SKIPPED_M'::visit_task_status THEN status
        WHEN status = 'SKIPPED'::visit_task_status THEN status
        WHEN status = 'ERROR'::visit_task_status THEN status
        WHEN due_at IS NOT NULL AND due_at < now() AND (status <> ALL (ARRAY['COMPLETED'::visit_task_status, 'SKIPPED_M'::visit_task_status, 'SKIPPED'::visit_task_status])) THEN 'DELAYED'::visit_task_status
        ELSE status
    END AS operational_status,
    CASE
        WHEN task_type = 'SALE_ODV'::visit_task_type THEN (SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at) FROM saga_zoho_links szl JOIN saga_transactions st ON st.id = szl.id_saga_transaction WHERE st.visit_id = vt.visit_id AND szl.type::text = 'SALE'::text)
        WHEN task_type = 'ODV_CABINET'::visit_task_type THEN (SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at) FROM saga_zoho_links szl JOIN saga_transactions st ON st.id = szl.id_saga_transaction WHERE st.visit_id = vt.visit_id AND szl.type::text = 'CABINET'::text)
        ELSE NULL::text
    END AS odv_id,
    CASE
        WHEN task_type = 'SALE_ODV'::visit_task_type THEN (SELECT COALESCE(sum(COALESCE((item.value ->> 'quantity'::text)::integer, (item.value ->> 'cantidad'::text)::integer, 0)), 0::bigint)::integer FROM saga_zoho_links szl JOIN saga_transactions st ON st.id = szl.id_saga_transaction, LATERAL jsonb_array_elements(COALESCE(szl.items, st.items)) item(value) WHERE st.visit_id = vt.visit_id AND szl.type::text = 'SALE'::text)
        WHEN task_type = 'ODV_CABINET'::visit_task_type THEN (SELECT COALESCE(sum(COALESCE((item.value ->> 'cantidad_entrada'::text)::integer, (item.value ->> 'quantity'::text)::integer, (item.value ->> 'cantidad'::text)::integer, 0)), 0::bigint)::integer FROM saga_zoho_links szl JOIN saga_transactions st ON st.id = szl.id_saga_transaction, LATERAL jsonb_array_elements(COALESCE(szl.items, st.items)) item(value) WHERE st.visit_id = vt.visit_id AND szl.type::text = 'CABINET'::text)
        ELSE NULL::integer
    END AS odv_total_pieces
FROM visit_tasks vt;

CREATE OR REPLACE VIEW analytics.v_recent_status_changes AS
SELECT cel.id,
    cel.client_id,
    c.client_name,
    cel.previous_status,
    cel.new_status,
    cel.changed_by,
    u.name AS changed_by_name,
    cel.changed_at,
    cel.reason,
    cel.days_in_previous_status,
    cel.metadata
FROM client_status_log cel
JOIN clients c ON c.client_id::text = cel.client_id::text
LEFT JOIN users u ON u.user_id::text = cel.changed_by::text
ORDER BY cel.changed_at DESC;

CREATE OR REPLACE VIEW analytics.v_clients_by_status AS
SELECT status,
    count(*) AS quantity,
    round(count(*)::numeric * 100.0 / NULLIF(sum(count(*)) OVER (), 0::numeric), 2) AS percentage
FROM clients
GROUP BY status
ORDER BY count(*) DESC;

CREATE OR REPLACE VIEW analytics.v_churn_metrics AS
SELECT date_trunc('month'::text, changed_at) AS month,
    count(*) FILTER (WHERE new_status = 'INACTIVE'::client_status) AS churns,
    count(*) FILTER (WHERE new_status = 'ACTIVE'::client_status AND previous_status = 'INACTIVE'::client_status) AS reactivations,
    count(*) FILTER (WHERE new_status = 'DOWNGRADING'::client_status) AS marked_downgrading,
    count(*) FILTER (WHERE new_status = 'SUSPENDED'::client_status) AS suspensions,
    (SELECT count(*) FROM clients WHERE clients.status = 'ACTIVE'::client_status) AS current_active,
    (SELECT count(*) FROM clients WHERE clients.status = 'INACTIVE'::client_status) AS current_inactive
FROM client_status_log
GROUP BY date_trunc('month'::text, changed_at)
ORDER BY date_trunc('month'::text, changed_at) DESC;

CREATE OR REPLACE VIEW analytics.v_client_active_time AS
WITH periods AS (
    SELECT cel.client_id,
        cel.new_status,
        cel.changed_at,
        lead(cel.changed_at) OVER (PARTITION BY cel.client_id ORDER BY cel.changed_at) AS next_change
    FROM client_status_log cel
)
SELECT p.client_id,
    c.client_name,
    c.status AS current_status,
    sum(CASE WHEN p.new_status = 'ACTIVE'::client_status THEN EXTRACT(day FROM COALESCE(p.next_change, now()) - p.changed_at) ELSE 0::numeric END)::integer AS total_active_days,
    count(*) FILTER (WHERE p.new_status = 'ACTIVE'::client_status) AS times_activated,
    count(*) FILTER (WHERE p.new_status = 'INACTIVE'::client_status) AS times_churned,
    min(p.changed_at) FILTER (WHERE p.new_status = 'ACTIVE'::client_status) AS first_activation,
    max(p.changed_at) FILTER (WHERE p.new_status = 'INACTIVE'::client_status) AS last_churn
FROM periods p
JOIN clients c ON c.client_id::text = p.client_id::text
GROUP BY p.client_id, c.client_name, c.status;

CREATE OR REPLACE VIEW audit.audit_log AS
SELECT id, table_name, record_id, action, audit_user_id, "timestamp", values_before, values_after, ip_address, user_agent
FROM public.audit_log;

CREATE OR REPLACE VIEW audit.client_status_log AS
SELECT id, client_id, previous_status, new_status, changed_by, changed_at, reason, metadata, days_in_previous_status
FROM public.client_status_log;

CREATE OR REPLACE VIEW audit.admin_notifications AS
SELECT id, type, title, message, metadata, read, created_at, for_user, read_at, read_by
FROM public.admin_notifications;


-- ---------------------------------------------------------------------------
-- 2. Recreate materialized views
-- ---------------------------------------------------------------------------

-- NOTE: Materialized views use new English table/column/enum names.
-- They are recreated from scratch and need REFRESH.

-- Materialized views will be recreated in a follow-up migration after function verification.
-- For now, create empty placeholders that will be populated via REFRESH.

-- ---------------------------------------------------------------------------
-- 3. Recreate triggers
-- ---------------------------------------------------------------------------

CREATE TRIGGER trg_app_config_updated_at BEFORE UPDATE ON app_config FOR EACH ROW EXECUTE FUNCTION fn_app_config_updated_at();
CREATE TRIGGER trg_client_status_log_days BEFORE INSERT ON client_status_log FOR EACH ROW EXECUTE FUNCTION fn_client_status_log_days();
CREATE TRIGGER trg_notify_status_change AFTER INSERT ON client_status_log FOR EACH ROW EXECUTE FUNCTION fn_notify_status_change();
CREATE TRIGGER trg_sync_client_active BEFORE INSERT ON clients FOR EACH ROW EXECUTE FUNCTION fn_sync_client_active();
CREATE TRIGGER trg_medication_embedding_stale AFTER UPDATE ON medications FOR EACH ROW EXECUTE FUNCTION chatbot.fn_mark_embedding_stale();
CREATE TRIGGER audit_inventory_movements AFTER INSERT ON inventory_movements FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();
CREATE TRIGGER trg_remove_available_sku_on_sale AFTER INSERT ON inventory_movements FOR EACH ROW EXECUTE FUNCTION fn_remove_available_sku_on_sale();
CREATE TRIGGER trg_sync_inventory AFTER INSERT ON inventory_movements FOR EACH ROW EXECUTE FUNCTION fn_sync_inventory_from_movements();
CREATE TRIGGER audit_saga_transactions AFTER INSERT ON saga_transactions FOR EACH ROW EXECUTE FUNCTION audit_trigger_func();
CREATE TRIGGER saga_transactions_audit AFTER INSERT ON saga_transactions FOR EACH ROW EXECUTE FUNCTION audit_saga_transactions();
CREATE TRIGGER update_saga_transactions_updated_at BEFORE UPDATE ON saga_transactions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER validate_saga_items_unique BEFORE INSERT ON saga_transactions FOR EACH ROW EXECUTE FUNCTION validate_unique_skus_in_items();
CREATE TRIGGER trg_notify_task_status AFTER UPDATE ON visit_tasks FOR EACH ROW EXECUTE FUNCTION trigger_notify_task_completed();
CREATE TRIGGER trg_notify_visit_completed AFTER UPDATE ON visits FOR EACH ROW EXECUTE FUNCTION notify_visit_completed();
CREATE TRIGGER trg_sync_saga_status BEFORE INSERT ON visits FOR EACH ROW EXECUTE FUNCTION fn_sync_saga_status();
CREATE TRIGGER update_cycle_surveys_updated_at BEFORE UPDATE ON archive.cycle_surveys FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ---------------------------------------------------------------------------
-- 4. Recreate RLS policies
-- ---------------------------------------------------------------------------

-- Enable RLS on all renamed tables
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.clients ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.zones ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.medications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.conditions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.medication_conditions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.visits ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.visit_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.visit_odvs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cabinet_inventory ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cabinet_client_available_skus ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cabinet_odv ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.inventory_movements ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.odv_sales ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.collections ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.collection_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.collection_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.collection_signatures ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.admin_notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.client_status_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.saga_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.saga_zoho_links ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.saga_compensations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.saga_adjustments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.visit_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.event_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.app_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_push_tokens ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_notification_preferences ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read app_config" ON app_config FOR SELECT USING (true);
CREATE POLICY service_role_all ON audit_log USING (true) WITH CHECK (true);
CREATE POLICY users_select ON users FOR SELECT USING (auth.uid() = auth_user_id OR is_admin());
CREATE POLICY own_profile_read ON users FOR SELECT USING (auth.uid() = auth_user_id);
CREATE POLICY own_profile_update ON users FOR UPDATE USING (auth.uid() = auth_user_id) WITH CHECK (auth.uid() = auth_user_id);
CREATE POLICY users_modify_insert ON users FOR INSERT WITH CHECK (is_admin());
CREATE POLICY users_modify_update ON users FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY users_modify_delete ON users FOR DELETE USING (is_admin());
CREATE POLICY clients_select ON clients FOR SELECT USING (can_access_client(client_id::text));
CREATE POLICY clients_modify_insert ON clients FOR INSERT WITH CHECK (is_admin());
CREATE POLICY clients_modify_update ON clients FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY clients_modify_delete ON clients FOR DELETE USING (is_admin());
CREATE POLICY zones_select ON zones FOR SELECT USING ((SELECT auth.role()) = 'authenticated');
CREATE POLICY zones_modify_insert ON zones FOR INSERT WITH CHECK (is_admin());
CREATE POLICY zones_modify_update ON zones FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY zones_modify_delete ON zones FOR DELETE USING (is_admin());
CREATE POLICY medications_select ON medications FOR SELECT USING ((SELECT auth.role()) = 'authenticated');
CREATE POLICY medications_modify_insert ON medications FOR INSERT WITH CHECK (is_admin());
CREATE POLICY medications_modify_update ON medications FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY medications_modify_delete ON medications FOR DELETE USING (is_admin());
CREATE POLICY conditions_select ON conditions FOR SELECT USING ((SELECT auth.role()) = 'authenticated');
CREATE POLICY conditions_modify_insert ON conditions FOR INSERT WITH CHECK (is_admin());
CREATE POLICY conditions_modify_update ON conditions FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY conditions_modify_delete ON conditions FOR DELETE USING (is_admin());
CREATE POLICY medication_conditions_select ON medication_conditions FOR SELECT USING ((SELECT auth.role()) = 'authenticated');
CREATE POLICY medication_conditions_modify_insert ON medication_conditions FOR INSERT WITH CHECK (is_admin());
CREATE POLICY medication_conditions_modify_update ON medication_conditions FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY medication_conditions_modify_delete ON medication_conditions FOR DELETE USING (is_admin());
CREATE POLICY cabinet_sku_select ON cabinet_client_available_skus FOR SELECT USING (can_access_client(client_id::text));
CREATE POLICY cabinet_sku_modify_insert ON cabinet_client_available_skus FOR INSERT WITH CHECK (is_admin());
CREATE POLICY cabinet_sku_modify_update ON cabinet_client_available_skus FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY cabinet_sku_modify_delete ON cabinet_client_available_skus FOR DELETE USING (is_admin());
CREATE POLICY "Service role full access" ON cabinet_odv USING (true) WITH CHECK (true);
CREATE POLICY "Users can view their cabinet_odv" ON cabinet_odv FOR SELECT USING (EXISTS (SELECT 1 FROM clients c WHERE c.client_id::text = cabinet_odv.client_id::text AND c.user_id::text = current_user_id()));
CREATE POLICY inventory_select ON cabinet_inventory FOR SELECT USING (can_access_client(client_id::text));
CREATE POLICY service_role_all ON cabinet_inventory USING (true) WITH CHECK (true);
CREATE POLICY service_role_all ON inventory_movements USING (true) WITH CHECK (true);
CREATE POLICY "Admins can insert logs" ON client_status_log FOR INSERT WITH CHECK (EXISTS (SELECT 1 FROM users WHERE users.auth_user_id = auth.uid() AND (users.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY "Admins can view status logs" ON client_status_log FOR SELECT USING (EXISTS (SELECT 1 FROM users WHERE users.user_id::text = (auth.jwt() ->> 'sub'::text) AND (users.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY "Admins can view notifications" ON admin_notifications FOR SELECT USING (EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role])) AND (admin_notifications.for_user IS NULL OR admin_notifications.for_user::text = u.user_id::text)));
CREATE POLICY "Admins can insert notifications" ON admin_notifications FOR INSERT WITH CHECK (EXISTS (SELECT 1 FROM users WHERE users.auth_user_id = auth.uid() AND (users.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY "Admins can update notifications" ON admin_notifications FOR UPDATE USING (EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY users_visits_select ON visits FOR SELECT USING (is_admin() OR (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid())));
CREATE POLICY users_visits_insert ON visits FOR INSERT WITH CHECK (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid()));
CREATE POLICY users_visits_update ON visits FOR UPDATE USING (is_admin() OR (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid())));
CREATE POLICY users_visit_tasks_select ON visit_tasks FOR SELECT USING (is_admin() OR (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid())));
CREATE POLICY users_visit_tasks_insert ON visit_tasks FOR INSERT WITH CHECK (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_visit_tasks_update ON visit_tasks FOR UPDATE USING (is_admin() OR (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid())));
CREATE POLICY users_visit_reports_select ON visit_reports FOR SELECT USING (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_visit_reports_insert ON visit_reports FOR INSERT WITH CHECK (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_visit_reports_update ON visit_reports FOR UPDATE USING (visit_id IN (SELECT v.visit_id FROM visits v JOIN users u ON v.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY admin_visit_reports_select ON visit_reports FOR SELECT USING (EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY admin_visit_reports_insert ON visit_reports FOR INSERT WITH CHECK (EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY admin_visit_reports_update ON visit_reports FOR UPDATE USING (EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['ADMIN'::user_role, 'OWNER'::user_role]))));
CREATE POLICY users_collections_select ON collections FOR SELECT USING (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid()));
CREATE POLICY users_collections_insert ON collections FOR INSERT WITH CHECK (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid()));
CREATE POLICY users_collections_update ON collections FOR UPDATE USING (user_id::text IN (SELECT users.user_id FROM users WHERE users.auth_user_id = auth.uid()));
CREATE POLICY admin_collections_select ON collections FOR SELECT USING (is_admin());
CREATE POLICY users_collection_evidence_select ON collection_evidence FOR SELECT USING (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_collection_evidence_insert ON collection_evidence FOR INSERT WITH CHECK (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY admin_collection_evidence_select ON collection_evidence FOR SELECT USING (is_admin());
CREATE POLICY users_collection_signatures_select ON collection_signatures FOR SELECT USING (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_collection_signatures_insert ON collection_signatures FOR INSERT WITH CHECK (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_collection_signatures_update ON collection_signatures FOR UPDATE USING (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY admin_collection_signatures_select ON collection_signatures FOR SELECT USING (is_admin());
CREATE POLICY users_collection_items_select ON collection_items FOR SELECT USING (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY users_collection_items_insert ON collection_items FOR INSERT WITH CHECK (recoleccion_id IN (SELECT r.recoleccion_id FROM collections r JOIN users u ON r.user_id::text = u.user_id::text WHERE u.auth_user_id = auth.uid()));
CREATE POLICY admin_collection_items_select ON collection_items FOR SELECT USING (is_admin());
CREATE POLICY odv_sales_select ON odv_sales FOR SELECT USING (can_access_client(client_id::text));
CREATE POLICY odv_sales_insert ON odv_sales FOR INSERT WITH CHECK (can_access_client(client_id::text));
CREATE POLICY odv_sales_update ON odv_sales FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY odv_sales_delete ON odv_sales FOR DELETE USING (is_admin());
CREATE POLICY service_role_all ON saga_transactions USING (true) WITH CHECK (true);
CREATE POLICY "Users can view saga_zoho_links" ON saga_zoho_links FOR SELECT USING (EXISTS (SELECT 1 FROM saga_transactions st JOIN visits v ON v.visit_id = st.visit_id WHERE st.id = saga_zoho_links.id_saga_transaction AND can_access_visit(v.visit_id)));
CREATE POLICY "Users can insert saga_zoho_links" ON saga_zoho_links FOR INSERT WITH CHECK (EXISTS (SELECT 1 FROM saga_transactions st JOIN visits v ON v.visit_id = st.visit_id WHERE st.id = saga_zoho_links.id_saga_transaction AND can_access_visit(v.visit_id)));
CREATE POLICY "Users can update saga_zoho_links" ON saga_zoho_links FOR UPDATE USING (EXISTS (SELECT 1 FROM saga_transactions st JOIN visits v ON v.visit_id = st.visit_id WHERE st.id = saga_zoho_links.id_saga_transaction AND can_access_visit(v.visit_id)));
CREATE POLICY saga_compensations_select_authenticated ON saga_compensations FOR SELECT USING (true);
CREATE POLICY saga_adjustments_select_authenticated ON saga_adjustments FOR SELECT USING (true);
CREATE POLICY service_role_all ON event_outbox USING (true) WITH CHECK (true);
CREATE POLICY notifications_select ON notifications FOR SELECT USING (user_id::text = get_current_user_id()::text);
CREATE POLICY notifications_update ON notifications FOR UPDATE USING (user_id::text = get_current_user_id()::text);
CREATE POLICY push_tokens_select ON user_push_tokens FOR SELECT USING (user_id::text = get_current_user_id()::text);
CREATE POLICY push_tokens_insert ON user_push_tokens FOR INSERT WITH CHECK (user_id::text = get_current_user_id()::text);
CREATE POLICY push_tokens_update ON user_push_tokens FOR UPDATE USING (user_id::text = get_current_user_id()::text) WITH CHECK (user_id::text = get_current_user_id()::text);
CREATE POLICY push_tokens_delete ON user_push_tokens FOR DELETE USING (user_id::text = get_current_user_id()::text);
CREATE POLICY preferences_select ON user_notification_preferences FOR SELECT USING (user_id::text = get_current_user_id()::text);
CREATE POLICY preferences_insert ON user_notification_preferences FOR INSERT WITH CHECK (user_id::text = get_current_user_id()::text);
CREATE POLICY preferences_update ON user_notification_preferences FOR UPDATE USING (user_id::text = get_current_user_id()::text);

-- Chatbot schema policies
ALTER TABLE chatbot.config ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.usage_limits ENABLE ROW LEVEL SECURITY;
CREATE POLICY config_read ON chatbot.config FOR SELECT USING (true);
CREATE POLICY conversations_own ON chatbot.conversations USING (user_id::text = (SELECT u.user_id FROM users u WHERE u.auth_user_id = auth.uid())::text OR EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['OWNER'::user_role, 'ADMIN'::user_role]))));
CREATE POLICY usage_own ON chatbot.usage_limits FOR SELECT USING (user_id::text = (SELECT u.user_id FROM users u WHERE u.auth_user_id = auth.uid())::text OR EXISTS (SELECT 1 FROM users u WHERE u.auth_user_id = auth.uid() AND (u.role = ANY (ARRAY['OWNER'::user_role, 'ADMIN'::user_role]))));

-- Archive schema policies
ALTER TABLE archive.cabinet_cycles ENABLE ROW LEVEL SECURITY;
ALTER TABLE archive.cycle_surveys ENABLE ROW LEVEL SECURITY;
CREATE POLICY cycles_select ON archive.cabinet_cycles FOR SELECT USING (can_access_client(client_id::text));
CREATE POLICY cycles_insert ON archive.cabinet_cycles FOR INSERT WITH CHECK (can_access_client(client_id::text) AND user_id::text = current_user_id());
CREATE POLICY cycles_update ON archive.cabinet_cycles FOR UPDATE USING (is_admin()) WITH CHECK (is_admin());
CREATE POLICY cycles_delete ON archive.cabinet_cycles FOR DELETE USING (is_admin());
CREATE POLICY service_role_all ON archive.cycle_surveys USING (true) WITH CHECK (true);

-- ---------------------------------------------------------------------------
-- 5. Grant permissions
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA analytics TO authenticated;
GRANT USAGE ON SCHEMA analytics TO anon;
GRANT USAGE ON SCHEMA archive TO authenticated;
GRANT USAGE ON SCHEMA audit TO authenticated;
GRANT USAGE ON SCHEMA chatbot TO authenticated;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';

COMMIT;
