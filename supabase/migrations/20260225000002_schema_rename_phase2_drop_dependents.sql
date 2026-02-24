-- ============================================================================
-- PHASE 2: Drop All Dependent Objects
-- ============================================================================
-- Must drop in reverse dependency order:
-- 1. Materialized views
-- 2. Views (public, analytics, audit)
-- 3. RLS policies
-- 4. Triggers
-- 5. Functions (public wrappers first, then analytics, then operational)
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Drop materialized views
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS public.mv_balance_metrics CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_brand_performance CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_cumulative_daily CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_doctor_stats CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_opportunity_matrix CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_padecimiento_performance CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.mv_product_interest CASCADE;

-- ---------------------------------------------------------------------------
-- 2. Drop views
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS public.v_clientes_con_inventario CASCADE;
DROP VIEW IF EXISTS public.v_visitas_operativo CASCADE;
DROP VIEW IF EXISTS public.v_visit_tasks_operativo CASCADE;
DROP VIEW IF EXISTS analytics.v_cambios_estado_recientes CASCADE;
DROP VIEW IF EXISTS analytics.v_clientes_por_estado CASCADE;
DROP VIEW IF EXISTS analytics.v_metricas_desercion CASCADE;
DROP VIEW IF EXISTS analytics.v_tiempo_activo_clientes CASCADE;
DROP VIEW IF EXISTS audit.audit_log CASCADE;
DROP VIEW IF EXISTS audit.cliente_estado_log CASCADE;
DROP VIEW IF EXISTS audit.notificaciones_admin CASCADE;

-- ---------------------------------------------------------------------------
-- 3. Drop triggers
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_app_config_updated_at ON app_config;
DROP TRIGGER IF EXISTS trg_cliente_estado_log_dias ON cliente_estado_log;
DROP TRIGGER IF EXISTS trg_notificar_cambio_estado ON cliente_estado_log;
DROP TRIGGER IF EXISTS trg_sync_cliente_activo ON clientes;
DROP TRIGGER IF EXISTS trg_medicamento_embedding_stale ON medicamentos;
DROP TRIGGER IF EXISTS audit_movimientos_inventario ON movimientos_inventario;
DROP TRIGGER IF EXISTS trg_remove_sku_disponible_on_venta ON movimientos_inventario;
DROP TRIGGER IF EXISTS trg_sync_inventario ON movimientos_inventario;
DROP TRIGGER IF EXISTS audit_saga_transactions ON saga_transactions;
DROP TRIGGER IF EXISTS saga_transactions_audit ON saga_transactions;
DROP TRIGGER IF EXISTS update_saga_transactions_updated_at ON saga_transactions;
DROP TRIGGER IF EXISTS validate_saga_items_unique ON saga_transactions;
DROP TRIGGER IF EXISTS trg_notify_task_status ON visit_tasks;
DROP TRIGGER IF EXISTS trg_notify_visit_completed ON visitas;
DROP TRIGGER IF EXISTS trg_sync_saga_status ON visitas;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'archive') THEN
    DROP TRIGGER IF EXISTS update_encuestas_ciclo_updated_at ON archive.encuestas_ciclo;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 4. Drop ALL functions
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.audit_saga_transactions() CASCADE;
DROP FUNCTION IF EXISTS public.audit_trigger_func() CASCADE;
DROP FUNCTION IF EXISTS public.audit_visit_graph(text) CASCADE;
DROP FUNCTION IF EXISTS public.can_access_cliente(text) CASCADE;
DROP FUNCTION IF EXISTS public.can_access_visita(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.clasificacion_base(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.consolidate_duplicate_items(jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.create_notification(character varying, text, text, text, jsonb, text, integer) CASCADE;
DROP FUNCTION IF EXISTS public.current_user_id() CASCADE;
DROP FUNCTION IF EXISTS public.deduplicate_saga_items(jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.fn_app_config_updated_at() CASCADE;
DROP FUNCTION IF EXISTS public.fn_cliente_estado_log_dias() CASCADE;
DROP FUNCTION IF EXISTS public.fn_crear_movimiento_creacion_lote() CASCADE;
DROP FUNCTION IF EXISTS public.fn_crear_notificacion(tipo_notificacion, character varying, text, jsonb, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.fn_notificar_cambio_estado() CASCADE;
DROP FUNCTION IF EXISTS public.fn_remove_sku_disponible_on_venta() CASCADE;
DROP FUNCTION IF EXISTS public.fn_sync_cliente_activo() CASCADE;
DROP FUNCTION IF EXISTS public.fn_sync_inventario_from_movements() CASCADE;
DROP FUNCTION IF EXISTS public.fn_sync_saga_status() CASCADE;
DROP FUNCTION IF EXISTS public.get_auditoria_cliente(character varying) CASCADE;
DROP FUNCTION IF EXISTS public.get_balance_metrics() CASCADE;
DROP FUNCTION IF EXISTS public.get_botiquin_data() CASCADE;
DROP FUNCTION IF EXISTS public.get_brand_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_conversion_details(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_actual_data(character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_historico_data(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_logistica_data(character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_logistica_detalle() CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_skus_valor_por_visita(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_stats_generales_con_comparacion() CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_stats_por_medico() CASCADE;
DROP FUNCTION IF EXISTS public.get_corte_stats_por_medico_con_comparacion() CASCADE;
DROP FUNCTION IF EXISTS public.get_crosssell_significancia() CASCADE;
DROP FUNCTION IF EXISTS public.get_current_user_id() CASCADE;
DROP FUNCTION IF EXISTS public.get_dashboard_data(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_dashboard_static() CASCADE;
DROP FUNCTION IF EXISTS public.get_direccion_movimiento(tipo_movimiento_botiquin) CASCADE;
DROP FUNCTION IF EXISTS public.get_facturacion_composicion(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_facturacion_composicion() CASCADE;
DROP FUNCTION IF EXISTS public.get_filtros_disponibles() CASCADE;
DROP FUNCTION IF EXISTS public.get_historico_conversiones_evolucion(date, date, text, character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS public.get_historico_skus_valor_por_visita(date, date, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.get_impacto_botiquin_resumen(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_impacto_detalle(text, character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_market_analysis(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_padecimiento_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_product_interest(integer, character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_recoleccion_activa() CASCADE;
DROP FUNCTION IF EXISTS public.get_recurring_data() CASCADE;
DROP FUNCTION IF EXISTS public.get_sankey_conversion_flows(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS public.is_admin() CASCADE;
DROP FUNCTION IF EXISTS public.is_current_user_admin() CASCADE;
DROP FUNCTION IF EXISTS public.notify_admins(text, text, text, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.notify_visit_completed() CASCADE;
DROP FUNCTION IF EXISTS public.publish_saga_event() CASCADE;
DROP FUNCTION IF EXISTS public.rebuild_movimientos_inventario() CASCADE;
DROP FUNCTION IF EXISTS public.refresh_all_materialized_views() CASCADE;
DROP FUNCTION IF EXISTS public.regenerar_movimientos_desde_saga(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_compensate_task(uuid, character varying, text, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_force_task_status(uuid, character varying, text, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_get_all_visits(integer, integer, text, text, date, date) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_get_visit_detail(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_retry_pivot(uuid, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_admin_rollback_visit(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_cambiar_estado_cliente(character varying, estado_cliente, character varying, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_can_access_task(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_cancel_task(uuid, character varying, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_cancel_visit(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_cliente_tuvo_botiquin(text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_compensate_saga(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_complete_recoleccion(uuid, text, text, text, text[]) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_confirm_odv(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_confirm_odv_with_cotizacion(uuid, text, text, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_confirm_saga_pivot(uuid, text, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_consolidate_visitas() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_count_notificaciones_no_leidas() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_create_visit(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_create_visit(character varying, integer, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_cliente_estado_historial(character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_corte_items(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_corte_permanencia_items(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_lev_post_corte_items(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_levantamiento_items(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_next_visit_type(character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_notificaciones_no_leidas() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_odv_items(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_rangos_cliente() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_user_notifications(character varying, integer, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_visit_odvs(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_get_visit_saga_summary(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_marcar_notificacion_leida(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_mark_all_notifications_read(character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_mark_notification_read(uuid, character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_migrate_dev_legacy() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_migrate_full_history() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_migrate_legacy_sagas() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_owner_delete_visit(uuid, text, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_save_borrador_step(uuid, text, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_set_manual_botiquin_odv_id(uuid, text, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_set_manual_odv_id(uuid, text) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_skip_recoleccion(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_skip_venta_odv(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_start_task(uuid, visit_task_tipo) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_submit_corte(uuid, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_submit_informe_visita(uuid, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_submit_lev_post_corte(uuid, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_submit_levantamiento_inicial(uuid, jsonb) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_sync_botiquin_skus_disponibles(character varying) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_try_complete_visit(uuid) CASCADE;
DROP FUNCTION IF EXISTS public.rpc_verify_consolidation() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_verify_dev_migration() CASCADE;
DROP FUNCTION IF EXISTS public.rpc_verify_migration_consistency() CASCADE;
DROP FUNCTION IF EXISTS public.saga_outbox_trigger() CASCADE;
DROP FUNCTION IF EXISTS public.set_updated_at() CASCADE;
DROP FUNCTION IF EXISTS public.trigger_generate_movements_from_saga() CASCADE;
DROP FUNCTION IF EXISTS public.trigger_notify_task_completed() CASCADE;
DROP FUNCTION IF EXISTS public.trigger_notify_visit_completed() CASCADE;
DROP FUNCTION IF EXISTS public.trigger_refresh_stats() CASCADE;
DROP FUNCTION IF EXISTS public.update_rango_y_facturacion_actual() CASCADE;
DROP FUNCTION IF EXISTS public.update_updated_at_column() CASCADE;
DROP FUNCTION IF EXISTS public.upsert_push_token(character varying, text, text) CASCADE;
DROP FUNCTION IF EXISTS public.validate_unique_skus_in_items() CASCADE;
DROP FUNCTION IF EXISTS analytics.clasificacion_base(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_auditoria_cliente(character varying) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_balance_metrics() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_botiquin_data() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_brand_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_conversion_details(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_actual_data(character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_actual_rango() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_anterior_stats() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_filtros_disponibles() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_historico_data(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_logistica_data(character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_logistica_detalle() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_skus_valor_por_visita(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_stats_generales() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_stats_generales_con_comparacion() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_stats_por_medico() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_corte_stats_por_medico_con_comparacion() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_crosssell_significancia() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_dashboard_data(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_dashboard_static() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_facturacion_composicion(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_facturacion_composicion_legacy() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_filtros_disponibles() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_historico_conversiones_evolucion(date, date, text, character varying[], character varying[], character varying[]) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_historico_skus_valor_por_visita(date, date, character varying) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_impacto_botiquin_resumen(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_impacto_detalle(text, character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_market_analysis(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_padecimiento_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_product_interest(integer, character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_recoleccion_activa() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_recurring_data() CASCADE;
DROP FUNCTION IF EXISTS analytics.get_sankey_conversion_flows(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS analytics.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date) CASCADE;
DROP FUNCTION IF EXISTS chatbot.check_and_increment_usage(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS chatbot.clasificacion_por_cliente(character varying) CASCADE;
DROP FUNCTION IF EXISTS chatbot.fn_mark_embedding_stale() CASCADE;
DROP FUNCTION IF EXISTS chatbot.fuzzy_search_clientes(text, character varying, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.fuzzy_search_medicamentos(text, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_inventario_doctor(character varying, character varying, boolean) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_movimientos_doctor(character varying, character varying, boolean, text, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_precios_medicamentos(text, character varying) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_ranking_ventas_completo(integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_recolecciones_usuario(character varying, character varying, integer, boolean) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_remaining_queries(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_rendimiento_marcas_completo() CASCADE;
DROP FUNCTION IF EXISTS chatbot.get_ventas_odv_usuario(character varying, boolean, character varying, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.match_fichas(vector, double precision, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.match_medicamentos(vector, double precision, integer) CASCADE;
DROP FUNCTION IF EXISTS chatbot.rollback_usage(character varying) CASCADE;

COMMIT;
