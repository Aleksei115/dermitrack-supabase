-- ============================================================================
-- PHASE 3: Rename Enum Types, Tables, Columns, Indexes, Constraints
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Rename enum types
-- ---------------------------------------------------------------------------
ALTER TYPE estado_cliente RENAME TO client_status;
ALTER TYPE estado_saga_transaction RENAME TO saga_transaction_status;
ALTER TYPE rol_usuario RENAME TO user_role;
ALTER TYPE tipo_ciclo_botiquin RENAME TO cabinet_cycle_type;
ALTER TYPE tipo_evento_outbox RENAME TO outbox_event_type;
ALTER TYPE tipo_movimiento_botiquin RENAME TO cabinet_movement_type;
ALTER TYPE tipo_movimiento_inventario RENAME TO inventory_movement_type;
ALTER TYPE tipo_notificacion RENAME TO notification_type;
ALTER TYPE tipo_odv RENAME TO odv_type;
ALTER TYPE tipo_saga_transaction RENAME TO saga_transaction_type;
ALTER TYPE tipo_zoho_link RENAME TO zoho_link_type;
ALTER TYPE visit_estado RENAME TO visit_status;
ALTER TYPE visit_task_estado RENAME TO visit_task_status;
ALTER TYPE visit_task_tipo RENAME TO visit_task_type;
ALTER TYPE visit_tipo RENAME TO visit_type;

-- ---------------------------------------------------------------------------
-- 2. Rename tables (public schema)
-- ---------------------------------------------------------------------------
ALTER TABLE public.botiquin_clientes_sku_disponibles RENAME TO cabinet_client_available_skus;
ALTER TABLE public.botiquin_odv RENAME TO cabinet_odv;
ALTER TABLE public.cliente_estado_log RENAME TO client_status_log;
ALTER TABLE public.clientes RENAME TO clients;
ALTER TABLE public.inventario_botiquin RENAME TO cabinet_inventory;
ALTER TABLE public.medicamento_padecimientos RENAME TO medication_conditions;
ALTER TABLE public.medicamentos RENAME TO medications;
ALTER TABLE public.movimientos_inventario RENAME TO inventory_movements;
ALTER TABLE public.notificaciones_admin RENAME TO admin_notifications;
ALTER TABLE public.padecimientos RENAME TO conditions;
ALTER TABLE public.recolecciones RENAME TO collections;
ALTER TABLE public.recolecciones_evidencias RENAME TO collection_evidence;
ALTER TABLE public.recolecciones_firmas RENAME TO collection_signatures;
ALTER TABLE public.recolecciones_items RENAME TO collection_items;
ALTER TABLE public.usuarios RENAME TO users;
ALTER TABLE public.ventas_odv RENAME TO odv_sales;
ALTER TABLE public.visita_informes RENAME TO visit_reports;
-- NOTE: visita_odvs table does not exist in the database, skipping
ALTER TABLE public.visitas RENAME TO visits;
ALTER TABLE public.zonas RENAME TO zones;

-- Archive schema tables
ALTER TABLE archive.ciclos_botiquin RENAME TO cabinet_cycles;
ALTER TABLE archive.encuestas_ciclo RENAME TO cycle_surveys;

-- Chatbot schema tables
ALTER TABLE chatbot.ficha_tecnica_chunks RENAME TO data_sheet_chunks;
ALTER TABLE chatbot.medicamento_embeddings RENAME TO medication_embeddings;

-- ---------------------------------------------------------------------------
-- 3. Rename columns
-- ---------------------------------------------------------------------------

-- Table: admin_notifications
ALTER TABLE public.admin_notifications RENAME COLUMN leida TO read;
ALTER TABLE public.admin_notifications RENAME COLUMN leida_at TO read_at;
ALTER TABLE public.admin_notifications RENAME COLUMN leida_por TO read_by;
ALTER TABLE public.admin_notifications RENAME COLUMN mensaje TO message;
ALTER TABLE public.admin_notifications RENAME COLUMN para_usuario TO for_user;
ALTER TABLE public.admin_notifications RENAME COLUMN tipo TO type;
ALTER TABLE public.admin_notifications RENAME COLUMN titulo TO title;

-- Table: audit_log
ALTER TABLE public.audit_log RENAME COLUMN accion TO action;
ALTER TABLE public.audit_log RENAME COLUMN registro_id TO record_id;
ALTER TABLE public.audit_log RENAME COLUMN tabla TO table_name;
ALTER TABLE public.audit_log RENAME COLUMN usuario_id TO audit_user_id;
ALTER TABLE public.audit_log RENAME COLUMN valores_antes TO values_before;
ALTER TABLE public.audit_log RENAME COLUMN valores_despues TO values_after;

-- Table: cabinet_client_available_skus
ALTER TABLE public.cabinet_client_available_skus RENAME COLUMN fecha_ingreso TO intake_date;
ALTER TABLE public.cabinet_client_available_skus RENAME COLUMN id_cliente TO client_id;

-- Table: cabinet_inventory
ALTER TABLE public.cabinet_inventory RENAME COLUMN cantidad_disponible TO available_quantity;
ALTER TABLE public.cabinet_inventory RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.cabinet_inventory RENAME COLUMN precio_unitario TO unit_price;
ALTER TABLE public.cabinet_inventory RENAME COLUMN ultima_actualizacion TO last_updated;

-- Table: cabinet_odv
ALTER TABLE public.cabinet_odv RENAME COLUMN cantidad TO quantity;
ALTER TABLE public.cabinet_odv RENAME COLUMN estado_factura TO invoice_status;
ALTER TABLE public.cabinet_odv RENAME COLUMN fecha TO date;
ALTER TABLE public.cabinet_odv RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.cabinet_odv RENAME COLUMN id_venta TO sale_id;

-- Table: client_status_log
ALTER TABLE public.client_status_log RENAME COLUMN dias_en_estado_anterior TO days_in_previous_status;
ALTER TABLE public.client_status_log RENAME COLUMN estado_anterior TO previous_status;
ALTER TABLE public.client_status_log RENAME COLUMN estado_nuevo TO new_status;
ALTER TABLE public.client_status_log RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.client_status_log RENAME COLUMN razon TO reason;

-- Table: clients
ALTER TABLE public.clients RENAME COLUMN activo TO active;
ALTER TABLE public.clients RENAME COLUMN estado TO status;
ALTER TABLE public.clients RENAME COLUMN facturacion_actual TO current_billing;
ALTER TABLE public.clients RENAME COLUMN facturacion_promedio TO avg_billing;
ALTER TABLE public.clients RENAME COLUMN facturacion_total TO total_billing;
ALTER TABLE public.clients RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.clients RENAME COLUMN id_cliente_zoho_botiquin TO zoho_cabinet_client_id;
ALTER TABLE public.clients RENAME COLUMN id_usuario TO user_id;
ALTER TABLE public.clients RENAME COLUMN id_zona TO zone_id;
ALTER TABLE public.clients RENAME COLUMN meses_con_venta TO months_with_sales;
ALTER TABLE public.clients RENAME COLUMN nombre_cliente TO client_name;
ALTER TABLE public.clients RENAME COLUMN rango TO tier;
ALTER TABLE public.clients RENAME COLUMN rango_actual TO current_tier;

-- Table: collection_evidence
ALTER TABLE public.collection_evidence RENAME COLUMN evidencia_id TO evidence_id;

-- Table: collection_items
ALTER TABLE public.collection_items RENAME COLUMN cantidad TO quantity;

-- Table: collections
ALTER TABLE public.collections RENAME COLUMN cedis_observaciones TO cedis_observations;
ALTER TABLE public.collections RENAME COLUMN cedis_responsable_nombre TO cedis_responsible_name;
ALTER TABLE public.collections RENAME COLUMN entregada_at TO delivered_at;
ALTER TABLE public.collections RENAME COLUMN estado TO status;
ALTER TABLE public.collections RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.collections RENAME COLUMN id_usuario TO user_id;
ALTER TABLE public.collections RENAME COLUMN latitud TO latitude;
ALTER TABLE public.collections RENAME COLUMN longitud TO longitude;

-- Table: conditions
ALTER TABLE public.conditions RENAME COLUMN id_padecimiento TO condition_id;
ALTER TABLE public.conditions RENAME COLUMN nombre TO name;

-- Table: event_outbox
ALTER TABLE public.event_outbox RENAME COLUMN error_mensaje TO error_message;
ALTER TABLE public.event_outbox RENAME COLUMN evento_tipo TO event_type;
ALTER TABLE public.event_outbox RENAME COLUMN intentos TO attempts;
ALTER TABLE public.event_outbox RENAME COLUMN procesado TO processed;
ALTER TABLE public.event_outbox RENAME COLUMN procesado_en TO processed_at;
ALTER TABLE public.event_outbox RENAME COLUMN proximo_intento TO next_attempt;

-- Table: inventory_movements
ALTER TABLE public.inventory_movements RENAME COLUMN cantidad TO quantity;
ALTER TABLE public.inventory_movements RENAME COLUMN cantidad_antes TO quantity_before;
ALTER TABLE public.inventory_movements RENAME COLUMN cantidad_despues TO quantity_after;
ALTER TABLE public.inventory_movements RENAME COLUMN fecha_movimiento TO movement_date;
ALTER TABLE public.inventory_movements RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.inventory_movements RENAME COLUMN precio_unitario TO unit_price;
ALTER TABLE public.inventory_movements RENAME COLUMN tipo TO type;

-- Table: medication_conditions
ALTER TABLE public.medication_conditions RENAME COLUMN id_padecimiento TO condition_id;

-- Table: medications
ALTER TABLE public.medications RENAME COLUMN codigo_barras TO barcode;
ALTER TABLE public.medications RENAME COLUMN contenido TO content;
ALTER TABLE public.medications RENAME COLUMN descripcion TO description;
ALTER TABLE public.medications RENAME COLUMN fabricante TO manufacturer;
ALTER TABLE public.medications RENAME COLUMN ficha_tecnica_url TO data_sheet_url;
ALTER TABLE public.medications RENAME COLUMN marca TO brand;
ALTER TABLE public.medications RENAME COLUMN precio TO price;
ALTER TABLE public.medications RENAME COLUMN producto TO product;
ALTER TABLE public.medications RENAME COLUMN ultima_actualizacion TO last_updated;

-- Table: odv_sales
ALTER TABLE public.odv_sales RENAME COLUMN cantidad TO quantity;
ALTER TABLE public.odv_sales RENAME COLUMN estado_factura TO invoice_status;
ALTER TABLE public.odv_sales RENAME COLUMN fecha TO date;
ALTER TABLE public.odv_sales RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.odv_sales RENAME COLUMN id_venta TO sale_id;
ALTER TABLE public.odv_sales RENAME COLUMN precio TO price;

-- Table: saga_transactions
ALTER TABLE public.saga_transactions RENAME COLUMN estado TO status;
ALTER TABLE public.saga_transactions RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.saga_transactions RENAME COLUMN id_usuario TO user_id;
ALTER TABLE public.saga_transactions RENAME COLUMN tipo TO type;

-- Table: saga_zoho_links
ALTER TABLE public.saga_zoho_links RENAME COLUMN tipo TO type;

-- Table: users
ALTER TABLE public.users RENAME COLUMN activo TO active;
ALTER TABLE public.users RENAME COLUMN fecha_creacion TO created_date;
ALTER TABLE public.users RENAME COLUMN id_usuario TO user_id;
ALTER TABLE public.users RENAME COLUMN nombre TO name;
ALTER TABLE public.users RENAME COLUMN rol TO role;

-- NOTE: visit_odvs table does not exist, column renames skipped

-- Table: visit_reports
ALTER TABLE public.visit_reports RENAME COLUMN completada TO completed;
ALTER TABLE public.visit_reports RENAME COLUMN cumplimiento_score TO compliance_score;
ALTER TABLE public.visit_reports RENAME COLUMN etiqueta TO label;
ALTER TABLE public.visit_reports RENAME COLUMN fecha_completada TO completed_date;
ALTER TABLE public.visit_reports RENAME COLUMN informe_id TO report_id;
ALTER TABLE public.visit_reports RENAME COLUMN respuestas TO responses;

-- Table: visit_tasks
ALTER TABLE public.visit_tasks RENAME COLUMN estado TO status;
ALTER TABLE public.visit_tasks RENAME COLUMN task_tipo TO task_type;

-- Table: visits
ALTER TABLE public.visits RENAME COLUMN estado TO status;
ALTER TABLE public.visits RENAME COLUMN etiqueta TO label;
ALTER TABLE public.visits RENAME COLUMN id_ciclo TO cycle_id;
ALTER TABLE public.visits RENAME COLUMN id_cliente TO client_id;
ALTER TABLE public.visits RENAME COLUMN id_usuario TO user_id;
ALTER TABLE public.visits RENAME COLUMN tipo TO type;

-- Table: zones
ALTER TABLE public.zones RENAME COLUMN activo TO active;
ALTER TABLE public.zones RENAME COLUMN id_zona TO zone_id;
ALTER TABLE public.zones RENAME COLUMN nombre TO name;

-- Table: archive.cabinet_cycles
ALTER TABLE archive.cabinet_cycles RENAME COLUMN fecha_creacion TO created_date;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN id_ciclo TO cycle_id;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN id_ciclo_anterior TO previous_cycle_id;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN id_cliente TO client_id;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN id_usuario TO user_id;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN latitud TO latitude;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN longitud TO longitude;
ALTER TABLE archive.cabinet_cycles RENAME COLUMN tipo TO type;

-- Table: archive.cycle_surveys
ALTER TABLE archive.cycle_surveys RENAME COLUMN completada TO completed;
ALTER TABLE archive.cycle_surveys RENAME COLUMN fecha_completada TO completed_date;
ALTER TABLE archive.cycle_surveys RENAME COLUMN id_ciclo TO cycle_id;
ALTER TABLE archive.cycle_surveys RENAME COLUMN respuestas TO responses;

-- Table: chatbot.conversations
ALTER TABLE chatbot.conversations RENAME COLUMN id_usuario TO user_id;

-- Table: chatbot.messages
ALTER TABLE chatbot.messages RENAME COLUMN context_cliente_id TO context_client_id;

-- Table: chatbot.usage_limits
ALTER TABLE chatbot.usage_limits RENAME COLUMN fecha TO date;
ALTER TABLE chatbot.usage_limits RENAME COLUMN id_usuario TO user_id;

-- ---------------------------------------------------------------------------
-- 4. Rename indexes
-- ---------------------------------------------------------------------------
ALTER INDEX IF EXISTS idx_audit_tabla_registro RENAME TO idx_audit_table_record;
ALTER INDEX IF EXISTS idx_audit_usuario RENAME TO idx_audit_user_id;
ALTER INDEX IF EXISTS idx_botiquin_clientes_sku RENAME TO idx_cabinet_client_available_skus_sku;
ALTER INDEX IF EXISTS idx_botiquin_odv_fecha RENAME TO idx_cabinet_odv_date;
ALTER INDEX IF EXISTS idx_botiquin_odv_id_cliente RENAME TO idx_cabinet_odv_client_id;
ALTER INDEX IF EXISTS idx_botiquin_odv_odv_id RENAME TO idx_cabinet_odv_odv_id;
ALTER INDEX IF EXISTS idx_botiquin_odv_sku RENAME TO idx_cabinet_odv_sku;
ALTER INDEX IF EXISTS idx_ciclos_id_ciclo_anterior RENAME TO idx_cabinet_cycles_previous_cycle_id;
ALTER INDEX IF EXISTS idx_ciclos_id_cliente RENAME TO idx_cabinet_cycles_client_id;
ALTER INDEX IF EXISTS idx_ciclos_id_usuario RENAME TO idx_cabinet_cycles_user_id;
ALTER INDEX IF EXISTS idx_cliente_estado_log_cliente RENAME TO idx_client_status_log_client_id;
ALTER INDEX IF EXISTS idx_cliente_estado_log_estado_nuevo RENAME TO idx_client_status_log_new_status;
ALTER INDEX IF EXISTS idx_cliente_estado_log_fecha RENAME TO idx_client_status_log_date;
ALTER INDEX IF EXISTS idx_clientes_estado RENAME TO idx_clients_status;
ALTER INDEX IF EXISTS idx_clientes_id_usuario RENAME TO idx_clients_user_id;
ALTER INDEX IF EXISTS idx_clientes_id_zona RENAME TO idx_clients_zone_id;
ALTER INDEX IF EXISTS idx_conversations_usuario RENAME TO idx_conversations_user_id;
ALTER INDEX IF EXISTS idx_inventario_cliente RENAME TO idx_cabinet_inventory_client_id;
ALTER INDEX IF EXISTS idx_inventario_sku RENAME TO idx_cabinet_inventory_sku;
ALTER INDEX IF EXISTS idx_med_padec_id_padecimiento RENAME TO idx_medication_conditions_condition_id;
ALTER INDEX IF EXISTS idx_mov_inv_task_id RENAME TO idx_inv_movements_task_id;
ALTER INDEX IF EXISTS idx_movimientos_cliente RENAME TO idx_inventory_movements_client_id;
ALTER INDEX IF EXISTS idx_movimientos_fecha RENAME TO idx_inventory_movements_date;
ALTER INDEX IF EXISTS idx_movimientos_inv_cliente_sku RENAME TO idx_inventory_movements_client_sku;
ALTER INDEX IF EXISTS idx_movimientos_inv_tipo RENAME TO idx_inventory_movements_type;
ALTER INDEX IF EXISTS idx_movimientos_saga RENAME TO idx_inventory_movements_saga;
ALTER INDEX IF EXISTS idx_movimientos_saga_zoho_link RENAME TO idx_inventory_movements_saga_zoho_link;
ALTER INDEX IF EXISTS idx_movimientos_sku RENAME TO idx_inventory_movements_sku;
ALTER INDEX IF EXISTS idx_movimientos_task_id RENAME TO idx_inventory_movements_task_id;
ALTER INDEX IF EXISTS idx_notificaciones_no_leidas RENAME TO idx_admin_notifications_unread;
ALTER INDEX IF EXISTS idx_notificaciones_para_usuario RENAME TO idx_admin_notifications_for_user;
ALTER INDEX IF EXISTS idx_notificaciones_tipo RENAME TO idx_admin_notifications_type;
ALTER INDEX IF EXISTS idx_outbox_pendientes RENAME TO idx_outbox_pending;
ALTER INDEX IF EXISTS idx_recolecciones_evidencias_recoleccion RENAME TO idx_collection_evidence_collection_id;
ALTER INDEX IF EXISTS idx_recolecciones_usuario_estado RENAME TO idx_collections_user_status;
ALTER INDEX IF EXISTS idx_recolecciones_visit RENAME TO idx_collections_visit;
ALTER INDEX IF EXISTS idx_saga_odv_links_odv RENAME TO idx_saga_zoho_links_odv;
ALTER INDEX IF EXISTS idx_saga_odv_links_saga RENAME TO idx_saga_zoho_links_saga;
ALTER INDEX IF EXISTS idx_saga_transactions_estado RENAME TO idx_saga_transactions_status;
ALTER INDEX IF EXISTS idx_saga_transactions_usuario RENAME TO idx_saga_transactions_user_id;
ALTER INDEX IF EXISTS idx_saga_tx_cliente RENAME TO idx_saga_tx_client_id;
ALTER INDEX IF EXISTS idx_saga_tx_tipo RENAME TO idx_saga_tx_type;
ALTER INDEX IF EXISTS idx_saga_visit_tipo RENAME TO idx_saga_visit_type;
ALTER INDEX IF EXISTS idx_usuarios_id_zoho RENAME TO idx_users_zoho_id;
ALTER INDEX IF EXISTS idx_ventas_odv_id_cliente RENAME TO idx_odv_sales_client_id;
ALTER INDEX IF EXISTS idx_visit_tasks_estado RENAME TO idx_visit_tasks_status;
ALTER INDEX IF EXISTS idx_visita_informes_completada RENAME TO idx_visit_reports_completed;
ALTER INDEX IF EXISTS idx_visitas_ciclo RENAME TO idx_visits_cycle_id;
ALTER INDEX IF EXISTS idx_visitas_cliente_estado RENAME TO idx_visits_client_status;
ALTER INDEX IF EXISTS idx_visitas_usuario_estado RENAME TO idx_visits_user_status;

COMMIT;
