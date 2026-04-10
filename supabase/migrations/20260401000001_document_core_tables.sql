-- ============================================================================
-- Migration 2: COMMENT ON for 30 tables + critical columns + 16 enums
-- Fase 0: Governance — self-documenting schema for AI introspection
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- CORE ENTITIES
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE clients IS
'Doctors/pharmacies that receive pharmaceutical product cabinets. Each client has a unique client_id (= Zoho normal account ID). Lifecycle: ACTIVE → DOWNGRADING → INACTIVE. Tier (Diamante/Oro/Plata/Bronce) computed monthly from ODV billing.';

COMMENT ON COLUMN clients.client_id IS 'PK. Matches Zoho normal account ID (id_cliente_zoho_normal).';
COMMENT ON COLUMN clients.zoho_cabinet_client_id IS 'Alternate Zoho ID for consignment/cabinet account. Used by import-odv to route CSV rows to cabinet_odv table.';
COMMENT ON COLUMN clients.status IS 'Lifecycle: ACTIVE (has cabinet), DOWNGRADING (0 items post-cutoff), INACTIVE (cabinet removed), SUSPENDED (admin action).';
COMMENT ON COLUMN clients.tier IS 'Revenue tier: DIAMANTE (highest), ORO, PLATA, BRONCE. Computed by update_tier_and_current_billing() from ODV sales monthly average.';
COMMENT ON COLUMN clients.current_billing IS 'Monthly average ODV billing (pesos). Computed by update_tier_and_current_billing(). Source: odv_sales table.';

COMMENT ON TABLE users IS
'Sales representatives and administrators. Auth via Supabase Auth. Role determines access: REP (field), ADMIN (management), OWNER (full access).';

COMMENT ON COLUMN users.user_id IS 'PK. Internal user ID (varchar). Maps to Supabase auth via auth_user_id.';
COMMENT ON COLUMN users.auth_user_id IS 'FK to auth.users.id (UUID). Used by RLS policies: auth.uid() = auth_user_id.';
COMMENT ON COLUMN users.role IS 'OWNER: full access. ADMIN: management + analytics. REP: field operations only.';
COMMENT ON COLUMN users.id_zoho IS 'Zoho CRM user ID for CRM integration. Zone is on clients table, not users.';

COMMENT ON TABLE medications IS
'Product catalog. Each SKU is a unique pharmaceutical product. Price is current list price (frozen in inventory_movements.unit_price at movement time).';

COMMENT ON COLUMN medications.sku IS 'PK. Unique product identifier (e.g., "DER-CRM-50ML").';
COMMENT ON COLUMN medications.brand IS 'Brand name (e.g., "Dermicare", "Skinpro"). Used for brand performance analytics and filtering.';
COMMENT ON COLUMN medications.product IS 'Human-readable product name.';
COMMENT ON COLUMN medications.price IS 'Current list price. Frozen as unit_price in inventory_movements at movement time.';
COMMENT ON COLUMN medications.top IS 'True if SKU is in the "top products" list for analytics emphasis.';
COMMENT ON COLUMN medications.barcode IS 'Product barcode for scanning.';
COMMENT ON COLUMN medications.data_sheet_url IS 'URL to the product data sheet (ficha tecnica) PDF in Supabase Storage.';

COMMENT ON TABLE zones IS 'Geographic sales territories. Each rep (user) is assigned to one zone.';
COMMENT ON TABLE conditions IS 'Medical conditions/diseases (padecimientos). Linked to medications via medication_conditions junction table.';
COMMENT ON TABLE medication_conditions IS 'Junction table: which medications treat which conditions. M:N relationship.';

-- ═══════════════════════════════════════════════════════════════════════════
-- VISIT SYSTEM
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE visits IS
'Field visits by sales reps to doctors. Two types: VISIT_INITIAL_PLACEMENT (first cabinet setup, 3 tasks) and VISIT_CUTOFF (monthly cycle, 6 tasks). Lifecycle: PENDING → IN_PROGRESS → COMPLETED/CANCELLED.';

COMMENT ON COLUMN visits.visit_id IS 'PK. UUID generated on creation.';
COMMENT ON COLUMN visits.client_id IS 'FK to clients. The doctor being visited.';
COMMENT ON COLUMN visits.user_id IS 'FK to users. The sales rep performing the visit.';
COMMENT ON COLUMN visits.type IS 'VISIT_INITIAL_PLACEMENT (3 tasks) or VISIT_CUTOFF (6 tasks).';
COMMENT ON COLUMN visits.status IS 'PENDING → IN_PROGRESS → COMPLETED/CANCELLED. Controlled by task completion.';
COMMENT ON COLUMN visits.saga_status IS 'RUNNING (tasks in progress), COMPLETED (all done), COMPENSATED (rolled back).';
COMMENT ON COLUMN visits.cycle_id IS 'FK to cycles. Links visit to a business cycle (optional).';
COMMENT ON COLUMN visits.corte_number IS 'Sequential cutoff number for this client (1st cutoff, 2nd, etc).';

COMMENT ON TABLE visit_tasks IS
'Individual tasks within a visit. Executed sequentially by step_order. Transaction types define reversibility: COMPENSABLE (can undo), VALIDATION (confirms external doc), RETRYABLE (must complete).';

COMMENT ON COLUMN visit_tasks.visit_id IS 'FK to visits. Part of composite PK.';
COMMENT ON COLUMN visit_tasks.task_type IS 'INITIAL_PLACEMENT, ODV_CABINET, CUTOFF, SALE_ODV, COLLECTION, POST_CUTOFF_PLACEMENT, VISIT_REPORT.';
COMMENT ON COLUMN visit_tasks.task_id IS 'Unique UUID for this task instance. Used as FK from inventory_movements.task_id.';
COMMENT ON COLUMN visit_tasks.transaction_type IS 'COMPENSABLE: can be undone. PIVOT/VALIDATION: confirms external data (ODV). RETRYABLE: must complete, idempotent.';
COMMENT ON COLUMN visit_tasks.step_order IS 'Execution order within visit. Lower = earlier. Tasks must be completed in order.';
COMMENT ON COLUMN visit_tasks.status IS 'PENDING → IN_PROGRESS → COMPLETED/SKIPPED/SKIPPED_M. Drives visit progress.';
COMMENT ON COLUMN visit_tasks.reference_id IS 'ID of the related record (saga_transaction_id, collection_id, etc).';

COMMENT ON TABLE visit_reports IS
'Visit report submissions. One per visit. Contains survey responses as JSONB + evidence photos.';

-- ═══════════════════════════════════════════════════════════════════════════
-- INVENTORY SYSTEM
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE inventory_movements IS
'Immutable ledger of all cabinet inventory changes. Every PLACEMENT (+), SALE (-), COLLECTION (-) is recorded with quantity snapshots. Source of truth for M1 revenue in clasificacion_base(). Extensibility: location_id can be added as nullable column for multi-location support.';

COMMENT ON COLUMN inventory_movements.id IS 'PK. Auto-incrementing bigint.';
COMMENT ON COLUMN inventory_movements.client_id IS 'FK to clients. The doctor whose cabinet was affected.';
COMMENT ON COLUMN inventory_movements.sku IS 'FK to medications. The product that moved.';
COMMENT ON COLUMN inventory_movements.type IS 'PLACEMENT: products added to cabinet (+qty). SALE: products sold from cabinet (-qty, generates M1 revenue). COLLECTION: products returned from cabinet (-qty).';
COMMENT ON COLUMN inventory_movements.quantity IS 'Number of units moved. Always >= 0.';
COMMENT ON COLUMN inventory_movements.quantity_before IS 'Cabinet stock BEFORE this movement. Enables audit trail verification.';
COMMENT ON COLUMN inventory_movements.quantity_after IS 'Cabinet stock AFTER this movement. quantity_after = quantity_before ± quantity.';
COMMENT ON COLUMN inventory_movements.unit_price IS 'Price frozen at movement time. Used for M1 revenue: SUM(quantity * unit_price) WHERE type=SALE.';
COMMENT ON COLUMN inventory_movements.movement_date IS 'When the movement occurred. Defaults to now().';
COMMENT ON COLUMN inventory_movements.task_id IS 'FK to visit_tasks.task_id. Links movement to the specific visit task that created it.';
COMMENT ON COLUMN inventory_movements.id_saga_transaction IS '[DEPRECATED] FK to saga_transactions. Being replaced by visit_id direct link.';
COMMENT ON COLUMN inventory_movements.id_saga_zoho_link IS '[DEPRECATED] FK to saga_zoho_links. Being replaced by cabinet_sale_odv_ids.';

COMMENT ON TABLE cabinet_inventory IS
'Current aggregated cabinet stock per (client, SKU). Derived from inventory_movements via triggers. available_quantity = SUM(placements) - SUM(sales + collections). Deleted when quantity reaches 0. Extensibility: location_id and reserved_quantity can be added for multi-location/reservation support.';

COMMENT ON COLUMN cabinet_inventory.client_id IS 'FK to clients. Part of composite PK.';
COMMENT ON COLUMN cabinet_inventory.sku IS 'FK to medications. Part of composite PK.';
COMMENT ON COLUMN cabinet_inventory.available_quantity IS 'Current stock on hand. Always >= 0. Updated by triggers on inventory_movements.';
COMMENT ON COLUMN cabinet_inventory.unit_price IS 'Latest unit price for this SKU in this cabinet.';

-- ═══════════════════════════════════════════════════════════════════════════
-- ZOHO / ODV DATA
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE odv_sales IS
'Recurring sales orders (ODVs) imported from Zoho CRM via CSV. NOT generated by the app. Source for M2/M3 revenue in clasificacion_base(). Deduped by (odv_id, client_id, sku).';

COMMENT ON COLUMN odv_sales.odv_id IS 'Zoho order ID (e.g., "DCOdV-12345"). Links to cabinet_sale_odv_ids for M1/M2 deduplication.';
COMMENT ON COLUMN odv_sales.client_id IS 'FK to clients. The doctor who placed the ODV order.';
COMMENT ON COLUMN odv_sales.sku IS 'FK to medications. The product ordered.';

COMMENT ON TABLE cabinet_odv IS
'Cabinet consignment orders imported from Zoho CRM via CSV. Uses zoho_cabinet_client_id mapping. Display-only in Logistics tab for audit purposes.';

-- ═══════════════════════════════════════════════════════════════════════════
-- SAGA SYSTEM (deprecated — being replaced by direct inventory operations)
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE saga_transactions IS
'[DEPRECATED — being replaced by direct inventory RPCs] Payload for visit tasks requiring Zoho sync. DRAFT → CONFIRMED → generates inventory_movements. Will be removed after full migration to direct RPCs.';

COMMENT ON COLUMN saga_transactions.visit_id IS '[DEPRECATED] FK to visits. Being replaced by inventory_movements.visit_id direct link.';

COMMENT ON TABLE saga_zoho_links IS
'[DEPRECATED — being replaced by cabinet_sale_odv_ids] Links saga transactions to Zoho ODV IDs. Used by clasificacion_base for M1/M2 deduplication.';

COMMENT ON TABLE saga_adjustments IS
'[DEPRECATED] Adjustments to saga transactions.';

COMMENT ON TABLE saga_compensations IS
'[DEPRECATED] Compensation records for reversed saga transactions.';

-- ═══════════════════════════════════════════════════════════════════════════
-- COLLECTIONS
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE collections IS
'Product return/collection events. Lifecycle: PENDIENTE (registered at cutoff) → IN_TRANSIT (rep picked up from cabinet, en route to CEDIS) → ENTREGADA (delivered to CEDIS with signature). Inventory movements generated at ENTREGADA step.';

COMMENT ON COLUMN collections.collection_id IS 'PK. UUID auto-generated.';
COMMENT ON COLUMN collections.visit_id IS 'FK to visits. The visit that registered this collection.';
COMMENT ON COLUMN collections.client_id IS 'FK to clients. The doctor whose cabinet is being collected from.';
COMMENT ON COLUMN collections.user_id IS 'FK to users. The rep handling the collection.';
COMMENT ON COLUMN collections.status IS 'PENDIENTE: items registered at cutoff. IN_TRANSIT: rep picked up items, driving to CEDIS. ENTREGADA: delivered with signature + evidence photos.';
COMMENT ON COLUMN collections.delivered_at IS 'Timestamp of CEDIS delivery. Set by rpc_register_collection_delivery().';
COMMENT ON COLUMN collections.cedis_responsible_name IS 'Name of CEDIS person who received the products.';

COMMENT ON TABLE collection_items IS 'Products included in a collection. Each row = (collection_id, SKU, quantity).';
COMMENT ON TABLE collection_signatures IS 'Digital signatures for collection delivery confirmation at CEDIS.';
COMMENT ON TABLE collection_evidence IS 'Evidence photos (product condition, delivery receipt) for collections.';

-- ═══════════════════════════════════════════════════════════════════════════
-- NOTIFICATIONS & ADMIN
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE notifications IS 'Push notifications sent to users. Tracks read_at for unread badge count.';
COMMENT ON TABLE admin_notifications IS 'Admin-visible notifications (task errors, visit issues). Optionally targeted to specific admin via for_user.';
COMMENT ON TABLE user_push_tokens IS 'Expo push tokens for sending notifications to mobile devices.';
COMMENT ON TABLE user_notification_preferences IS 'Per-user notification preferences (which types to receive).';
COMMENT ON TABLE app_config IS 'Application configuration key-value store. Global settings.';

-- ═══════════════════════════════════════════════════════════════════════════
-- AUDIT & EVENTS
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE audit_log IS 'Audit trail for all significant operations. Who did what, when, with what data.';
COMMENT ON TABLE event_outbox IS 'Transactional outbox for events that need external delivery (webhooks, notifications).';
COMMENT ON TABLE client_status_log IS 'History of client status changes (ACTIVE→DOWNGRADING, etc) with reason and who changed it.';

-- ═══════════════════════════════════════════════════════════════════════════
-- OTHER TABLES
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TABLE cabinet_client_available_skus IS 'Which SKUs are available for placement in each client''s cabinet. Admin-configured.';

-- ═══════════════════════════════════════════════════════════════════════════
-- ENUMS (all 16)
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON TYPE cabinet_movement_type IS 'PLACEMENT: add to cabinet (+qty). SALE: sell from cabinet (-qty). COLLECTION: return from cabinet (-qty). HOLDING: marker for remaining stock at cutoff.';
COMMENT ON TYPE client_status IS 'ACTIVE: has active cabinet. DOWNGRADING: 0 items post-cutoff. INACTIVE: cabinet removed. SUSPENDED: admin suspended.';
COMMENT ON TYPE saga_transaction_type IS '[DEPRECATED] INITIAL_PLACEMENT, POST_CUTOFF_PLACEMENT, SALE, COLLECTION, CUTOFF, etc.';
COMMENT ON TYPE saga_transaction_status IS '[DEPRECATED] DRAFT: reversible. CONFIRMED: movements generated. CANCELLED_F: rolled back.';
COMMENT ON TYPE user_role IS 'OWNER: full system access. ADMIN: management + analytics. REP: field operations only.';
COMMENT ON TYPE visit_type IS 'VISIT_INITIAL_PLACEMENT: first cabinet setup (3 tasks). VISIT_CUTOFF: monthly cycle (6 tasks).';
COMMENT ON TYPE visit_task_type IS 'INITIAL_PLACEMENT, ODV_CABINET, CUTOFF, SALE_ODV, COLLECTION, POST_CUTOFF_PLACEMENT, VISIT_REPORT.';
COMMENT ON TYPE visit_task_status IS 'PENDING, IN_PROGRESS, COMPLETED, SKIPPED, SKIPPED_M (manual skip). ERROR, CANCELLED for failures.';
COMMENT ON TYPE visit_status IS 'PENDING: not started. IN_PROGRESS: rep is on-site. COMPLETED: all tasks done. CANCELLED: visit aborted.';
COMMENT ON TYPE transaction_type IS 'COMPENSABLE: reversible step. PIVOT: legacy point-of-no-return (→VALIDATION in new visits). RETRYABLE: must complete, idempotent.';
COMMENT ON TYPE notification_type IS 'TASK_COMPLETED, TASK_ERROR, VISIT_COMPLETED, ADMIN_ALERT, SYSTEM.';
COMMENT ON TYPE inventory_movement_type IS 'INBOUND: stock entering system. OUTBOUND: stock leaving system. Legacy — cabinet_movement_type is preferred.';
