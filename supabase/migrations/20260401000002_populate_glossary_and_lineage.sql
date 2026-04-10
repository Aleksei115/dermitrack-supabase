-- ============================================================================
-- Migration 3: Populate business glossary (25+ terms) + data lineage
-- Fase 0: Governance — domain knowledge for AI agents
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- BUSINESS GLOSSARY
-- ═══════════════════════════════════════════════════════════════════════════

INSERT INTO metadata.business_glossary (term, definition, category, related_tables, related_functions, examples) VALUES

-- Core Metrics
('M1 (Cabinet Sale)',
 'Revenue from products sold directly FROM the doctor''s cabinet. Tracked via inventory_movements WHERE type=SALE. Revenue = SUM(quantity * unit_price). This is "direct cabinet revenue" — the product was physically sold from the cabinet.',
 'metric', ARRAY['inventory_movements', 'cabinet_inventory'], ARRAY['clasificacion_base'],
 'Doctor sells cream directly from cabinet to patient → M1 sale'),

('M2 (Cabinet→ODV Conversion)',
 'Product first appeared in cabinet (via PLACEMENT), then sold from cabinet (SALE movement), and the doctor later purchases the same SKU via regular ODV order. Shows the cabinet drove adoption of the product into the doctor''s regular purchasing. This is a temporal correlation, not proven causation.',
 'metric', ARRAY['inventory_movements', 'odv_sales'], ARRAY['clasificacion_base'],
 'Cream placed in cabinet Jan 1 → sold from cabinet Jan 5 → ODV order for same cream Feb 1 → M2'),

('M3 (Exposure→ODV)',
 'Product was PLACED in cabinet (exposure) but never sold from it, yet the doctor later purchases the same SKU via ODV. Shows cabinet exposure may have influenced the purchase decision. Weaker signal than M2.',
 'metric', ARRAY['inventory_movements', 'odv_sales'], ARRAY['clasificacion_base'],
 'Cream placed in cabinet Jan 1, never sold from cabinet → ODV order for same cream Feb 1 → M3'),

('clasificacion_base',
 'THE SINGLE SOURCE OF TRUTH for M1/M2/M3 classification. SQL function that maps every (client_id, sku) pair to its M-type based on inventory_movements and odv_sales data. Used by 19+ downstream analytics RPCs.',
 'metric', ARRAY['inventory_movements', 'odv_sales', 'cabinet_sale_odv_ids'], ARRAY['clasificacion_base', 'get_dashboard_data'],
 'SELECT * FROM clasificacion_base() WHERE client_id = ''ABC123'''),

-- Core Processes
('Cutoff (Corte)',
 'Monthly cycle where a sales rep visits a doctor, counts and registers sales from the cabinet (SALE movements), registers products to collect (COLLECTION), and restocks with new products (POST_CUTOFF_PLACEMENT). Creates a VISIT_CUTOFF with 6 sequential tasks.',
 'process', ARRAY['visits', 'visit_tasks', 'inventory_movements', 'collections'], ARRAY['rpc_register_cutoff', 'rpc_create_visit'],
 'Visit doctor → sold 5 units from cabinet, collected 3 expired → restock 4 new units → link ODV'),

('Placement (Levantamiento)',
 'Adding products to a doctor''s cabinet. Creates PLACEMENT movements (+quantity). Two subtypes: INITIAL_PLACEMENT = first cabinet setup for new client. POST_CUTOFF_PLACEMENT = restocking during monthly cutoff.',
 'process', ARRAY['inventory_movements', 'cabinet_inventory', 'cabinet_inventory_lots'], ARRAY['rpc_register_placement', 'rpc_register_post_cutoff_placement'],
 'Place 30 products in new cabinet → INITIAL_PLACEMENT movements → lots created with expiry dates'),

('Collection (Recoleccion)',
 'Returning products from doctor''s cabinet back to CEDIS warehouse. Flow: PENDIENTE (registered) → IN_TRANSIT (rep picked up, driving) → ENTREGADA (delivered with signature + evidence photos). Inventory COLLECTION movements generated on delivery.',
 'process', ARRAY['collections', 'collection_items', 'collection_signatures', 'collection_evidence', 'inventory_movements'], ARRAY['rpc_register_collection_delivery', 'rpc_start_collection_transit'],
 NULL),

('Compensation (Compensacion)',
 'Rollback of a visit''s inventory effects. Creates reverse movements (negative quantities for placements, positive for sales). Only allowed before VALIDATION tasks are completed. Restores cabinet_inventory and lot statuses.',
 'process', ARRAY['saga_compensations', 'inventory_movements', 'cabinet_inventory_lots'], ARRAY['rpc_compensate_visit_v2'],
 'Compensate visit → placement of 10 reversed → cabinet_inventory reduced by 10 → lots restored to active'),

('Validation (Validacion)',
 'The step where a rep confirms that inventory movements match the external Zoho ODV document. SALE_ODV validates SALE movements; ODV_CABINET validates PLACEMENT movements. Does NOT generate new movements — only marks existing ones as validated=true and links the ODV ID.',
 'process', ARRAY['visit_tasks', 'inventory_movements', 'cabinet_sale_odv_ids'], ARRAY['rpc_link_odv'],
 'Rep uploads ODV PDF → rpc_link_odv verifies items → movements marked validated=true'),

-- Core Entities
('Cabinet (Botiquin)',
 'Physical product display/storage at a doctor''s office. Contains pharmaceutical samples for the doctor to prescribe or sell to patients. Stock tracked per (client_id, SKU) in cabinet_inventory. Each product lot has an expiry date tracked in cabinet_inventory_lots.',
 'entity', ARRAY['cabinet_inventory', 'cabinet_inventory_lots', 'inventory_movements'], NULL,
 NULL),

('ODV (Orden de Venta)',
 'Sales order in Zoho CRM. Imported via CSV by admin. Two types: regular ODV (odv_sales — doctor''s normal purchases) and cabinet consignment ODV (cabinet_odv — products placed in cabinet). ODV IDs format: DCOdV-12345.',
 'entity', ARRAY['odv_sales', 'cabinet_odv'], NULL,
 'DCOdV-12345'),

('Visit (Visita)',
 'Field visit by a sales rep to a doctor''s office. Two types: VISIT_INITIAL_PLACEMENT (first cabinet setup, 3 tasks: placement→ODV confirm→report) and VISIT_CUTOFF (monthly cycle, 6 tasks: cutoff→ODV confirm→collection→restocking→ODV confirm→report).',
 'entity', ARRAY['visits', 'visit_tasks', 'visit_reports'], ARRAY['rpc_create_visit'],
 NULL),

('Client (Cliente/Doctor)',
 'A doctor or pharmacy with a pharmaceutical cabinet. Identified by client_id (= Zoho normal account ID). Has a tier (DIAMANTE/ORO/PLATA/BRONCE) based on ODV billing. Status lifecycle: ACTIVE → DOWNGRADING → INACTIVE.',
 'entity', ARRAY['clients', 'cabinet_inventory'], NULL,
 NULL),

('Sales Rep (Representante)',
 'Field pharmaceutical sales representative. Visits doctors monthly for cutoffs. Has a zone (territory). App user with role=REP.',
 'entity', ARRAY['users', 'visits', 'zones'], NULL,
 NULL),

-- Dimensions
('Tier (Nivel)',
 'Client revenue classification based on monthly ODV billing average. DIAMANTE: highest spenders. ORO: high. PLATA: medium. BRONCE: low. Computed by update_tier_and_current_billing() function.',
 'dimension', ARRAY['clients'], ARRAY['update_tier_and_current_billing'],
 NULL),

('Zone (Zona)',
 'Geographic sales territory. Each rep is assigned to one zone. All clients in a zone are served by the assigned rep.',
 'dimension', ARRAY['zones', 'users', 'clients'], NULL,
 NULL),

('Shelf Life (Vida Util)',
 'Duration in months before a medication expires after placement in a cabinet. Stored in medications.shelf_life_months. Default: 12 months. expiry_date = placement_date + shelf_life_months.',
 'dimension', ARRAY['medications', 'cabinet_inventory_lots'], NULL,
 'shelf_life_months=3 → placed March 1 → expires June 1'),

('FEFO (First Expired First Out)',
 'Mandatory removal strategy for pharmaceutical cabinet inventory. When selling or collecting products, always consume lots with the earliest expiry_date first. Implemented by _consume_lots_fefo() internal function.',
 'process', ARRAY['cabinet_inventory_lots'], ARRAY['_consume_lots_fefo'],
 'Cabinet has Lot A (expires April) + Lot B (expires July) → sale of 5 → consumes from Lot A first'),

-- Enum explanations
('Visit Task Types',
 'Sequential tasks within a visit. INITIAL_PLACEMENT (place products), ODV_CABINET (validate cabinet ODV), CUTOFF (register sales), SALE_ODV (validate sale ODV), COLLECTION (return products), POST_CUTOFF_PLACEMENT (restock), VISIT_REPORT (submit report).',
 'enum', ARRAY['visit_tasks'], NULL,
 NULL),

('Transaction Types',
 'Task reversibility classification. COMPENSABLE: can be undone (creates reverse movements). VALIDATION: confirms external document (marks validated=true, no new movements). RETRYABLE: must succeed, idempotent.',
 'enum', ARRAY['visit_tasks'], NULL,
 NULL),

('Movement Types',
 'PLACEMENT (+qty): products enter cabinet. SALE (-qty): products sold from cabinet (generates M1 revenue). COLLECTION (-qty): products returned from cabinet. HOLDING: marker for remaining stock at cutoff (informational only).',
 'enum', ARRAY['inventory_movements'], NULL,
 NULL),

('Collection Statuses',
 'PENDIENTE: collection registered during cutoff, items identified. IN_TRANSIT: rep has picked up items from cabinet, driving to CEDIS warehouse. ENTREGADA: products delivered to CEDIS with signature and evidence photos.',
 'enum', ARRAY['collections'], NULL,
 NULL),

-- Analytics concepts
('Dashboard Static Data',
 'Data loaded once on dashboard mount via get_dashboard_static(). Includes available filters (brands, doctors, conditions) and general cutoff KPIs with % change. Does NOT change with filter selections.',
 'metric', NULL, ARRAY['get_dashboard_static'],
 NULL),

('Dashboard Dynamic Data',
 'Data loaded per filter change via get_dashboard_data(5 params). Returns 6 sections: clasificacionBase, impactoResumen, marketAnalysis, conversionDetails, facturacionComposicion, sankeyFlows.',
 'metric', NULL, ARRAY['get_dashboard_data'],
 NULL),

('Channel Contribution',
 'Percentage of total revenue that flows through the cabinet channel. Formula: (direct_cabinet_sales + exposed_product_odvs) / total_revenue * 100. Uses temporal correlation, NOT causal attribution.',
 'metric', ARRAY['inventory_movements', 'odv_sales'], ARRAY['get_facturacion_composicion'],
 NULL);


-- ═══════════════════════════════════════════════════════════════════════════
-- DATA LINEAGE
-- ═══════════════════════════════════════════════════════════════════════════

INSERT INTO metadata.data_lineage (table_name, source_system, update_frequency, is_derived, derived_from, notes) VALUES
('clients', 'manual', 'on-change', false, NULL,
 'Created by admin via Edge Function. Updated by mobile app (status changes) and triggers (tier/billing).'),
('users', 'edge_function', 'on-change', false, NULL,
 'Created by admin-users Edge Function. Auth via Supabase Auth.'),
('medications', 'manual', 'monthly', false, NULL,
 'Product catalog. Prices updated manually by admin.'),
('zones', 'manual', 'rarely', false, NULL,
 'Geographic territories. Set up once, rarely changed.'),
('conditions', 'manual', 'rarely', false, NULL,
 'Medical conditions. Set up once, rarely changed.'),
('medication_conditions', 'manual', 'rarely', false, NULL,
 'Junction table. Set up when new products are added.'),
('visits', 'mobile_app', 'real-time', false, NULL,
 'Created by rpc_create_visit when rep starts a visit on mobile app.'),
('visit_tasks', 'rpc', 'real-time', true, ARRAY['visits'],
 'Auto-created by rpc_create_visit based on visit type. Updated by task-specific RPCs.'),
('visit_reports', 'mobile_app', 'real-time', false, NULL,
 'Survey responses submitted by rep. One per visit.'),
('inventory_movements', 'rpc', 'real-time', false, NULL,
 'Created by rpc_register_cutoff, rpc_register_placement, rpc_register_collection_delivery. IMMUTABLE — never updated or deleted.'),
('cabinet_inventory', 'trigger', 'real-time', true, ARRAY['inventory_movements'],
 'Derived: aggregated stock from inventory_movements. Updated by triggers. Deleted when quantity=0.'),
('odv_sales', 'csv_import', 'weekly', false, NULL,
 'Imported from Zoho CRM via import-odv Edge Function. CSV/XLSX upload by admin.'),
('cabinet_odv', 'csv_import', 'weekly', false, NULL,
 'Cabinet consignment ODVs from Zoho. Routed by zoho_cabinet_client_id mapping.'),
('collections', 'mobile_app', 'real-time', false, NULL,
 'Created by rpc_register_cutoff (PENDIENTE). Updated by rpc_start_collection_transit (IN_TRANSIT) and rpc_register_collection_delivery (ENTREGADA).'),
('collection_items', 'mobile_app', 'real-time', true, ARRAY['collections'],
 'Items within a collection. Created during cutoff registration.'),
('collection_signatures', 'mobile_app', 'real-time', true, ARRAY['collections'],
 'Delivery signatures. Created on collection delivery at CEDIS.'),
('collection_evidence', 'mobile_app', 'real-time', true, ARRAY['collections'],
 'Evidence photos. Uploaded during collection delivery.'),
('saga_transactions', 'rpc', 'real-time', false, NULL,
 '[DEPRECATED] Created by saga RPCs. Being replaced by direct inventory operations.'),
('saga_zoho_links', 'rpc', 'real-time', false, NULL,
 '[DEPRECATED] Created by saga confirm. Being replaced by cabinet_sale_odv_ids.'),
('notifications', 'trigger', 'real-time', false, NULL,
 'Push notifications generated by triggers on visit/task events.'),
('admin_notifications', 'trigger', 'real-time', false, NULL,
 'Admin alerts generated by triggers on errors/issues.'),
('audit_log', 'trigger', 'real-time', true, NULL,
 'Audit trail populated by triggers on significant operations.'),
('client_status_log', 'trigger', 'real-time', true, ARRAY['clients'],
 'History of client status changes. Populated by trigger on clients.status update.');


-- ═══════════════════════════════════════════════════════════════════════════
-- COLUMN CLASSIFICATION (PII)
-- ═══════════════════════════════════════════════════════════════════════════

INSERT INTO metadata.column_classification (schema_name, table_name, column_name, classification, pii_type) VALUES
('public', 'users', 'name', 'pii', 'name'),
('public', 'users', 'phone', 'pii', 'phone'),
('public', 'users', 'email', 'pii', 'email'),
('public', 'users', 'auth_user_id', 'confidential', NULL),
('public', 'clients', 'client_name', 'pii', 'name'),
('public', 'collections', 'latitude', 'internal', 'location'),
('public', 'collections', 'longitude', 'internal', 'location'),
('public', 'collections', 'cedis_responsible_name', 'pii', 'name'),
('public', 'collection_signatures', 'signature_url', 'confidential', NULL),
('public', 'user_push_tokens', 'token', 'confidential', NULL);
