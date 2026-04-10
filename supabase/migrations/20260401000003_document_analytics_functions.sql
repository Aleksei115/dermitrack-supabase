-- ============================================================================
-- Migration 4: COMMENT ON FUNCTION for top analytics, chatbot, and RPC functions
-- Fase 0: Governance — function documentation for AI introspection
-- ============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- ANALYTICS CORE (analytics schema)
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON FUNCTION analytics.clasificacion_base(character varying[], character varying[], character varying[], date, date) IS
'SINGLE SOURCE OF TRUTH for M1/M2/M3 revenue classification. Maps every (client_id, sku) to M1 (cabinet sale), M2 (cabinet→ODV conversion), M3 (exposure→ODV). Params: p_doctors, p_brands, p_conditions, p_start_date, p_end_date. Used by 19+ downstream RPCs.';

COMMENT ON FUNCTION analytics.get_dashboard_data(character varying[], character varying[], character varying[], date, date) IS
'Consolidated analytics RPC called per filter change. Returns 6 JSON sections: clasificacionBase, impactoResumen, marketAnalysis, conversionDetails, facturacionComposicion, sankeyFlows. Params: p_doctors, p_brands, p_conditions, p_start_date, p_end_date.';

COMMENT ON FUNCTION analytics.get_dashboard_static() IS
'Static dashboard data loaded ONCE on mount. Returns: corteFiltros (available brands/doctors/conditions), corteStatsGenerales (cutoff KPIs with % change vs previous), corteProgress (visit completion stats).';

-- ── Cutoff RPCs ─────────────────────────────────────────────────────────────

COMMENT ON FUNCTION analytics.get_current_cutoff_data(character varying[], character varying[], character varying[]) IS
'Cutoff → Current tab. Returns KPIs (sales/placement/collection amounts + % change vs previous cutoff) + per-doctor grid rows with detailed stats. Server-side filtered by doctors/brands/conditions.';

COMMENT ON FUNCTION analytics.get_historical_cutoff_data(character varying[], character varying[], character varying[], date, date) IS
'Cutoff → Historical tab. All-time KPIs (M1 sales, placements, active stock, collections) + date-filtered per-visit detail rows. Supports date range and entity filters.';

COMMENT ON FUNCTION analytics.get_cutoff_logistics_data(character varying[], character varying[], character varying[]) IS
'Cutoff → Logistics tab. Per-SKU movement detail: saga state, ODV links, collection evidence, movement timestamps. Heavy on joins — returns comprehensive audit trail per movement.';

COMMENT ON FUNCTION analytics.get_current_cutoff_range() IS
'Detects the current cutoff date range by finding the latest contiguous period without >3 day gaps in visit activity. Used internally by cutoff RPCs.';

COMMENT ON FUNCTION analytics.get_cutoff_general_stats() IS
'Current cutoff summary KPIs: total sales value, placement value, collection value, active stock, doctor count.';

COMMENT ON FUNCTION analytics.get_cutoff_general_stats_with_comparison() IS
'Current cutoff KPIs + % change vs previous cutoff. Each metric includes current, previous, and change_pct.';

COMMENT ON FUNCTION analytics.get_cutoff_stats_by_doctor() IS
'Per-doctor cutoff stats: sales value, placement value, SKU count, movement count. One row per doctor.';

COMMENT ON FUNCTION analytics.get_cutoff_stats_by_doctor_with_comparison() IS
'Per-doctor cutoff stats WITH comparison to previous cutoff. Includes current/previous values and % change per metric.';

COMMENT ON FUNCTION analytics.get_cutoff_logistics_detail() IS
'Legacy logistics detail (no filters). Returns all movement details with saga/ODV info. Replaced by get_cutoff_logistics_data() with filter params.';

COMMENT ON FUNCTION analytics.get_cutoff_available_filters() IS
'Returns brands/doctors/conditions scoped to current cutoff period only. Replaced by get_available_filters() which returns all options.';

COMMENT ON FUNCTION analytics.get_previous_cutoff_stats() IS
'Previous cutoff summary KPIs for comparison calculations.';

COMMENT ON FUNCTION analytics.get_cutoff_skus_value_per_visit(character varying, character varying) IS
'SKU-level value breakdown per visit in current cutoff. Optional filters: p_client_id, p_brand.';

COMMENT ON FUNCTION analytics.get_historical_skus_value_per_visit(date, date, character varying) IS
'Historical SKU-level value per visit. Filters: p_start_date, p_end_date, p_client_id.';

-- ── Conversion & Adoption ───────────────────────────────────────────────────

COMMENT ON FUNCTION analytics.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) IS
'Conversion summary: total adoptions, total conversions, total value generated from converted products.';

COMMENT ON FUNCTION analytics.get_conversion_details(character varying[], character varying[], character varying[], date, date) IS
'Per-client/SKU conversion detail: classification (M2/M3), first placement date, first ODV date, days to convert, revenue.';

COMMENT ON FUNCTION analytics.get_sankey_conversion_flows(character varying[], character varying[], character varying[], date, date) IS
'M2/M3 flow data for Sankey diagrams. Source→target node pairs with value/count for visualization.';

COMMENT ON FUNCTION analytics.get_historical_conversions_evolution(date, date, text, character varying[], character varying[], character varying[]) IS
'Time-series of conversions over time. Grouping: day/week/month. Shows M2/M3 conversion counts and revenue per period.';

COMMENT ON FUNCTION analytics.get_crosssell_significance() IS
'Statistical significance test for cross-sell patterns using chi-squared test. Tests if cabinet exposure significantly increases ODV purchase probability.';

-- ── Market & Performance ────────────────────────────────────────────────────

COMMENT ON FUNCTION analytics.get_brand_performance(character varying[], character varying[], character varying[], date, date) IS
'Revenue and pieces by brand. Includes M1 cabinet revenue + ODV revenue breakdown per brand.';

COMMENT ON FUNCTION analytics.get_condition_performance(character varying[], character varying[], character varying[], date, date) IS
'Revenue and pieces by medical condition (padecimiento). Routes through medication_conditions.';

COMMENT ON FUNCTION analytics.get_yoy_by_condition(character varying[], character varying[], character varying[], date, date) IS
'Year-over-year growth by condition. Compares current period vs same period last year.';

COMMENT ON FUNCTION analytics.get_product_interest(integer, character varying[], character varying[], character varying[], date, date) IS
'Product interest: movement breakdown by type (sale/placement/collection/stock) per SKU. p_limit controls top-N.';

COMMENT ON FUNCTION analytics.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date) IS
'Condition opportunity matrix. Cross-tabulates conditions vs revenue potential for strategic planning.';

COMMENT ON FUNCTION analytics.get_market_analysis(character varying[], character varying[], character varying[], date, date) IS
'Combined market analysis: brand + condition performance + YoY in a single response.';

-- ── Inventory & Financial ───────────────────────────────────────────────────

COMMENT ON FUNCTION analytics.get_balance_metrics() IS
'Inventory balance: total inbound (placements) vs outbound (sales + collections). Overall stock health metrics.';

COMMENT ON FUNCTION analytics.get_cabinet_impact_summary(character varying[], character varying[], character varying[], date, date) IS
'Cabinet impact summary: M1/M2/M3/M4 revenue breakdown. Shows how much revenue flows through each classification.';

COMMENT ON FUNCTION analytics.get_impact_detail(text, character varying[], character varying[], character varying[], date, date) IS
'Drill-down detail by metric type (M1/M2/M3/M4). Shows per-client/SKU rows for the selected metric.';

COMMENT ON FUNCTION analytics.get_billing_composition(character varying[], character varying[], character varying[], date, date) IS
'Per-client billing composition: baseline ODV vs M1 (cabinet sale) vs M2 (converted) vs M3 (exposed). Shows revenue attribution per doctor.';

COMMENT ON FUNCTION analytics.get_billing_composition_legacy() IS
'Legacy billing composition without filter params. Returns all clients. Replaced by parameterized version.';

COMMENT ON FUNCTION analytics.get_active_collection() IS
'Active collection metrics: pending collections, items in transit, recently delivered. Dashboard collection status.';

COMMENT ON FUNCTION analytics.get_available_filters() IS
'Returns ALL available brands, doctors (active clients), and conditions for dashboard filter dropdowns. Not scoped to cutoff.';

-- ── Data Sources ────────────────────────────────────────────────────────────

COMMENT ON FUNCTION analytics.get_cabinet_data() IS
'Raw cabinet movement data (botiquin). Loaded into Redux for client-side filtering. Warning: subject to PostgREST 1000-row limit — frontend uses .limit(5000).';

COMMENT ON FUNCTION analytics.get_recurring_data() IS
'Raw recurring sales (ODV) data. Loaded into Redux for client-side filtering. Warning: subject to PostgREST 1000-row limit — frontend uses .limit(5000).';

COMMENT ON FUNCTION analytics.get_client_audit(character varying) IS
'Complete audit trail for a single client: all movements, visits, collections, ODVs in chronological order.';

-- ═══════════════════════════════════════════════════════════════════════════
-- PUBLIC WRAPPERS (SECURITY DEFINER proxies to analytics schema)
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON FUNCTION public.clasificacion_base(character varying[], character varying[], character varying[], date, date) IS
'Public SECURITY DEFINER wrapper for analytics.clasificacion_base(). See analytics schema version for full docs.';

COMMENT ON FUNCTION public.get_dashboard_data(character varying[], character varying[], character varying[], date, date) IS
'Public SECURITY DEFINER wrapper for analytics.get_dashboard_data().';

COMMENT ON FUNCTION public.get_dashboard_static() IS
'Public SECURITY DEFINER wrapper for analytics.get_dashboard_static().';

COMMENT ON FUNCTION public.get_current_cutoff_data(character varying[], character varying[], character varying[]) IS
'Public SECURITY DEFINER wrapper for analytics.get_current_cutoff_data().';

COMMENT ON FUNCTION public.get_available_filters() IS
'Public SECURITY DEFINER wrapper for analytics.get_available_filters().';

-- ═══════════════════════════════════════════════════════════════════════════
-- CHATBOT FUNCTIONS
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON FUNCTION chatbot.fuzzy_search_clients(text, character varying, integer) IS
'Fuzzy search for clients by name. Uses word similarity for typo tolerance. p_user_id scopes to rep''s clients (NULL for admin).';

COMMENT ON FUNCTION chatbot.fuzzy_search_medications(text, integer) IS
'Fuzzy search for medications by product name or SKU.';

COMMENT ON FUNCTION chatbot.get_doctor_inventory(character varying, character varying, boolean) IS
'Get doctor''s current cabinet inventory. Shows all SKUs with quantities. Access-controlled by user_id.';

COMMENT ON FUNCTION chatbot.get_doctor_movements(character varying, character varying, boolean, text, integer) IS
'Get doctor''s recent inventory movements. p_source: botiquin/odv/ambos. p_limit controls row count.';

COMMENT ON FUNCTION chatbot.classification_by_client(character varying) IS
'Get M1/M2/M3 classification for all products of a specific client. Chatbot-friendly wrapper around clasificacion_base.';

COMMENT ON FUNCTION chatbot.get_complete_brand_performance() IS
'Brand performance summary for chatbot. All brands with revenue and piece counts.';

COMMENT ON FUNCTION chatbot.get_complete_sales_ranking(integer) IS
'Top doctors by total sales. p_limit_count defaults to 20.';

COMMENT ON FUNCTION chatbot.get_medication_prices(text, character varying) IS
'Medication price lookup. Optional search term and brand filter.';

COMMENT ON FUNCTION chatbot.get_refill_recommendations(character varying) IS
'AI-powered restock recommendations for a doctor based on sell-through rates and current stock.';

COMMENT ON FUNCTION chatbot.get_user_collections(character varying, character varying, integer, boolean) IS
'Collection history for a user''s clients. Optional client filter and limit.';

COMMENT ON FUNCTION chatbot.get_user_odv_sales(character varying, boolean, character varying, integer) IS
'ODV sales for a user''s clients. Optional SKU filter and limit.';

COMMENT ON FUNCTION chatbot.get_visit_status(character varying, boolean) IS
'Visit completion status for a user or all users (admin). Shows pending/completed visits.';

COMMENT ON FUNCTION chatbot.check_and_increment_usage(character varying, character varying) IS
'Rate limiting for chatbot. Checks remaining queries and increments counter. Returns remaining count.';

COMMENT ON FUNCTION chatbot.get_remaining_queries(character varying, character varying) IS
'Get remaining chatbot query count for a user/role without incrementing.';

COMMENT ON FUNCTION chatbot.get_data_sheets_by_skus(text[]) IS
'Get product data sheets (fichas tecnicas) for given SKUs. Returns full text content for RAG.';

-- ═══════════════════════════════════════════════════════════════════════════
-- MOBILE APP RPCs (public schema)
-- ═══════════════════════════════════════════════════════════════════════════

COMMENT ON FUNCTION public.rpc_create_visit(character varying, character varying) IS
'Create a new visit for a client. Auto-creates sequential visit_tasks based on visit type. Returns visit_id. Overload: (client_id, type).';

COMMENT ON FUNCTION public.rpc_create_visit(character varying, integer, character varying) IS
'Create a new visit for a client with cycle_id. Auto-creates sequential visit_tasks. Returns visit_id. Overload: (client_id, cycle_id, type).';

COMMENT ON FUNCTION public.rpc_get_cutoff_items(uuid) IS
'Get detailed items for a visit''s cutoff task. Returns saga items with SKU details and pricing.';

COMMENT ON FUNCTION public.rpc_owner_delete_visit(uuid, text, text) IS
'OWNER-only: delete a visit and all related data. Compensates movements if needed. Audit logged.';

COMMENT ON FUNCTION public.is_admin() IS
'Check if current user has ADMIN or OWNER role. Used in RLS policies.';

COMMENT ON FUNCTION public.current_user_id() IS
'Get current user''s internal user_id from auth.uid(). Used in RLS and RPCs.';

COMMENT ON FUNCTION public.can_access_client(text) IS
'Check if current user can access a specific client (owns the client''s zone or is admin).';

COMMENT ON FUNCTION public.can_access_visit(uuid) IS
'Check if current user can access a specific visit (owns the visit or is admin).';

COMMENT ON FUNCTION public.create_notification(character varying, text, text, text, jsonb, text, integer) IS
'Create a push notification for a user. Handles dedup via dedup_key and optional expiry.';

NOTIFY pgrst, 'reload schema';
