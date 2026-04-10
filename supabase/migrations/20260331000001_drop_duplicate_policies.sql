-- ============================================================================
-- Migration 0B: Drop duplicate Spanish RLS policies
-- Fix: Remove ~25 Spanish duplicate policies (identical logic, different name)
-- Keep only English-named policies for consistency
-- ============================================================================

-- visit_reports (drop 6 Spanish duplicates)
DROP POLICY IF EXISTS "usuarios_visita_informes_select" ON visit_reports;
DROP POLICY IF EXISTS "usuarios_visita_informes_insert" ON visit_reports;
DROP POLICY IF EXISTS "usuarios_visita_informes_update" ON visit_reports;
DROP POLICY IF EXISTS "admin_visita_informes_select" ON visit_reports;
DROP POLICY IF EXISTS "admin_visita_informes_insert" ON visit_reports;
DROP POLICY IF EXISTS "admin_visita_informes_update" ON visit_reports;

-- collections (drop 3 Spanish duplicates)
DROP POLICY IF EXISTS "usuarios_recolecciones_select" ON collections;
DROP POLICY IF EXISTS "usuarios_recolecciones_insert" ON collections;
DROP POLICY IF EXISTS "usuarios_recolecciones_update" ON collections;

-- collection_items (drop 2 Spanish duplicates)
DROP POLICY IF EXISTS "usuarios_recolecciones_items_select" ON collection_items;
DROP POLICY IF EXISTS "usuarios_recolecciones_items_insert" ON collection_items;

-- collection_signatures (drop 3 Spanish duplicates)
DROP POLICY IF EXISTS "usuarios_recolecciones_firmas_select" ON collection_signatures;
DROP POLICY IF EXISTS "usuarios_recolecciones_firmas_insert" ON collection_signatures;
DROP POLICY IF EXISTS "usuarios_recolecciones_firmas_update" ON collection_signatures;

-- collection_evidence (drop 2 Spanish duplicates)
DROP POLICY IF EXISTS "usuarios_recolecciones_evidencias_select" ON collection_evidence;
DROP POLICY IF EXISTS "usuarios_recolecciones_evidencias_insert" ON collection_evidence;

-- admin_notifications (drop 3 Spanish duplicates)
DROP POLICY IF EXISTS "Admins ven notificaciones" ON admin_notifications;
DROP POLICY IF EXISTS "Admins pueden insertar notificaciones" ON admin_notifications;
DROP POLICY IF EXISTS "Admins pueden actualizar notificaciones" ON admin_notifications;

-- client_status_log (drop 2 Spanish duplicates)
DROP POLICY IF EXISTS "Admins pueden ver logs de estado" ON client_status_log;
DROP POLICY IF EXISTS "Admins pueden insertar logs" ON client_status_log;

-- Reference tables (drop 1 each)
DROP POLICY IF EXISTS "zonas_select" ON zones;
DROP POLICY IF EXISTS "padecimientos_select" ON conditions;
DROP POLICY IF EXISTS "medicamentos_select" ON medications;
DROP POLICY IF EXISTS "medicamento_padecimientos_select" ON medication_conditions;

-- visits/visit_tasks (drop 1 each)
DROP POLICY IF EXISTS "usuarios_visitas_insert" ON visits;
DROP POLICY IF EXISTS "usuarios_visit_tasks_insert" ON visit_tasks;
