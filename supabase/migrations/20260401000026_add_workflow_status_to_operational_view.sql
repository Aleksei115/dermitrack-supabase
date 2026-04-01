-- ============================================================================
-- Migration 26: Add workflow_status to v_visits_operational
--
-- The view was missing workflow_status, so compensated visits appeared as
-- COMPLETED in the doctor detail / history screens. This migration:
-- 1. Drops and recreates view (column order changed, so CREATE OR REPLACE
--    won't work)
-- 2. Adds workflow_status, started_at, last_activity_at, metadata columns
-- 3. Updates operational_status to return 'COMPENSATED' when
--    workflow_status = 'COMPENSATED'
-- ============================================================================

DROP VIEW IF EXISTS public.v_visits_operational;

CREATE VIEW public.v_visits_operational AS
SELECT v.visit_id,
    v.client_id,
    v.user_id,
    u.name AS user_name,
    v.type,
    v.status,
    v.workflow_status,
    v.cycle_id,
    v.created_at,
    v.started_at,
    v.due_at,
    v.completed_at,
    v.last_activity_at,
    v.metadata,
    c.client_name,
    c.zone_id,
    c.tier,
    ( SELECT count(*) AS count
           FROM public.visit_tasks vt
          WHERE (vt.visit_id = v.visit_id)) AS total_tasks,
    ( SELECT count(*) AS count
           FROM public.visit_tasks vt
          WHERE ((vt.visit_id = v.visit_id) AND (vt.status = 'COMPLETED'::public.visit_task_status))) AS completed_tasks,
        CASE
            WHEN (v.workflow_status = 'COMPENSATED') THEN 'COMPENSATED'::text
            WHEN (v.status = 'CANCELLED'::public.visit_status) THEN 'CANCELLED'::text
            WHEN (v.status = 'COMPLETED'::public.visit_status) THEN 'COMPLETED'::text
            WHEN ((v.due_at < now()) AND (v.status <> 'COMPLETED'::public.visit_status)) THEN 'DELAYED'::text
            WHEN (EXISTS ( SELECT 1
               FROM public.visit_tasks vt
              WHERE ((vt.visit_id = v.visit_id) AND (vt.status = ANY (ARRAY['IN_PROGRESS'::public.visit_task_status, 'PENDING_SYNC'::public.visit_task_status, 'COMPLETED'::public.visit_task_status]))))) THEN 'IN_PROGRESS'::text
            ELSE 'PENDING'::text
        END AS operational_status
   FROM ((public.visits v
     JOIN public.clients c ON (((v.client_id)::text = (c.client_id)::text)))
     JOIN public.users u ON (((v.user_id)::text = (u.user_id)::text)));

GRANT SELECT ON TABLE public.v_visits_operational TO authenticated;

NOTIFY pgrst, 'reload schema';
