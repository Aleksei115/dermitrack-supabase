-- Add ODV info (odv_id, odv_total_piezas) to v_visit_tasks_operativo view
--
-- The VisitDetailScreen task rows show only a generic label — no ODV ID or
-- piece count. This replaces the view with two new columns via CASE +
-- correlated subqueries that only fire for ODV-type tasks (VENTA_ODV,
-- ODV_BOTIQUIN), so non-ODV tasks have zero overhead.

CREATE OR REPLACE VIEW public.v_visit_tasks_operativo AS
SELECT
    (vt.task_id)::text AS task_id,
    vt.visit_id,
    vt.task_tipo,
    vt.estado,
    vt.required,
    vt.created_at,
    vt.started_at,
    vt.completed_at,
    vt.due_at,
    vt.last_activity_at,
    vt.reference_table,
    vt.reference_id,
    vt.metadata,
    (vt.transaction_type)::text AS transaction_type,
    vt.step_order,
    'NOT_NEEDED'::text AS compensation_status,
    '{}'::jsonb AS input_payload,
    '{}'::jsonb AS output_result,
    NULL::jsonb AS compensation_payload,
    (gen_random_uuid())::text AS idempotency_key,
    0 AS retry_count,
    3 AS max_retries,
    NULL::text AS last_error,
    NULL::timestamp with time zone AS compensation_executed_at,
    CASE
        WHEN (vt.estado = 'COMPLETADO'::public.visit_task_estado) THEN 'COMPLETADO'::public.visit_task_estado
        WHEN (vt.estado = 'OMITIDO'::public.visit_task_estado) THEN vt.estado
        WHEN (vt.estado = 'OMITIDA'::public.visit_task_estado) THEN vt.estado
        WHEN (vt.estado = 'ERROR'::public.visit_task_estado) THEN vt.estado
        WHEN ((vt.due_at IS NOT NULL) AND (vt.due_at < now()) AND (vt.estado <> ALL (ARRAY['COMPLETADO'::public.visit_task_estado, 'OMITIDO'::public.visit_task_estado, 'OMITIDA'::public.visit_task_estado]))) THEN 'RETRASADO'::public.visit_task_estado
        ELSE vt.estado
    END AS estado_operativo,
    -- ODV ID(s) for VENTA_ODV / ODV_BOTIQUIN tasks
    CASE
        WHEN vt.task_tipo = 'VENTA_ODV' THEN (
            SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at)
            FROM public.saga_zoho_links szl
            JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
            WHERE st.visit_id = vt.visit_id
              AND szl.tipo::text = 'VENTA'
        )
        WHEN vt.task_tipo = 'ODV_BOTIQUIN' THEN (
            SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at)
            FROM public.saga_zoho_links szl
            JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
            WHERE st.visit_id = vt.visit_id
              AND szl.tipo::text = 'BOTIQUIN'
        )
        ELSE NULL
    END AS odv_id,
    -- Total pieces across all ODVs for this task type
    CASE
        WHEN vt.task_tipo = 'VENTA_ODV' THEN (
            SELECT COALESCE(SUM(
                COALESCE((item->>'cantidad')::int, 0)
            ), 0)::int
            FROM public.saga_zoho_links szl
            JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction,
            jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
            WHERE st.visit_id = vt.visit_id
              AND szl.tipo::text = 'VENTA'
        )
        WHEN vt.task_tipo = 'ODV_BOTIQUIN' THEN (
            SELECT COALESCE(SUM(
                COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
            ), 0)::int
            FROM public.saga_zoho_links szl
            JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction,
            jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
            WHERE st.visit_id = vt.visit_id
              AND szl.tipo::text = 'BOTIQUIN'
        )
        ELSE NULL
    END AS odv_total_piezas
FROM public.visit_tasks vt;

NOTIFY pgrst, 'reload schema';
