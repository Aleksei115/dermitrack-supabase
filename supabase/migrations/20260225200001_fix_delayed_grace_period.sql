-- Fix: DELAYED status should only trigger 1 day AFTER due_at, not immediately
-- The user expects a grace period: task is only "delayed" when more than 24 hours
-- have passed since the scheduled date (due_at).

CREATE OR REPLACE VIEW public.v_visit_tasks_operational AS
SELECT
    task_id::text AS task_id,
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
    -- operational_status: DELAYED only after 1 day grace period past due_at
    CASE
        WHEN status = 'COMPLETED' THEN 'COMPLETED'::visit_task_status
        WHEN status = 'SKIPPED_M' THEN status
        WHEN status = 'SKIPPED' THEN status
        WHEN status = 'ERROR' THEN status
        WHEN due_at IS NOT NULL
             AND (due_at + interval '1 day') < now()
             AND status NOT IN ('COMPLETED', 'SKIPPED_M', 'SKIPPED')
        THEN 'DELAYED'::visit_task_status
        ELSE status
    END AS operational_status,
    -- odv_id aggregation
    CASE
        WHEN task_type = 'SALE_ODV' THEN (
            SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at)
            FROM saga_zoho_links szl
            JOIN saga_transactions st ON st.id = szl.id_saga_transaction
            WHERE st.visit_id = vt.visit_id AND szl.type::text = 'SALE'
        )
        WHEN task_type = 'ODV_CABINET' THEN (
            SELECT string_agg(szl.zoho_id, ', ' ORDER BY szl.created_at)
            FROM saga_zoho_links szl
            JOIN saga_transactions st ON st.id = szl.id_saga_transaction
            WHERE st.visit_id = vt.visit_id AND szl.type::text = 'CABINET'
        )
        ELSE NULL
    END AS odv_id,
    -- odv_total_pieces with English + Spanish fallback
    CASE
        WHEN task_type = 'SALE_ODV' THEN (
            SELECT COALESCE(SUM(COALESCE(
                (item->>'quantity')::int,
                (item->>'cantidad')::int,
                0
            )), 0)::int
            FROM saga_zoho_links szl
            JOIN saga_transactions st ON st.id = szl.id_saga_transaction,
            LATERAL jsonb_array_elements(COALESCE(szl.items, st.items)) item
            WHERE st.visit_id = vt.visit_id AND szl.type::text = 'SALE'
        )
        WHEN task_type = 'ODV_CABINET' THEN (
            SELECT COALESCE(SUM(COALESCE(
                (item->>'quantity')::int,
                (item->>'input_quantity')::int,
                (item->>'cantidad_entrada')::int,
                (item->>'cantidad')::int,
                0
            )), 0)::int
            FROM saga_zoho_links szl
            JOIN saga_transactions st ON st.id = szl.id_saga_transaction,
            LATERAL jsonb_array_elements(COALESCE(szl.items, st.items)) item
            WHERE st.visit_id = vt.visit_id AND szl.type::text = 'CABINET'
        )
        ELSE NULL
    END AS odv_total_pieces
FROM visit_tasks vt;
