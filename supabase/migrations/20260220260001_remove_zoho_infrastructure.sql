-- Phase 1: Remove Zoho infrastructure
-- Drops dead Zoho tables, functions, and queue references.
-- KEEPS: saga_zoho_links, ventas_odv, botiquin_odv, import-odv edge function.

BEGIN;

-- 1. Drop zoho_tokens table (cascade drops FK, trigger, policies)
DROP TABLE IF EXISTS zoho_tokens CASCADE;

-- 2. Drop zoho_health_status table (cascade drops policies)
DROP TABLE IF EXISTS zoho_health_status CASCADE;

-- 3. Drop process_zoho_retry_queue (dead — queue processor for removed Zoho sync)
DROP FUNCTION IF EXISTS process_zoho_retry_queue();

-- 4. Update rpc_admin_retry_pivot: remove zoho_health_status check and pgmq queue send
CREATE OR REPLACE FUNCTION public.rpc_admin_retry_pivot(p_saga_transaction_id uuid, p_admin_id character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_compensation_id UUID;
BEGIN
    -- Verify permissions
    IF NOT EXISTS (
        SELECT 1 FROM usuarios
        WHERE id_usuario = p_admin_id AND rol IN ('ADMINISTRADOR', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden reintentar PIVOT';
    END IF;

    -- Get saga
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    IF v_saga.estado NOT IN ('FALLIDA', 'ERROR') THEN
        RAISE EXCEPTION 'Solo se pueden reintentar transacciones en estado FALLIDA o ERROR';
    END IF;

    -- Create compensation record for RETRY
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        p_saga_transaction_id,
        p_admin_id,
        'Reintento manual de PIVOT',
        'RETRY',
        jsonb_build_object('estado', v_saga.estado::text),
        jsonb_build_object('estado', 'PENDIENTE_SYNC'),
        'PENDING'
    ) RETURNING id INTO v_compensation_id;

    -- Update state to PENDIENTE_SYNC for retry
    UPDATE saga_transactions
    SET
        estado = 'PENDIENTE_SYNC',
        updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    RETURN jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', 'Reintento de PIVOT registrado'
    );
END;
$function$;

-- 5. Update rpc_admin_compensate_task: remove pgmq queue send for Zoho sync
CREATE OR REPLACE FUNCTION public.rpc_admin_compensate_task(p_saga_transaction_id uuid, p_admin_id character varying, p_reason text, p_new_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_compensation_id UUID;
    v_old_items JSONB;
    v_result JSONB;
BEGIN
    -- Verify admin permissions
    IF NOT EXISTS (
        SELECT 1 FROM usuarios
        WHERE id_usuario = p_admin_id AND rol IN ('ADMINISTRADOR', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden compensar tareas';
    END IF;

    -- Get saga transaction
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    v_old_items := v_saga.items;

    -- Create compensation record
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        p_saga_transaction_id,
        p_admin_id,
        p_reason,
        'ADJUSTMENT',
        jsonb_build_object('items', v_old_items, 'estado', v_saga.estado),
        jsonb_build_object('items', p_new_items),
        'PENDING'
    ) RETURNING id INTO v_compensation_id;

    -- Register individual adjustments
    INSERT INTO saga_adjustments (
        compensation_id,
        saga_transaction_id,
        item_sku,
        old_quantity,
        new_quantity,
        adjustment_reason
    )
    SELECT
        v_compensation_id,
        p_saga_transaction_id,
        COALESCE(old_item->>'sku', new_item->>'sku'),
        COALESCE((old_item->>'cantidad')::INTEGER, 0),
        COALESCE((new_item->>'cantidad')::INTEGER, 0),
        p_reason
    FROM
        jsonb_array_elements(v_old_items) WITH ORDINALITY AS old_items(old_item, ord)
    FULL OUTER JOIN
        jsonb_array_elements(p_new_items) WITH ORDINALITY AS new_items(new_item, ord2)
    ON old_item->>'sku' = new_item->>'sku'
    WHERE COALESCE((old_item->>'cantidad')::INTEGER, 0) != COALESCE((new_item->>'cantidad')::INTEGER, 0);

    -- Update saga_transactions with new items
    UPDATE saga_transactions
    SET
        items = p_new_items,
        updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    v_result := jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', 'Compensación registrada'
    );

    RETURN v_result;
END;
$function$;

-- 6. Notify PostgREST to reload schema
NOTIFY pgrst, 'reload schema';

COMMIT;
