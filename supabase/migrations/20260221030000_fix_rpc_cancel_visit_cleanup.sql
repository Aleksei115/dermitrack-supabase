-- Fix rpc_cancel_visit to:
-- 1. Allow cancelling COMPLETADO visits (not just BORRADOR/PENDIENTE)
-- 2. Delete all associated data (movements, sagas, tasks, recolecciones, etc.)
-- 3. Recalculate inventario_botiquin for affected (id_cliente, sku) pairs
--
-- The visit record itself is preserved with estado = CANCELADO.

CREATE OR REPLACE FUNCTION public.rpc_cancel_visit(
  p_visit_id uuid,
  p_reason text DEFAULT NULL
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_visita record;
  v_saga_ids uuid[];
  v_affected_pairs record;
  v_new_qty integer;
BEGIN
  -- Verify visit exists and is not already cancelled
  SELECT visit_id, estado, id_cliente
  INTO v_visita
  FROM public.visitas
  WHERE visit_id = p_visit_id
  AND estado != 'CANCELADO';

  IF v_visita IS NULL THEN
    RETURN false;
  END IF;

  -- Collect all saga IDs for this visit
  SELECT array_agg(id)
  INTO v_saga_ids
  FROM public.saga_transactions
  WHERE visit_id = p_visit_id;

  -- If there are sagas, clean up all dependent data
  IF v_saga_ids IS NOT NULL AND array_length(v_saga_ids, 1) > 0 THEN

    -- Step 1: Save affected (id_cliente, sku) pairs before deleting movements
    CREATE TEMP TABLE _affected_inventory ON COMMIT DROP AS
      SELECT DISTINCT id_cliente, sku
      FROM public.movimientos_inventario
      WHERE id_saga_transaction = ANY(v_saga_ids);

    -- Step 2: Delete movimientos_inventario
    DELETE FROM public.movimientos_inventario
    WHERE id_saga_transaction = ANY(v_saga_ids);

    -- Step 3: Recalculate inventario_botiquin for each affected pair
    FOR v_affected_pairs IN SELECT id_cliente, sku FROM _affected_inventory LOOP
      SELECT COALESCE(SUM(
        CASE
          WHEN tipo = 'CREACION' THEN cantidad
          WHEN tipo IN ('VENTA', 'RECOLECCION') THEN -cantidad
          ELSE 0
        END
      ), 0)
      INTO v_new_qty
      FROM public.movimientos_inventario
      WHERE id_cliente = v_affected_pairs.id_cliente
      AND sku = v_affected_pairs.sku;

      IF v_new_qty > 0 THEN
        INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
        VALUES (v_affected_pairs.id_cliente, v_affected_pairs.sku, v_new_qty, now())
        ON CONFLICT (id_cliente, sku) DO UPDATE
        SET cantidad_disponible = v_new_qty,
            ultima_actualizacion = now();
      ELSE
        DELETE FROM public.inventario_botiquin
        WHERE id_cliente = v_affected_pairs.id_cliente
        AND sku = v_affected_pairs.sku;
      END IF;
    END LOOP;

    DROP TABLE IF EXISTS _affected_inventory;

    -- Step 4: Delete saga_adjustments (FK to saga_compensations + saga_transactions, no cascade)
    DELETE FROM public.saga_adjustments
    WHERE saga_transaction_id = ANY(v_saga_ids);

    -- Step 5: Delete saga_compensations (FK to saga_transactions, no cascade)
    DELETE FROM public.saga_compensations
    WHERE saga_transaction_id = ANY(v_saga_ids);

    -- Step 6: Delete event_outbox (FK to saga_transactions, no cascade)
    DELETE FROM public.event_outbox
    WHERE saga_transaction_id = ANY(v_saga_ids);

    -- Step 7: Delete saga_transactions (cascades → saga_zoho_links)
    DELETE FROM public.saga_transactions
    WHERE id = ANY(v_saga_ids);

  END IF;

  -- Step 8: Delete recolecciones (cascades → items, evidencias, firmas)
  DELETE FROM public.recolecciones
  WHERE visit_id = p_visit_id;

  -- Step 9: Delete visit_tasks
  DELETE FROM public.visit_tasks
  WHERE visit_id = p_visit_id;

  -- Step 10: Delete visita_informes
  DELETE FROM public.visita_informes
  WHERE visit_id = p_visit_id;

  -- Step 11: Update visita to CANCELADO
  UPDATE public.visitas
  SET
    estado = 'CANCELADO',
    updated_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'cancel_reason', p_reason,
      'cancelled_at', now()::text,
      'previous_estado', v_visita.estado::text
    )
  WHERE visit_id = p_visit_id;

  RETURN true;
END;
$function$;

-- Ensure permissions
GRANT EXECUTE ON FUNCTION public.rpc_cancel_visit(uuid, text) TO authenticated;

NOTIFY pgrst, 'reload schema';
