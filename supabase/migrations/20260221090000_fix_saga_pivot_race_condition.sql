-- Fix race condition between rpc_confirm_saga_pivot and
-- trigger_generate_movements_from_saga.
--
-- Problem: Both code paths generate movements for the same saga.
-- The trigger fires on UPDATE (estado → CONFIRMADO) in the same transaction,
-- generating movements BEFORE the RPC's FOR loop runs. The RPC then reads
-- stale/modified inventory and creates duplicate movements with wrong values.
--
-- Fix: Add the same guard the trigger uses — check if movements already exist
-- before the RPC's inline generation loop. If the trigger already handled it,
-- only link the zoho FK.

CREATE OR REPLACE FUNCTION public.rpc_confirm_saga_pivot(
  p_saga_id uuid,
  p_zoho_id text DEFAULT NULL,
  p_zoho_items jsonb DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_zoho_link_id integer;
  v_task_tipo text;
  v_zoho_link_tipo tipo_zoho_link;
  v_item record;
  v_cantidad_antes integer;
  v_cantidad_despues integer;
  v_tipo_movimiento tipo_movimiento_botiquin;
  v_already_confirmed boolean := false;
  v_precio_unitario numeric;
BEGIN
  -- 1. Get and validate saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  IF v_saga.estado = 'CONFIRMADO' THEN
    v_already_confirmed := true;
    IF p_zoho_id IS NULL THEN
      SELECT id INTO v_zoho_link_id
      FROM public.saga_zoho_links
      WHERE id_saga_transaction = p_saga_id
      LIMIT 1;

      RETURN jsonb_build_object(
        'success', true,
        'already_confirmed', true,
        'saga_id', p_saga_id,
        'zoho_link_id', v_zoho_link_id
      );
    END IF;
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determine tipo de zoho_link, task_tipo and tipo_movimiento
  CASE v_saga.tipo::text
    WHEN 'LEVANTAMIENTO_INICIAL' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';
    WHEN 'LEV_POST_CORTE' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';
    WHEN 'VENTA' THEN
      v_zoho_link_tipo := 'VENTA';
      v_task_tipo := 'VENTA_ODV';
      v_tipo_movimiento := 'VENTA';
    WHEN 'RECOLECCION' THEN
      v_zoho_link_tipo := 'DEVOLUCION';
      v_task_tipo := 'RECOLECCION';
      v_tipo_movimiento := 'RECOLECCION';
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado: %', v_saga.tipo;
  END CASE;

  -- 3. Change saga state to CONFIRMADO (ONLY on first confirmation)
  -- NOTE: This fires trigger_generate_movements_from_saga in the same transaction.
  -- The trigger may generate movements BEFORE control returns here.
  IF NOT v_already_confirmed THEN
    UPDATE public.saga_transactions
    SET
      estado = 'CONFIRMADO'::estado_saga_transaction,
      updated_at = now()
    WHERE id = p_saga_id;
  END IF;

  -- 4. Create/update saga_zoho_link
  IF p_zoho_id IS NOT NULL THEN
    INSERT INTO public.saga_zoho_links (
      id_saga_transaction,
      zoho_id,
      tipo,
      items,
      zoho_sync_status,
      created_at,
      updated_at
    )
    VALUES (
      p_saga_id,
      p_zoho_id,
      v_zoho_link_tipo,
      COALESCE(p_zoho_items, v_saga.items),
      'synced',
      now(),
      now()
    )
    ON CONFLICT (id_saga_transaction, zoho_id)
    DO UPDATE SET
      items = COALESCE(EXCLUDED.items, saga_zoho_links.items),
      zoho_sync_status = 'synced',
      updated_at = now()
    RETURNING id INTO v_zoho_link_id;
  END IF;

  -- 5. Create movements OR assign FK (anti-duplication)
  IF v_already_confirmed THEN
    IF v_zoho_link_id IS NOT NULL THEN
      IF p_zoho_items IS NOT NULL THEN
        UPDATE movimientos_inventario
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND tipo != 'PERMANENCIA'
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_zoho_items) elem
            WHERE elem->>'sku' = movimientos_inventario.sku
          );
      ELSE
        UPDATE movimientos_inventario
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND tipo != 'PERMANENCIA';
      END IF;
    END IF;

    RETURN jsonb_build_object(
      'success', true,
      'already_confirmed', true,
      'saga_id', p_saga_id,
      'zoho_link_id', v_zoho_link_id
    );
  ELSE
    -- GUARD: Check if trigger already generated movements for this saga.
    -- The AFTER UPDATE trigger on saga_transactions fires in the same transaction
    -- and may have already created movements + updated inventory.
    -- Without this guard, the FOR loop below would read stale inventory and
    -- create duplicate movements with wrong cantidad_antes/cantidad_despues.
    IF EXISTS (SELECT 1 FROM movimientos_inventario WHERE id_saga_transaction = p_saga_id) THEN
      -- Movements already generated by trigger — only link zoho FK
      IF v_zoho_link_id IS NOT NULL THEN
        UPDATE movimientos_inventario
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND tipo != 'PERMANENCIA';
      END IF;
    ELSE
      -- Trigger did not generate movements — do it inline (fallback)
      FOR v_item IN
        SELECT
          (item->>'sku')::varchar as sku,
          (item->>'cantidad')::int as cantidad
        FROM jsonb_array_elements(v_saga.items) as item
      LOOP
        SELECT COALESCE(cantidad_disponible, 0)
        INTO v_cantidad_antes
        FROM public.inventario_botiquin
        WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;

        IF v_cantidad_antes IS NULL THEN
          v_cantidad_antes := 0;
        END IF;

        IF v_tipo_movimiento = 'CREACION' THEN
          v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
        ELSE
          v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
        END IF;

        -- Freeze current catalog price for this SKU
        SELECT precio INTO v_precio_unitario
        FROM public.medicamentos
        WHERE sku = v_item.sku;

        INSERT INTO public.movimientos_inventario (
          id_saga_transaction,
          id_saga_zoho_link,
          id_cliente,
          sku,
          tipo,
          cantidad,
          cantidad_antes,
          cantidad_despues,
          fecha_movimiento,
          precio_unitario
        )
        VALUES (
          p_saga_id,
          v_zoho_link_id,
          v_saga.id_cliente,
          v_item.sku,
          v_tipo_movimiento,
          v_item.cantidad,
          v_cantidad_antes,
          v_cantidad_despues,
          now(),
          v_precio_unitario
        );

        IF v_cantidad_despues > 0 THEN
          INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion, precio_unitario)
          VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues, now(), v_precio_unitario)
          ON CONFLICT (id_cliente, sku)
          DO UPDATE SET
            cantidad_disponible = v_cantidad_despues,
            ultima_actualizacion = now(),
            precio_unitario = COALESCE(v_precio_unitario, inventario_botiquin.precio_unitario);
        ELSE
          DELETE FROM public.inventario_botiquin
          WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
        END IF;

        IF v_saga.tipo::text = 'VENTA' THEN
          DELETE FROM public.botiquin_clientes_sku_disponibles
          WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
        END IF;
      END LOOP;
    END IF;
  END IF;

  -- 6. Update visit_tasks (ONLY on first confirmation)
  IF NOT v_already_confirmed THEN
    IF v_zoho_link_id IS NOT NULL THEN
      UPDATE public.visit_tasks
      SET
        estado = 'COMPLETADO',
        completed_at = COALESCE(completed_at, now()),
        reference_table = 'saga_zoho_links',
        reference_id = v_zoho_link_id::text,
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_tipo = v_task_tipo::visit_task_tipo;
    ELSE
      UPDATE public.visit_tasks
      SET
        estado = 'COMPLETADO',
        completed_at = COALESCE(completed_at, now()),
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_tipo = v_task_tipo::visit_task_tipo;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'zoho_link_id', v_zoho_link_id,
    'tipo', v_saga.tipo,
    'items_count', jsonb_array_length(v_saga.items)
  );
END;
$function$;

NOTIFY pgrst, 'reload schema';
