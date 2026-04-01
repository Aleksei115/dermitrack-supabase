-- ============================================================================
-- Migration 8: New direct RPCs without saga pattern
-- Fase 1: Coexist with existing saga RPCs during transition
-- ============================================================================

-- ── Prerequisite: Add VALIDATION to transaction_type enum ───────────────────

ALTER TYPE transaction_type ADD VALUE IF NOT EXISTS 'VALIDATION';

-- ── Prerequisite: Add transit_started_at to collections ─────────────────────

ALTER TABLE collections ADD COLUMN IF NOT EXISTS transit_started_at timestamptz;

COMMENT ON COLUMN collections.transit_started_at IS
'Timestamp when rep started transit to CEDIS. Set by rpc_start_collection_transit(). NULL until IN_TRANSIT.';

-- ═══════════════════════════════════════════════════════════════════════════
-- HELPER: FEFO lot consumption
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION _consume_lots_fefo(
  p_client_id character varying,
  p_sku character varying,
  p_quantity integer,
  p_mode text,              -- 'sale' or 'collection'
  p_movement_id bigint DEFAULT NULL
)
RETURNS integer
LANGUAGE plpgsql
SET search_path = 'public'
AS $$
DECLARE
  v_remaining integer := p_quantity;
  v_lot RECORD;
  v_consume integer;
  v_new_status lot_status;
BEGIN
  -- Determine target status based on mode
  v_new_status := CASE p_mode
    WHEN 'sale' THEN 'consumed'::lot_status
    WHEN 'collection' THEN 'collected'::lot_status
    ELSE 'consumed'::lot_status
  END;

  -- Consume lots FEFO: earliest expiry first, NULL expiry last
  FOR v_lot IN
    SELECT id, remaining_quantity
    FROM cabinet_inventory_lots
    WHERE client_id = p_client_id
      AND sku = p_sku
      AND status = 'active'
      AND remaining_quantity > 0
    ORDER BY expiry_date ASC NULLS LAST, placement_date ASC, id ASC
    FOR UPDATE  -- Lock rows to prevent concurrent consumption
  LOOP
    EXIT WHEN v_remaining <= 0;

    v_consume := LEAST(v_lot.remaining_quantity, v_remaining);

    IF v_consume = v_lot.remaining_quantity THEN
      -- Fully consume this lot
      UPDATE cabinet_inventory_lots
      SET remaining_quantity = 0,
          status = v_new_status,
          consumed_by_movement_id = p_movement_id
      WHERE id = v_lot.id;
    ELSE
      -- Partially consume
      UPDATE cabinet_inventory_lots
      SET remaining_quantity = remaining_quantity - v_consume
      WHERE id = v_lot.id;
    END IF;

    v_remaining := v_remaining - v_consume;
  END LOOP;

  -- Return how many were actually consumed (may be less if not enough stock)
  RETURN p_quantity - v_remaining;
END;
$$;

COMMENT ON FUNCTION _consume_lots_fefo(character varying, character varying, integer, text, bigint) IS
'Internal FEFO (First Expired First Out) helper. Consumes lots from cabinet_inventory_lots in expiry order. Mode: sale→consumed, collection→collected. Returns actual quantity consumed.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_register_placement: Immediate PLACEMENT movements + lots
-- Replaces: rpc_submit_initial_placement + rpc_confirm_saga_pivot
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_register_placement(
  p_visit_id uuid,
  p_items jsonb  -- [{sku, quantity}]
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_item RECORD;
  v_qty integer;
  v_qty_before integer;
  v_qty_after integer;
  v_unit_price numeric;
  v_shelf_life integer;
  v_movement_id bigint;
  v_task_id uuid;
  v_items_processed integer := 0;
  v_total_qty integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.type, v.status
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Get task_id for the INITIAL_PLACEMENT or POST_CUTOFF_PLACEMENT task
  -- (This RPC is only for INITIAL_PLACEMENT visits)
  SELECT vt.task_id INTO v_task_id
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = 'INITIAL_PLACEMENT';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No INITIAL_PLACEMENT task found for visit %', p_visit_id;
  END IF;

  -- Idempotency: if task already completed, return early
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'INITIAL_PLACEMENT'
      AND status IN ('COMPLETED', 'SKIPPED', 'CANCELLED')
  ) THEN
    RETURN jsonb_build_object('success', true, 'already_completed', true);
  END IF;

  -- Validate items
  IF p_items IS NULL OR jsonb_array_length(p_items) = 0 THEN
    RAISE EXCEPTION 'Items array is empty';
  END IF;

  -- Process each item: create immediate PLACEMENT movements
  FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
  LOOP
    v_qty := COALESCE(
      (v_item.value->>'quantity')::integer,
      (v_item.value->>'cantidad')::integer,
      0
    );

    IF v_qty <= 0 THEN CONTINUE; END IF;

    -- Get current stock
    SELECT COALESCE(ci.available_quantity, 0) INTO v_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_visit.client_id
      AND ci.sku = v_item.value->>'sku';

    IF NOT FOUND THEN v_qty_before := 0; END IF;

    v_qty_after := v_qty_before + v_qty;

    -- Get unit price and shelf life from medications
    SELECT m.price, m.shelf_life_months
    INTO v_unit_price, v_shelf_life
    FROM medications m WHERE m.sku = v_item.value->>'sku';

    -- Create PLACEMENT movement (validated=false, needs ODV_CABINET confirmation)
    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_visit.client_id, v_item.value->>'sku', v_qty, v_qty_before, v_qty_after,
      now(), 'PLACEMENT', v_unit_price, v_task_id, p_visit_id, false
    ) RETURNING id INTO v_movement_id;

    -- Create lot with expiry tracking
    INSERT INTO cabinet_inventory_lots (
      client_id, sku, movement_id, visit_id, quantity, remaining_quantity,
      placement_date, expiry_date, status
    ) VALUES (
      v_visit.client_id, v_item.value->>'sku', v_movement_id, p_visit_id,
      v_qty, v_qty,
      CURRENT_DATE,
      CASE WHEN v_shelf_life IS NOT NULL
        THEN (CURRENT_DATE + (v_shelf_life || ' months')::interval)::date
        ELSE NULL
      END,
      'active'
    );

    -- cabinet_inventory is updated by trg_sync_inventory trigger

    v_items_processed := v_items_processed + 1;
    v_total_qty := v_total_qty + v_qty;
  END LOOP;

  -- Mark INITIAL_PLACEMENT task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'items_count', v_items_processed,
        'total_quantity', v_total_qty,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = 'INITIAL_PLACEMENT';

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'items_processed', v_items_processed,
    'total_quantity', v_total_qty
  );
END;
$$;

COMMENT ON FUNCTION rpc_register_placement(uuid, jsonb) IS
'Register initial product placement. Creates PLACEMENT movements immediately (validated=false) + cabinet lots with expiry. Movements are validated by rpc_link_odv(type=CABINET) later. Replaces rpc_submit_initial_placement + saga pivot.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_register_cutoff: Immediate SALE movements + FEFO + collection PENDING
-- Replaces: rpc_submit_cutoff + rpc_confirm_saga_pivot (for SALE)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_register_cutoff(
  p_visit_id uuid,
  p_items jsonb  -- [{sku, sold, collected}]
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_item RECORD;
  v_sold integer;
  v_collected integer;
  v_qty_before integer;
  v_qty_after integer;
  v_unit_price numeric;
  v_task_id uuid;
  v_sale_task_id uuid;
  v_collection_id uuid;
  v_movement_id bigint;
  v_total_sold integer := 0;
  v_total_collected integer := 0;
  v_items_processed integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.type
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Get CUTOFF task_id
  SELECT vt.task_id INTO v_task_id
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id AND vt.task_type = 'CUTOFF';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No CUTOFF task found for visit %', p_visit_id;
  END IF;

  -- Idempotency
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'CUTOFF'
      AND status IN ('COMPLETED', 'SKIPPED', 'CANCELLED')
  ) THEN
    RETURN jsonb_build_object('success', true, 'already_completed', true);
  END IF;

  IF p_items IS NULL OR jsonb_array_length(p_items) = 0 THEN
    RAISE EXCEPTION 'Items array is empty';
  END IF;

  -- Get SALE_ODV task_id for linking movements to sale task
  SELECT vt.task_id INTO v_sale_task_id
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id AND vt.task_type = 'SALE_ODV';

  -- Process items
  FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
  LOOP
    v_sold := COALESCE(
      (v_item.value->>'sold')::integer,
      (v_item.value->>'vendido')::integer,
      0
    );
    v_collected := COALESCE(
      (v_item.value->>'collected')::integer,
      (v_item.value->>'recolectado')::integer,
      0
    );

    IF v_sold <= 0 AND v_collected <= 0 THEN CONTINUE; END IF;

    -- Get unit price
    SELECT m.price INTO v_unit_price
    FROM medications m WHERE m.sku = v_item.value->>'sku';

    -- ── SALE movements (immediate, validated=false) ──────────────────────
    IF v_sold > 0 THEN
      -- Get current stock
      SELECT COALESCE(ci.available_quantity, 0) INTO v_qty_before
      FROM cabinet_inventory ci
      WHERE ci.client_id = v_visit.client_id AND ci.sku = v_item.value->>'sku';
      IF NOT FOUND THEN v_qty_before := 0; END IF;

      v_qty_after := GREATEST(0, v_qty_before - v_sold);

      -- Create SALE movement
      INSERT INTO inventory_movements (
        client_id, sku, quantity, quantity_before, quantity_after,
        movement_date, type, unit_price, task_id, visit_id, validated
      ) VALUES (
        v_visit.client_id, v_item.value->>'sku', v_sold, v_qty_before, v_qty_after,
        now(), 'SALE', v_unit_price, v_task_id, p_visit_id, false
      ) RETURNING id INTO v_movement_id;

      -- FEFO lot consumption
      PERFORM _consume_lots_fefo(
        v_visit.client_id, v_item.value->>'sku', v_sold, 'sale', v_movement_id
      );

      -- cabinet_inventory updated by trigger
      -- Remove from available SKUs if stock is 0
      IF v_qty_after = 0 THEN
        DELETE FROM cabinet_client_available_skus
        WHERE client_id = v_visit.client_id AND sku = v_item.value->>'sku';
      END IF;

      v_total_sold := v_total_sold + v_sold;
    END IF;

    -- ── COLLECTION items (PENDING, no movement yet) ─────────────────────
    IF v_collected > 0 THEN
      -- Create or get collection record
      IF v_collection_id IS NULL THEN
        INSERT INTO collections (visit_id, client_id, user_id, status)
        VALUES (p_visit_id, v_visit.client_id, v_visit.user_id, 'PENDIENTE')
        ON CONFLICT DO NOTHING;

        SELECT c.collection_id INTO v_collection_id
        FROM collections c WHERE c.visit_id = p_visit_id;
      END IF;

      -- Upsert collection item
      INSERT INTO collection_items (collection_id, sku, quantity)
      VALUES (v_collection_id, v_item.value->>'sku', v_collected)
      ON CONFLICT (collection_id, sku) DO UPDATE
        SET quantity = EXCLUDED.quantity;

      v_total_collected := v_total_collected + v_collected;
    END IF;

    v_items_processed := v_items_processed + 1;
  END LOOP;

  -- Mark CUTOFF task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'items_count', v_items_processed,
        'total_sold', v_total_sold,
        'total_collected', v_total_collected,
        'collection_id', v_collection_id,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = 'CUTOFF';

  -- Skip SALE_ODV task if no sales
  IF v_total_sold = 0 AND v_sale_task_id IS NOT NULL THEN
    UPDATE visit_tasks
    SET status = 'SKIPPED',
        completed_at = now(),
        metadata = metadata || '{"skipped_reason": "no_sales"}'::jsonb
    WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV';
  END IF;

  -- Skip COLLECTION task if no collections
  IF v_total_collected = 0 THEN
    UPDATE visit_tasks
    SET status = 'SKIPPED',
        completed_at = now(),
        metadata = metadata || '{"skipped_reason": "no_collections"}'::jsonb
    WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'items_processed', v_items_processed,
    'total_sold', v_total_sold,
    'total_collected', v_total_collected,
    'collection_id', v_collection_id
  );
END;
$$;

COMMENT ON FUNCTION rpc_register_cutoff(uuid, jsonb) IS
'Register monthly cutoff. Creates SALE movements immediately (validated=false) + FEFO lot consumption. Collection items registered as PENDIENTE. Sales validated by rpc_link_odv(type=SALE) later. Replaces rpc_submit_cutoff + saga pivot.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_register_post_cutoff_placement: Immediate PLACEMENT movements + lots
-- Replaces: rpc_submit_post_cutoff_placement + rpc_confirm_saga_pivot
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_register_post_cutoff_placement(
  p_visit_id uuid,
  p_items jsonb  -- [{sku, quantity}]
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_client RECORD;
  v_item RECORD;
  v_qty integer;
  v_qty_before integer;
  v_qty_after integer;
  v_unit_price numeric;
  v_shelf_life integer;
  v_movement_id bigint;
  v_task_id uuid;
  v_items_processed integer := 0;
  v_total_qty integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Prerequisite: CUTOFF must be completed
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'CUTOFF'
      AND status = 'COMPLETED'
  ) THEN
    RAISE EXCEPTION 'CUTOFF task must be completed before POST_CUTOFF_PLACEMENT';
  END IF;

  -- Prerequisite: SALE_ODV must be completed or skipped
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV'
      AND status IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M')
  ) THEN
    RAISE EXCEPTION 'SALE_ODV task must be completed before POST_CUTOFF_PLACEMENT';
  END IF;

  -- Get task_id
  SELECT vt.task_id INTO v_task_id
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id AND vt.task_type = 'POST_CUTOFF_PLACEMENT';

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No POST_CUTOFF_PLACEMENT task found for visit %', p_visit_id;
  END IF;

  -- Idempotency
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'POST_CUTOFF_PLACEMENT'
      AND status IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M', 'CANCELLED')
  ) THEN
    RETURN jsonb_build_object('success', true, 'already_completed', true);
  END IF;

  -- Handle empty items: client downgrade logic
  IF p_items IS NULL OR jsonb_array_length(p_items) = 0 THEN
    -- Get client status
    SELECT c.status INTO v_client
    FROM clients c WHERE c.client_id = v_visit.client_id;

    IF v_client.status = 'DOWNGRADING' THEN
      -- Transition to INACTIVE
      UPDATE clients SET status = 'INACTIVE' WHERE client_id = v_visit.client_id;
      INSERT INTO client_status_log (client_id, old_status, new_status, changed_by, reason)
      VALUES (v_visit.client_id, 'DOWNGRADING', 'INACTIVE', v_visit.user_id,
              'No items in post-cutoff placement — auto-inactivated');
    END IF;

    -- Mark task as SKIPPED_M
    UPDATE visit_tasks
    SET status = 'SKIPPED_M',
        completed_at = now(),
        metadata = metadata || jsonb_build_object(
          'skipped_reason', 'no_items',
          'client_was_downgrading', COALESCE(v_client.status = 'DOWNGRADING', false),
          'direct_rpc', true
        )
    WHERE visit_id = p_visit_id AND task_type = 'POST_CUTOFF_PLACEMENT';

    -- Also skip ODV_CABINET since no placement to validate
    UPDATE visit_tasks
    SET status = 'SKIPPED',
        completed_at = now(),
        metadata = metadata || '{"skipped_reason": "no_post_cutoff_placement"}'::jsonb
    WHERE visit_id = p_visit_id AND task_type = 'ODV_CABINET'
      AND status NOT IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M');

    RETURN jsonb_build_object(
      'success', true,
      'visit_id', p_visit_id,
      'items_processed', 0,
      'skipped', true
    );
  END IF;

  -- Process items: create PLACEMENT movements
  FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
  LOOP
    v_qty := COALESCE(
      (v_item.value->>'quantity')::integer,
      (v_item.value->>'cantidad')::integer,
      0
    );

    IF v_qty <= 0 THEN CONTINUE; END IF;

    SELECT COALESCE(ci.available_quantity, 0) INTO v_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_visit.client_id AND ci.sku = v_item.value->>'sku';
    IF NOT FOUND THEN v_qty_before := 0; END IF;

    v_qty_after := v_qty_before + v_qty;

    SELECT m.price, m.shelf_life_months
    INTO v_unit_price, v_shelf_life
    FROM medications m WHERE m.sku = v_item.value->>'sku';

    -- Create PLACEMENT movement (validated=false)
    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_visit.client_id, v_item.value->>'sku', v_qty, v_qty_before, v_qty_after,
      now(), 'PLACEMENT', v_unit_price, v_task_id, p_visit_id, false
    ) RETURNING id INTO v_movement_id;

    -- Create lot with expiry
    INSERT INTO cabinet_inventory_lots (
      client_id, sku, movement_id, visit_id, quantity, remaining_quantity,
      placement_date, expiry_date, status
    ) VALUES (
      v_visit.client_id, v_item.value->>'sku', v_movement_id, p_visit_id,
      v_qty, v_qty,
      CURRENT_DATE,
      CASE WHEN v_shelf_life IS NOT NULL
        THEN (CURRENT_DATE + (v_shelf_life || ' months')::interval)::date
        ELSE NULL
      END,
      'active'
    );

    v_items_processed := v_items_processed + 1;
    v_total_qty := v_total_qty + v_qty;
  END LOOP;

  -- Mark task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'items_count', v_items_processed,
        'total_quantity', v_total_qty,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = 'POST_CUTOFF_PLACEMENT';

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'items_processed', v_items_processed,
    'total_quantity', v_total_qty
  );
END;
$$;

COMMENT ON FUNCTION rpc_register_post_cutoff_placement(uuid, jsonb) IS
'Register post-cutoff restocking placement. Creates PLACEMENT movements immediately (validated=false) + lots with expiry. Validates prerequisites (CUTOFF + SALE_ODV done). Handles client downgrade if 0 items. Replaces rpc_submit_post_cutoff_placement + saga pivot.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_link_odv: Validate movements + link Zoho ODV
-- Replaces: rpc_confirm_saga_pivot / rpc_set_manual_odv_id / rpc_set_manual_botiquin_odv_id
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_link_odv(
  p_visit_id uuid,
  p_odv_id text,
  p_odv_type text,          -- 'SALE' or 'CABINET'
  p_items jsonb DEFAULT NULL -- Optional: [{sku, quantity}] for Zoho item matching
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_task_type visit_task_type;
  v_movement_type cabinet_movement_type;
  v_normalized_odv text;
  v_updated_count integer;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Validate and normalize ODV ID
  v_normalized_odv := TRIM(p_odv_id);
  IF v_normalized_odv ~ '^\d{1,5}$' THEN
    v_normalized_odv := 'DCOdV-' || v_normalized_odv;
  END IF;

  IF v_normalized_odv !~ '^DCOdV-[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Invalid ODV ID format: %. Expected DCOdV-NNNNN', p_odv_id;
  END IF;

  -- Map ODV type to task type and movement type
  CASE p_odv_type
    WHEN 'SALE' THEN
      v_task_type := 'SALE_ODV';
      v_movement_type := 'SALE';
    WHEN 'CABINET' THEN
      v_task_type := 'ODV_CABINET';
      v_movement_type := 'PLACEMENT';
    ELSE
      RAISE EXCEPTION 'Invalid odv_type: %. Must be SALE or CABINET', p_odv_type;
  END CASE;

  -- Idempotency: if validation task already completed
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = v_task_type
      AND status IN ('COMPLETED', 'SKIPPED')
  ) THEN
    RETURN jsonb_build_object('success', true, 'already_completed', true);
  END IF;

  -- Mark movements as validated
  UPDATE inventory_movements
  SET validated = true
  WHERE visit_id = p_visit_id
    AND type = v_movement_type
    AND validated = false;

  GET DIAGNOSTICS v_updated_count = ROW_COUNT;

  -- Record ODV link in cabinet_sale_odv_ids (created in migration 10)
  -- For now, store in visit_tasks metadata
  -- This will be migrated to cabinet_sale_odv_ids in Fase 3

  -- Mark validation task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'odv_id', v_normalized_odv,
        'odv_type', p_odv_type,
        'movements_validated', v_updated_count,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = v_task_type;

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'odv_id', v_normalized_odv,
    'movements_validated', v_updated_count
  );
END;
$$;

COMMENT ON FUNCTION rpc_link_odv(uuid, text, text, jsonb) IS
'Validate movements and link Zoho ODV. Marks existing movements as validated=true (no new movements created). p_odv_type: SALE validates SALE movements, CABINET validates PLACEMENT movements. Replaces rpc_confirm_saga_pivot/rpc_set_manual_odv_id.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_start_collection_transit: PENDIENTE → IN_TRANSIT
-- NEW RPC (no saga equivalent)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_start_collection_transit(
  p_collection_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_collection RECORD;
BEGIN
  SELECT c.collection_id, c.visit_id, c.status, c.user_id
  INTO v_collection
  FROM collections c WHERE c.collection_id = p_collection_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Collection not found: %', p_collection_id;
  END IF;

  IF v_collection.status = 'IN_TRANSIT' THEN
    RETURN jsonb_build_object('success', true, 'already_in_transit', true);
  END IF;

  IF v_collection.status != 'PENDIENTE' THEN
    RAISE EXCEPTION 'Collection must be PENDIENTE to start transit. Current: %', v_collection.status;
  END IF;

  UPDATE collections
  SET status = 'IN_TRANSIT',
      transit_started_at = now(),
      updated_at = now()
  WHERE collection_id = p_collection_id;

  RETURN jsonb_build_object(
    'success', true,
    'collection_id', p_collection_id,
    'status', 'IN_TRANSIT',
    'transit_started_at', now()
  );
END;
$$;

COMMENT ON FUNCTION rpc_start_collection_transit(uuid) IS
'Transition collection from PENDIENTE to IN_TRANSIT. Called when rep picks up products from cabinet and starts driving to CEDIS. Sets transit_started_at timestamp.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_register_collection_delivery: IN_TRANSIT → ENTREGADA + COLLECTION movements
-- Replaces: rpc_complete_collection
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_register_collection_delivery(
  p_visit_id uuid,
  p_responsible text,
  p_observations text DEFAULT NULL,
  p_signature_path text DEFAULT NULL,
  p_evidence_paths text[] DEFAULT '{}'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_collection RECORD;
  v_item RECORD;
  v_task_id uuid;
  v_qty_before integer;
  v_qty_after integer;
  v_unit_price numeric;
  v_movement_id bigint;
  v_total_collected integer := 0;
  v_evidence text;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Find collection
  SELECT c.* INTO v_collection
  FROM collections c WHERE c.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No collection found for visit %', p_visit_id;
  END IF;

  -- Allow delivery from PENDIENTE (legacy) or IN_TRANSIT
  IF v_collection.status NOT IN ('PENDIENTE', 'IN_TRANSIT') THEN
    IF v_collection.status = 'ENTREGADA' THEN
      RETURN jsonb_build_object('success', true, 'already_delivered', true);
    END IF;
    RAISE EXCEPTION 'Collection must be PENDIENTE or IN_TRANSIT. Current: %', v_collection.status;
  END IF;

  -- Validate required fields
  IF p_responsible IS NULL OR TRIM(p_responsible) = '' THEN
    RAISE EXCEPTION 'CEDIS responsible name is required';
  END IF;

  -- Get COLLECTION task_id
  SELECT vt.task_id INTO v_task_id
  FROM visit_tasks vt
  WHERE vt.visit_id = p_visit_id AND vt.task_type = 'COLLECTION';

  -- Store signature
  IF p_signature_path IS NOT NULL AND p_signature_path != '' THEN
    INSERT INTO collection_signatures (collection_id, signature_url)
    VALUES (v_collection.collection_id, p_signature_path)
    ON CONFLICT (collection_id) DO UPDATE
      SET signature_url = EXCLUDED.signature_url;
  END IF;

  -- Store evidence photos
  FOREACH v_evidence IN ARRAY p_evidence_paths
  LOOP
    INSERT INTO collection_evidence (collection_id, evidence_url)
    VALUES (v_collection.collection_id, v_evidence)
    ON CONFLICT DO NOTHING;
  END LOOP;

  -- Create COLLECTION movements for each item (FEFO consumption)
  FOR v_item IN
    SELECT ci.sku, ci.quantity
    FROM collection_items ci
    WHERE ci.collection_id = v_collection.collection_id
  LOOP
    IF v_item.quantity <= 0 THEN CONTINUE; END IF;

    -- Get current stock
    SELECT COALESCE(ci.available_quantity, 0) INTO v_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_visit.client_id AND ci.sku = v_item.sku;
    IF NOT FOUND THEN v_qty_before := 0; END IF;

    v_qty_after := GREATEST(0, v_qty_before - v_item.quantity);

    SELECT m.price INTO v_unit_price
    FROM medications m WHERE m.sku = v_item.sku;

    -- Create COLLECTION movement (validated=true, collections don't need ODV)
    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_visit.client_id, v_item.sku, v_item.quantity, v_qty_before, v_qty_after,
      now(), 'COLLECTION', v_unit_price, v_task_id, p_visit_id, true
    ) RETURNING id INTO v_movement_id;

    -- FEFO lot consumption
    PERFORM _consume_lots_fefo(
      v_visit.client_id, v_item.sku, v_item.quantity, 'collection', v_movement_id
    );

    v_total_collected := v_total_collected + v_item.quantity;
  END LOOP;

  -- Update collection status to ENTREGADA
  UPDATE collections
  SET status = 'ENTREGADA',
      delivered_at = now(),
      cedis_responsible_name = p_responsible,
      cedis_observations = p_observations,
      updated_at = now()
  WHERE collection_id = v_collection.collection_id;

  -- Mark COLLECTION task as COMPLETED
  UPDATE visit_tasks
  SET status = 'COMPLETED',
      completed_at = now(),
      last_activity_at = now(),
      metadata = metadata || jsonb_build_object(
        'collection_id', v_collection.collection_id,
        'total_collected', v_total_collected,
        'direct_rpc', true
      )
  WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';

  RETURN jsonb_build_object(
    'success', true,
    'collection_id', v_collection.collection_id,
    'total_collected', v_total_collected
  );
END;
$$;

COMMENT ON FUNCTION rpc_register_collection_delivery(uuid, text, text, text, text[]) IS
'Complete collection delivery at CEDIS. Creates COLLECTION movements (validated=true) + FEFO lot consumption. Stores signature and evidence. Updates collection status to ENTREGADA. Replaces rpc_complete_collection.';

-- ═══════════════════════════════════════════════════════════════════════════
-- rpc_compensate_visit_v2: Reverse movements + restore lots
-- Replaces: rpc_compensate_saga (but works post-pivot too)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION rpc_compensate_visit_v2(
  p_visit_id uuid,
  p_reason text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_movement RECORD;
  v_reverse_qty_before integer;
  v_reverse_qty_after integer;
  v_movements_reversed integer := 0;
  v_lots_restored integer := 0;
BEGIN
  -- Validate visit
  SELECT v.visit_id, v.client_id, v.user_id, v.saga_status
  INTO v_visit
  FROM visits v WHERE v.visit_id = p_visit_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Cannot compensate if VALIDATION tasks are already completed
  IF EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id
      AND task_type IN ('SALE_ODV', 'ODV_CABINET')
      AND status = 'COMPLETED'
  ) THEN
    RAISE EXCEPTION 'Cannot compensate: validation tasks already completed. ODV has been linked.';
  END IF;

  -- Cannot compensate if already compensated
  IF v_visit.saga_status = 'COMPENSATED' THEN
    RETURN jsonb_build_object('success', true, 'already_compensated', true);
  END IF;

  -- Reverse all movements for this visit
  FOR v_movement IN
    SELECT id, client_id, sku, quantity, type, unit_price, task_id
    FROM inventory_movements
    WHERE visit_id = p_visit_id
    ORDER BY id DESC  -- Reverse in opposite order
  LOOP
    -- Get current stock for reverse
    SELECT COALESCE(ci.available_quantity, 0) INTO v_reverse_qty_before
    FROM cabinet_inventory ci
    WHERE ci.client_id = v_movement.client_id AND ci.sku = v_movement.sku;
    IF NOT FOUND THEN v_reverse_qty_before := 0; END IF;

    -- Reverse: PLACEMENT becomes negative, SALE/COLLECTION become positive
    CASE v_movement.type
      WHEN 'PLACEMENT' THEN
        v_reverse_qty_after := GREATEST(0, v_reverse_qty_before - v_movement.quantity);
      WHEN 'SALE', 'COLLECTION' THEN
        v_reverse_qty_after := v_reverse_qty_before + v_movement.quantity;
    END CASE;

    -- Create reverse movement (always validated=true)
    INSERT INTO inventory_movements (
      client_id, sku, quantity, quantity_before, quantity_after,
      movement_date, type, unit_price, task_id, visit_id, validated
    ) VALUES (
      v_movement.client_id, v_movement.sku, v_movement.quantity,
      v_reverse_qty_before, v_reverse_qty_after,
      now(),
      -- Reverse type: PLACEMENT reversal uses COLLECTION, SALE/COLLECTION reversal uses PLACEMENT
      CASE v_movement.type
        WHEN 'PLACEMENT' THEN 'COLLECTION'::cabinet_movement_type
        ELSE 'PLACEMENT'::cabinet_movement_type
      END,
      v_movement.unit_price, v_movement.task_id, p_visit_id, true
    );

    -- cabinet_inventory updated by trigger

    v_movements_reversed := v_movements_reversed + 1;
  END LOOP;

  -- Restore consumed/collected lots back to active
  UPDATE cabinet_inventory_lots
  SET status = 'active',
      remaining_quantity = quantity,  -- Restore to original
      consumed_by_movement_id = NULL
  WHERE visit_id = p_visit_id
    AND status IN ('consumed', 'collected');

  GET DIAGNOSTICS v_lots_restored = ROW_COUNT;

  -- Delete lots created by this visit's placements (they were reversed)
  DELETE FROM cabinet_inventory_lots
  WHERE visit_id = p_visit_id AND status = 'active';

  -- Mark all tasks as COMPENSATED
  UPDATE visit_tasks
  SET status = 'COMPENSATED',
      metadata = metadata || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason
      )
  WHERE visit_id = p_visit_id
    AND status NOT IN ('COMPENSATED', 'SKIPPED', 'SKIPPED_M');

  -- Mark visit as compensated
  UPDATE visits
  SET saga_status = 'COMPENSATED',
      updated_at = now(),
      metadata = metadata || jsonb_build_object(
        'compensated_at', now(),
        'compensation_reason', p_reason,
        'movements_reversed', v_movements_reversed,
        'lots_restored', v_lots_restored
      )
  WHERE visit_id = p_visit_id;

  -- Delete associated collections
  DELETE FROM collection_evidence WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collection_signatures WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collection_items WHERE collection_id IN (
    SELECT collection_id FROM collections WHERE visit_id = p_visit_id
  );
  DELETE FROM collections WHERE visit_id = p_visit_id;

  -- Log compensation in audit_log
  INSERT INTO audit_log (table_name, record_id, action, audit_user_id, values_before, values_after)
  VALUES ('visits', p_visit_id::text, 'COMPENSATE',
          (SELECT current_user_id()),
          jsonb_build_object('saga_status', v_visit.saga_status),
          jsonb_build_object('saga_status', 'COMPENSATED', 'reason', p_reason,
                             'movements_reversed', v_movements_reversed, 'lots_restored', v_lots_restored));

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'movements_reversed', v_movements_reversed,
    'lots_restored', v_lots_restored
  );
END;
$$;

COMMENT ON FUNCTION rpc_compensate_visit_v2(uuid, text) IS
'Compensate (rollback) a visit. Creates reverse movements (validated=true) + restores consumed lots to active + deletes placement lots. Only allowed before VALIDATION tasks complete. Replaces rpc_compensate_saga.';

-- ═══════════════════════════════════════════════════════════════════════════
-- QUERY RPCs (read-only)
-- ═══════════════════════════════════════════════════════════════════════════

-- ── Expiring items for a rep's clients ──────────────────────────────────────

CREATE OR REPLACE FUNCTION rpc_get_expiring_items(
  p_user_id character varying,
  p_days_ahead integer DEFAULT 30
)
RETURNS TABLE (
  client_id character varying,
  client_name character varying,
  sku character varying,
  product character varying,
  brand character varying,
  remaining_quantity integer,
  expiry_date date,
  days_until_expiry integer,
  placement_date date
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = 'public'
AS $$
  SELECT
    l.client_id,
    c.client_name,
    l.sku,
    m.product,
    m.brand,
    l.remaining_quantity,
    l.expiry_date,
    (l.expiry_date - CURRENT_DATE)::integer AS days_until_expiry,
    l.placement_date
  FROM cabinet_inventory_lots l
  JOIN clients c ON l.client_id = c.client_id
  JOIN medications m ON l.sku = m.sku
  JOIN users u ON c.user_id::text = u.user_id::text
  WHERE u.user_id = p_user_id
    AND l.status = 'active'
    AND l.expiry_date IS NOT NULL
    AND l.expiry_date <= CURRENT_DATE + (p_days_ahead || ' days')::interval
  ORDER BY l.expiry_date ASC, c.client_name, m.product;
$$;

COMMENT ON FUNCTION rpc_get_expiring_items(character varying, integer) IS
'Get items expiring within p_days_ahead days for all clients in the rep''s zone. Used for expiry alert banners.';

-- ── Visit summary without saga dependency ───────────────────────────────────

CREATE OR REPLACE FUNCTION rpc_get_visit_summary_v2(
  p_visit_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = 'public'
AS $$
DECLARE
  v_visit RECORD;
  v_tasks jsonb;
  v_movements jsonb;
  v_collection jsonb;
BEGIN
  SELECT v.* INTO v_visit FROM visits v WHERE v.visit_id = p_visit_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Visit not found: %', p_visit_id;
  END IF;

  -- Tasks
  SELECT jsonb_agg(jsonb_build_object(
    'task_type', vt.task_type,
    'status', vt.status,
    'transaction_type', vt.transaction_type,
    'step_order', vt.step_order,
    'completed_at', vt.completed_at,
    'metadata', vt.metadata
  ) ORDER BY vt.step_order)
  INTO v_tasks
  FROM visit_tasks vt WHERE vt.visit_id = p_visit_id;

  -- Movements
  SELECT jsonb_agg(jsonb_build_object(
    'id', im.id,
    'sku', im.sku,
    'product', m.product,
    'type', im.type,
    'quantity', im.quantity,
    'unit_price', im.unit_price,
    'validated', im.validated,
    'movement_date', im.movement_date
  ) ORDER BY im.movement_date)
  INTO v_movements
  FROM inventory_movements im
  JOIN medications m ON im.sku = m.sku
  WHERE im.visit_id = p_visit_id;

  -- Collection
  SELECT jsonb_build_object(
    'collection_id', c.collection_id,
    'status', c.status,
    'transit_started_at', c.transit_started_at,
    'delivered_at', c.delivered_at,
    'items', (
      SELECT jsonb_agg(jsonb_build_object('sku', ci.sku, 'quantity', ci.quantity))
      FROM collection_items ci WHERE ci.collection_id = c.collection_id
    )
  )
  INTO v_collection
  FROM collections c WHERE c.visit_id = p_visit_id;

  RETURN jsonb_build_object(
    'visit', jsonb_build_object(
      'visit_id', v_visit.visit_id,
      'client_id', v_visit.client_id,
      'type', v_visit.type,
      'status', v_visit.status,
      'saga_status', v_visit.saga_status,
      'created_at', v_visit.created_at
    ),
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'movements', COALESCE(v_movements, '[]'::jsonb),
    'collection', v_collection
  );
END;
$$;

COMMENT ON FUNCTION rpc_get_visit_summary_v2(uuid) IS
'Complete visit summary without saga dependency. Returns visit info, tasks with validated status, movements, and collection status.';

-- ── Pending validations for a rep ───────────────────────────────────────────

CREATE OR REPLACE FUNCTION rpc_get_pending_validations(
  p_user_id character varying
)
RETURNS TABLE (
  visit_id uuid,
  client_id character varying,
  client_name character varying,
  task_type visit_task_type,
  movement_count bigint,
  total_value numeric,
  created_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = 'public'
AS $$
  SELECT
    v.visit_id,
    v.client_id,
    c.client_name,
    vt.task_type,
    COUNT(im.id) AS movement_count,
    SUM(im.quantity * im.unit_price) AS total_value,
    v.created_at
  FROM visits v
  JOIN clients c ON v.client_id = c.client_id
  JOIN visit_tasks vt ON v.visit_id = vt.visit_id
    AND vt.task_type IN ('SALE_ODV', 'ODV_CABINET')
    AND vt.status = 'PENDING'
  JOIN inventory_movements im ON im.visit_id = v.visit_id
    AND im.validated = false
  WHERE v.user_id = p_user_id
    AND v.status IN ('PENDING', 'IN_PROGRESS')
  GROUP BY v.visit_id, v.client_id, c.client_name, vt.task_type, v.created_at
  ORDER BY v.created_at ASC;
$$;

COMMENT ON FUNCTION rpc_get_pending_validations(character varying) IS
'Get visits with unvalidated movements for a rep. Shows which SALE_ODV/ODV_CABINET tasks still need ODV linking.';

-- ═══════════════════════════════════════════════════════════════════════════
-- GRANTS
-- ═══════════════════════════════════════════════════════════════════════════

GRANT EXECUTE ON FUNCTION _consume_lots_fefo(character varying, character varying, integer, text, bigint) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_register_placement(uuid, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_register_cutoff(uuid, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_register_post_cutoff_placement(uuid, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_link_odv(uuid, text, text, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_start_collection_transit(uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_register_collection_delivery(uuid, text, text, text, text[]) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_compensate_visit_v2(uuid, text) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_get_expiring_items(character varying, integer) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_get_visit_summary_v2(uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION rpc_get_pending_validations(character varying) TO authenticated;

NOTIFY pgrst, 'reload schema';
