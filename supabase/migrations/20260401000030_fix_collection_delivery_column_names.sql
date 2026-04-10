-- Fix: rpc_register_collection_delivery referenced non-existent columns
-- "signature_url" and "evidence_url" instead of "storage_path" in
-- collection_signatures and collection_evidence tables.

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

  -- Store signature (FIX: column is "storage_path", not "signature_url")
  IF p_signature_path IS NOT NULL AND p_signature_path != '' THEN
    INSERT INTO collection_signatures (collection_id, storage_path)
    VALUES (v_collection.collection_id, p_signature_path)
    ON CONFLICT (collection_id) DO UPDATE
      SET storage_path = EXCLUDED.storage_path;
  END IF;

  -- Store evidence photos (FIX: column is "storage_path", not "evidence_url")
  FOREACH v_evidence IN ARRAY p_evidence_paths
  LOOP
    INSERT INTO collection_evidence (collection_id, storage_path)
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

NOTIFY pgrst, 'reload schema';
