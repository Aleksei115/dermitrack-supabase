-- Fix rpc_get_visit_summary_v2: add product names to collection items + add ODVs section with items
CREATE OR REPLACE FUNCTION public.rpc_get_visit_summary_v2(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_visit RECORD;
  v_tasks jsonb;
  v_movements jsonb;
  v_collection jsonb;
  v_odvs jsonb;
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

  -- Collection (with product names from medications)
  SELECT jsonb_build_object(
    'collection_id', c.collection_id,
    'status', c.status,
    'transit_started_at', c.transit_started_at,
    'delivered_at', c.delivered_at,
    'items', COALESCE(
      (SELECT jsonb_agg(jsonb_build_object(
        'sku', ci.sku,
        'product', COALESCE(m.product, ci.sku),
        'quantity', ci.quantity
      ))
      FROM collection_items ci
      LEFT JOIN medications m ON m.sku = ci.sku
      WHERE ci.collection_id = c.collection_id
      ), '[]'::jsonb
    )
  )
  INTO v_collection
  FROM collections c WHERE c.visit_id = p_visit_id;

  -- ODVs from cabinet_sale_odv_ids with items from inventory_movements
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', cso.odv_id,
      'odv_type', cso.odv_type::text,
      'created_at', cso.created_at,
      'items', COALESCE(
        (SELECT jsonb_agg(jsonb_build_object(
          'sku', im.sku,
          'product', COALESCE(m.product, im.sku),
          'quantity', im.quantity
        ))
        FROM inventory_movements im
        LEFT JOIN medications m ON m.sku = im.sku
        WHERE im.visit_id = p_visit_id
          AND im.type = CASE cso.odv_type
            WHEN 'SALE' THEN 'SALE'::cabinet_movement_type
            ELSE 'PLACEMENT'::cabinet_movement_type
          END
        ), '[]'::jsonb
      )
    ) as odv_data
    FROM cabinet_sale_odv_ids cso
    WHERE cso.visit_id = p_visit_id
    ORDER BY cso.created_at
  ) sub;

  RETURN jsonb_build_object(
    'visit', jsonb_build_object(
      'visit_id', v_visit.visit_id,
      'client_id', v_visit.client_id,
      'type', v_visit.type,
      'status', v_visit.status,
      'workflow_status', v_visit.workflow_status,
      'created_at', v_visit.created_at
    ),
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'movements', COALESCE(v_movements, '[]'::jsonb),
    'collection', v_collection,
    'odvs', v_odvs
  );
END;
$function$;

NOTIFY pgrst, 'reload schema';
