-- Fix rpc_admin_get_visit_detail: use VALIDATION instead of PIVOT for transaction_type
CREATE OR REPLACE FUNCTION public.rpc_admin_get_visit_detail(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_visit jsonb;
  v_tasks jsonb;
  v_odvs jsonb;
  v_movements jsonb;
  v_report jsonb;
  v_collections jsonb;
BEGIN
  SELECT u.user_id, u.role::text
  INTO v_user_id, v_user_rol
  FROM public.users u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  SELECT jsonb_build_object(
    'visit_id', v.visit_id,
    'client_id', v.client_id,
    'client_name', c.client_name,
    'user_id', v.user_id,
    'user_name', u.name,
    'type', v.type::text,
    'status', v.status::text,
    'workflow_status', COALESCE(v.workflow_status::text,
      CASE WHEN v.status = 'COMPLETED' THEN 'COMPLETED'
           WHEN v.status = 'CANCELLED' THEN 'COMPENSATED'
           ELSE 'RUNNING' END
    ),
    'label', v.label,
    'created_at', v.created_at,
    'started_at', v.started_at,
    'completed_at', v.completed_at,
    'metadata', v.metadata
  ), v.client_id
  INTO v_visit, v_id_cliente
  FROM public.visits v
  JOIN public.clients c ON c.client_id = v.client_id
  LEFT JOIN public.users u ON u.user_id = v.user_id
  WHERE v.visit_id = p_visit_id;

  IF v_visit IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Get visit_tasks
  SELECT jsonb_agg(row_data)
  INTO v_tasks
  FROM (
    SELECT jsonb_build_object(
      'task_id', COALESCE(vt.task_id::text, vt.task_type::text || '-' || p_visit_id::text),
      'task_type', vt.task_type::text,
      'status', vt.status::text,
      'required', vt.required,
      'created_at', vt.created_at,
      'started_at', vt.started_at,
      'completed_at', vt.completed_at,
      'due_at', vt.due_at,
      'metadata', vt.metadata,
      'transaction_type', CASE vt.task_type::text
        WHEN 'INITIAL_PLACEMENT' THEN 'COMPENSABLE'
        WHEN 'CUTOFF' THEN 'COMPENSABLE'
        WHEN 'POST_CUTOFF_PLACEMENT' THEN 'COMPENSABLE'
        WHEN 'ODV_CABINET' THEN 'VALIDATION'
        WHEN 'SALE_ODV' THEN 'VALIDATION'
        ELSE 'RETRYABLE'
      END,
      'step_order', CASE vt.task_type::text
        WHEN 'INITIAL_PLACEMENT' THEN 1
        WHEN 'CUTOFF' THEN 1
        WHEN 'SALE_ODV' THEN 2
        WHEN 'COLLECTION' THEN 3
        WHEN 'POST_CUTOFF_PLACEMENT' THEN 4
        WHEN 'ODV_CABINET' THEN 5
        WHEN 'VISIT_REPORT' THEN 6
        ELSE 99
      END,
      'compensation_status', 'NOT_NEEDED'
    ) as row_data
    FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
    ORDER BY CASE vt.task_type::text
      WHEN 'INITIAL_PLACEMENT' THEN 1
      WHEN 'CUTOFF' THEN 1
      WHEN 'SALE_ODV' THEN 2
      WHEN 'COLLECTION' THEN 3
      WHEN 'POST_CUTOFF_PLACEMENT' THEN 4
      WHEN 'ODV_CABINET' THEN 5
      WHEN 'VISIT_REPORT' THEN 6
      ELSE 99
    END
  ) sub;

  -- Get ODVs from cabinet_sale_odv_ids
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', cso.odv_id,
      'odv_numero', cso.odv_id,
      'type', cso.odv_type::text,
      'odv_date', cso.created_at,
      'status', 'linked',
      'total_piezas', COALESCE(
        (SELECT SUM(im.quantity)::int
         FROM inventory_movements im
         WHERE im.visit_id = p_visit_id
           AND im.type = CASE cso.odv_type
             WHEN 'SALE' THEN 'SALE'::cabinet_movement_type
             ELSE 'PLACEMENT'::cabinet_movement_type
           END
        ), 0
      ),
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

  -- Get movements (directly via visit_id)
  SELECT jsonb_build_object(
    'total', COALESCE(mov_stats.cnt, 0),
    'total_cantidad', COALESCE(mov_stats.suma_cantidad, 0),
    'unique_skus', COALESCE(mov_stats.skus_unicos, 0),
    'by_tipo', COALESCE(mov_tipos.tipos, '{}'::jsonb),
    'items', COALESCE(mov_items.items, '[]'::jsonb)
  )
  INTO v_movements
  FROM (
    SELECT
      COUNT(*)::int as cnt,
      COALESCE(SUM(mi.quantity), 0)::int as suma_cantidad,
      COUNT(DISTINCT mi.sku)::int as skus_unicos
    FROM public.inventory_movements mi
    WHERE mi.visit_id = p_visit_id
  ) mov_stats,
  (
    SELECT jsonb_object_agg(type::text, suma_cantidad) as tipos
    FROM (
      SELECT mi.type, COALESCE(SUM(mi.quantity), 0)::int as suma_cantidad
      FROM public.inventory_movements mi
      WHERE mi.visit_id = p_visit_id
      GROUP BY mi.type
    ) sub
  ) mov_tipos,
  (
    SELECT jsonb_agg(row_data) as items
    FROM (
      SELECT jsonb_build_object(
        'sku', mi.sku,
        'type', mi.type::text,
        'quantity', mi.quantity,
        'quantity_before', mi.quantity_before,
        'quantity_after', mi.quantity_after,
        'created_at', mi.movement_date
      ) as row_data
      FROM public.inventory_movements mi
      WHERE mi.visit_id = p_visit_id
      ORDER BY mi.movement_date
      LIMIT 100
    ) sub
  ) mov_items;

  -- Get visit report
  SELECT jsonb_build_object(
    'report_id', vi.report_id,
    'completed', vi.completed,
    'compliance_score', vi.compliance_score,
    'label', vi.label,
    'responses', vi.responses,
    'completed_date', vi.completed_date,
    'created_at', vi.created_at
  )
  INTO v_report
  FROM public.visit_reports vi
  WHERE vi.visit_id = p_visit_id;

  -- Get collections with items
  SELECT jsonb_agg(row_data)
  INTO v_collections
  FROM (
    SELECT jsonb_build_object(
      'collection_id', r.collection_id,
      'status', r.status,
      'latitud', r.latitude,
      'longitud', r.longitude,
      'cedis_observations', r.cedis_observations,
      'cedis_responsible_name', r.cedis_responsible_name,
      'delivered_at', r.delivered_at,
      'created_at', r.created_at,
      'metadata', r.metadata,
      'items', COALESCE(
        (SELECT jsonb_agg(jsonb_build_object(
          'sku', ci.sku,
          'product', COALESCE(m.product, ci.sku),
          'quantity', ci.quantity
        ))
        FROM public.collection_items ci
        LEFT JOIN public.medications m ON m.sku = ci.sku
        WHERE ci.collection_id = r.collection_id
        ), '[]'::jsonb
      )
    ) as row_data
    FROM public.collections r
    WHERE r.visit_id = p_visit_id
    ORDER BY r.created_at
  ) sub;

  RETURN jsonb_build_object(
    'visit', v_visit,
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'odvs', COALESCE(v_odvs, '[]'::jsonb),
    'movements', COALESCE(v_movements, '{"total": 0, "total_cantidad": 0, "unique_skus": 0, "by_tipo": {}, "items": []}'::jsonb),
    'report', v_report,
    'collections', COALESCE(v_collections, '[]'::jsonb)
  );
END;
$function$;

NOTIFY pgrst, 'reload schema';
