-- =============================================================================
-- Fix rpc_admin_get_all_visits: add total_pieces + collections_count
-- Fix rpc_get_visit_saga_summary: all JSON keys → English
-- =============================================================================

-- 1. rpc_admin_get_all_visits — add total_pieces and collections_count
CREATE OR REPLACE FUNCTION public.rpc_admin_get_all_visits(
  p_limit integer DEFAULT 50,
  p_offset integer DEFAULT 0,
  p_status text DEFAULT NULL,
  p_search text DEFAULT NULL,
  p_date_from date DEFAULT NULL,
  p_date_to date DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_visits jsonb;
  v_total int;
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

  SELECT COUNT(*)
  INTO v_total
  FROM public.visits v
  JOIN public.clients c ON c.client_id = v.client_id
  WHERE (p_status IS NULL OR v.status::text = p_status)
    AND (p_search IS NULL OR c.client_name ILIKE '%' || p_search || '%')
    AND (p_date_from IS NULL OR v.created_at::date >= p_date_from)
    AND (p_date_to IS NULL OR v.created_at::date <= p_date_to);

  SELECT jsonb_agg(row_data)
  INTO v_visits
  FROM (
    SELECT jsonb_build_object(
      'visit_id', v.visit_id,
      'client_id', v.client_id,
      'client_name', c.client_name,
      'user_id', v.user_id,
      'user_name', u.name,
      'type', v.type::text,
      'status', v.status::text,
      'saga_status', COALESCE(
        CASE WHEN v.status = 'COMPLETED' THEN 'COMPLETED'
             WHEN v.status = 'CANCELLED' THEN 'COMPENSATED'
             ELSE 'RUNNING' END,
        'RUNNING'
      ),
      'label', v.label,
      'created_at', v.created_at,
      'started_at', v.started_at,
      'completed_at', v.completed_at,
      'metadata', v.metadata,
      'tasks_count', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id),
      'tasks_completed', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND vt.status = 'COMPLETED'),
      'odvs_count', (SELECT COUNT(*) FROM saga_transactions st WHERE st.visit_id = v.visit_id),
      'total_pieces', COALESCE((
        SELECT SUM(
          COALESCE((item->>'quantity')::int, (item->>'cantidad_entrada')::int, 0)
        )
        FROM saga_transactions st,
             jsonb_array_elements(st.items) AS item
        WHERE st.visit_id = v.visit_id
      ), 0)::int,
      'collections_count', (SELECT COUNT(*) FROM collections col WHERE col.visit_id = v.visit_id)
    ) as row_data
    FROM public.visits v
    JOIN public.clients c ON c.client_id = v.client_id
    LEFT JOIN public.users u ON u.user_id = v.user_id
    WHERE (p_status IS NULL OR v.status::text = p_status)
      AND (p_search IS NULL OR c.client_name ILIKE '%' || p_search || '%')
      AND (p_date_from IS NULL OR v.created_at::date >= p_date_from)
      AND (p_date_to IS NULL OR v.created_at::date <= p_date_to)
    ORDER BY v.created_at DESC
    LIMIT p_limit
    OFFSET p_offset
  ) sub;

  RETURN jsonb_build_object(
    'visits', COALESCE(v_visits, '[]'::jsonb),
    'total', v_total,
    'limit', p_limit,
    'offset', p_offset
  );
END;
$function$;

-- 2. rpc_get_visit_saga_summary — ALL keys in English
CREATE OR REPLACE FUNCTION public.rpc_get_visit_saga_summary(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_client_id text;
  v_visit_type text;
  v_corte_items jsonb;
  v_placement_items jsonb;
  v_post_cutoff_items jsonb;
  v_odvs_sale jsonb;
  v_odvs_cabinet jsonb;
  v_movements_summary jsonb;
  v_collection_items jsonb;
  v_has_movements boolean;
  v_mov_total_count int;
  v_mov_total_quantity int;
  v_mov_unique_skus int;
  v_mov_by_type jsonb;
  v_total_sold int;
  v_total_collected int;
  v_total_placement int;
  v_total_post_cutoff int;
  v_total_collection int;
BEGIN
  SELECT v.client_id, v.type::text
  INTO v_client_id, v_visit_type
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_client_id IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_visit(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  SELECT EXISTS(
    SELECT 1 FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
  ) INTO v_has_movements;

  -- 1. CORTE: try visit_tasks.metadata first
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'sold', COALESCE((item->>'vendido')::int, 0),
      'collected', COALESCE((item->>'recolectado')::int, 0),
      'holding', 0
    )
  )
  INTO v_corte_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = 'CUTOFF'
    AND vt.status = 'COMPLETED'
    AND vt.metadata->'items' IS NOT NULL
    AND jsonb_array_length(vt.metadata->'items') > 0
    AND (COALESCE((item->>'vendido')::int, 0) > 0 OR COALESCE((item->>'recolectado')::int, 0) > 0);

  IF v_corte_items IS NOT NULL THEN
    SELECT
      COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0)::int,
      COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)::int
    INTO v_total_sold, v_total_collected
    FROM public.visit_tasks vt,
         jsonb_array_elements(vt.metadata->'items') AS item
    WHERE vt.visit_id = p_visit_id
      AND vt.task_type = 'CUTOFF'
      AND vt.status = 'COMPLETED';
  ELSIF v_has_movements THEN
    SELECT jsonb_agg(item_data)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'product', product,
        'sold', sold,
        'collected', collected,
        'holding', holding
      ) as item_data
      FROM (
        SELECT
          mi.sku,
          COALESCE(m.product, mi.sku) as product,
          COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0)::int as sold,
          COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0)::int as collected,
          COALESCE(SUM(CASE WHEN mi.type::text = 'HOLDING' THEN mi.quantity ELSE 0 END), 0)::int as holding
        FROM public.inventory_movements mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medications m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.type::text IN ('SALE', 'COLLECTION', 'HOLDING')
        GROUP BY mi.sku, m.product
        HAVING SUM(CASE WHEN mi.type::text IN ('SALE', 'COLLECTION') THEN mi.quantity ELSE 0 END) > 0
           OR SUM(CASE WHEN mi.type::text = 'HOLDING' THEN mi.quantity ELSE 0 END) > 0
      ) grouped
    ) items;

    SELECT
      COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0)::int,
      COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0)::int
    INTO v_total_sold, v_total_collected
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text IN ('SALE', 'COLLECTION');
  ELSE
    SELECT jsonb_agg(combined_item)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'product', COALESCE(m.product, item->>'sku'),
        'sold', COALESCE((item->>'quantity')::int, (item->>'vendido')::int, 0),
        'collected', 0,
        'holding', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medications m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.type::text = 'SALE'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0

      UNION ALL

      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'product', COALESCE(m.product, item->>'sku'),
        'sold', 0,
        'collected', COALESCE(
          (item->>'cantidad_salida')::int,
          (item->>'quantity')::int,
          0
        ),
        'holding', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medications m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.type::text = 'COLLECTION'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
    ) items
    WHERE (combined_item->>'sold')::int > 0
       OR (combined_item->>'collected')::int > 0;

    SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
    INTO v_total_sold
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'SALE'
      AND st.items IS NOT NULL;

    SELECT COALESCE(SUM(COALESCE((item->>'cantidad_salida')::int, (item->>'quantity')::int, 0)), 0)::int
    INTO v_total_collected
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL;
  END IF;

  -- 2. INITIAL_PLACEMENT
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)
    )
  )
  INTO v_placement_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)), 0)::int
  INTO v_total_placement
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL;

  -- 3. POST_CUTOFF_PLACEMENT
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'quantity')::int, 0),
      'is_holding', COALESCE((item->>'es_permanencia')::boolean, false)
    )
  )
  INTO v_post_cutoff_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'quantity')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
  INTO v_total_post_cutoff
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL;

  -- 4. ODVs SALE
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_sale
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'date', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'type', szl.type::text,
      'total_pieces', (
        SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'product', COALESCE(m.product, item->>'sku'),
            'sold_quantity', COALESCE((item->>'quantity')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
        LEFT JOIN public.medications m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.type::text = 'SALE'
    ORDER BY szl.created_at
  ) sub;

  -- 5. ODVs CABINET
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_cabinet
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'date', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'type', szl.type::text,
      'total_pieces', (
        SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'product', COALESCE(m.product, item->>'sku'),
            'quantity', COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
        LEFT JOIN public.medications m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.type::text = 'CABINET'
    ORDER BY szl.created_at
  ) sub;

  -- 6. Movements summary
  SELECT
    COALESCE(COUNT(*)::int, 0),
    COALESCE(SUM(mi.quantity)::int, 0),
    COALESCE(COUNT(DISTINCT mi.sku)::int, 0)
  INTO v_mov_total_count, v_mov_total_quantity, v_mov_unique_skus
  FROM public.inventory_movements mi
  JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
  WHERE st.visit_id = p_visit_id;

  SELECT COALESCE(jsonb_object_agg(type_text, total), '{}'::jsonb)
  INTO v_mov_by_type
  FROM (
    SELECT mi.type::text as type_text, SUM(mi.quantity)::int as total
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    GROUP BY mi.type
  ) sub;

  v_movements_summary := jsonb_build_object(
    'total_movements', v_mov_total_count,
    'total_quantity', v_mov_total_quantity,
    'unique_skus', v_mov_unique_skus,
    'by_type', v_mov_by_type
  );

  -- 7. Collection items
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'recolectado')::int, 0)
    )
  )
  INTO v_collection_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = 'CUTOFF'
    AND vt.status = 'COMPLETED'
    AND vt.metadata->'items' IS NOT NULL
    AND COALESCE((item->>'recolectado')::int, 0) > 0;

  IF v_collection_items IS NOT NULL THEN
    v_total_collection := v_total_collected;
  ELSIF v_has_movements THEN
    SELECT jsonb_agg(item_data)
    INTO v_collection_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'product', product,
        'quantity', quantity
      ) as item_data
      FROM (
        SELECT
          mi.sku,
          COALESCE(m.product, mi.sku) as product,
          SUM(mi.quantity)::int as quantity
        FROM public.inventory_movements mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medications m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.type::text = 'COLLECTION'
        GROUP BY mi.sku, m.product
        HAVING SUM(mi.quantity) > 0
      ) grouped
    ) items;

    v_total_collection := v_total_collected;
  ELSE
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'product', COALESCE(m.product, item->>'sku'),
        'quantity', COALESCE(
          (item->>'cantidad_salida')::int,
          (item->>'quantity')::int,
          0
        )
      )
    )
    INTO v_collection_items
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    LEFT JOIN public.medications m ON m.sku = item->>'sku'
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL
      AND COALESCE((item->>'cantidad_salida')::int, (item->>'quantity')::int, 0) > 0;

    v_total_collection := v_total_collected;
  END IF;

  -- Return with ALL ENGLISH keys
  RETURN jsonb_build_object(
    'visit_id', p_visit_id,
    'visit_type', v_visit_type,
    'client_id', v_client_id,
    'corte', jsonb_build_object(
      'items', COALESCE(v_corte_items, '[]'::jsonb),
      'total_sold', COALESCE(v_total_sold, 0),
      'total_collected', COALESCE(v_total_collected, 0)
    ),
    'levantamiento', jsonb_build_object(
      'items', COALESCE(v_placement_items, '[]'::jsonb),
      'total_pieces', COALESCE(v_total_placement, 0)
    ),
    'lev_post_corte', jsonb_build_object(
      'items', COALESCE(v_post_cutoff_items, '[]'::jsonb),
      'total_pieces', COALESCE(v_total_post_cutoff, 0)
    ),
    'collection', jsonb_build_object(
      'items', COALESCE(v_collection_items, '[]'::jsonb),
      'total_pieces', COALESCE(v_total_collection, 0)
    ),
    'odvs', jsonb_build_object(
      'sale', COALESCE(v_odvs_sale, '[]'::jsonb),
      'cabinet', COALESCE(v_odvs_cabinet, '[]'::jsonb),
      'all', COALESCE(v_odvs_sale, '[]'::jsonb) || COALESCE(v_odvs_cabinet, '[]'::jsonb)
    ),
    'movements', v_movements_summary
  );
END;
$function$;

NOTIFY pgrst, 'reload schema';
