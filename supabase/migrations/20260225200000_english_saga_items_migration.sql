-- =============================================================================
-- Migration: Rename Spanish → English fields in saga_transactions.items
-- + Update RPCs to accept/output English field names with Spanish COALESCE
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- PART 1: Data migration — rename fields in saga_transactions.items
-- ─────────────────────────────────────────────────────────────────────────────
-- Each saga type has a different "actionable quantity" field:
--   SALE:                 cantidad (simple) or cantidad_salida (full structure)
--   COLLECTION:           cantidad (simple) or cantidad_salida (full structure)
--   INITIAL_PLACEMENT:    cantidad (simple) or cantidad_entrada (full structure)
--   POST_CUTOFF_PLACEMENT: cantidad
--   HOLDING:              cantidad
-- We unify all into a `quantity` field that the trigger and RPCs expect.
-- Split per type to avoid correlated subquery issues with type reference.

-- SALE: quantity = COALESCE(cantidad, cantidad_salida)
UPDATE saga_transactions
SET items = (
  SELECT jsonb_agg(
    item - 'cantidad' - 'cantidad_entrada' - 'cantidad_salida' - 'cantidad_permanencia'
    || jsonb_strip_nulls(jsonb_build_object(
      'quantity', COALESCE(item->'cantidad', item->'cantidad_salida'),
      'input_quantity', item->'cantidad_entrada',
      'output_quantity', item->'cantidad_salida',
      'holding_quantity', item->'cantidad_permanencia'
    ))
  )
  FROM jsonb_array_elements(items) AS item
)
WHERE type = 'SALE' AND items IS NOT NULL AND jsonb_array_length(items) > 0;

-- COLLECTION: quantity = COALESCE(cantidad, cantidad_salida)
UPDATE saga_transactions
SET items = (
  SELECT jsonb_agg(
    item - 'cantidad' - 'cantidad_entrada' - 'cantidad_salida' - 'cantidad_permanencia'
    || jsonb_strip_nulls(jsonb_build_object(
      'quantity', COALESCE(item->'cantidad', item->'cantidad_salida'),
      'input_quantity', item->'cantidad_entrada',
      'output_quantity', item->'cantidad_salida',
      'holding_quantity', item->'cantidad_permanencia'
    ))
  )
  FROM jsonb_array_elements(items) AS item
)
WHERE type = 'COLLECTION' AND items IS NOT NULL AND jsonb_array_length(items) > 0;

-- INITIAL_PLACEMENT: quantity = COALESCE(cantidad, cantidad_entrada)
UPDATE saga_transactions
SET items = (
  SELECT jsonb_agg(
    item - 'cantidad' - 'cantidad_entrada' - 'cantidad_salida' - 'cantidad_permanencia'
    || jsonb_strip_nulls(jsonb_build_object(
      'quantity', COALESCE(item->'cantidad', item->'cantidad_entrada'),
      'input_quantity', item->'cantidad_entrada',
      'output_quantity', item->'cantidad_salida',
      'holding_quantity', item->'cantidad_permanencia'
    ))
  )
  FROM jsonb_array_elements(items) AS item
)
WHERE type = 'INITIAL_PLACEMENT' AND items IS NOT NULL AND jsonb_array_length(items) > 0;

-- POST_CUTOFF_PLACEMENT: cantidad → quantity, producto → product, es_permanencia → is_holding
UPDATE saga_transactions
SET items = (
  SELECT jsonb_agg(
    item - 'cantidad' - 'producto' - 'es_permanencia'
    || jsonb_strip_nulls(jsonb_build_object(
      'quantity', item->'cantidad',
      'product', item->'producto',
      'is_holding', item->'es_permanencia'
    ))
  )
  FROM jsonb_array_elements(items) AS item
)
WHERE type = 'POST_CUTOFF_PLACEMENT' AND items IS NOT NULL AND jsonb_array_length(items) > 0;

-- HOLDING: cantidad → quantity
UPDATE saga_transactions
SET items = (
  SELECT jsonb_agg(
    item - 'cantidad'
    || jsonb_build_object('quantity', item->'cantidad')
  )
  FROM jsonb_array_elements(items) AS item
)
WHERE type = 'HOLDING' AND items IS NOT NULL AND jsonb_array_length(items) > 0;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 2: rpc_submit_cutoff — read sold/collected from English frontend input
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rpc_submit_cutoff(p_visit_id uuid, p_items jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_saga_venta_id uuid;
  v_saga_collection_id uuid;
  v_collection_id uuid;
  v_total_vendido integer := 0;
  v_total_recolectado integer := 0;
  v_items_venta jsonb;
  v_items_recoleccion jsonb;
BEGIN
  -- Calculate totals (English first, Spanish fallback)
  SELECT
    COALESCE(SUM(COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0)), 0),
    COALESCE(SUM(COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0)), 0)
  INTO v_total_vendido, v_total_recolectado
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item;

  -- Filter SALE items
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'quantity', COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0)
    )
  ) INTO v_items_venta
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0) > 0;

  -- Filter COLLECTION items
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'quantity', COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0)
    )
  ) INTO v_items_recoleccion
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0) > 0;

  -- Get visit data
  SELECT v.user_id, v.client_id
  INTO v_id_usuario, v_id_cliente
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- CREATE SALE SAGA
  IF v_total_vendido > 0 THEN
    INSERT INTO public.saga_transactions (
      type, status, client_id, user_id,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'SALE'::saga_transaction_type,
      'DRAFT'::saga_transaction_status,
      v_id_cliente,
      v_id_usuario,
      COALESCE(v_items_venta, '[]'::jsonb),
      jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'NORMAL',
        'zoho_required', true
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_venta_id;

    UPDATE public.visit_tasks
    SET
      reference_table = NULL,
      reference_id = NULL,
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV';
  ELSE
    UPDATE public.visit_tasks
    SET
      status = 'SKIPPED',
      completed_at = now(),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV';
  END IF;

  -- CREATE COLLECTION SAGA
  IF v_total_recolectado > 0 THEN
    INSERT INTO public.saga_transactions (
      type, status, client_id, user_id,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'COLLECTION'::saga_transaction_type,
      'DRAFT'::saga_transaction_status,
      v_id_cliente,
      v_id_usuario,
      COALESCE(v_items_recoleccion, '[]'::jsonb),
      jsonb_build_object('visit_id', p_visit_id),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_collection_id;

    INSERT INTO public.collections (
      visit_id, client_id, user_id, status
    )
    SELECT p_visit_id, v_id_cliente, v_id_usuario, 'PENDING'
    WHERE NOT EXISTS (
      SELECT 1 FROM public.collections r WHERE r.visit_id = p_visit_id
    )
    RETURNING collection_id INTO v_collection_id;

    IF v_collection_id IS NULL THEN
      SELECT collection_id INTO v_collection_id
      FROM public.collections WHERE visit_id = p_visit_id LIMIT 1;
    END IF;

    INSERT INTO public.collection_items (collection_id, sku, quantity)
    SELECT
      v_collection_id,
      (item->>'sku')::varchar,
      (item->>'quantity')::int
    FROM jsonb_array_elements(COALESCE(v_items_recoleccion, '[]'::jsonb)) AS item
    ON CONFLICT (collection_id, sku) DO UPDATE
    SET quantity = EXCLUDED.quantity;

    UPDATE public.visit_tasks
    SET
      reference_table = NULL,
      reference_id = NULL,
      metadata = jsonb_build_object('collection_id', v_collection_id),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';
  ELSE
    UPDATE public.visit_tasks
    SET
      status = 'SKIPPED',
      completed_at = now(),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';
  END IF;

  -- MARK CUTOFF COMPLETED
  UPDATE public.visit_tasks
  SET
    status = 'COMPLETED',
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    metadata = jsonb_build_object(
      'items', p_items,
      'saga_venta_id', v_saga_venta_id,
      'saga_collection_id', v_saga_collection_id,
      'total_vendido', v_total_vendido,
      'total_recolectado', v_total_recolectado
    ),
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = 'CUTOFF';

  RETURN jsonb_build_object(
    'success', true,
    'saga_venta_id', v_saga_venta_id,
    'saga_collection_id', v_saga_collection_id,
    'total_vendido', v_total_vendido,
    'total_recolectado', v_total_recolectado
  );
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 3: rpc_admin_get_visit_detail — COALESCE for quantity in ODV section
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rpc_admin_get_visit_detail(p_visit_id uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
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
        WHEN 'ODV_CABINET' THEN 'PIVOT'
        WHEN 'SALE_ODV' THEN 'PIVOT'
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

  -- Get ODVs from saga_zoho_links (comprehensive COALESCE for quantity)
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'type', szl.type::text,
      'odv_date', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'saga_type', st.type::text,
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE(
              (item->>'quantity')::int,
              (item->>'input_quantity')::int,
              (item->>'cantidad')::int,
              (item->>'cantidad_entrada')::int,
              0
            )
          )
          FROM jsonb_array_elements(st.items) AS item
        ),
        0
      )::int,
      'items', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'sku', item->>'sku',
              'product', COALESCE(m.product, item->>'sku'),
              'quantity', COALESCE(
                (item->>'quantity')::int,
                (item->>'input_quantity')::int,
                (item->>'cantidad')::int,
                (item->>'cantidad_entrada')::int,
                0
              )
            )
          )
          FROM jsonb_array_elements(st.items) AS item
          LEFT JOIN medications m ON m.sku = item->>'sku'
          WHERE item->>'sku' IS NOT NULL
        ),
        '[]'::jsonb
      )
    ) as odv_data
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    ORDER BY szl.created_at
  ) sub;

  -- Get movements
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
    WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
  ) mov_stats,
  (
    SELECT jsonb_object_agg(type::text, suma_cantidad) as tipos
    FROM (
      SELECT mi.type, COALESCE(SUM(mi.quantity), 0)::int as suma_cantidad
      FROM public.inventory_movements mi
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
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
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
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

  -- Get collections
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
      'metadata', r.metadata
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
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 4: rpc_admin_get_all_visits — COALESCE for total_pieces
-- ─────────────────────────────────────────────────────────────────────────────
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
          COALESCE(
            (item->>'quantity')::int,
            (item->>'cantidad')::int,
            (item->>'cantidad_entrada')::int,
            0
          )
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


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 5: rpc_get_visit_odvs — COALESCE for quantity in items
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rpc_get_visit_odvs(p_visit_id uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_result jsonb;
BEGIN
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_result
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'type', szl.type::text,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'date', szl.created_at,
      'saga_id', st.id,
      'saga_type', st.type::text,
      'items', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'sku', item->>'sku',
              'product', COALESCE(m.product, item->>'sku'),
              'quantity', COALESCE(
                (item->>'quantity')::int,
                (item->>'input_quantity')::int,
                (item->>'cantidad')::int,
                (item->>'cantidad_entrada')::int,
                0
              )
            )
          )
          FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
          LEFT JOIN medications m ON m.sku = item->>'sku'
          WHERE item->>'sku' IS NOT NULL
        ),
        '[]'::jsonb
      ),
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE(
              (item->>'quantity')::int,
              (item->>'input_quantity')::int,
              (item->>'cantidad')::int,
              (item->>'cantidad_entrada')::int,
              0
            )
          )
          FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
        ),
        0
      )::int
    ) as odv_data
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    ORDER BY szl.created_at
  ) sub;

  RETURN v_result;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 6: rpc_get_post_cutoff_placement_items — COALESCE for is_holding
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rpc_get_post_cutoff_placement_items(p_visit_id uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_items jsonb;
  v_permanencia_skus text[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.visits WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_visit(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Get holding SKUs from CUTOFF for this visit (English + Spanish fallback)
  SELECT ARRAY_AGG(DISTINCT item->>'sku')
  INTO v_permanencia_skus
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'CUTOFF'
    AND st.items IS NOT NULL
    AND COALESCE(
      (item->>'is_holding')::boolean,
      (item->>'permanencia')::boolean,
      false
    ) = true;

  -- Also check inventory_movements with HOLDING type
  IF v_permanencia_skus IS NULL THEN
    SELECT ARRAY_AGG(DISTINCT mi.sku)
    INTO v_permanencia_skus
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text = 'HOLDING';
  END IF;

  -- First look in POST_CUTOFF_PLACEMENT
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_items IS NOT NULL THEN
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'quantity', COALESCE(
          (item->>'quantity')::int,
          (item->>'cantidad')::int,
          0
        ),
        'es_permanencia', COALESCE(
          (item->>'is_holding')::boolean,
          (item->>'es_permanencia')::boolean,
          (item->>'sku') = ANY(v_permanencia_skus)
        )
      )
    )
    INTO v_items
    FROM jsonb_array_elements(v_items) AS item
    WHERE COALESCE(
      (item->>'quantity')::int,
      (item->>'cantidad')::int,
      0
    ) > 0;

    RETURN COALESCE(v_items, '[]'::jsonb);
  END IF;

  -- Fallback: check inventory_movements with HOLDING
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', items.sku,
      'quantity', items.quantity,
      'es_permanencia', true
    )
  )
  INTO v_items
  FROM (
    SELECT mi.sku, COUNT(*)::int as quantity
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text = 'HOLDING'
    GROUP BY mi.sku
  ) items;

  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 7: rpc_confirm_saga_pivot — COALESCE for quantity in inline fallback
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.rpc_confirm_saga_pivot(p_saga_id uuid, p_zoho_id text DEFAULT NULL::text, p_zoho_items jsonb DEFAULT NULL::jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_saga record;
  v_zoho_link_id integer;
  v_task_tipo text;
  v_zoho_link_tipo zoho_link_type;
  v_item record;
  v_cantidad_antes integer;
  v_cantidad_despues integer;
  v_movement_type cabinet_movement_type;
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

  IF v_saga.status = 'CONFIRMED' THEN
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

  IF v_saga.status = 'CANCELLED_F' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determine types
  CASE v_saga.type::text
    WHEN 'INITIAL_PLACEMENT' THEN
      v_zoho_link_tipo := 'CABINET';
      v_task_tipo := 'ODV_CABINET';
      v_movement_type := 'PLACEMENT';
    WHEN 'POST_CUTOFF_PLACEMENT' THEN
      v_zoho_link_tipo := 'CABINET';
      v_task_tipo := 'ODV_CABINET';
      v_movement_type := 'PLACEMENT';
    WHEN 'SALE' THEN
      v_zoho_link_tipo := 'SALE';
      v_task_tipo := 'SALE_ODV';
      v_movement_type := 'SALE';
    WHEN 'COLLECTION' THEN
      v_zoho_link_tipo := 'RETURN';
      v_task_tipo := 'COLLECTION';
      v_movement_type := 'COLLECTION';
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado: %', v_saga.type;
  END CASE;

  -- 3. Change saga state (ONLY on first confirmation)
  IF NOT v_already_confirmed THEN
    UPDATE public.saga_transactions
    SET
      status = 'CONFIRMED'::saga_transaction_status,
      updated_at = now()
    WHERE id = p_saga_id;
  END IF;

  -- 4. Create/update saga_zoho_link
  IF p_zoho_id IS NOT NULL THEN
    INSERT INTO public.saga_zoho_links (
      id_saga_transaction,
      zoho_id,
      type,
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
        UPDATE inventory_movements
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND type != 'HOLDING'
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_zoho_items) elem
            WHERE elem->>'sku' = inventory_movements.sku
          );
      ELSE
        UPDATE inventory_movements
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND type != 'HOLDING';
      END IF;
    END IF;

    RETURN jsonb_build_object(
      'success', true,
      'already_confirmed', true,
      'saga_id', p_saga_id,
      'zoho_link_id', v_zoho_link_id
    );
  ELSE
    -- GUARD: Check if trigger already generated movements
    IF EXISTS (SELECT 1 FROM inventory_movements WHERE id_saga_transaction = p_saga_id) THEN
      IF v_zoho_link_id IS NOT NULL THEN
        UPDATE inventory_movements
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND type != 'HOLDING';
      END IF;
    ELSE
      -- Inline fallback: generate movements (comprehensive COALESCE)
      FOR v_item IN
        SELECT
          (item->>'sku')::varchar as sku,
          COALESCE(
            (item->>'quantity')::int,
            (item->>'input_quantity')::int,
            (item->>'output_quantity')::int,
            (item->>'cantidad')::int,
            (item->>'cantidad_entrada')::int,
            (item->>'cantidad_salida')::int,
            0
          ) as quantity
        FROM jsonb_array_elements(v_saga.items) as item
      LOOP
        SELECT COALESCE(available_quantity, 0)
        INTO v_cantidad_antes
        FROM public.cabinet_inventory
        WHERE client_id = v_saga.client_id AND sku = v_item.sku;

        IF v_cantidad_antes IS NULL THEN
          v_cantidad_antes := 0;
        END IF;

        IF v_movement_type = 'PLACEMENT' THEN
          v_cantidad_despues := v_cantidad_antes + v_item.quantity;
        ELSE
          v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.quantity);
        END IF;

        SELECT price INTO v_precio_unitario
        FROM public.medications
        WHERE sku = v_item.sku;

        INSERT INTO public.inventory_movements (
          id_saga_transaction,
          id_saga_zoho_link,
          client_id,
          sku,
          type,
          quantity,
          quantity_before,
          quantity_after,
          movement_date,
          unit_price
        )
        VALUES (
          p_saga_id,
          v_zoho_link_id,
          v_saga.client_id,
          v_item.sku,
          v_movement_type,
          v_item.quantity,
          v_cantidad_antes,
          v_cantidad_despues,
          now(),
          v_precio_unitario
        );

        IF v_cantidad_despues > 0 THEN
          INSERT INTO public.cabinet_inventory (client_id, sku, available_quantity, last_updated, unit_price)
          VALUES (v_saga.client_id, v_item.sku, v_cantidad_despues, now(), v_precio_unitario)
          ON CONFLICT (client_id, sku)
          DO UPDATE SET
            available_quantity = v_cantidad_despues,
            last_updated = now(),
            unit_price = COALESCE(v_precio_unitario, cabinet_inventory.unit_price);
        ELSE
          DELETE FROM public.cabinet_inventory
          WHERE client_id = v_saga.client_id AND sku = v_item.sku;
        END IF;

        IF v_saga.type::text = 'SALE' THEN
          DELETE FROM public.cabinet_client_available_skus
          WHERE client_id = v_saga.client_id AND sku = v_item.sku;
        END IF;
      END LOOP;
    END IF;
  END IF;

  -- 6. Update visit_tasks (ONLY on first confirmation)
  IF NOT v_already_confirmed THEN
    IF v_zoho_link_id IS NOT NULL THEN
      UPDATE public.visit_tasks
      SET
        status = 'COMPLETED',
        completed_at = COALESCE(completed_at, now()),
        reference_table = 'saga_zoho_links',
        reference_id = v_zoho_link_id::text,
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_type = v_task_tipo::visit_task_type;
    ELSE
      UPDATE public.visit_tasks
      SET
        status = 'COMPLETED',
        completed_at = COALESCE(completed_at, now()),
        last_activity_at = now()
      WHERE visit_id = v_saga.visit_id
      AND task_type = v_task_tipo::visit_task_type;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'zoho_link_id', v_zoho_link_id,
    'type', v_saga.type,
    'items_count', jsonb_array_length(v_saga.items)
  );
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 8: trigger_generate_movements_from_saga — COALESCE for quantity
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.trigger_generate_movements_from_saga() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_movement_type cabinet_movement_type;
  v_zoho_link_id integer;
  v_link_count integer;
  v_precio_unitario numeric;
BEGIN
  IF NEW.status != 'CONFIRMED' THEN
    RETURN NEW;
  END IF;

  IF EXISTS (SELECT 1 FROM inventory_movements WHERE id_saga_transaction = NEW.id) THEN
    RETURN NEW;
  END IF;

  IF NEW.items IS NULL OR jsonb_array_length(NEW.items) = 0 THEN
    RETURN NEW;
  END IF;

  SELECT COUNT(*) INTO v_link_count
  FROM saga_zoho_links WHERE id_saga_transaction = NEW.id;

  FOR v_item IN
    SELECT
      item->>'sku' as sku,
      COALESCE(
        (item->>'quantity')::int,
        (item->>'input_quantity')::int,
        (item->>'output_quantity')::int,
        (item->>'cantidad')::int,
        (item->>'cantidad_entrada')::int,
        (item->>'cantidad_salida')::int,
        0
      ) as quantity,
      item->>'movement_type' as movement_type
    FROM jsonb_array_elements(NEW.items) as item
  LOOP
    IF v_item.movement_type = 'HOLDING' THEN
      CONTINUE;
    END IF;

    SELECT COALESCE(quantity_after, 0)
    INTO v_cantidad_antes
    FROM inventory_movements
    WHERE client_id = NEW.client_id
      AND sku = v_item.sku
    ORDER BY movement_date DESC, id DESC
    LIMIT 1;

    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    CASE v_item.movement_type
      WHEN 'PLACEMENT' THEN
        v_movement_type := 'PLACEMENT';
        v_cantidad_despues := v_cantidad_antes + v_item.quantity;
      WHEN 'SALE' THEN
        v_movement_type := 'SALE';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.quantity);
      WHEN 'COLLECTION' THEN
        v_movement_type := 'COLLECTION';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.quantity);
      ELSE
        CONTINUE;
    END CASE;

    v_zoho_link_id := NULL;
    IF v_link_count = 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = NEW.id;
    ELSIF v_link_count > 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = NEW.id
        AND szl.items IS NOT NULL
        AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = v_item.sku)
      ORDER BY szl.id LIMIT 1;
    END IF;

    SELECT price INTO v_precio_unitario
    FROM medications
    WHERE sku = v_item.sku;

    INSERT INTO inventory_movements (
      id_saga_transaction, id_saga_zoho_link, client_id, sku, type,
      quantity, quantity_before, quantity_after, movement_date, unit_price
    ) VALUES (
      NEW.id, v_zoho_link_id, NEW.client_id, v_item.sku, v_movement_type,
      v_item.quantity, v_cantidad_antes, v_cantidad_despues,
      COALESCE(NEW.created_at, now()), v_precio_unitario
    );

    IF v_cantidad_despues > 0 THEN
      INSERT INTO cabinet_inventory (client_id, sku, available_quantity, last_updated, unit_price)
      VALUES (NEW.client_id, v_item.sku, v_cantidad_despues, now(), v_precio_unitario)
      ON CONFLICT (client_id, sku)
      DO UPDATE SET
        available_quantity = v_cantidad_despues,
        last_updated = now(),
        unit_price = COALESCE(v_precio_unitario, cabinet_inventory.unit_price);
    ELSE
      DELETE FROM cabinet_inventory
      WHERE client_id = NEW.client_id AND sku = v_item.sku;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- PART 9: rpc_get_visit_saga_summary — COALESCE for sold/vendido, collected/recolectado
-- This RPC reads from visit_tasks.metadata (original frontend payload) which
-- may have Spanish (vendido/recolectado) or English (sold/collected) fields.
-- ─────────────────────────────────────────────────────────────────────────────
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

  -- 1. CORTE: try visit_tasks.metadata first (English + Spanish fallback)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'sold', COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0),
      'collected', COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0),
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
    AND (
      COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0) > 0
      OR COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0) > 0
    );

  IF v_corte_items IS NOT NULL THEN
    SELECT
      COALESCE(SUM(COALESCE((item->>'sold')::int, (item->>'vendido')::int, 0)), 0)::int,
      COALESCE(SUM(COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0)), 0)::int
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
        'sold', COALESCE(
          (item->>'quantity')::int,
          (item->>'sold')::int,
          (item->>'vendido')::int,
          (item->>'cantidad')::int,
          0
        ),
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
          (item->>'quantity')::int,
          (item->>'output_quantity')::int,
          (item->>'cantidad_salida')::int,
          (item->>'cantidad')::int,
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

    SELECT COALESCE(SUM(COALESCE(
      (item->>'quantity')::int,
      (item->>'cantidad')::int,
      0
    )), 0)::int
    INTO v_total_sold
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'SALE'
      AND st.items IS NOT NULL;

    SELECT COALESCE(SUM(COALESCE(
      (item->>'quantity')::int,
      (item->>'output_quantity')::int,
      (item->>'cantidad_salida')::int,
      (item->>'cantidad')::int,
      0
    )), 0)::int
    INTO v_total_collected
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL;
  END IF;

  -- 2. INITIAL_PLACEMENT (English + Spanish fallback)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE(
        (item->>'quantity')::int,
        (item->>'input_quantity')::int,
        (item->>'cantidad_entrada')::int,
        (item->>'cantidad')::int,
        0
      )
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
    AND COALESCE(
      (item->>'quantity')::int,
      (item->>'input_quantity')::int,
      (item->>'cantidad_entrada')::int,
      (item->>'cantidad')::int,
      0
    ) > 0;

  SELECT COALESCE(SUM(COALESCE(
    (item->>'quantity')::int,
    (item->>'input_quantity')::int,
    (item->>'cantidad_entrada')::int,
    (item->>'cantidad')::int,
    0
  )), 0)::int
  INTO v_total_placement
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL;

  -- 3. POST_CUTOFF_PLACEMENT (English + Spanish fallback)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE(
        (item->>'quantity')::int,
        (item->>'cantidad')::int,
        0
      ),
      'is_holding', COALESCE(
        (item->>'is_holding')::boolean,
        (item->>'es_permanencia')::boolean,
        false
      )
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
    AND COALESCE((item->>'quantity')::int, (item->>'cantidad')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE(
    (item->>'quantity')::int,
    (item->>'cantidad')::int,
    0
  )), 0)::int
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
        SELECT COALESCE(SUM(COALESCE(
          (item->>'quantity')::int,
          (item->>'cantidad')::int,
          0
        )), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'product', COALESCE(m.product, item->>'sku'),
            'sold_quantity', COALESCE(
              (item->>'quantity')::int,
              (item->>'cantidad')::int,
              0
            )
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
        SELECT COALESCE(SUM(COALESCE(
          (item->>'quantity')::int,
          (item->>'input_quantity')::int,
          (item->>'cantidad_entrada')::int,
          (item->>'cantidad')::int,
          0
        )), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'product', COALESCE(m.product, item->>'sku'),
            'quantity', COALESCE(
              (item->>'quantity')::int,
              (item->>'input_quantity')::int,
              (item->>'cantidad_entrada')::int,
              (item->>'cantidad')::int,
              0
            )
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

  -- 7. Collection items (English + Spanish fallback from metadata)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE(
        (item->>'collected')::int,
        (item->>'recolectado')::int,
        0
      )
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
    AND COALESCE((item->>'collected')::int, (item->>'recolectado')::int, 0) > 0;

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
          (item->>'quantity')::int,
          (item->>'output_quantity')::int,
          (item->>'cantidad_salida')::int,
          (item->>'cantidad')::int,
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
      AND COALESCE(
        (item->>'quantity')::int,
        (item->>'output_quantity')::int,
        (item->>'cantidad_salida')::int,
        (item->>'cantidad')::int,
        0
      ) > 0;

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


-- ─────────────────────────────────────────────────────────────────────────────
-- Reload PostgREST schema cache
-- ─────────────────────────────────────────────────────────────────────────────
NOTIFY pgrst, 'reload schema';
