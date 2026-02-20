-- Fix: ODV items source in rpc_get_visit_saga_summary and rpc_get_visit_odvs
--
-- When a saga has multiple ODVs, each ODV was showing ALL items from the saga
-- (st.items) instead of only its own items (szl.items). This caused duplicate
-- items across ODVs.
--
-- Change: Use COALESCE(szl.items, st.items) so per-ODV items from
-- saga_zoho_links are used when available, falling back to saga_transactions.items
-- for legacy data.

-- 1. Fix rpc_get_visit_odvs
CREATE OR REPLACE FUNCTION public.rpc_get_visit_odvs(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result jsonb;
BEGIN
  -- Obtener todas las ODVs vinculadas a la visita a través de saga_zoho_links
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_result
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'tipo', szl.tipo::text,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'fecha', szl.created_at,
      'saga_id', st.id,
      'saga_tipo', st.tipo::text,
      'items', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'sku', item->>'sku',
              'producto', COALESCE(m.producto, item->>'sku'),
              'cantidad', COALESCE(
                (item->>'cantidad')::int,
                (item->>'cantidad_entrada')::int,
                0
              )
            )
          )
          FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
          LEFT JOIN medicamentos m ON m.sku = item->>'sku'
          WHERE item->>'sku' IS NOT NULL
        ),
        '[]'::jsonb
      ),
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE(
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
$function$;


-- 2. Fix rpc_get_visit_saga_summary
CREATE OR REPLACE FUNCTION public.rpc_get_visit_saga_summary(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente text;
  v_visit_tipo text;
  v_corte_items jsonb;
  v_levantamiento_items jsonb;
  v_lev_post_corte_items jsonb;
  v_odvs_venta jsonb;
  v_odvs_botiquin jsonb;
  v_movimientos_resumen jsonb;
  v_recoleccion_items jsonb;
  v_has_movimientos boolean;
  v_mov_total_count int;
  v_mov_total_cantidad int;
  v_mov_unique_skus int;
  v_mov_by_tipo jsonb;
  v_total_vendido int;
  v_total_recolectado int;
  v_total_levantamiento int;
  v_total_lev_post_corte int;
  v_total_recoleccion int;
BEGIN
  -- Verificar que la visita existe y obtener info básica
  SELECT v.id_cliente, v.tipo::text
  INTO v_id_cliente, v_visit_tipo
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar acceso
  IF NOT public.can_access_visita(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Verificar si hay movimientos_inventario (fuente de verdad)
  SELECT EXISTS(
    SELECT 1 FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
  ) INTO v_has_movimientos;

  -- 1. CORTE: FIRST try to read from visit_tasks.metadata (most reliable source)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'vendido', COALESCE((item->>'vendido')::int, 0),
      'recolectado', COALESCE((item->>'recolectado')::int, 0),
      'permanencia', 0
    )
  )
  INTO v_corte_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_tipo = 'CORTE'
    AND vt.estado = 'COMPLETADO'
    AND vt.metadata->'items' IS NOT NULL
    AND jsonb_array_length(vt.metadata->'items') > 0
    AND (COALESCE((item->>'vendido')::int, 0) > 0 OR COALESCE((item->>'recolectado')::int, 0) > 0);

  IF v_corte_items IS NOT NULL THEN
    -- Calculate totals from the corte items
    SELECT
      COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0)::int,
      COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.visit_tasks vt,
         jsonb_array_elements(vt.metadata->'items') AS item
    WHERE vt.visit_id = p_visit_id
      AND vt.task_tipo = 'CORTE'
      AND vt.estado = 'COMPLETADO';
  ELSIF v_has_movimientos THEN
    -- FALLBACK: Use movimientos_inventario if visit_tasks.metadata is empty
    SELECT jsonb_agg(item_data)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'producto', producto,
        'vendido', vendido,
        'recolectado', recolectado,
        'permanencia', permanencia
      ) as item_data
      FROM (
        SELECT
          mi.sku,
          COALESCE(m.producto, mi.sku) as producto,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'VENTA' THEN mi.cantidad ELSE 0 END), 0)::int as vendido,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'RECOLECCION' THEN mi.cantidad ELSE 0 END), 0)::int as recolectado,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN mi.cantidad ELSE 0 END), 0)::int as permanencia
        FROM public.movimientos_inventario mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medicamentos m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.tipo::text IN ('VENTA', 'RECOLECCION', 'PERMANENCIA')
        GROUP BY mi.sku, m.producto
        -- FIX: Use mi.cantidad for PERMANENCIA in HAVING clause
        HAVING SUM(CASE WHEN mi.tipo::text IN ('VENTA', 'RECOLECCION') THEN mi.cantidad ELSE 0 END) > 0
           OR SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN mi.cantidad ELSE 0 END) > 0
      ) grouped
    ) items;

    SELECT
      COALESCE(SUM(CASE WHEN mi.tipo::text = 'VENTA' THEN mi.cantidad ELSE 0 END), 0)::int,
      COALESCE(SUM(CASE WHEN mi.tipo::text = 'RECOLECCION' THEN mi.cantidad ELSE 0 END), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text IN ('VENTA', 'RECOLECCION');
  ELSE
    -- FALLBACK: Use saga_transactions.items
    SELECT jsonb_agg(combined_item)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'vendido', COALESCE((item->>'cantidad')::int, (item->>'vendido')::int, 0),
        'recolectado', 0,
        'permanencia', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.tipo::text = 'VENTA'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0

      UNION ALL

      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'vendido', 0,
        'recolectado', COALESCE(
          (item->>'cantidad_salida')::int,
          (item->>'cantidad')::int,
          0
        ),
        'permanencia', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.tipo::text = 'RECOLECCION'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
    ) items
    WHERE (combined_item->>'vendido')::int > 0
       OR (combined_item->>'recolectado')::int > 0;

    SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
    INTO v_total_vendido
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'VENTA'
      AND st.items IS NOT NULL;

    SELECT COALESCE(SUM(COALESCE((item->>'cantidad_salida')::int, (item->>'cantidad')::int, 0)), 0)::int
    INTO v_total_recolectado
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'RECOLECCION'
      AND st.items IS NOT NULL;
  END IF;

  -- 2. LEVANTAMIENTO_INICIAL
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
    )
  )
  INTO v_levantamiento_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)), 0)::int
  INTO v_total_levantamiento
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
    AND st.items IS NOT NULL;

  -- 3. LEV_POST_CORTE
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'cantidad')::int, 0),
      'es_permanencia', COALESCE((item->>'es_permanencia')::boolean, false)
    )
  )
  INTO v_lev_post_corte_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEV_POST_CORTE'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
  INTO v_total_lev_post_corte
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEV_POST_CORTE'
    AND st.items IS NOT NULL;

  -- 4. ODVs de VENTA (usando saga_zoho_links.tipo = 'VENTA')
  --    FIX: Use COALESCE(szl.items, st.items) for per-ODV items
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_venta
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'fecha', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'tipo', szl.tipo::text,
      'total_piezas', (
        SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'producto', COALESCE(m.producto, item->>'sku'),
            'cantidad_vendida', COALESCE((item->>'cantidad')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
        LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.tipo::text = 'VENTA'
    ORDER BY szl.created_at
  ) sub;

  -- 5. ODVs de BOTIQUIN (usando saga_zoho_links.tipo = 'BOTIQUIN')
  --    FIX: Use COALESCE(szl.items, st.items) for per-ODV items
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_botiquin
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'fecha', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'tipo', szl.tipo::text,
      'total_piezas', (
        SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'producto', COALESCE(m.producto, item->>'sku'),
            'cantidad', COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
        LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.tipo::text = 'BOTIQUIN'
    ORDER BY szl.created_at
  ) sub;

  -- 6. Resumen de movimientos
  SELECT
    COALESCE(COUNT(*)::int, 0),
    COALESCE(SUM(mi.cantidad)::int, 0),
    COALESCE(COUNT(DISTINCT mi.sku)::int, 0)
  INTO v_mov_total_count, v_mov_total_cantidad, v_mov_unique_skus
  FROM public.movimientos_inventario mi
  JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
  WHERE st.visit_id = p_visit_id;

  SELECT COALESCE(jsonb_object_agg(tipo_text, suma), '{}'::jsonb)
  INTO v_mov_by_tipo
  FROM (
    SELECT mi.tipo::text as tipo_text, SUM(mi.cantidad)::int as suma
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    GROUP BY mi.tipo
  ) sub;

  v_movimientos_resumen := jsonb_build_object(
    'total_movimientos', v_mov_total_count,
    'total_cantidad', v_mov_total_cantidad,
    'unique_skus', v_mov_unique_skus,
    'by_tipo', v_mov_by_tipo
  );

  -- 7. Items de recolección (from CORTE task metadata or movimientos)
  -- First try visit_tasks.metadata from CORTE (filtered to recolectado > 0)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'recolectado')::int, 0)
    )
  )
  INTO v_recoleccion_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_tipo = 'CORTE'
    AND vt.estado = 'COMPLETADO'
    AND vt.metadata->'items' IS NOT NULL
    AND COALESCE((item->>'recolectado')::int, 0) > 0;

  IF v_recoleccion_items IS NOT NULL THEN
    v_total_recoleccion := v_total_recolectado;
  ELSIF v_has_movimientos THEN
    SELECT jsonb_agg(item_data)
    INTO v_recoleccion_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'producto', producto,
        'cantidad', cantidad
      ) as item_data
      FROM (
        SELECT
          mi.sku,
          COALESCE(m.producto, mi.sku) as producto,
          SUM(mi.cantidad)::int as cantidad
        FROM public.movimientos_inventario mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medicamentos m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.tipo::text = 'RECOLECCION'
        GROUP BY mi.sku, m.producto
        HAVING SUM(mi.cantidad) > 0
      ) grouped
    ) items;

    v_total_recoleccion := v_total_recolectado;
  ELSE
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'cantidad', COALESCE(
          (item->>'cantidad_salida')::int,
          (item->>'cantidad')::int,
          0
        )
      )
    )
    INTO v_recoleccion_items
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'RECOLECCION'
      AND st.items IS NOT NULL
      AND COALESCE((item->>'cantidad_salida')::int, (item->>'cantidad')::int, 0) > 0;

    v_total_recoleccion := v_total_recolectado;
  END IF;

  RETURN jsonb_build_object(
    'visit_id', p_visit_id,
    'visit_tipo', v_visit_tipo,
    'id_cliente', v_id_cliente,
    'corte', jsonb_build_object(
      'items', COALESCE(v_corte_items, '[]'::jsonb),
      'total_vendido', COALESCE(v_total_vendido, 0),
      'total_recolectado', COALESCE(v_total_recolectado, 0)
    ),
    'levantamiento', jsonb_build_object(
      'items', COALESCE(v_levantamiento_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_levantamiento, 0)
    ),
    'lev_post_corte', jsonb_build_object(
      'items', COALESCE(v_lev_post_corte_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_lev_post_corte, 0)
    ),
    'recoleccion', jsonb_build_object(
      'items', COALESCE(v_recoleccion_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_recoleccion, 0)
    ),
    'odvs', jsonb_build_object(
      'venta', COALESCE(v_odvs_venta, '[]'::jsonb),
      'botiquin', COALESCE(v_odvs_botiquin, '[]'::jsonb),
      'all', COALESCE(v_odvs_venta, '[]'::jsonb) || COALESCE(v_odvs_botiquin, '[]'::jsonb)
    ),
    'movimientos', v_movimientos_resumen
  );
END;
$function$;
