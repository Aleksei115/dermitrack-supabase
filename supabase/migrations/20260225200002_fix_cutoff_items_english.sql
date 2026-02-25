-- Fix: rpc_get_cutoff_items returns Spanish field names (vendido, recolectado, etc.)
-- Update to return English field names (sold, collected, current_quantity, is_holding)

CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_items(p_visit_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_items jsonb;
BEGIN
  -- Verificar que la visita existe
  IF NOT EXISTS (SELECT 1 FROM public.visits WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar acceso a la visita
  IF NOT public.can_access_visit(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- 1. Primero buscar en visit_tasks metadata del CORTE (nuevo formato)
  SELECT vt.metadata->'items' INTO v_items
  FROM public.visit_tasks vt
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = 'CUTOFF'
    AND vt.status = 'COMPLETED'
    AND vt.metadata->'items' IS NOT NULL
    AND jsonb_array_length(vt.metadata->'items') > 0;

  IF v_items IS NOT NULL THEN
    RETURN v_items;
  END IF;

  -- 2. Buscar en saga_transactions type CORTE (formato legacy)
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'CUTOFF'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_items IS NOT NULL THEN
    RETURN v_items;
  END IF;

  -- 3. Combinar items de sagas SALE y COLLECTION con nombres en ingles
  SELECT jsonb_agg(combined_item)
  INTO v_items
  FROM (
    -- Items de SALE: quantity representa lo vendido
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'current_quantity', 0,
      'sold', COALESCE((item->>'quantity')::int, (item->>'sold')::int, (item->>'vendido')::int, 0),
      'collected', 0,
      'is_holding', false
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'SALE'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0

    UNION ALL

    -- Items de COLLECTION: output_quantity/cantidad_salida representa lo recolectado
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'current_quantity', 0,
      'sold', 0,
      'collected', COALESCE(
        (item->>'output_quantity')::int,
        (item->>'cantidad_salida')::int,
        (item->>'collected')::int,
        (item->>'recolectado')::int,
        (item->>'quantity')::int,
        0
      ),
      'is_holding', COALESCE((item->>'holding_quantity')::int, (item->>'cantidad_permanencia')::int, 0) > 0
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0
  ) items
  WHERE (combined_item->>'sold')::int > 0
     OR (combined_item->>'collected')::int > 0;

  IF v_items IS NOT NULL AND jsonb_array_length(v_items) > 0 THEN
    RETURN v_items;
  END IF;

  -- 4. Buscar en inventory_movements (usando type semántico)
  SELECT jsonb_agg(item_data)
  INTO v_items
  FROM (
    SELECT jsonb_build_object(
      'sku', mi.sku,
      'current_quantity', 0,
      'sold', COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0),
      'collected', COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0),
      'is_holding', CASE WHEN SUM(CASE WHEN mi.type::text = 'HOLDING' THEN 1 ELSE 0 END) > 0 THEN true ELSE false END
    ) as item_data
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text IN ('SALE', 'COLLECTION', 'HOLDING')
    GROUP BY mi.sku
    HAVING SUM(CASE WHEN mi.type::text IN ('SALE', 'COLLECTION') THEN mi.quantity ELSE 0 END) > 0
       OR SUM(CASE WHEN mi.type::text = 'HOLDING' THEN 1 ELSE 0 END) > 0
  ) items;

  -- Retornar items o array vacío
  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$$;
