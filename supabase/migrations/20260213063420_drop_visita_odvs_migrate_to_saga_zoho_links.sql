-- Drop visita_odvs table and migrate all RPCs to use saga_zoho_links + saga_transactions
--
-- visita_odvs is a denormalized table that duplicates data from saga_zoho_links.
-- It stopped being populated by the current saga flow, causing 21 post-Nov 2025 visits
-- to show empty ODV data. This migration rewrites RPCs to use saga_zoho_links directly,
-- then drops the table.

---------------------------------------------------------------------
-- 1. rpc_get_odv_items(uuid)
--    Frontend: venta/ODV screens
--    Change: visita_odvs → saga_zoho_links JOIN saga_transactions
--    NOTE: This function is NOT changed — kept as-is from previous deploy
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.rpc_get_odv_items(p_visit_id uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'public'
    AS $$
DECLARE
  v_items jsonb;
  v_odv_ids jsonb;
  v_odv_id_list text[];
  v_sold_skus text[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.visitas WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_visita(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Get all ODV IDs from saga_zoho_links for this visit
  SELECT ARRAY_AGG(szl.zoho_id)
  INTO v_odv_id_list
  FROM public.saga_zoho_links szl
  JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
  WHERE st.visit_id = p_visit_id
    AND szl.tipo IN ('VENTA', 'BOTIQUIN');

  -- 1. Get items from movimientos_inventario with tipo = 'VENTA'
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', items.sku,
      'cantidad_vendida', items.cantidad_vendida
    )
  ),
  ARRAY_AGG(items.sku)
  INTO v_items, v_sold_skus
  FROM (
    SELECT mi.sku, SUM(mi.cantidad) as cantidad_vendida
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text = 'VENTA'
      AND mi.cantidad > 0
    GROUP BY mi.sku
  ) items;

  -- If found items from movimientos, get ODV IDs that have those SKUs
  IF v_items IS NOT NULL AND v_sold_skus IS NOT NULL THEN
    SELECT jsonb_agg(DISTINCT
      jsonb_build_object(
        'odv_id', odv_data.odv_id,
        'fecha', odv_data.fecha_odv,
        'total_piezas', odv_data.total_piezas
      )
    )
    INTO v_odv_ids
    FROM (
      SELECT vo.odv_id,
             MIN(vo.fecha) as fecha_odv,
             SUM(vo.cantidad) as total_piezas
      FROM public.ventas_odv vo
      WHERE vo.sku = ANY(v_sold_skus)
        AND vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.odv_id
    ) odv_data;
  END IF;

  -- 2. If no items from movimientos, try ventas_odv using ODV IDs
  IF v_items IS NULL AND v_odv_id_list IS NOT NULL THEN
    -- Get items from ventas_odv
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', items.sku,
        'cantidad_vendida', items.cantidad_vendida
      )
    )
    INTO v_items
    FROM (
      SELECT vo.sku, SUM(vo.cantidad) as cantidad_vendida
      FROM public.ventas_odv vo
      WHERE vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.sku
    ) items;

    -- Get ODV IDs with their actual totals from ventas_odv
    SELECT jsonb_agg(
      jsonb_build_object(
        'odv_id', odv_data.odv_id,
        'fecha', odv_data.fecha_odv,
        'total_piezas', odv_data.total_piezas
      ) ORDER BY odv_data.fecha_odv
    )
    INTO v_odv_ids
    FROM (
      SELECT vo.odv_id,
             MIN(vo.fecha) as fecha_odv,
             SUM(vo.cantidad) as total_piezas
      FROM public.ventas_odv vo
      WHERE vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.odv_id
    ) odv_data;
  END IF;

  -- 3. If still no items, try CORTE items with 'vendido'
  IF v_items IS NULL THEN
    SELECT st.items INTO v_items
    FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'CORTE'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0
    ORDER BY st.created_at DESC
    LIMIT 1;

    IF v_items IS NOT NULL THEN
      SELECT jsonb_agg(
        jsonb_build_object(
          'sku', item->>'sku',
          'cantidad_vendida', COALESCE((item->>'vendido')::int, 0)
        )
      )
      INTO v_items
      FROM jsonb_array_elements(v_items) AS item
      WHERE COALESCE((item->>'vendido')::int, 0) > 0;

      -- Get ODV IDs from saga_zoho_links for CORTE fallback
      SELECT jsonb_agg(
        jsonb_build_object(
          'odv_id', szl.zoho_id,
          'fecha', szl.created_at::date,
          'total_piezas', 0
        ) ORDER BY szl.created_at
      )
      INTO v_odv_ids
      FROM public.saga_zoho_links szl
      JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
      WHERE st.visit_id = p_visit_id
        AND szl.tipo IN ('VENTA', 'BOTIQUIN');
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'odv_ids', COALESCE(v_odv_ids, '[]'::jsonb),
    'odv_id', (
      SELECT szl.zoho_id
      FROM public.saga_zoho_links szl
      JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
      WHERE st.visit_id = p_visit_id
        AND szl.tipo IN ('VENTA', 'BOTIQUIN')
      ORDER BY szl.created_at
      LIMIT 1
    ),
    'items', COALESCE(v_items, '[]'::jsonb)
  );
END;
$$;

---------------------------------------------------------------------
-- 2. rpc_admin_get_visit_detail(uuid)
--    Admin panel: visit detail
--    Restored from remote_schema.sql with:
--    - SET search_path, saga_status, no saga_transactions section
--    - ODVs from saga_zoho_links + st.items JSONB
--    - Movimientos use mi.tipo (not tipo_legacy)
---------------------------------------------------------------------
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
  v_movimientos jsonb;
  v_informe jsonb;
  v_recolecciones jsonb;
BEGIN
  -- Verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  -- Get visit with client info
  SELECT jsonb_build_object(
    'visit_id', v.visit_id,
    'id_cliente', v.id_cliente,
    'nombre_cliente', c.nombre_cliente,
    'id_usuario', v.id_usuario,
    'nombre_usuario', u.nombre,
    'tipo', v.tipo::text,
    'estado', v.estado::text,
    'saga_status', COALESCE(
      CASE WHEN v.estado = 'COMPLETADO' THEN 'COMPLETED'
           WHEN v.estado = 'CANCELADO' THEN 'COMPENSATED'
           ELSE 'RUNNING' END,
      'RUNNING'
    ),
    'etiqueta', v.etiqueta,
    'created_at', v.created_at,
    'started_at', v.started_at,
    'completed_at', v.completed_at,
    'metadata', v.metadata
  ), v.id_cliente
  INTO v_visit, v_id_cliente
  FROM public.visitas v
  JOIN public.clientes c ON c.id_cliente = v.id_cliente
  LEFT JOIN public.usuarios u ON u.id_usuario = v.id_usuario
  WHERE v.visit_id = p_visit_id;

  IF v_visit IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Get visit_tasks
  SELECT jsonb_agg(row_data)
  INTO v_tasks
  FROM (
    SELECT jsonb_build_object(
      'task_id', COALESCE(vt.task_id::text, vt.task_tipo::text || '-' || p_visit_id::text),
      'task_tipo', vt.task_tipo::text,
      'estado', vt.estado::text,
      'required', vt.required,
      'created_at', vt.created_at,
      'started_at', vt.started_at,
      'completed_at', vt.completed_at,
      'due_at', vt.due_at,
      'metadata', vt.metadata,
      'transaction_type', CASE vt.task_tipo::text
        WHEN 'LEVANTAMIENTO_INICIAL' THEN 'COMPENSABLE'
        WHEN 'CORTE' THEN 'COMPENSABLE'
        WHEN 'LEV_POST_CORTE' THEN 'COMPENSABLE'
        WHEN 'ODV_BOTIQUIN' THEN 'PIVOT'
        WHEN 'VENTA_ODV' THEN 'PIVOT'
        ELSE 'RETRYABLE'
      END,
      'step_order', CASE vt.task_tipo::text
        WHEN 'LEVANTAMIENTO_INICIAL' THEN 1
        WHEN 'CORTE' THEN 1
        WHEN 'VENTA_ODV' THEN 2
        WHEN 'RECOLECCION' THEN 3
        WHEN 'LEV_POST_CORTE' THEN 4
        WHEN 'ODV_BOTIQUIN' THEN 5
        WHEN 'INFORME_VISITA' THEN 6
        ELSE 99
      END,
      'compensation_status', 'NOT_NEEDED'
    ) as row_data
    FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
    ORDER BY CASE vt.task_tipo::text
      WHEN 'LEVANTAMIENTO_INICIAL' THEN 1
      WHEN 'CORTE' THEN 1
      WHEN 'VENTA_ODV' THEN 2
      WHEN 'RECOLECCION' THEN 3
      WHEN 'LEV_POST_CORTE' THEN 4
      WHEN 'ODV_BOTIQUIN' THEN 5
      WHEN 'INFORME_VISITA' THEN 6
      ELSE 99
    END
  ) sub;

  -- Get ODVs from saga_zoho_links
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'tipo', szl.tipo::text,
      'fecha_odv', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'saga_tipo', st.tipo::text,
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE((item->>'cantidad')::int, (item->>'cantidad_entrada')::int, 0)
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
              'producto', COALESCE(m.producto, item->>'sku'),
              'cantidad', COALESCE((item->>'cantidad')::int, (item->>'cantidad_entrada')::int, 0)
            )
          )
          FROM jsonb_array_elements(st.items) AS item
          LEFT JOIN medicamentos m ON m.sku = item->>'sku'
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

  -- Get movimientos with detailed items
  -- by_tipo now shows SUM of cantidad per tipo (not count of movements)
  SELECT jsonb_build_object(
    'total', COALESCE(mov_stats.cnt, 0),
    'total_cantidad', COALESCE(mov_stats.suma_cantidad, 0),
    'unique_skus', COALESCE(mov_stats.skus_unicos, 0),
    'by_tipo', COALESCE(mov_tipos.tipos, '{}'::jsonb),
    'items', COALESCE(mov_items.items, '[]'::jsonb)
  )
  INTO v_movimientos
  FROM (
    SELECT
      COUNT(*)::int as cnt,
      COALESCE(SUM(mi.cantidad), 0)::int as suma_cantidad,
      COUNT(DISTINCT mi.sku)::int as skus_unicos
    FROM public.movimientos_inventario mi
    WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
  ) mov_stats,
  (
    -- SUM cantidad by tipo (not COUNT)
    SELECT jsonb_object_agg(tipo::text, suma_cantidad) as tipos
    FROM (
      SELECT mi.tipo, COALESCE(SUM(mi.cantidad), 0)::int as suma_cantidad
      FROM public.movimientos_inventario mi
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
      GROUP BY mi.tipo
    ) sub
  ) mov_tipos,
  (
    SELECT jsonb_agg(row_data) as items
    FROM (
      SELECT jsonb_build_object(
        'sku', mi.sku,
        'tipo', mi.tipo::text,
        'cantidad', mi.cantidad,
        'cantidad_antes', mi.cantidad_antes,
        'cantidad_despues', mi.cantidad_despues,
        'created_at', mi.fecha_movimiento
      ) as row_data
      FROM public.movimientos_inventario mi
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
      ORDER BY mi.fecha_movimiento
      LIMIT 100
    ) sub
  ) mov_items;

  -- Get informe de visita
  SELECT jsonb_build_object(
    'informe_id', vi.informe_id,
    'completada', vi.completada,
    'cumplimiento_score', vi.cumplimiento_score,
    'etiqueta', vi.etiqueta,
    'respuestas', vi.respuestas,
    'fecha_completada', vi.fecha_completada,
    'created_at', vi.created_at
  )
  INTO v_informe
  FROM public.visita_informes vi
  WHERE vi.visit_id = p_visit_id;

  -- Get recolecciones
  SELECT jsonb_agg(row_data)
  INTO v_recolecciones
  FROM (
    SELECT jsonb_build_object(
      'recoleccion_id', r.recoleccion_id,
      'estado', r.estado,
      'latitud', r.latitud,
      'longitud', r.longitud,
      'cedis_observaciones', r.cedis_observaciones,
      'cedis_responsable_nombre', r.cedis_responsable_nombre,
      'entregada_at', r.entregada_at,
      'created_at', r.created_at,
      'metadata', r.metadata
    ) as row_data
    FROM public.recolecciones r
    WHERE r.visit_id = p_visit_id
    ORDER BY r.created_at
  ) sub;

  RETURN jsonb_build_object(
    'visit', v_visit,
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'odvs', COALESCE(v_odvs, '[]'::jsonb),
    'movimientos', COALESCE(v_movimientos, '{"total": 0, "total_cantidad": 0, "unique_skus": 0, "by_tipo": {}, "items": []}'::jsonb),
    'informe', v_informe,
    'recolecciones', COALESCE(v_recolecciones, '[]'::jsonb)
  );
END;
$function$;

---------------------------------------------------------------------
-- 3. Drop 4-param overload of rpc_admin_get_all_visits
--    Only the 6-param version exists in remote_schema; frontend doesn't use 4-param
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS public.rpc_admin_get_all_visits(integer, integer, text, text);

---------------------------------------------------------------------
-- 4. rpc_admin_get_all_visits(6-param)
--    Admin panel: global visits list with date filters
--    Restored from remote_schema.sql with:
--    - SET search_path, saga_status
--    - No odvs_count, no id_ciclo
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.rpc_admin_get_all_visits(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_estado text DEFAULT NULL::text, p_search text DEFAULT NULL::text, p_fecha_desde date DEFAULT NULL::date, p_fecha_hasta date DEFAULT NULL::date)
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
  -- Verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  -- Get total count
  SELECT COUNT(*)
  INTO v_total
  FROM public.visitas v
  JOIN public.clientes c ON c.id_cliente = v.id_cliente
  WHERE (p_estado IS NULL OR v.estado::text = p_estado)
    AND (p_search IS NULL OR c.nombre_cliente ILIKE '%' || p_search || '%')
    AND (p_fecha_desde IS NULL OR v.created_at::date >= p_fecha_desde)
    AND (p_fecha_hasta IS NULL OR v.created_at::date <= p_fecha_hasta);

  -- Get visits with client info and resource counts
  SELECT jsonb_agg(row_data)
  INTO v_visits
  FROM (
    SELECT jsonb_build_object(
      'visit_id', v.visit_id,
      'id_cliente', v.id_cliente,
      'nombre_cliente', c.nombre_cliente,
      'id_usuario', v.id_usuario,
      'nombre_usuario', u.nombre,
      'tipo', v.tipo::text,
      'estado', v.estado::text,
      'saga_status', COALESCE(
        CASE WHEN v.estado = 'COMPLETADO' THEN 'COMPLETED'
             WHEN v.estado = 'CANCELADO' THEN 'COMPENSATED'
             ELSE 'RUNNING' END,
        'RUNNING'
      ),
      'etiqueta', v.etiqueta,
      'created_at', v.created_at,
      'started_at', v.started_at,
      'completed_at', v.completed_at,
      'metadata', v.metadata,
      'tasks_count', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id),
      'tasks_completed', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND vt.estado = 'COMPLETADO'),
      'sagas_count', (SELECT COUNT(*) FROM saga_transactions st WHERE st.visit_id = v.visit_id)
    ) as row_data
    FROM public.visitas v
    JOIN public.clientes c ON c.id_cliente = v.id_cliente
    LEFT JOIN public.usuarios u ON u.id_usuario = v.id_usuario
    WHERE (p_estado IS NULL OR v.estado::text = p_estado)
      AND (p_search IS NULL OR c.nombre_cliente ILIKE '%' || p_search || '%')
      AND (p_fecha_desde IS NULL OR v.created_at::date >= p_fecha_desde)
      AND (p_fecha_hasta IS NULL OR v.created_at::date <= p_fecha_hasta)
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

---------------------------------------------------------------------
-- 5. rpc_admin_rollback_visit(uuid, text)
--    Admin: visit rollback
--    Merged from remote_schema.sql (inventory restoration) + new fixes:
--    - Delete saga_zoho_links, saga_compensations, saga_adjustments before saga_transactions (FK NO ACTION)
--    - Remove visita_odvs step (table dropped)
--    - Allow OWNER role
--    - SET search_path TO 'public'
---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.rpc_admin_rollback_visit(p_visit_id uuid, p_razon text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_id_ciclo integer;
  v_saga_ids uuid[];
  v_recoleccion_ids uuid[];
  v_deleted_event_outbox int := 0;
  v_deleted_movimientos int := 0;
  v_deleted_zoho_links int := 0;
  v_deleted_compensations int := 0;
  v_deleted_adjustments int := 0;
  v_deleted_saga int := 0;
  v_deleted_tasks int := 0;
  v_deleted_recolecciones int := 0;
  v_deleted_rec_items int := 0;
  v_deleted_rec_firmas int := 0;
  v_deleted_rec_evidencias int := 0;
  v_deleted_informes int := 0;
  v_visit_data jsonb;
  -- Variables para restauración de inventario
  v_current_visit_had_lev_post_corte boolean := false;
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_restore_source text := NULL;
  v_count_inventario_restored int := 0;
  v_inventory_reverted boolean := false;
BEGIN
  -- Get current user and verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden ejecutar rollback de visitas';
  END IF;

  -- Get visit info and snapshot before deletion
  SELECT
    v.id_cliente,
    v.id_ciclo,
    jsonb_build_object(
      'visit_id', v.visit_id,
      'id_cliente', v.id_cliente,
      'id_usuario', v.id_usuario,
      'id_ciclo', v.id_ciclo,
      'tipo', v.tipo::text,
      'estado', v.estado::text,
      'created_at', v.created_at,
      'etiqueta', v.etiqueta,
      'metadata', v.metadata
    )
  INTO v_id_cliente, v_id_ciclo, v_visit_data
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- ============ RESTAURAR INVENTARIO ANTES DE BORRAR ============
  SELECT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.tipo = 'LEV_POST_CORTE'
      AND st.estado = 'CONFIRMADO'
  ) INTO v_current_visit_had_lev_post_corte;

  IF v_current_visit_had_lev_post_corte THEN
    -- Buscar la última visita COMPLETADA del mismo cliente (excluyendo la actual)
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.estado = 'COMPLETADO'
      AND v.tipo IN ('VISITA_CORTE', 'VISITA_LEVANTAMIENTO_INICIAL')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      -- Intentar LEV_POST_CORTE primero
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.tipo = 'LEV_POST_CORTE'
        AND st.estado = 'CONFIRMADO'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        -- Fallback a LEVANTAMIENTO_INICIAL
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.tipo = 'LEVANTAMIENTO_INICIAL'
          AND st.estado = 'CONFIRMADO'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- Restaurar inventario si encontramos items
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      DELETE FROM public.inventario_botiquin WHERE id_cliente = v_id_cliente;

      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'cantidad')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      INSERT INTO public.botiquin_clientes_sku_disponibles (id_cliente, sku, fecha_ingreso)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0
      ON CONFLICT (id_cliente, sku) DO NOTHING;

      v_inventory_reverted := true;
    ELSE
      v_restore_source := 'Sin visita completada anterior - inventario no modificado';
    END IF;
  END IF;
  -- ============ FIN RESTAURACIÓN DE INVENTARIO ============

  -- Get saga transaction IDs for this visit
  SELECT ARRAY_AGG(st.id) INTO v_saga_ids
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id;

  -- Get recoleccion IDs for this visit
  SELECT ARRAY_AGG(r.recoleccion_id) INTO v_recoleccion_ids
  FROM public.recolecciones r
  WHERE r.visit_id = p_visit_id;

  -- DELETE IN ORDER (child tables first)

  IF v_saga_ids IS NOT NULL THEN
    -- 1. Delete event_outbox (references saga_transactions)
    WITH deleted AS (
      DELETE FROM public.event_outbox
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_event_outbox FROM deleted;

    -- 2. Delete movimientos_inventario (references saga_transactions)
    WITH deleted AS (
      DELETE FROM public.movimientos_inventario
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_movimientos FROM deleted;

    -- 3. Delete saga_zoho_links (references saga_transactions, FK NO ACTION)
    WITH deleted AS (
      DELETE FROM public.saga_zoho_links
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_zoho_links FROM deleted;

    -- 4. Delete saga_compensations (references saga_transactions, FK NO ACTION)
    WITH deleted AS (
      DELETE FROM public.saga_compensations
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_compensations FROM deleted;

    -- 5. Delete saga_adjustments (references saga_transactions, FK NO ACTION)
    WITH deleted AS (
      DELETE FROM public.saga_adjustments
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_adjustments FROM deleted;

    -- 6. Delete saga_transactions (now safe — all children removed)
    WITH deleted AS (
      DELETE FROM public.saga_transactions
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_saga FROM deleted;
  END IF;

  -- 7. Delete visit_tasks
  WITH deleted AS (
    DELETE FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_tasks FROM deleted;

  -- 8. Delete recolecciones and related tables
  IF v_recoleccion_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.recolecciones_evidencias
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_evidencias FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_firmas
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_firmas FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_items
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_items FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_recolecciones FROM deleted;
  END IF;

  -- 9. Delete visita_informes
  WITH deleted AS (
    DELETE FROM public.visita_informes
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_informes FROM deleted;

  -- 10. Update visit status to CANCELADO
  UPDATE public.visitas
  SET
    estado = 'CANCELADO',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'rollback_at', NOW(),
      'rollback_by', v_user_id,
      'rollback_razon', p_razon,
      'rollback_deleted', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored
    )
  WHERE visit_id = p_visit_id;

  -- LOG TO AUDIT_LOG
  INSERT INTO public.audit_log (
    tabla,
    registro_id,
    accion,
    usuario_id,
    valores_antes,
    valores_despues
  )
  VALUES (
    'visitas',
    p_visit_id::text,
    'DELETE',
    v_user_id,
    v_visit_data || jsonb_build_object(
      'accion_tipo', 'ADMIN_ROLLBACK',
      'razon', p_razon
    ),
    jsonb_build_object(
      'deleted_counts', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored,
      'executed_at', NOW(),
      'executed_by', v_user_id
    )
  );

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'id_cliente', v_id_cliente,
    'id_ciclo', v_id_ciclo,
    'executed_by', v_user_id,
    'razon', p_razon,
    'deleted', jsonb_build_object(
      'event_outbox', v_deleted_event_outbox,
      'movimientos_inventario', v_deleted_movimientos,
      'saga_zoho_links', v_deleted_zoho_links,
      'saga_compensations', v_deleted_compensations,
      'saga_adjustments', v_deleted_adjustments,
      'saga_transactions', v_deleted_saga,
      'visit_tasks', v_deleted_tasks,
      'recolecciones', v_deleted_recolecciones,
      'recolecciones_items', v_deleted_rec_items,
      'recolecciones_firmas', v_deleted_rec_firmas,
      'recolecciones_evidencias', v_deleted_rec_evidencias,
      'visita_informes', v_deleted_informes
    ),
    'inventory_reverted', v_inventory_reverted,
    'inventory_restore_source', v_restore_source,
    'inventory_items_restored', v_count_inventario_restored,
    'last_completed_visit_id', v_last_completed_visit_id,
    'message', 'Rollback completado exitosamente'
  );
END;
$function$;

---------------------------------------------------------------------
-- 6. Drop visita_odvs table (all references removed above)
---------------------------------------------------------------------
DROP POLICY IF EXISTS "Users can view their visita_odvs" ON public.visita_odvs;
DROP TABLE IF EXISTS public.visita_odvs CASCADE;
