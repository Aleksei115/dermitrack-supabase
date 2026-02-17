-- Auditoria de Grafos Dirigidos: Visitas × Clientes
-- Traza el path completo: CLIENT → VISIT → TASK → { saga, zoho_link, odv_lines, recolecciones, informes, movimientos }
-- Detecta anomalias automaticamente segun 12 reglas de integridad

CREATE OR REPLACE FUNCTION public.audit_visit_graph(p_id_cliente TEXT DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = public
AS $$
DECLARE
  v_result JSONB := '[]'::JSONB;
  v_visit RECORD;
  v_visit_json JSONB;
  v_tasks JSONB;
  v_sagas JSONB;
  v_recoleccion JSONB;
  v_informe JSONB;
  v_visita_odvs_json JSONB;
  v_anomalias TEXT[] := '{}';
  v_task RECORD;
  v_saga RECORD;
  v_link RECORD;
  v_ref_exists BOOLEAN;
  v_saga_items_count INT;
  v_saga_total_qty INT;
  v_saga_format TEXT;
  v_mov_summary JSONB;
  v_links_json JSONB;
  v_odv_lines INT;
  v_odv_total_qty INT;
  v_saga_odv_total_qty INT;
  v_rec RECORD;
  v_inf RECORD;
  v_has_movimientos BOOLEAN;
BEGIN
  -- Iterate over visits (optionally filtered by client)
  FOR v_visit IN
    SELECT v.visit_id, v.id_cliente, v.tipo::TEXT, v.estado::TEXT, v.created_at
    FROM public.visitas v
    WHERE (p_id_cliente IS NULL OR v.id_cliente = p_id_cliente)
    ORDER BY v.id_cliente, v.created_at
  LOOP
    v_anomalias := '{}';

    -- =====================
    -- TASKS
    -- =====================
    v_tasks := '[]'::JSONB;
    FOR v_task IN
      SELECT
        vt.task_tipo::TEXT,
        vt.estado::TEXT AS task_estado,
        vt.reference_table,
        vt.reference_id,
        vt.transaction_type::TEXT,
        vt.step_order
      FROM public.visit_tasks vt
      WHERE vt.visit_id = v_visit.visit_id
      ORDER BY vt.step_order
    LOOP
      -- Check reference_id validity
      v_ref_exists := NULL;
      IF v_task.reference_table IS NOT NULL AND v_task.reference_id IS NOT NULL THEN
        IF v_task.reference_table = 'saga_zoho_links' THEN
          SELECT EXISTS(
            SELECT 1 FROM public.saga_zoho_links WHERE id = v_task.reference_id::INT
          ) INTO v_ref_exists;
        ELSIF v_task.reference_table = 'saga_transactions' THEN
          SELECT EXISTS(
            SELECT 1 FROM public.saga_transactions WHERE id = v_task.reference_id::UUID
          ) INTO v_ref_exists;
        END IF;
      END IF;

      -- ANOMALY 1: Task COMPLETADO with reference_table but no reference_id
      IF v_task.task_estado = 'COMPLETADO'
         AND v_task.reference_table IS NOT NULL
         AND v_task.reference_id IS NULL
         AND v_task.task_tipo IN ('VENTA_ODV', 'ODV_BOTIQUIN')
      THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: ' || v_task.task_tipo || ' COMPLETADO con reference_table=' || v_task.reference_table || ' pero sin reference_id');
      END IF;

      -- ANOMALY 2: reference_id points to non-existent record
      IF v_ref_exists IS NOT NULL AND NOT v_ref_exists THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: ' || v_task.task_tipo || ' reference_id=' || v_task.reference_id || ' no existe en ' || v_task.reference_table);
      END IF;

      v_tasks := v_tasks || jsonb_build_object(
        'task_tipo', v_task.task_tipo,
        'estado', v_task.task_estado,
        'reference_table', v_task.reference_table,
        'reference_id', v_task.reference_id,
        'ref_exists', v_ref_exists,
        'transaction_type', v_task.transaction_type
      );
    END LOOP;

    -- =====================
    -- SAGAS
    -- =====================
    v_sagas := '[]'::JSONB;
    FOR v_saga IN
      SELECT
        st.id AS saga_id,
        st.tipo::TEXT AS saga_tipo,
        st.estado::TEXT AS saga_estado,
        st.items,
        st.created_at
      FROM public.saga_transactions st
      WHERE st.visit_id = v_visit.visit_id
      ORDER BY st.created_at
    LOOP
      -- Determine format
      IF v_saga.items IS NULL OR v_saga.items = 'null'::JSONB THEN
        v_saga_format := 'EMPTY';
        v_saga_items_count := 0;
        v_saga_total_qty := 0;
      ELSIF jsonb_typeof(v_saga.items) = 'array' THEN
        SELECT COUNT(*), COALESCE(SUM((item->>'cantidad')::INT), 0)
        INTO v_saga_items_count, v_saga_total_qty
        FROM jsonb_array_elements(v_saga.items) AS item
        WHERE item ? 'sku';

        IF v_saga_items_count = 0 THEN
          -- Check if LEGACY format (has total_salida key)
          IF v_saga.items->0 ? 'total_salida' THEN
            v_saga_format := 'LEGACY';
            v_saga_total_qty := COALESCE((v_saga.items->0->>'total_salida')::INT, 0);
            v_saga_items_count := 0;
          ELSE
            v_saga_format := 'EMPTY';
          END IF;
        ELSE
          v_saga_format := 'NEW';
        END IF;
      ELSIF jsonb_typeof(v_saga.items) = 'object' THEN
        -- LEGACY single-object format
        v_saga_format := 'LEGACY';
        v_saga_total_qty := COALESCE((v_saga.items->>'total_salida')::INT, 0);
        v_saga_items_count := 0;
      ELSE
        v_saga_format := 'UNKNOWN';
        v_saga_items_count := 0;
        v_saga_total_qty := 0;
      END IF;

      -- Zoho links for this saga
      v_links_json := '[]'::JSONB;
      FOR v_link IN
        SELECT
          szl.id AS link_id,
          szl.tipo::TEXT AS link_tipo,
          szl.zoho_id,
          szl.zoho_sync_status
        FROM public.saga_zoho_links szl
        WHERE szl.id_saga_transaction = v_saga.saga_id
        ORDER BY szl.id
      LOOP
        -- Count ODV lines based on link type
        v_odv_lines := 0;
        v_odv_total_qty := 0;
        IF v_link.link_tipo = 'VENTA' THEN
          SELECT COUNT(*), COALESCE(SUM(cantidad), 0)
          INTO v_odv_lines, v_odv_total_qty
          FROM public.ventas_odv
          WHERE odv_id = v_link.zoho_id;
        ELSIF v_link.link_tipo = 'BOTIQUIN' THEN
          SELECT COUNT(*), COALESCE(SUM(cantidad), 0)
          INTO v_odv_lines, v_odv_total_qty
          FROM public.botiquin_odv
          WHERE odv_id = v_link.zoho_id;
        ELSIF v_link.link_tipo = 'DEVOLUCION' THEN
          -- Devoluciones don't have a separate ODV table
          v_odv_lines := 0;
          v_odv_total_qty := 0;
        END IF;

        v_links_json := v_links_json || jsonb_build_object(
          'link_id', v_link.link_id,
          'tipo', v_link.link_tipo,
          'zoho_id', v_link.zoho_id,
          'sync_status', v_link.zoho_sync_status,
          'odv_lines', v_odv_lines,
          'odv_total_qty', v_odv_total_qty
        );
      END LOOP;

      -- ANOMALY 5: Saga qty vs ODV qty mismatch (sum across ALL VENTA links for this saga)
      IF v_saga.saga_tipo = 'VENTA' AND v_saga.saga_estado = 'CONFIRMADO' AND v_saga_total_qty > 0 THEN
        SELECT COALESCE(SUM(vo.cantidad), 0)
        INTO v_saga_odv_total_qty
        FROM public.saga_zoho_links szl
        JOIN public.ventas_odv vo ON vo.odv_id = szl.zoho_id
        WHERE szl.id_saga_transaction = v_saga.saga_id
          AND szl.tipo = 'VENTA';

        IF v_saga_odv_total_qty > 0 AND v_saga_total_qty <> v_saga_odv_total_qty THEN
          v_anomalias := array_append(v_anomalias,
            'WARN: saga VENTA ' || v_saga.saga_id::TEXT || ' tiene ' || v_saga_total_qty || ' pzs pero suma ODV = ' || v_saga_odv_total_qty || ' pzs');
        END IF;
      END IF;

      -- Movimientos for this saga, grouped by tipo
      SELECT jsonb_object_agg(
        tipo_group,
        jsonb_build_object('count', cnt, 'total_qty', total_q)
      )
      INTO v_mov_summary
      FROM (
        SELECT
          mi.tipo::TEXT AS tipo_group,
          COUNT(*) AS cnt,
          SUM(mi.cantidad) AS total_q
        FROM public.movimientos_inventario mi
        WHERE mi.id_saga_transaction = v_saga.saga_id
        GROUP BY mi.tipo
      ) sub;

      IF v_mov_summary IS NULL THEN
        v_mov_summary := '{}'::JSONB;
      END IF;

      -- ANOMALY 3: Saga CONFIRMADO without movimientos
      v_has_movimientos := (v_mov_summary <> '{}'::JSONB);
      IF v_saga.saga_estado = 'CONFIRMADO' AND NOT v_has_movimientos
         AND v_saga.saga_tipo IN ('VENTA', 'RECOLECCION', 'LEVANTAMIENTO_INICIAL', 'LEV_POST_CORTE')
      THEN
        v_anomalias := array_append(v_anomalias,
          'WARN: saga ' || v_saga.saga_tipo || ' ' || v_saga.saga_id::TEXT || ' CONFIRMADO sin movimientos_inventario');
      END IF;

      -- ANOMALY 4: Saga VENTA CONFIRMADO without VENTA zoho_link
      IF v_saga.saga_tipo = 'VENTA' AND v_saga.saga_estado = 'CONFIRMADO' THEN
        IF NOT EXISTS (
          SELECT 1 FROM public.saga_zoho_links
          WHERE id_saga_transaction = v_saga.saga_id AND tipo = 'VENTA'
        ) THEN
          v_anomalias := array_append(v_anomalias,
            'WARN: saga VENTA ' || v_saga.saga_id::TEXT || ' CONFIRMADO sin saga_zoho_links tipo VENTA');
        END IF;
      END IF;

      -- ANOMALY 8: LEGACY format with total_salida=0 but CONFIRMADO
      IF v_saga_format = 'LEGACY' AND v_saga_total_qty = 0 AND v_saga.saga_estado = 'CONFIRMADO' THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: saga ' || v_saga.saga_tipo || ' ' || v_saga.saga_id::TEXT || ' LEGACY con total_salida=0 en estado CONFIRMADO (deberia ser CANCELADA)');
      END IF;

      -- ANOMALY 9: EMPTY saga in CONFIRMADO
      IF v_saga_format = 'EMPTY' AND v_saga.saga_estado = 'CONFIRMADO' THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: saga ' || v_saga.saga_tipo || ' ' || v_saga.saga_id::TEXT || ' sin items (EMPTY) en estado CONFIRMADO (deberia ser CANCELADA)');
      END IF;

      -- ANOMALY 12: LEV_POST_CORTE total_qty != 30
      IF v_saga.saga_tipo = 'LEV_POST_CORTE' AND v_saga.saga_estado = 'CONFIRMADO'
         AND v_saga_total_qty <> 30 AND v_saga_total_qty > 0
      THEN
        v_anomalias := array_append(v_anomalias,
          'INFO: saga LEV_POST_CORTE ' || v_saga.saga_id::TEXT || ' total_qty=' || v_saga_total_qty || ' (esperado ~30)');
      END IF;

      v_sagas := v_sagas || jsonb_build_object(
        'saga_id', v_saga.saga_id,
        'tipo', v_saga.saga_tipo,
        'estado', v_saga.saga_estado,
        'format', v_saga_format,
        'items_count', v_saga_items_count,
        'total_qty', v_saga_total_qty,
        'zoho_links', v_links_json,
        'movimientos', v_mov_summary
      );
    END LOOP;

    -- =====================
    -- RECOLECCION
    -- =====================
    v_recoleccion := NULL;
    SELECT INTO v_rec
      r.recoleccion_id,
      r.estado,
      (SELECT COUNT(*) FROM public.recolecciones_items ri WHERE ri.recoleccion_id = r.recoleccion_id) AS items_count,
      (SELECT COUNT(*) FROM public.recolecciones_evidencias re WHERE re.recoleccion_id = r.recoleccion_id) AS evidencias_count,
      EXISTS(SELECT 1 FROM public.recolecciones_firmas rf WHERE rf.recoleccion_id = r.recoleccion_id) AS tiene_firma
    FROM public.recolecciones r
    WHERE r.visit_id = v_visit.visit_id
    LIMIT 1;

    IF FOUND THEN
      v_recoleccion := jsonb_build_object(
        'recoleccion_id', v_rec.recoleccion_id,
        'estado', v_rec.estado,
        'items_count', v_rec.items_count,
        'evidencias_count', v_rec.evidencias_count,
        'tiene_firma', v_rec.tiene_firma
      );
    END IF;

    -- ANOMALY 6: RECOLECCION task COMPLETADO but no record in recolecciones
    IF EXISTS (
      SELECT 1 FROM public.visit_tasks
      WHERE visit_id = v_visit.visit_id
        AND task_tipo = 'RECOLECCION'
        AND estado = 'COMPLETADO'
    ) AND v_recoleccion IS NULL THEN
      v_anomalias := array_append(v_anomalias,
        'INFO: RECOLECCION task COMPLETADO pero sin registro en recolecciones (esperado pre-R6)');
    END IF;

    -- =====================
    -- INFORME
    -- =====================
    v_informe := NULL;
    SELECT INTO v_inf
      vi.completada,
      vi.cumplimiento_score
    FROM public.visita_informes vi
    WHERE vi.visit_id = v_visit.visit_id
    LIMIT 1;

    IF FOUND THEN
      v_informe := jsonb_build_object(
        'completada', v_inf.completada,
        'cumplimiento_score', v_inf.cumplimiento_score
      );
    END IF;

    -- ANOMALY 7: INFORME_VISITA task COMPLETADO but no visita_informes
    IF EXISTS (
      SELECT 1 FROM public.visit_tasks
      WHERE visit_id = v_visit.visit_id
        AND task_tipo = 'INFORME_VISITA'
        AND estado = 'COMPLETADO'
    ) AND v_informe IS NULL THEN
      v_anomalias := array_append(v_anomalias,
        'ERROR: INFORME_VISITA task COMPLETADO pero sin registro en visita_informes');
    END IF;

    -- =====================
    -- VISITA_ODVS
    -- =====================
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'odv_id', vo.odv_id,
      'tipo', vo.tipo::TEXT,
      'total_piezas', vo.total_piezas
    ) ORDER BY vo.id), '[]'::JSONB)
    INTO v_visita_odvs_json
    FROM public.visita_odvs vo
    WHERE vo.visit_id = v_visit.visit_id;

    -- =====================
    -- ANOMALY 10: Movimientos without valid saga_transaction for this visit
    -- =====================
    IF EXISTS (
      SELECT 1
      FROM public.movimientos_inventario mi
      WHERE mi.id_cliente = v_visit.id_cliente
        AND mi.id_saga_transaction IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM public.saga_transactions st
          WHERE st.id = mi.id_saga_transaction
        )
    ) THEN
      v_anomalias := array_append(v_anomalias,
        'ERROR: movimientos_inventario con id_saga_transaction que no existe en saga_transactions (cliente ' || v_visit.id_cliente || ')');
    END IF;

    -- =====================
    -- Build visit JSON
    -- =====================
    v_visit_json := jsonb_build_object(
      'visit_id', v_visit.visit_id,
      'id_cliente', v_visit.id_cliente,
      'tipo', v_visit.tipo,
      'estado', v_visit.estado,
      'created_at', v_visit.created_at,
      'tasks', v_tasks,
      'sagas', v_sagas,
      'recoleccion', v_recoleccion,
      'informe', v_informe,
      'visita_odvs', v_visita_odvs_json,
      'anomalias', to_jsonb(v_anomalias)
    );

    v_result := v_result || v_visit_json;
  END LOOP;

  RETURN v_result;
END;
$$;

-- Grant execute to authenticated users (for admin use)
GRANT EXECUTE ON FUNCTION public.audit_visit_graph(TEXT) TO authenticated;

COMMENT ON FUNCTION public.audit_visit_graph IS
'Auditoria de grafos dirigidos: traza el path completo de cada visita (tasks, sagas, zoho_links, ODVs, recolecciones, informes, movimientos) y detecta anomalias automaticamente.
Parametro: p_id_cliente (opcional) - filtra por cliente. Si NULL, retorna todos.
Retorna: JSONB array con una entrada por visita.';
