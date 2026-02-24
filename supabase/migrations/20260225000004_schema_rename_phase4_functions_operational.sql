-- ============================================================================
-- PHASE 4: Recreate Operational Functions (public schema)
-- ============================================================================
-- All function bodies updated with new English table/column/enum names.
-- Function names themselves renamed where applicable.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.audit_saga_transactions()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  INSERT INTO audit_log (
    table_name, record_id, action, user_id,
    values_before, values_after, timestamp
  ) VALUES (
    TG_TABLE_NAME,
    COALESCE(NEW.id::text, OLD.id::text),
    TG_OP,
    COALESCE(NEW.user_id, OLD.user_id),
    CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END,
    NOW()
  );
  RETURN COALESCE(NEW, OLD);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.audit_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  usuario_actual VARCHAR;
BEGIN
  -- Prefer explicit app user id, then map from auth.uid()
  usuario_actual := current_setting('app.current_user_id', TRUE);
  IF usuario_actual IS NULL THEN
    usuario_actual := public.current_user_id();
  END IF;

  -- INSERT
  IF TG_OP = 'INSERT' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      NEW.id::text,
      'INSERT',
      usuario_actual,
      NULL,
      to_jsonb(NEW)
    );
    RETURN NEW;

  -- UPDATE
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      NEW.id::text,
      'UPDATE',
      usuario_actual,
      to_jsonb(OLD),
      to_jsonb(NEW)
    );
    RETURN NEW;

  -- DELETE
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      OLD.id::text,
      'DELETE',
      usuario_actual,
      to_jsonb(OLD),
      NULL
    );
    RETURN OLD;
  END IF;

  RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.audit_visit_graph(p_client_id text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result JSONB := '[]'::JSONB;
  v_visit RECORD;
  v_visit_json JSONB;
  v_tasks JSONB;
  v_sagas JSONB;
  v_recoleccion JSONB;
  v_informe JSONB;
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
    SELECT v.visit_id, v.client_id, v.type::TEXT, v.status::TEXT, v.created_at
    FROM public.visits v
    WHERE (p_client_id IS NULL OR v.client_id = p_client_id)
    ORDER BY v.client_id, v.created_at
  LOOP
    v_anomalias := '{}';

    -- =====================
    -- TASKS
    -- =====================
    v_tasks := '[]'::JSONB;
    FOR v_task IN
      SELECT
        vt.task_type::TEXT,
        vt.status::TEXT AS task_estado,
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
      IF v_task.task_estado = 'COMPLETED'
         AND v_task.reference_table IS NOT NULL
         AND v_task.reference_id IS NULL
         AND v_task.task_type IN ('SALE_ODV', 'ODV_CABINET')
      THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: ' || v_task.task_type || ' COMPLETADO con reference_table=' || v_task.reference_table || ' pero sin reference_id');
      END IF;

      -- ANOMALY 2: reference_id points to non-existent record
      IF v_ref_exists IS NOT NULL AND NOT v_ref_exists THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: ' || v_task.task_type || ' reference_id=' || v_task.reference_id || ' no existe en ' || v_task.reference_table);
      END IF;

      v_tasks := v_tasks || jsonb_build_object(
        'task_type', v_task.task_type,
        'status', v_task.task_estado,
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
        st.type::TEXT AS saga_type,
        st.status::TEXT AS saga_status,
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
        SELECT COUNT(*), COALESCE(SUM((item->>'quantity')::INT), 0)
        INTO v_saga_items_count, v_saga_total_qty
        FROM jsonb_array_elements(v_saga.items) AS item
        WHERE item ? 'sku';

        IF v_saga_items_count = 0 THEN
          -- Check if LEGACY format (has total_outbound key)
          IF v_saga.items->0 ? 'total_outbound' THEN
            v_saga_format := 'LEGACY';
            v_saga_total_qty := COALESCE((v_saga.items->0->>'total_outbound')::INT, 0);
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
        v_saga_total_qty := COALESCE((v_saga.items->>'total_outbound')::INT, 0);
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
          szl.type::TEXT AS link_type,
          szl.zoho_id,
          szl.zoho_sync_status
        FROM public.saga_zoho_links szl
        WHERE szl.id_saga_transaction = v_saga.saga_id
        ORDER BY szl.id
      LOOP
        -- Count ODV lines based on link type
        v_odv_lines := 0;
        v_odv_total_qty := 0;
        IF v_link.link_type = 'SALE' THEN
          SELECT COUNT(*), COALESCE(SUM(quantity), 0)
          INTO v_odv_lines, v_odv_total_qty
          FROM public.odv_sales
          WHERE odv_id = v_link.zoho_id;
        ELSIF v_link.link_type = 'CABINET' THEN
          SELECT COUNT(*), COALESCE(SUM(quantity), 0)
          INTO v_odv_lines, v_odv_total_qty
          FROM public.cabinet_odv
          WHERE odv_id = v_link.zoho_id;
        ELSIF v_link.link_type = 'RETURN' THEN
          -- Devoluciones don't have a separate ODV table
          v_odv_lines := 0;
          v_odv_total_qty := 0;
        END IF;

        v_links_json := v_links_json || jsonb_build_object(
          'link_id', v_link.link_id,
          'type', v_link.link_type,
          'zoho_id', v_link.zoho_id,
          'sync_status', v_link.zoho_sync_status,
          'odv_lines', v_odv_lines,
          'odv_total_qty', v_odv_total_qty
        );
      END LOOP;

      -- ANOMALY 5: Saga qty vs ODV qty mismatch (sum across ALL VENTA links for this saga)
      IF v_saga.saga_type = 'SALE' AND v_saga.saga_status = 'CONFIRMED' AND v_saga_total_qty > 0 THEN
        SELECT COALESCE(SUM(vo.quantity), 0)
        INTO v_saga_odv_total_qty
        FROM public.saga_zoho_links szl
        JOIN public.odv_sales vo ON vo.odv_id = szl.zoho_id
        WHERE szl.id_saga_transaction = v_saga.saga_id
          AND szl.type = 'SALE';

        IF v_saga_odv_total_qty > 0 AND v_saga_total_qty <> v_saga_odv_total_qty THEN
          v_anomalias := array_append(v_anomalias,
            'WARN: saga VENTA ' || v_saga.saga_id::TEXT || ' tiene ' || v_saga_total_qty || ' pzs pero suma ODV = ' || v_saga_odv_total_qty || ' pzs');
        END IF;
      END IF;

      -- Movimientos for this saga, grouped by type
      SELECT jsonb_object_agg(
        tipo_group,
        jsonb_build_object('count', cnt, 'total_qty', total_q)
      )
      INTO v_mov_summary
      FROM (
        SELECT
          mi.type::TEXT AS tipo_group,
          COUNT(*) AS cnt,
          SUM(mi.quantity) AS total_q
        FROM public.inventory_movements mi
        WHERE mi.id_saga_transaction = v_saga.saga_id
        GROUP BY mi.type
      ) sub;

      IF v_mov_summary IS NULL THEN
        v_mov_summary := '{}'::JSONB;
      END IF;

      -- ANOMALY 3: Saga CONFIRMADO without movimientos
      v_has_movimientos := (v_mov_summary <> '{}'::JSONB);
      IF v_saga.saga_status = 'CONFIRMED' AND NOT v_has_movimientos
         AND v_saga.saga_type IN ('SALE', 'COLLECTION', 'INITIAL_PLACEMENT', 'POST_CUTOFF_PLACEMENT')
      THEN
        v_anomalias := array_append(v_anomalias,
          'WARN: saga ' || v_saga.saga_type || ' ' || v_saga.saga_id::TEXT || ' CONFIRMADO sin inventory_movements');
      END IF;

      -- ANOMALY 4: Saga VENTA CONFIRMADO without VENTA zoho_link
      IF v_saga.saga_type = 'SALE' AND v_saga.saga_status = 'CONFIRMED' THEN
        IF NOT EXISTS (
          SELECT 1 FROM public.saga_zoho_links
          WHERE id_saga_transaction = v_saga.saga_id AND type = 'SALE'
        ) THEN
          v_anomalias := array_append(v_anomalias,
            'WARN: saga VENTA ' || v_saga.saga_id::TEXT || ' CONFIRMADO sin saga_zoho_links type VENTA');
        END IF;
      END IF;

      -- ANOMALY 8: LEGACY format with total_outbound=0 but CONFIRMADO
      IF v_saga_format = 'LEGACY' AND v_saga_total_qty = 0 AND v_saga.saga_status = 'CONFIRMED' THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: saga ' || v_saga.saga_type || ' ' || v_saga.saga_id::TEXT || ' LEGACY con total_outbound=0 en status CONFIRMADO (deberia ser CANCELADA)');
      END IF;

      -- ANOMALY 9: EMPTY saga in CONFIRMADO
      IF v_saga_format = 'EMPTY' AND v_saga.saga_status = 'CONFIRMED' THEN
        v_anomalias := array_append(v_anomalias,
          'ERROR: saga ' || v_saga.saga_type || ' ' || v_saga.saga_id::TEXT || ' sin items (EMPTY) en status CONFIRMADO (deberia ser CANCELADA)');
      END IF;

      -- ANOMALY 12: LEV_POST_CORTE total_qty != 30
      IF v_saga.saga_type = 'POST_CUTOFF_PLACEMENT' AND v_saga.saga_status = 'CONFIRMED'
         AND v_saga_total_qty <> 30 AND v_saga_total_qty > 0
      THEN
        v_anomalias := array_append(v_anomalias,
          'INFO: saga LEV_POST_CORTE ' || v_saga.saga_id::TEXT || ' total_qty=' || v_saga_total_qty || ' (esperado ~30)');
      END IF;

      v_sagas := v_sagas || jsonb_build_object(
        'saga_id', v_saga.saga_id,
        'type', v_saga.saga_type,
        'status', v_saga.saga_status,
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
      r.status,
      (SELECT COUNT(*) FROM public.collection_items ri WHERE ri.recoleccion_id = r.recoleccion_id) AS items_count,
      (SELECT COUNT(*) FROM public.collection_evidence re WHERE re.recoleccion_id = r.recoleccion_id) AS evidencias_count,
      EXISTS(SELECT 1 FROM public.collection_signatures rf WHERE rf.recoleccion_id = r.recoleccion_id) AS tiene_firma
    FROM public.collections r
    WHERE r.visit_id = v_visit.visit_id
    LIMIT 1;

    IF FOUND THEN
      v_recoleccion := jsonb_build_object(
        'recoleccion_id', v_rec.recoleccion_id,
        'status', v_rec.status,
        'items_count', v_rec.items_count,
        'evidencias_count', v_rec.evidencias_count,
        'tiene_firma', v_rec.tiene_firma
      );
    END IF;

    -- ANOMALY 6: RECOLECCION task COMPLETADO but no record in collections
    IF EXISTS (
      SELECT 1 FROM public.visit_tasks
      WHERE visit_id = v_visit.visit_id
        AND task_type = 'COLLECTION'
        AND status = 'COMPLETED'
    ) AND v_recoleccion IS NULL THEN
      v_anomalias := array_append(v_anomalias,
        'INFO: RECOLECCION task COMPLETADO pero sin registro en collections (esperado pre-R6)');
    END IF;

    -- =====================
    -- INFORME
    -- =====================
    v_informe := NULL;
    SELECT INTO v_inf
      vi.completed,
      vi.compliance_score
    FROM public.visit_reports vi
    WHERE vi.visit_id = v_visit.visit_id
    LIMIT 1;

    IF FOUND THEN
      v_informe := jsonb_build_object(
        'completed', v_inf.completed,
        'compliance_score', v_inf.compliance_score
      );
    END IF;

    -- ANOMALY 7: INFORME_VISITA task COMPLETADO but no visit_reports
    IF EXISTS (
      SELECT 1 FROM public.visit_tasks
      WHERE visit_id = v_visit.visit_id
        AND task_type = 'VISIT_REPORT'
        AND status = 'COMPLETED'
    ) AND v_informe IS NULL THEN
      v_anomalias := array_append(v_anomalias,
        'ERROR: INFORME_VISITA task COMPLETADO pero sin registro en visit_reports');
    END IF;

    -- =====================
    -- ANOMALY 10: Movimientos without valid saga_transaction for this visit
    -- =====================
    IF EXISTS (
      SELECT 1
      FROM public.inventory_movements mi
      WHERE mi.client_id = v_visit.client_id
        AND mi.id_saga_transaction IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM public.saga_transactions st
          WHERE st.id = mi.id_saga_transaction
        )
    ) THEN
      v_anomalias := array_append(v_anomalias,
        'ERROR: inventory_movements con id_saga_transaction que no existe en saga_transactions (cliente ' || v_visit.client_id || ')');
    END IF;

    -- =====================
    -- Build visit JSON
    -- =====================
    v_visit_json := jsonb_build_object(
      'visit_id', v_visit.visit_id,
      'client_id', v_visit.client_id,
      'type', v_visit.type,
      'status', v_visit.status,
      'created_at', v_visit.created_at,
      'tasks', v_tasks,
      'sagas', v_sagas,
      'recoleccion', v_recoleccion,
      'informe', v_informe,
      'anomalias', to_jsonb(v_anomalias)
    );

    v_result := v_result || v_visit_json;
  END LOOP;

  RETURN v_result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.can_access_client(p_client_id text)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select public.is_admin()
    or exists (
      select 1
      from public.clients c
      where c.client_id = p_client_id
        and c.user_id = public.current_user_id()
    );
$function$
;

CREATE OR REPLACE FUNCTION public.can_access_visit(p_visit_id uuid)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  SELECT public.is_admin()
    OR EXISTS (
      SELECT 1 FROM public.visits v
      WHERE v.visit_id = p_visit_id
        AND v.user_id = public.current_user_id()
    )
    OR EXISTS (
      SELECT 1 FROM public.visits v
      JOIN public.clients c ON c.client_id = v.client_id
      WHERE v.visit_id = p_visit_id
        AND c.user_id = public.current_user_id()
    );
$function$
;

CREATE OR REPLACE FUNCTION public.clasificacion_base(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, padecimiento character varying, brand character varying, es_top boolean, m_type text, first_event_date date, revenue_botiquin numeric, revenue_odv numeric, cantidad_odv numeric, num_transacciones_odv bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.consolidate_duplicate_items(items_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
DECLARE
  result jsonb := '[]'::jsonb;
  item_record record;
BEGIN
  -- Agrupar por SKU y movement_type, sumando cantidades
  FOR item_record IN
    SELECT 
      item->>'sku' as sku,
      item->>'movement_type' as movement_type,
      SUM((item->>'quantity')::int) as cantidad_total
    FROM jsonb_array_elements(items_json) as item
    GROUP BY item->>'sku', item->>'movement_type'
  LOOP
    result := result || jsonb_build_object(
      'sku', item_record.sku,
      'quantity', item_record.cantidad_total,
      'movement_type', item_record.movement_type
    );
  END LOOP;
  
  RETURN result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.create_notification(p_user_id character varying, p_type text, p_title text, p_body text, p_data jsonb DEFAULT '{}'::jsonb, p_dedup_key text DEFAULT NULL::text, p_expires_in_hours integer DEFAULT NULL::integer)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_notification_id UUID;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Calculate expiration if provided
    IF p_expires_in_hours IS NOT NULL THEN
        v_expires_at := NOW() + (p_expires_in_hours || ' hours')::INTERVAL;
    END IF;

    -- Insert notification (with dedup if key provided)
    IF p_dedup_key IS NOT NULL THEN
        INSERT INTO notifications (
            user_id, type, title, body, data, dedup_key, expires_at
        ) VALUES (
            p_user_id, p_type, p_title, p_body, p_data, p_dedup_key, v_expires_at
        )
        ON CONFLICT (user_id, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
        RETURNING id INTO v_notification_id;
    ELSE
        INSERT INTO notifications (
            user_id, type, title, body, data, expires_at
        ) VALUES (
            p_user_id, p_type, p_title, p_body, p_data, v_expires_at
        )
        RETURNING id INTO v_notification_id;
    END IF;

    -- Push notification is now handled via database webhook on INSERT
    -- No need for pgmq queue

    RETURN v_notification_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.current_user_id()
 RETURNS text
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select u.user_id
  from public.users u
  where u.auth_user_id = auth.uid()
  limit 1;
$function$
;

CREATE OR REPLACE FUNCTION public.deduplicate_saga_items(items_array jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  result jsonb;
BEGIN
  -- Agrupa por SKU y movement_type, sumando cantidades
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', sku,
      'movement_type', movement_type,
      'quantity', total_cantidad
    )
  )
  INTO result
  FROM (
    SELECT 
      item->>'sku' as sku,
      item->>'movement_type' as movement_type,
      SUM((item->>'quantity')::int) as total_cantidad
    FROM jsonb_array_elements(items_array) as item
    GROUP BY item->>'sku', item->>'movement_type'
  ) aggregated;
  
  RETURN result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_app_config_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_client_status_log_days()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Buscar el último cambio de status para este cliente
  SELECT EXTRACT(DAY FROM (now() - changed_at))::integer
  INTO NEW.days_in_previous_status
  FROM public.client_status_log
  WHERE client_id = NEW.client_id
  ORDER BY changed_at DESC
  LIMIT 1;

  -- Si es el primer registro, días = NULL (ya está por defecto)
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_create_batch_placement_movement()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
    INSERT INTO public.movimientos_botiquin (
        id_lote,
        cycle_id,
        movement_date,
        type,
        quantity
    )
    VALUES (
        NEW.id_lote,
        NEW.id_ciclo_ingreso,
        COALESCE(NEW.intake_date, CURRENT_TIMESTAMP),
        'PLACEMENT'::public.cabinet_movement_type,
        NEW.cantidad_inicial
    );

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_create_notification(p_type notification_type, p_titulo character varying, p_mensaje text DEFAULT NULL::text, p_metadata jsonb DEFAULT '{}'::jsonb, p_para_usuario character varying DEFAULT NULL::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id UUID;
BEGIN
  INSERT INTO public.admin_notifications (type, title, message, metadata, for_user)
  VALUES (p_type, p_titulo, p_mensaje, p_metadata, p_para_usuario)
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_notify_status_change()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_nombre_cliente VARCHAR;
BEGIN
  -- Obtener name del cliente
  SELECT client_name INTO v_nombre_cliente
  FROM public.clients
  WHERE client_id = NEW.client_id;

  -- Crear notificación según el nuevo status
  IF NEW.new_status = 'DOWNGRADING' THEN
    PERFORM fn_create_notification(
      'CLIENT_DOWNGRADING',
      'Cliente marcado para baja: ' || COALESCE(v_nombre_cliente, NEW.client_id),
      NEW.reason,
      jsonb_build_object(
        'client_id', NEW.client_id, 
        'changed_by', NEW.changed_by,
        'previous_status', NEW.previous_status
      )
    );
  ELSIF NEW.new_status = 'INACTIVE' THEN
    PERFORM fn_create_notification(
      'CLIENT_INACTIVE',
      'Cliente dado de baja: ' || COALESCE(v_nombre_cliente, NEW.client_id),
      COALESCE(NEW.reason, 'Baja completed'),
      jsonb_build_object(
        'client_id', NEW.client_id,
        'automatico', NEW.metadata->>'automatico' = 'true'
      )
    );
  ELSIF NEW.new_status = 'ACTIVE' AND NEW.previous_status = 'INACTIVE' THEN
    PERFORM fn_create_notification(
      'CLIENT_REACTIVATED',
      'Cliente reactivado: ' || COALESCE(v_nombre_cliente, NEW.client_id),
      NEW.reason,
      jsonb_build_object(
        'client_id', NEW.client_id, 
        'changed_by', NEW.changed_by
      )
    );
  ELSIF NEW.new_status = 'SUSPENDED' THEN
    PERFORM fn_create_notification(
      'CLIENT_SUSPENDED',
      'Cliente suspendido: ' || COALESCE(v_nombre_cliente, NEW.client_id),
      NEW.reason,
      jsonb_build_object(
        'client_id', NEW.client_id, 
        'changed_by', NEW.changed_by
      )
    );
  END IF;

  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_remove_available_sku_on_sale()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Solo actuar en movimientos type VENTA
  IF NEW.type = 'SALE' THEN
    DELETE FROM public.cabinet_client_available_skus
    WHERE client_id = NEW.client_id
      AND sku = NEW.sku;
    
    -- Log en audit_log si existe la infraestructura
    BEGIN
      INSERT INTO public.audit_log (
        table_name,
        record_id,
        action,
        user_id,
        values_before,
        values_after
      )
      VALUES (
        'cabinet_client_available_skus',
        NEW.client_id || ':' || NEW.sku,
        'DELETE',
        NULL,  -- Sistema automático
        jsonb_build_object(
          'client_id', NEW.client_id,
          'sku', NEW.sku,
          'motivo', 'movimiento_venta',
          'movimiento_id', NEW.id
        ),
        NULL
      );
    EXCEPTION WHEN OTHERS THEN
      -- Si falla el log, no interrumpir la operación
      NULL;
    END;
  END IF;
  
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_sync_client_active()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Sincronizar campo active basándose en status
  NEW.active := (NEW.status IN ('ACTIVE', 'DOWNGRADING', 'SUSPENDED'));
  NEW.updated_at := now();
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_sync_inventory_from_movements()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  IF NEW.quantity_after IS NOT NULL THEN
    INSERT INTO cabinet_inventory (client_id, sku, available_quantity, last_updated)
    VALUES (NEW.client_id, NEW.sku, NEW.quantity_after, now())
    ON CONFLICT (client_id, sku)
    DO UPDATE SET
      available_quantity = EXCLUDED.available_quantity,
      last_updated = EXCLUDED.last_updated;
  END IF;
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_sync_saga_status()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  NEW.saga_status := CASE
    WHEN NEW.status = 'COMPLETED' THEN 'COMPLETED'
    WHEN NEW.status = 'CANCELLED' THEN 'COMPENSATED'
    ELSE 'RUNNING'
  END;
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_client_audit(p_client character varying)
 RETURNS json
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT analytics.get_client_audit(p_client);
$function$
;

CREATE OR REPLACE FUNCTION public.get_balance_metrics()
 RETURNS TABLE(concepto text, valor_creado numeric, valor_ventas numeric, valor_recoleccion numeric, valor_permanencia_entrada numeric, valor_permanencia_virtual numeric, valor_calculado_total numeric, diferencia numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_balance_metrics(); $function$
;

CREATE OR REPLACE FUNCTION public.get_cabinet_data()
 RETURNS TABLE(sku character varying, id_movimiento bigint, movement_type text, quantity integer, movement_date text, id_lote text, intake_date text, cantidad_inicial integer, available_quantity integer, client_id character varying, client_name character varying, tier character varying, avg_billing numeric, total_billing numeric, product character varying, price numeric, brand character varying, top boolean, padecimiento character varying)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cabinet_data(); $function$
;

CREATE OR REPLACE FUNCTION public.get_brand_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(brand character varying, valor numeric, piezas integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_brand_performance(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_conversion_details(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(m_type text, client_id character varying, client_name character varying, sku character varying, product character varying, fecha_botiquin date, fecha_primera_odv date, dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric, valor_botiquin numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_conversion_details(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$
;

CREATE OR REPLACE FUNCTION public.get_conversion_metrics(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric, valor_botiquin numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_conversion_metrics(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$
;

CREATE OR REPLACE FUNCTION public.get_current_cutoff_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT analytics.get_current_cutoff_data(p_doctors, p_brands, p_conditions);
$function$
;

CREATE OR REPLACE FUNCTION public.get_historical_cutoff_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$
  SELECT analytics.get_historical_cutoff_data(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_logistics_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(nombre_asesor text, client_name character varying, client_id character varying, fecha_visita text, sku character varying, product character varying, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_status text, odv_botiquin text, odv_venta text, recoleccion_id uuid, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_cutoff_logistics_data(p_doctors, p_brands, p_conditions);
$function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_logistics_detail()
 RETURNS TABLE(nombre_asesor text, client_name text, client_id text, fecha_visita date, sku text, product text, cantidad_colocada integer, qty_venta integer, qty_recoleccion integer, total_corte integer, destino text, saga_status text, odv_botiquin text, odv_venta text, recoleccion_id text, recoleccion_estado text, evidencia_paths text[], firma_path text, observaciones text, quien_recibio text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_logistics_detail(); $function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_skus_value_per_visit(p_client_id character varying DEFAULT NULL::character varying, p_brand character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, brand character varying)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_skus_value_per_visit(p_client_id, p_brand); $function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_general_stats_with_comparison()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_general_stats_with_comparison(); $function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_stats_by_doctor()
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, skus_creados text, skus_recolectados text, tiene_venta boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_stats_by_doctor(); $function$
;

CREATE OR REPLACE FUNCTION public.get_cutoff_stats_by_doctor_with_comparison()
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, tiene_venta boolean, valor_venta_anterior numeric, porcentaje_cambio numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_stats_by_doctor_with_comparison(); $function$
;

CREATE OR REPLACE FUNCTION public.get_crosssell_significance()
 RETURNS TABLE(exposed_total integer, exposed_with_crosssell integer, exposed_conversion_pct numeric, unexposed_total integer, unexposed_with_crosssell integer, unexposed_conversion_pct numeric, chi_squared numeric, significancia text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_crosssell_significance(); $function$
;

CREATE OR REPLACE FUNCTION public.get_current_user_id()
 RETURNS character varying
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
    SELECT user_id FROM users WHERE auth_user_id = auth.uid()
$function$
;

CREATE OR REPLACE FUNCTION public.get_dashboard_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_data(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$
;

CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$
;

CREATE OR REPLACE FUNCTION public.get_movement_direction(p_type cabinet_movement_type)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
  SELECT CASE 
    WHEN p_type IN ('PLACEMENT', 'HOLDING') THEN 1
    WHEN p_type IN ('SALE', 'COLLECTION') THEN -1
    ELSE 0
  END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_billing_composition(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, rango_anterior character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_billing_composition(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$
;

CREATE OR REPLACE FUNCTION public.get_billing_composition()
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, pct_crecimiento numeric, pct_vinculado numeric, valor_vinculado numeric, piezas_vinculadas bigint, skus_vinculados bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_billing_composition_legacy(); $function$
;

CREATE OR REPLACE FUNCTION public.get_available_filters()
 RETURNS TABLE(marcas character varying[], medicos jsonb, conditions character varying[], fecha_primer_levantamiento date)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_available_filters(); $function$
;

CREATE OR REPLACE FUNCTION public.get_historical_conversions_evolution(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_grouping text DEFAULT 'day'::text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(fecha_grupo date, fecha_label text, pares_total integer, pares_botiquin integer, pares_directo integer, valor_total numeric, valor_botiquin numeric, valor_directo numeric, num_transacciones integer, num_clientes integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_historical_conversions_evolution(p_start_date, p_end_date, p_grouping, p_doctors, p_brands, p_conditions); $function$
;

CREATE OR REPLACE FUNCTION public.get_historical_skus_value_per_visit(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_client_id character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, piezas_venta integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_historical_skus_value_per_visit(p_start_date, p_end_date, p_client_id); $function$
;

CREATE OR REPLACE FUNCTION public.get_cabinet_impact_summary(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(adopciones integer, revenue_adopciones numeric, conversiones integer, revenue_conversiones numeric, exposiciones integer, revenue_exposiciones numeric, crosssell_pares integer, revenue_crosssell numeric, revenue_total_impacto numeric, revenue_total_odv numeric, porcentaje_impacto numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_cabinet_impact_summary(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_impact_detail(p_metric text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, quantity integer, price numeric, valor numeric, date date, detalle text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_impact_detail(p_metric, p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_market_analysis(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, sku character varying, product character varying, brand character varying, padecimiento character varying, es_top boolean, venta_pz bigint, venta_valor numeric, creacion_pz bigint, creacion_valor numeric, recoleccion_pz bigint, recoleccion_valor numeric, stock_activo_pz bigint, conversiones_m2 bigint, revenue_m2 numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_market_analysis(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_opportunity_matrix(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, venta integer, recoleccion integer, valor numeric, converted_qty integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_opportunity_matrix(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_condition_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, valor numeric, piezas integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_condition_performance(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_product_interest(p_limit integer DEFAULT 15, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(product character varying, venta integer, creacion integer, recoleccion integer, stock_activo integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_product_interest(p_limit, p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_active_collection()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT analytics.get_active_collection(); $function$
;

CREATE OR REPLACE FUNCTION public.get_recurring_data()
 RETURNS TABLE(client_id character varying, sku character varying, date date, quantity integer, price numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_recurring_data(); $function$
;

CREATE OR REPLACE FUNCTION public.get_sankey_conversion_flows(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku text, product text, categoria text, valor_odv numeric, cantidad_odv numeric, num_transacciones bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_sankey_conversion_flows(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.get_yoy_padecimiento(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(padecimiento character varying, anio integer, valor numeric, crecimiento numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_yoy_padecimiento(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$
;

CREATE OR REPLACE FUNCTION public.is_admin()
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select exists (
    select 1
    from public.users u
    where u.auth_user_id = auth.uid()
      and u.role IN ('ADMIN', 'OWNER')
      and u.active = true
  );
$function$
;

CREATE OR REPLACE FUNCTION public.is_current_user_admin()
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
    SELECT EXISTS (
        SELECT 1 FROM users
        WHERE auth_user_id = auth.uid() AND role IN ('ADMIN', 'OWNER')
    )
$function$
;

CREATE OR REPLACE FUNCTION public.notify_admins(p_type text, p_title text, p_body text, p_data jsonb DEFAULT '{}'::jsonb)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_admin RECORD;
    v_count INTEGER := 0;
BEGIN
    FOR v_admin IN
        SELECT user_id FROM users WHERE role IN ('ADMIN', 'OWNER') AND active = true
    LOOP
        PERFORM create_notification(
            v_admin.user_id, p_type, p_title, p_body, p_data
        );
        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.notify_visit_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_admin RECORD;
  v_user_nombre TEXT;
  v_cliente_nombre TEXT;
  v_dedup_key TEXT;
BEGIN
  -- Solo actuar cuando status cambia a COMPLETADO
  IF NEW.status = 'COMPLETED' AND (OLD.status IS NULL OR OLD.status != 'COMPLETED') THEN

    -- Obtener name del asesor que completó la visita
    SELECT u.name INTO v_user_nombre
    FROM users u
    WHERE u.user_id = NEW.user_id;

    -- Obtener name del cliente/médico
    SELECT c.client_name INTO v_cliente_nombre
    FROM clients c
    WHERE c.client_id = NEW.client_id;

    -- Clave de deduplicación para evitar notificaciones duplicadas
    v_dedup_key := 'visit_completed_' || NEW.visit_id::text;

    -- Insertar notificación para cada ADMIN y OWNER active
    FOR v_admin IN
      SELECT user_id FROM users
      WHERE role IN ('ADMIN', 'OWNER') AND active = true
    LOOP
      -- Solo insertar si no existe ya una notificación con esta dedup_key para este usuario
      INSERT INTO notifications (
        user_id, 
        type, 
        title, 
        body, 
        data, 
        dedup_key,
        created_at
      )
      SELECT
        v_admin.user_id,
        'TASK_COMPLETED',
        'Visita Completada',
        COALESCE(v_user_nombre, 'Un asesor') || ' terminó su visita con ' || COALESCE(v_cliente_nombre, 'un cliente'),
        jsonb_build_object(
          'visit_id', NEW.visit_id,
          'user_id', NEW.user_id,
          'user_name', v_user_nombre,
          'cliente_id', NEW.client_id,
          'cliente_name', v_cliente_nombre,
          'screen', 'visits',
          'visit_type', NEW.type
        ),
        v_dedup_key || '_' || v_admin.user_id,
        NOW()
      WHERE NOT EXISTS (
        SELECT 1 FROM notifications n 
        WHERE n.dedup_key = v_dedup_key || '_' || v_admin.user_id
      );
    END LOOP;
  END IF;

  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.publish_saga_event()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  IF NEW.status IN ('PENDING_CONFIRMATION', 'PROCESSING_ZOHO') THEN
    INSERT INTO event_outbox (
      event_type, saga_transaction_id, payload,
      processed, attempts, next_attempt
    ) VALUES (
      'SAGA_' || NEW.type::text,
      NEW.id,
      jsonb_build_object(
        'id', NEW.id, 'type', NEW.type, 'status', NEW.status,
        'client_id', NEW.client_id, 'items', NEW.items
      ),
      false, 0, NOW()
    );
  END IF;
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rebuild_inventory_movements()
 RETURNS TABLE(movimientos_creados bigint, inventario_final bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  saga_rec RECORD;
  item_rec RECORD;
  current_stock INTEGER;
  new_stock INTEGER;
  mov_type cabinet_movement_type;
  mov_count BIGINT := 0;
  inv_count BIGINT := 0;
  v_zoho_link_id INTEGER;
  v_link_count INTEGER;
BEGIN
  TRUNCATE TABLE inventory_movements RESTART IDENTITY CASCADE;
  TRUNCATE TABLE cabinet_inventory RESTART IDENTITY CASCADE;

  FOR saga_rec IN
    SELECT id, client_id, created_at, items
    FROM saga_transactions
    WHERE items IS NOT NULL
    ORDER BY created_at, id
  LOOP
    -- Pre-count links for this saga
    SELECT COUNT(*) INTO v_link_count
    FROM saga_zoho_links WHERE id_saga_transaction = saga_rec.id;

    FOR item_rec IN
      SELECT
        item->>'sku' as sku,
        (item->>'quantity')::int as quantity,
        item->>'movement_type' as movement_type
      FROM jsonb_array_elements(saga_rec.items) as item
      WHERE item->>'movement_type' != 'HOLDING'
    LOOP
      IF item_rec.movement_type = 'PLACEMENT' THEN
        mov_type := 'PLACEMENT';
      ELSIF item_rec.movement_type = 'SALE' THEN
        mov_type := 'SALE';
      ELSIF item_rec.movement_type = 'COLLECTION' THEN
        mov_type := 'COLLECTION';
      ELSE
        CONTINUE;
      END IF;

      SELECT COALESCE(available_quantity, 0)
      INTO current_stock
      FROM cabinet_inventory
      WHERE client_id = saga_rec.client_id AND sku = item_rec.sku;

      IF current_stock IS NULL THEN
        current_stock := 0;
      END IF;

      IF mov_type = 'PLACEMENT' THEN
        new_stock := current_stock + item_rec.quantity;
      ELSE
        new_stock := current_stock - item_rec.quantity;
      END IF;

      -- Resolve id_saga_zoho_link
      v_zoho_link_id := NULL;
      IF v_link_count = 1 THEN
        SELECT szl.id INTO v_zoho_link_id
        FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = saga_rec.id;
      ELSIF v_link_count > 1 THEN
        SELECT szl.id INTO v_zoho_link_id
        FROM saga_zoho_links szl
        WHERE szl.id_saga_transaction = saga_rec.id
          AND szl.items IS NOT NULL
          AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = item_rec.sku)
        ORDER BY szl.id LIMIT 1;
      END IF;

      INSERT INTO inventory_movements (
        id_saga_transaction,
        id_saga_zoho_link,
        client_id,
        sku,
        type,
        quantity,
        quantity_before,
        quantity_after,
        movement_date
      ) VALUES (
        saga_rec.id,
        v_zoho_link_id,
        saga_rec.client_id,
        item_rec.sku,
        mov_type,
        item_rec.quantity,
        current_stock,
        new_stock,
        saga_rec.created_at
      );

      mov_count := mov_count + 1;

      INSERT INTO cabinet_inventory (client_id, sku, available_quantity)
      VALUES (saga_rec.client_id, item_rec.sku, new_stock)
      ON CONFLICT (client_id, sku)
      DO UPDATE SET available_quantity = new_stock;

    END LOOP;
  END LOOP;

  SELECT COUNT(*) INTO inv_count FROM cabinet_inventory;

  RETURN QUERY SELECT mov_count, inv_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.refresh_all_materialized_views()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_balance_metrics;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cumulative_daily;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_opportunity_matrix;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_doctor_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_interest;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_brand_performance;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_padecimiento_performance;
  
  RAISE NOTICE 'Vistas materializadas actualizadas: %', NOW();
END;
$function$
;

CREATE OR REPLACE FUNCTION public.regenerate_movements_from_saga(p_saga_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_movement_type cabinet_movement_type;
  v_zoho_link_id integer;
  v_link_count integer;
BEGIN
  SELECT * INTO v_saga FROM saga_transactions WHERE id = p_saga_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'SAGA % no encontrada', p_saga_id;
  END IF;

  -- Pre-count links for this saga
  SELECT COUNT(*) INTO v_link_count
  FROM saga_zoho_links WHERE id_saga_transaction = p_saga_id;

  FOR v_item IN
    SELECT
      item->>'sku' as sku,
      (item->>'quantity')::int as quantity,
      item->>'movement_type' as movement_type
    FROM jsonb_array_elements(v_saga.items) as item
  LOOP
    SELECT COALESCE(available_quantity, 0)
    INTO v_cantidad_antes
    FROM cabinet_inventory
    WHERE client_id = v_saga.client_id AND sku = v_item.sku;

    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    CASE v_item.movement_type
      WHEN 'PLACEMENT' THEN
        v_movement_type := 'PLACEMENT';
        v_cantidad_despues := v_cantidad_antes + v_item.quantity;
      WHEN 'SALE' THEN
        v_movement_type := 'SALE';
        v_cantidad_despues := v_cantidad_antes - v_item.quantity;
      WHEN 'COLLECTION' THEN
        v_movement_type := 'COLLECTION';
        v_cantidad_despues := v_cantidad_antes - v_item.quantity;
      WHEN 'HOLDING' THEN
        CONTINUE;
      ELSE
        RAISE EXCEPTION 'Tipo de movimiento desconocido: %', v_item.movement_type;
    END CASE;

    -- Resolve id_saga_zoho_link
    v_zoho_link_id := NULL;
    IF v_link_count = 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = p_saga_id;
    ELSIF v_link_count > 1 THEN
      SELECT szl.id INTO v_zoho_link_id
      FROM saga_zoho_links szl
      WHERE szl.id_saga_transaction = p_saga_id
        AND szl.items IS NOT NULL
        AND EXISTS (SELECT 1 FROM jsonb_array_elements(szl.items) e WHERE e->>'sku' = v_item.sku)
      ORDER BY szl.id LIMIT 1;
    END IF;

    INSERT INTO inventory_movements (
      id_saga_transaction,
      id_saga_zoho_link,
      client_id,
      sku,
      type,
      quantity,
      quantity_before,
      quantity_after,
      movement_date
    ) VALUES (
      p_saga_id,
      v_zoho_link_id,
      v_saga.client_id,
      v_item.sku,
      v_movement_type,
      v_item.quantity,
      v_cantidad_antes,
      v_cantidad_despues,
      v_saga.created_at
    );

    INSERT INTO cabinet_inventory (client_id, sku, available_quantity)
    VALUES (v_saga.client_id, v_item.sku, v_cantidad_despues)
    ON CONFLICT (client_id, sku)
    DO UPDATE SET available_quantity = v_cantidad_despues;
  END LOOP;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_compensate_task(p_saga_transaction_id uuid, p_admin_id character varying, p_reason text, p_new_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_compensation_id UUID;
    v_old_items JSONB;
    v_result JSONB;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM users
        WHERE user_id = p_admin_id AND role IN ('ADMIN', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden compensar tareas';
    END IF;

    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    v_old_items := v_saga.items;

    INSERT INTO saga_compensations (
        saga_transaction_id, compensated_by, reason, compensation_type,
        old_state, new_state, zoho_sync_status
    ) VALUES (
        p_saga_transaction_id, p_admin_id, p_reason, 'ADJUSTMENT',
        jsonb_build_object('items', v_old_items, 'status', v_saga.status),
        jsonb_build_object('items', p_new_items), 'PENDING'
    ) RETURNING id INTO v_compensation_id;

    INSERT INTO saga_adjustments (
        compensation_id, saga_transaction_id, item_sku,
        old_quantity, new_quantity, adjustment_reason
    )
    SELECT v_compensation_id, p_saga_transaction_id,
        COALESCE(old_item->>'sku', new_item->>'sku'),
        COALESCE((old_item->>'quantity')::INTEGER, 0),
        COALESCE((new_item->>'quantity')::INTEGER, 0),
        p_reason
    FROM jsonb_array_elements(v_old_items) WITH ORDINALITY AS old_items(old_item, ord)
    FULL OUTER JOIN jsonb_array_elements(p_new_items) WITH ORDINALITY AS new_items(new_item, ord2)
    ON old_item->>'sku' = new_item->>'sku'
    WHERE COALESCE((old_item->>'quantity')::INTEGER, 0) != COALESCE((new_item->>'quantity')::INTEGER, 0);

    UPDATE saga_transactions
    SET items = p_new_items, updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    RETURN jsonb_build_object('success', true, 'compensation_id', v_compensation_id, 'message', 'Compensación registrada');
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_force_task_status(p_visit_task_id uuid, p_admin_id character varying, p_new_status text, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_task RECORD;
    v_saga RECORD;
    v_compensation_id UUID;
BEGIN
    -- Only OWNER can force states
    IF NOT EXISTS (
        SELECT 1 FROM users
        WHERE user_id = p_admin_id AND role = 'OWNER'
    ) THEN
        RAISE EXCEPTION 'Solo OWNER puede forzar estados de tareas';
    END IF;

    -- Validate new status
    IF p_new_status NOT IN ('PENDING', 'COMPLETED', 'ERROR', 'SKIPPED_M', 'SKIPPED') THEN
        RAISE EXCEPTION 'Estado inválido: %', p_new_status;
    END IF;

    -- Get task by task_id
    SELECT * INTO v_task FROM visit_tasks WHERE task_id = p_visit_task_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tarea no encontrada';
    END IF;

    -- Try to find related saga_transaction
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE visit_id = v_task.visit_id
      AND type::text ILIKE '%' || v_task.task_type::text || '%'
    LIMIT 1;

    -- Record in compensations (for audit)
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        v_saga.id,
        p_admin_id,
        p_reason,
        'FORCE_COMPLETE',
        jsonb_build_object('task_status', v_task.status::text),
        jsonb_build_object('task_status', p_new_status),
        'MANUAL'
    ) RETURNING id INTO v_compensation_id;

    -- Force status change
    UPDATE visit_tasks
    SET
        status = p_new_status::visit_task_status,
        completed_at = CASE WHEN p_new_status = 'COMPLETED' THEN NOW() ELSE completed_at END,
        metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
            'forced_at', NOW(),
            'forced_by', p_admin_id,
            'forced_reason', p_reason
        )
    WHERE task_id = p_visit_task_id;

    RETURN jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', format('Estado forzado a %s', p_new_status)
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_get_all_visits(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_status text DEFAULT NULL::text, p_search text DEFAULT NULL::text, p_date_from date DEFAULT NULL::date, p_date_to date DEFAULT NULL::date)
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

  -- Get total count
  SELECT COUNT(*)
  INTO v_total
  FROM public.visits v
  JOIN public.clients c ON c.client_id = v.client_id
  WHERE (p_status IS NULL OR v.status::text = p_status)
    AND (p_search IS NULL OR c.client_name ILIKE '%' || p_search || '%')
    AND (p_date_from IS NULL OR v.created_at::date >= p_date_from)
    AND (p_date_to IS NULL OR v.created_at::date <= p_date_to);

  -- Get visits with client info and resource counts
  SELECT jsonb_agg(row_data)
  INTO v_visits
  FROM (
    SELECT jsonb_build_object(
      'visit_id', v.visit_id,
      'client_id', v.client_id,
      'client_name', c.client_name,
      'user_id', v.user_id,
      'nombre_usuario', u.name,
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
      'sagas_count', (SELECT COUNT(*) FROM saga_transactions st WHERE st.visit_id = v.visit_id)
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
$function$
;

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

  -- Get visit with client info
  SELECT jsonb_build_object(
    'visit_id', v.visit_id,
    'client_id', v.client_id,
    'client_name', c.client_name,
    'user_id', v.user_id,
    'nombre_usuario', u.name,
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

  -- Get ODVs from saga_zoho_links
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'type', szl.type::text,
      'fecha_odv', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'saga_type', st.type::text,
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE((item->>'quantity')::int, (item->>'cantidad_entrada')::int, 0)
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
              'quantity', COALESCE((item->>'quantity')::int, (item->>'cantidad_entrada')::int, 0)
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

  -- Get movimientos with detailed items
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

  -- Get informe de visita
  SELECT jsonb_build_object(
    'report_id', vi.report_id,
    'completed', vi.completed,
    'compliance_score', vi.compliance_score,
    'label', vi.label,
    'responses', vi.responses,
    'completed_date', vi.completed_date,
    'created_at', vi.created_at
  )
  INTO v_informe
  FROM public.visit_reports vi
  WHERE vi.visit_id = p_visit_id;

  -- Get collections
  SELECT jsonb_agg(row_data)
  INTO v_recolecciones
  FROM (
    SELECT jsonb_build_object(
      'recoleccion_id', r.recoleccion_id,
      'status', r.status,
      'latitude', r.latitude,
      'longitude', r.longitude,
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
    'movimientos', COALESCE(v_movimientos, '{"total": 0, "total_cantidad": 0, "unique_skus": 0, "by_tipo": {}, "items": []}'::jsonb),
    'informe', v_informe,
    'collections', COALESCE(v_recolecciones, '[]'::jsonb)
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_retry_pivot(p_saga_transaction_id uuid, p_admin_id character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_compensation_id UUID;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM users
        WHERE user_id = p_admin_id AND role IN ('ADMIN', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden reintentar PIVOT';
    END IF;

    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    IF v_saga.status NOT IN ('FAILED', 'ERROR') THEN
        RAISE EXCEPTION 'Solo se pueden reintentar transacciones en status FALLIDA o ERROR';
    END IF;

    INSERT INTO saga_compensations (
        saga_transaction_id, compensated_by, reason, compensation_type,
        old_state, new_state, zoho_sync_status
    ) VALUES (
        p_saga_transaction_id, p_admin_id, 'Reintento manual de PIVOT', 'RETRY',
        jsonb_build_object('status', v_saga.status::text),
        jsonb_build_object('status', 'PENDING_SYNC'), 'PENDING'
    ) RETURNING id INTO v_compensation_id;

    UPDATE saga_transactions
    SET status = 'PENDING_SYNC', updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    RETURN jsonb_build_object('success', true, 'compensation_id', v_compensation_id, 'message', 'Reintento de PIVOT registrado');
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_rollback_visit(p_visit_id uuid, p_reason text DEFAULT NULL::text)
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
  SELECT u.user_id, u.role::text
  INTO v_user_id, v_user_rol
  FROM public.users u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden ejecutar rollback de visits';
  END IF;

  -- Get visit info and snapshot before deletion
  SELECT
    v.client_id,
    v.cycle_id,
    jsonb_build_object(
      'visit_id', v.visit_id,
      'client_id', v.client_id,
      'user_id', v.user_id,
      'cycle_id', v.cycle_id,
      'type', v.type::text,
      'status', v.status::text,
      'created_at', v.created_at,
      'label', v.label,
      'metadata', v.metadata
    )
  INTO v_id_cliente, v_id_ciclo, v_visit_data
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- ============ RESTAURAR INVENTARIO ANTES DE BORRAR ============
  SELECT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.type = 'POST_CUTOFF_PLACEMENT'
      AND st.status = 'CONFIRMED'
  ) INTO v_current_visit_had_lev_post_corte;

  IF v_current_visit_had_lev_post_corte THEN
    -- Buscar la última visita COMPLETADA del mismo cliente (excluyendo la actual)
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visits v
    WHERE v.client_id = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.status = 'COMPLETED'
      AND v.type IN ('VISIT_CUTOFF', 'VISIT_INITIAL_PLACEMENT')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      -- Intentar LEV_POST_CORTE primero
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.type = 'POST_CUTOFF_PLACEMENT'
        AND st.status = 'CONFIRMED'
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
          AND st.type = 'INITIAL_PLACEMENT'
          AND st.status = 'CONFIRMED'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- Restaurar inventario si encontramos items
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      DELETE FROM public.cabinet_inventory WHERE client_id = v_id_cliente;

      INSERT INTO public.cabinet_inventory (client_id, sku, available_quantity, last_updated)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'quantity')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      INSERT INTO public.cabinet_client_available_skus (client_id, sku, intake_date)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0
      ON CONFLICT (client_id, sku) DO NOTHING;

      v_inventory_reverted := true;
    ELSE
      v_restore_source := 'Sin visita completed anterior - inventario no modificado';
    END IF;
  END IF;
  -- ============ FIN RESTAURACIÓN DE INVENTARIO ============

  -- Get saga transaction IDs for this visit
  SELECT ARRAY_AGG(st.id) INTO v_saga_ids
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id;

  -- Get recoleccion IDs for this visit
  SELECT ARRAY_AGG(r.recoleccion_id) INTO v_recoleccion_ids
  FROM public.collections r
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

    -- 2. Delete inventory_movements (references saga_transactions)
    WITH deleted AS (
      DELETE FROM public.inventory_movements
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

  -- 8. Delete collections and related tables
  IF v_recoleccion_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.collection_evidence
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_evidencias FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collection_signatures
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_firmas FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collection_items
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_items FROM deleted;

    WITH deleted AS (
      DELETE FROM public.collections
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_recolecciones FROM deleted;
  END IF;

  -- 9. Delete visit_reports
  WITH deleted AS (
    DELETE FROM public.visit_reports
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_informes FROM deleted;

  -- 10. Update visit status to CANCELADO
  UPDATE public.visits
  SET
    status = 'CANCELLED',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'rollback_at', NOW(),
      'rollback_by', v_user_id,
      'rollback_razon', p_reason,
      'rollback_deleted', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'inventory_movements', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'collections', v_deleted_recolecciones,
        'collection_items', v_deleted_rec_items,
        'collection_signatures', v_deleted_rec_firmas,
        'collection_evidence', v_deleted_rec_evidencias,
        'visit_reports', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored
    )
  WHERE visit_id = p_visit_id;

  -- LOG TO AUDIT_LOG
  INSERT INTO public.audit_log (
    table_name,
    record_id,
    action,
    user_id,
    values_before,
    values_after
  )
  VALUES (
    'visits',
    p_visit_id::text,
    'DELETE',
    v_user_id,
    v_visit_data || jsonb_build_object(
      'accion_tipo', 'ADMIN_ROLLBACK',
      'reason', p_reason
    ),
    jsonb_build_object(
      'deleted_counts', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'inventory_movements', v_deleted_movimientos,
        'saga_zoho_links', v_deleted_zoho_links,
        'saga_compensations', v_deleted_compensations,
        'saga_adjustments', v_deleted_adjustments,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'collections', v_deleted_recolecciones,
        'collection_items', v_deleted_rec_items,
        'collection_signatures', v_deleted_rec_firmas,
        'collection_evidence', v_deleted_rec_evidencias,
        'visit_reports', v_deleted_informes
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
    'client_id', v_id_cliente,
    'cycle_id', v_id_ciclo,
    'executed_by', v_user_id,
    'reason', p_reason,
    'deleted', jsonb_build_object(
      'event_outbox', v_deleted_event_outbox,
      'inventory_movements', v_deleted_movimientos,
      'saga_zoho_links', v_deleted_zoho_links,
      'saga_compensations', v_deleted_compensations,
      'saga_adjustments', v_deleted_adjustments,
      'saga_transactions', v_deleted_saga,
      'visit_tasks', v_deleted_tasks,
      'collections', v_deleted_recolecciones,
      'collection_items', v_deleted_rec_items,
      'collection_signatures', v_deleted_rec_firmas,
      'collection_evidence', v_deleted_rec_evidencias,
      'visit_reports', v_deleted_informes
    ),
    'inventory_reverted', v_inventory_reverted,
    'inventory_restore_source', v_restore_source,
    'inventory_items_restored', v_count_inventario_restored,
    'last_completed_visit_id', v_last_completed_visit_id,
    'message', 'Rollback completado exitosamente'
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_change_client_status(p_client_id character varying, p_new_status client_status, p_user_id character varying, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_estado_actual public.client_status;
  v_user_rol VARCHAR;
  v_tiene_visita_activa BOOLEAN;
  v_nombre_cliente VARCHAR;
BEGIN
  -- 1. Validar permisos (solo ADMINISTRADOR u OWNER)
  SELECT role INTO v_user_rol FROM public.users WHERE user_id = p_user_id;
  IF v_user_rol IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado: %', p_user_id;
  END IF;
  
  IF v_user_rol NOT IN ('ADMIN', 'OWNER') THEN
    RAISE EXCEPTION 'Solo ADMIN u OWNER pueden cambiar status de cliente. Rol actual: %', v_user_rol;
  END IF;

  -- 2. Obtener status actual del cliente
  SELECT status, client_name INTO v_estado_actual, v_nombre_cliente 
  FROM public.clients WHERE client_id = p_client_id;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Cliente no encontrado: %', p_client_id;
  END IF;

  -- 3. Validar que no sea el mismo status
  IF v_estado_actual = p_new_status THEN
    RAISE EXCEPTION 'El cliente ya está en status %', p_new_status;
  END IF;

  -- 4. Validar transiciones permitidas (máquina de estados)
  -- ACTIVO -> EN_BAJA, SUSPENDIDO
  -- EN_BAJA -> ACTIVO, INACTIVO
  -- INACTIVO -> ACTIVO
  -- SUSPENDIDO -> ACTIVO, EN_BAJA
  IF NOT (
    (v_estado_actual = 'ACTIVE' AND p_new_status IN ('DOWNGRADING', 'SUSPENDED')) OR
    (v_estado_actual = 'DOWNGRADING' AND p_new_status IN ('ACTIVE', 'INACTIVE')) OR
    (v_estado_actual = 'INACTIVE' AND p_new_status = 'ACTIVE') OR
    (v_estado_actual = 'SUSPENDED' AND p_new_status IN ('ACTIVE', 'DOWNGRADING'))
  ) THEN
    RAISE EXCEPTION 'Transición no permitida: % -> %. Transiciones válidas desde %: %', 
      v_estado_actual, 
      p_new_status,
      v_estado_actual,
      CASE v_estado_actual
        WHEN 'ACTIVE' THEN 'EN_BAJA, SUSPENDIDO'
        WHEN 'DOWNGRADING' THEN 'ACTIVO, INACTIVO'
        WHEN 'INACTIVE' THEN 'ACTIVE'
        WHEN 'SUSPENDED' THEN 'ACTIVO, EN_BAJA'
      END;
  END IF;

  -- 5. Verificar si tiene visita activa
  SELECT EXISTS(
    SELECT 1 FROM public.visits
    WHERE client_id = p_client_id
    AND status NOT IN ('COMPLETED', 'CANCELLED')
  ) INTO v_tiene_visita_activa;

  -- 6. Actualizar status del cliente (el trigger sincroniza active)
  UPDATE public.clients
  SET status = p_new_status,
      updated_at = now()
  WHERE client_id = p_client_id;

  -- 7. Registrar en auditoría
  INSERT INTO public.client_status_log (
    client_id, 
    previous_status, 
    new_status, 
    changed_by, 
    reason,
    metadata
  )
  VALUES (
    p_client_id, 
    v_estado_actual, 
    p_new_status, 
    p_user_id, 
    p_reason,
    jsonb_build_object(
      'tiene_visita_activa', v_tiene_visita_activa,
      'client_name', v_nombre_cliente
    )
  );

  -- 8. Retornar resultado
  RETURN jsonb_build_object(
    'success', true,
    'client_id', p_client_id,
    'client_name', v_nombre_cliente,
    'previous_status', v_estado_actual,
    'new_status', p_new_status,
    'tiene_visita_activa', v_tiene_visita_activa,
    'message', CASE p_new_status
      WHEN 'DOWNGRADING' THEN 'Cliente marcado para baja. La visita actual (si existe) será la última.'
      WHEN 'INACTIVE' THEN 'Cliente dado de baja. Ya no recibirá visits.'
      WHEN 'SUSPENDED' THEN 'Cliente suspendido temporalmente.'
      WHEN 'ACTIVE' THEN 'Cliente reactivado exitosamente.'
    END
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_can_access_task(p_visit_id uuid, p_task_type text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_can_access boolean := true;
  v_reason text := NULL;
  v_prerequisite_estado text;
BEGIN
  -- Validaciones específicas por type de tarea
  CASE p_task_type
    WHEN 'POST_CUTOFF_PLACEMENT' THEN
      -- Requiere que CORTE esté completado
      SELECT vt.status::text INTO v_prerequisite_estado
      FROM public.visit_tasks vt
      WHERE vt.visit_id = p_visit_id AND vt.task_type = 'CUTOFF';
      
      IF v_prerequisite_estado IS NULL OR v_prerequisite_estado != 'COMPLETED' THEN
        v_can_access := false;
        v_reason := 'Debe completar el CORTE primero';
      END IF;

      -- Requiere que VENTA_ODV esté completed/omitida
      IF v_can_access THEN
        SELECT vt.status::text INTO v_prerequisite_estado
        FROM public.visit_tasks vt
        WHERE vt.visit_id = p_visit_id AND vt.task_type = 'SALE_ODV';
        
        IF v_prerequisite_estado IS NOT NULL 
           AND v_prerequisite_estado NOT IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M') THEN
          v_can_access := false;
          v_reason := 'Debe confirmar la ODV de Venta primero';
        END IF;
      END IF;

    WHEN 'ODV_CABINET' THEN
      -- Requiere LEV_POST_CORTE o LEVANTAMIENTO_INICIAL completado
      SELECT vt.status::text INTO v_prerequisite_estado
      FROM public.visit_tasks vt
      WHERE vt.visit_id = p_visit_id 
      AND vt.task_type IN ('POST_CUTOFF_PLACEMENT', 'INITIAL_PLACEMENT')
      AND vt.status IN ('COMPLETED', 'SKIPPED_M', 'SKIPPED')
      LIMIT 1;
      
      IF v_prerequisite_estado IS NULL THEN
        v_can_access := false;
        v_reason := 'Debe completar el levantamiento primero';
      END IF;

    WHEN 'VISIT_REPORT' THEN
      -- Puede acceder siempre (es la última tarea)
      v_can_access := true;

    ELSE
      -- Otras tareas: verificar step_order
      v_can_access := true;
  END CASE;

  RETURN jsonb_build_object(
    'can_access', v_can_access,
    'reason', v_reason,
    'task_type', p_task_type
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cancel_task(p_visit_id uuid, p_task_type character varying, p_reason text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_task_exists boolean;
BEGIN
  -- Verificar que la tarea existe y no está completed/cancelada
  SELECT EXISTS(
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_type = p_task_type::tipo_visit_task
    AND status NOT IN ('COMPLETED', 'CANCELLED')
  ) INTO v_task_exists;

  IF NOT v_task_exists THEN
    RETURN false;
  END IF;

  -- Cancelar la tarea
  UPDATE public.visit_tasks
  SET
    status = 'CANCELLED',
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('cancel_reason', p_reason)
  WHERE visit_id = p_visit_id
  AND task_type = p_task_type::tipo_visit_task;

  RETURN true;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cancel_visit(p_visit_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visita record;
  v_saga_ids uuid[];
  v_affected_pairs record;
  v_new_qty integer;
BEGIN
  SELECT visit_id, status, client_id
  INTO v_visita
  FROM public.visits
  WHERE visit_id = p_visit_id
  AND status != 'CANCELLED';

  IF v_visita IS NULL THEN
    RETURN false;
  END IF;

  SELECT array_agg(id)
  INTO v_saga_ids
  FROM public.saga_transactions
  WHERE visit_id = p_visit_id;

  IF v_saga_ids IS NOT NULL AND array_length(v_saga_ids, 1) > 0 THEN

    CREATE TEMP TABLE _affected_inventory ON COMMIT DROP AS
      SELECT DISTINCT client_id, sku
      FROM public.inventory_movements
      WHERE id_saga_transaction = ANY(v_saga_ids);

    DELETE FROM public.inventory_movements
    WHERE id_saga_transaction = ANY(v_saga_ids);

    FOR v_affected_pairs IN SELECT client_id, sku FROM _affected_inventory LOOP
      SELECT COALESCE(SUM(
        CASE
          WHEN type = 'PLACEMENT' THEN quantity
          WHEN type IN ('SALE', 'COLLECTION') THEN -quantity
          ELSE 0
        END
      ), 0)
      INTO v_new_qty
      FROM public.inventory_movements
      WHERE client_id = v_affected_pairs.client_id
      AND sku = v_affected_pairs.sku;

      IF v_new_qty > 0 THEN
        INSERT INTO public.cabinet_inventory (client_id, sku, available_quantity, last_updated)
        VALUES (v_affected_pairs.client_id, v_affected_pairs.sku, v_new_qty, now())
        ON CONFLICT (client_id, sku) DO UPDATE
        SET available_quantity = v_new_qty,
            last_updated = now();
      ELSE
        DELETE FROM public.cabinet_inventory
        WHERE client_id = v_affected_pairs.client_id
        AND sku = v_affected_pairs.sku;
      END IF;
    END LOOP;

    DROP TABLE IF EXISTS _affected_inventory;

    DELETE FROM public.saga_adjustments
    WHERE saga_transaction_id = ANY(v_saga_ids);

    DELETE FROM public.saga_compensations
    WHERE saga_transaction_id = ANY(v_saga_ids);

    DELETE FROM public.event_outbox
    WHERE saga_transaction_id = ANY(v_saga_ids);

    DELETE FROM public.saga_zoho_links
    WHERE id_saga_transaction = ANY(v_saga_ids);

    DELETE FROM public.saga_transactions
    WHERE id = ANY(v_saga_ids);

  END IF;

  DELETE FROM public.collections
  WHERE visit_id = p_visit_id;

  DELETE FROM public.visit_tasks
  WHERE visit_id = p_visit_id;

  DELETE FROM public.visit_reports
  WHERE visit_id = p_visit_id;

  UPDATE public.visits
  SET
    status = 'CANCELLED',
    updated_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'cancel_reason', p_reason,
      'cancelled_at', now()::text,
      'previous_estado', v_visita.status::text
    )
  WHERE visit_id = p_visit_id;

  RETURN true;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_client_had_cabinet(p_client_id text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Un cliente tuvo botiquín si tiene visits históricas
  RETURN EXISTS (
    SELECT 1 FROM public.visits WHERE client_id = p_client_id
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_compensate_saga(p_saga_id uuid, p_reason text DEFAULT 'Cancelado por usuario'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_task_tipo text;
BEGIN
  -- 1. Obtener y validar saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  IF v_saga.status = 'CONFIRMED' THEN
    RAISE EXCEPTION 'No se puede compensar una saga ya CONFIRMADA (PIVOT ejecutado): %', p_saga_id;
  END IF;

  IF v_saga.status = 'CANCELLED_F' THEN
    -- Ya cancelada, retornar éxito
    RETURN jsonb_build_object(
      'success', true,
      'already_cancelled', true,
      'saga_id', p_saga_id
    );
  END IF;

  -- 2. Determinar task_type según saga.type
  CASE v_saga.type::text
    WHEN 'INITIAL_PLACEMENT' THEN
      v_task_tipo := 'INITIAL_PLACEMENT';
    WHEN 'POST_CUTOFF_PLACEMENT' THEN
      v_task_tipo := 'POST_CUTOFF_PLACEMENT';
    WHEN 'SALE' THEN
      v_task_tipo := 'CUTOFF';
    WHEN 'COLLECTION' THEN
      v_task_tipo := 'CUTOFF';
    ELSE
      v_task_tipo := NULL;
  END CASE;

  -- 3. Cambiar status de saga a CANCELADA
  UPDATE public.saga_transactions
  SET 
    status = 'CANCELLED_F'::saga_transaction_status,
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'cancel_reason', p_reason,
      'cancelled_at', now()
    ),
    updated_at = now()
  WHERE id = p_saga_id;

  -- 4. Marcar tarea asociada como CANCELADO (si aplica)
  IF v_task_tipo IS NOT NULL THEN
    UPDATE public.visit_tasks
    SET 
      status = 'CANCELLED',
      metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'cancel_reason', p_reason,
        'cancelled_saga_id', p_saga_id
      ),
      last_activity_at = now()
    WHERE visit_id = v_saga.visit_id
    AND task_type = v_task_tipo::visit_task_type
    AND status NOT IN ('COMPLETED', 'SKIPPED');
  END IF;

  -- 5. Si es RECOLECCION, también eliminar de table_name collections
  IF v_saga.type::text = 'COLLECTION' THEN
    -- Eliminar items primero (FK)
    DELETE FROM public.collection_items
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.collections
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar evidencias
    DELETE FROM public.collection_evidence
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.collections
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar firmas
    DELETE FROM public.collection_signatures
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.collections
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar recolección
    DELETE FROM public.collections
    WHERE visit_id = v_saga.visit_id;
  END IF;

  -- ❌ NO necesita revertir inventory_movements
  -- ❌ NO necesita revertir cabinet_inventory
  -- Porque nunca se crearon (SSoT: solo se crean en PIVOT)

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'type', v_saga.type,
    'reason', p_reason
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_complete_collection(p_visit_id uuid, p_responsible text, p_observations text, p_signature_path text, p_evidence_paths text[])
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_recoleccion_id uuid;
  v_saga_id uuid;
  v_result jsonb;
BEGIN
  -- Obtener recolección de la visita
  SELECT r.recoleccion_id INTO v_recoleccion_id
  FROM public.collections r
  WHERE r.visit_id = p_visit_id
  LIMIT 1;

  IF v_recoleccion_id IS NULL THEN
    RAISE EXCEPTION 'No existe recolección para visit_id';
  END IF;

  -- Validar responsable
  IF p_responsible IS NULL OR length(trim(p_responsible)) = 0 THEN
    RAISE EXCEPTION 'Responsable CEDIS requerido';
  END IF;

  -- Validar firma
  IF p_signature_path IS NULL OR length(trim(p_signature_path)) = 0 THEN
    RAISE EXCEPTION 'Firma requerida';
  END IF;

  -- Insertar o actualizar firma (upsert)
  INSERT INTO public.collection_signatures (recoleccion_id, storage_path)
  VALUES (v_recoleccion_id, p_signature_path)
  ON CONFLICT (recoleccion_id) DO UPDATE
  SET storage_path = EXCLUDED.storage_path, signed_at = now();

  -- Insertar evidencias fotográficas
  IF p_evidence_paths IS NOT NULL THEN
    INSERT INTO public.collection_evidence (recoleccion_id, storage_path)
    SELECT v_recoleccion_id, unnest(p_evidence_paths)
    ON CONFLICT DO NOTHING;
  END IF;

  -- Actualizar recolección como ENTREGADA
  UPDATE public.collections
  SET
    status = 'ENTREGADA',
    delivered_at = now(),
    cedis_responsible_name = p_responsible,
    cedis_observations = p_observations,
    updated_at = now()
  WHERE recoleccion_id = v_recoleccion_id;

  -- Buscar saga de RECOLECCION
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND type::text = 'COLLECTION'
  ORDER BY created_at DESC LIMIT 1;

  -- Si existe saga, confirmarla para crear movimientos
  IF v_saga_id IS NOT NULL THEN
    SELECT rpc_confirm_saga_pivot(v_saga_id, NULL, NULL) INTO v_result;
  END IF;

  -- Completar tarea de recolección
  UPDATE public.visit_tasks
  SET status = 'COMPLETED', completed_at = now(), last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_confirm_odv(p_visit_id uuid, p_saga_type text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga_id uuid;
  v_task_tipo visit_task_type;
  v_result jsonb;
BEGIN
  -- Mapear saga_type a task_type
  v_task_tipo := CASE p_saga_type
    WHEN 'SALE' THEN 'SALE_ODV'::visit_task_type
    WHEN 'INITIAL_PLACEMENT' THEN 'ODV_CABINET'::visit_task_type
    WHEN 'POST_CUTOFF_PLACEMENT' THEN 'ODV_CABINET'::visit_task_type
    ELSE p_saga_type::visit_task_type
  END;

  -- Buscar saga
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND type::text = p_saga_type
  ORDER BY created_at DESC LIMIT 1;

  IF v_saga_id IS NULL THEN
    RAISE EXCEPTION 'Saga % no encontrada para la visita', p_saga_type;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  -- Si la saga ya está CONFIRMADO, la función retornará sin hacer nada
  SELECT rpc_confirm_saga_pivot(v_saga_id, NULL, NULL) INTO v_result;

  -- La tarea ya se actualiza dentro de rpc_confirm_saga_pivot
  -- pero por si acaso, asegurar que quede COMPLETADO
  UPDATE visit_tasks
  SET
    status = 'COMPLETED'::visit_task_status,
    completed_at = COALESCE(completed_at, now()),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'confirmed_at', now(),
      'saga_id', v_saga_id,
      'saga_type', p_saga_type
    )
  WHERE visit_id = p_visit_id 
  AND task_type = v_task_tipo
  AND status != 'COMPLETED'::visit_task_status;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_confirm_odv_with_cotizacion(p_visit_id uuid, p_odv_id text, p_saga_type text, p_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_id_cliente varchar;
  v_target_table text;
  v_inserted_count int := 0;
  v_verify_count int;
  v_pivot_result jsonb;
  v_zoho_link_id int;
  v_item record;
BEGIN
  RAISE NOTICE '[cotizacion:confirm] visit_id=%, odv_id=%, saga_type=%, item_count=%',
    p_visit_id, p_odv_id, p_saga_type, jsonb_array_length(p_items);

  SELECT * INTO v_saga
  FROM saga_transactions
  WHERE visit_id = p_visit_id
    AND type::text = p_saga_type
    AND status != 'CANCELLED_F'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'No se encontró saga % para visita %', p_saga_type, p_visit_id;
  END IF;

  v_id_cliente := v_saga.client_id;

  IF p_saga_type = 'SALE' THEN
    v_target_table := 'odv_sales';
  ELSIF p_saga_type IN ('INITIAL_PLACEMENT', 'POST_CUTOFF_PLACEMENT') THEN
    v_target_table := 'cabinet_odv';
  ELSE
    RAISE EXCEPTION 'Tipo de saga no soportado: %', p_saga_type;
  END IF;

  IF v_target_table = 'odv_sales' THEN
    FOR v_item IN
      SELECT
        (elem->>'sku')::varchar(50) AS sku,
        (elem->>'quantity')::int AS quantity,
        (elem->>'price')::numeric(10,2) AS price
      FROM jsonb_array_elements(p_items) AS elem
    LOOP
      INSERT INTO odv_sales (client_id, sku, odv_id, date, quantity, price, invoice_status)
      VALUES (v_id_cliente, v_item.sku, p_odv_id, CURRENT_DATE, v_item.quantity, v_item.price, 'cotizacion_aprobada')
      ON CONFLICT (odv_id, client_id, sku) DO NOTHING;
      IF FOUND THEN v_inserted_count := v_inserted_count + 1; END IF;
    END LOOP;
  ELSE
    FOR v_item IN
      SELECT
        (elem->>'sku')::varchar(50) AS sku,
        (elem->>'quantity')::int AS quantity
      FROM jsonb_array_elements(p_items) AS elem
    LOOP
      INSERT INTO cabinet_odv (client_id, sku, odv_id, date, quantity, invoice_status)
      VALUES (v_id_cliente, v_item.sku, p_odv_id, CURRENT_DATE, v_item.quantity, 'cotizacion_aprobada')
      ON CONFLICT (odv_id, client_id, sku) DO NOTHING;
      IF FOUND THEN v_inserted_count := v_inserted_count + 1; END IF;
    END LOOP;
  END IF;

  IF v_target_table = 'odv_sales' THEN
    SELECT COUNT(*) INTO v_verify_count FROM odv_sales WHERE odv_id = p_odv_id AND client_id = v_id_cliente;
  ELSE
    SELECT COUNT(*) INTO v_verify_count FROM cabinet_odv WHERE odv_id = p_odv_id AND client_id = v_id_cliente;
  END IF;

  IF v_verify_count = 0 THEN
    RAISE EXCEPTION 'No se encontraron registros en % para odv_id=%', v_target_table, p_odv_id;
  END IF;

  v_pivot_result := rpc_confirm_saga_pivot(
    p_saga_id := v_saga.id,
    p_zoho_id := p_odv_id,
    p_zoho_items := p_items
  );

  SELECT id INTO v_zoho_link_id
  FROM saga_zoho_links
  WHERE id_saga_transaction = v_saga.id AND zoho_id = p_odv_id;

  IF v_zoho_link_id IS NOT NULL THEN
    UPDATE saga_zoho_links
    SET zoho_sync_status = 'synced', zoho_synced_at = now(), updated_at = now()
    WHERE id = v_zoho_link_id;
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', v_saga.id,
    'zoho_link_id', v_zoho_link_id,
    'items_inserted', v_inserted_count,
    'pivot_result', v_pivot_result
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_confirm_saga_pivot(p_saga_id uuid, p_zoho_id text DEFAULT NULL::text, p_zoho_items jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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

  -- 2. Determine type de zoho_link, task_type and movement_type
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

  -- 3. Change saga state to CONFIRMADO (ONLY on first confirmation)
  -- NOTE: This fires trigger_generate_movements_from_saga in the same transaction.
  -- The trigger may generate movements BEFORE control returns here.
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
    -- GUARD: Check if trigger already generated movements for this saga.
    -- The AFTER UPDATE trigger on saga_transactions fires in the same transaction
    -- and may have already created movements + updated inventory.
    -- Without this guard, the FOR loop below would read stale inventory and
    -- create duplicate movements with wrong quantity_before/quantity_after.
    IF EXISTS (SELECT 1 FROM inventory_movements WHERE id_saga_transaction = p_saga_id) THEN
      -- Movements already generated by trigger — only link zoho FK
      IF v_zoho_link_id IS NOT NULL THEN
        UPDATE inventory_movements
        SET id_saga_zoho_link = v_zoho_link_id
        WHERE id_saga_transaction = p_saga_id
          AND id_saga_zoho_link IS NULL
          AND type != 'HOLDING';
      END IF;
    ELSE
      -- Trigger did not generate movements — do it inline (fallback)
      FOR v_item IN
        SELECT
          (item->>'sku')::varchar as sku,
          (item->>'quantity')::int as quantity
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

        -- Freeze current catalog price for this SKU
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
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_consolidate_visits()
 RETURNS TABLE(visitas_consolidated integer, visitas_deleted integer, sagas_moved integer, tasks_moved integer, informes_created integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_consolidated integer := 0;
  v_visitas_deleted integer := 0;
  v_sagas_moved integer := 0;
  v_tasks_moved integer := 0;
  v_informes_created integer := 0;
  v_row_count integer := 0;
  v_dup record;
  v_primary_visit_id uuid;
  v_duplicate_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Consolidar visits duplicadas
  -- =====================================================
  -- Encontrar grupos de visits duplicadas (mismo cliente, usuario, date)

  FOR v_dup IN
    SELECT
      v.client_id,
      v.user_id,
      DATE(v.created_at) as date,
      array_agg(v.visit_id ORDER BY v.created_at ASC) as visit_ids,
      COUNT(*) as num_visitas
    FROM public.visits v
    GROUP BY v.client_id, v.user_id, DATE(v.created_at)
    HAVING COUNT(*) > 1
  LOOP
    -- La primera visita (más antigua) será la principal
    v_primary_visit_id := v_dup.visit_ids[1];

    -- Procesar cada visita duplicada (desde la segunda en adelante)
    FOR i IN 2..array_length(v_dup.visit_ids, 1) LOOP
      v_duplicate_visit_id := v_dup.visit_ids[i];

      -- Mover saga_transactions al visit_id principal
      UPDATE public.saga_transactions
      SET visit_id = v_primary_visit_id, updated_at = now()
      WHERE visit_id = v_duplicate_visit_id;

      GET DIAGNOSTICS v_row_count = ROW_COUNT;
      v_sagas_moved := v_sagas_moved + v_row_count;

      -- Mover collections al visit_id principal
      UPDATE public.collections
      SET visit_id = v_primary_visit_id, updated_at = now()
      WHERE visit_id = v_duplicate_visit_id;

      -- Eliminar tareas duplicadas (las de la visita duplicada)
      DELETE FROM public.visit_tasks
      WHERE visit_id = v_duplicate_visit_id;

      GET DIAGNOSTICS v_row_count = ROW_COUNT;
      v_tasks_moved := v_tasks_moved + v_row_count;

      -- Eliminar la visita duplicada
      DELETE FROM public.visits
      WHERE visit_id = v_duplicate_visit_id;

      v_visitas_deleted := v_visitas_deleted + 1;
    END LOOP;

    -- Asegurar que la visita principal sea VISITA_CORTE si tiene tareas de corte
    UPDATE public.visits
    SET
      type = 'VISIT_CUTOFF'::public.visit_type,
      updated_at = now()
    WHERE visit_id = v_primary_visit_id
      AND EXISTS (
        SELECT 1 FROM public.saga_transactions st
        WHERE st.visit_id = v_primary_visit_id
          AND st.type::text IN ('CUTOFF', 'CUTOFF_RENEWAL', 'SALE_ODV', 'SALE', 'COLLECTION', 'POST_CUTOFF_PLACEMENT')
      );

    v_visitas_consolidated := v_visitas_consolidated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Recrear tareas completas para visits consolidadas
  -- =====================================================
  -- Para cada VISITA_CORTE, asegurar que tenga todas las tareas

  FOR v_dup IN
    SELECT v.visit_id, v.created_at
    FROM public.visits v
    WHERE v.type = 'VISIT_CUTOFF'
      AND v.metadata->>'migrated_from_legacy' = 'true'
  LOOP
    -- Verificar y crear tarea CORTE si no existe
    INSERT INTO public.visit_tasks (visit_id, task_type, status, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'CUTOFF', 'COMPLETED', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_type = 'CUTOFF');

    -- Verificar y crear tarea VENTA_ODV si no existe
    INSERT INTO public.visit_tasks (visit_id, task_type, status, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'SALE_ODV', 'COMPLETED', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_type = 'SALE_ODV');

    -- Verificar y crear tarea RECOLECCION si no existe
    INSERT INTO public.visit_tasks (visit_id, task_type, status, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'COLLECTION', 'COMPLETED', false, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_type = 'COLLECTION');

    -- Verificar y crear tarea LEV_POST_CORTE si no existe
    INSERT INTO public.visit_tasks (visit_id, task_type, status, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'POST_CUTOFF_PLACEMENT', 'COMPLETED', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_type = 'POST_CUTOFF_PLACEMENT');

    -- Verificar y crear tarea INFORME_VISITA si no existe
    INSERT INTO public.visit_tasks (visit_id, task_type, status, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'VISIT_REPORT', 'COMPLETED', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_type = 'VISIT_REPORT');
  END LOOP;

  -- =====================================================
  -- PASO 3: Vincular referencias de saga a tareas
  -- =====================================================

  -- Vincular CORTE/CORTE_RENOVACION a tarea CORTE
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('CUTOFF', 'CUTOFF_RENEWAL')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_type = 'CUTOFF'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('CUTOFF', 'CUTOFF_RENEWAL')
    );

  -- Vincular VENTA_ODV/VENTA a tarea VENTA_ODV
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('SALE_ODV', 'SALE')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_type = 'SALE_ODV'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('SALE_ODV', 'SALE')
    );

  -- Vincular LEV_POST_CORTE/LEVANTAMIENTO a tarea LEV_POST_CORTE
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('POST_CUTOFF_PLACEMENT', 'INITIAL_PLACEMENT')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY
        CASE st.type::text WHEN 'POST_CUTOFF_PLACEMENT' THEN 1 ELSE 2 END,
        st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_type = 'POST_CUTOFF_PLACEMENT'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text IN ('POST_CUTOFF_PLACEMENT', 'INITIAL_PLACEMENT')
    );

  -- Vincular RECOLECCION saga a tarea RECOLECCION
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text = 'COLLECTION'
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_type = 'COLLECTION'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.type::text = 'COLLECTION'
    );

  -- =====================================================
  -- PASO 4: Migrar cycle_surveys a visit_reports
  -- =====================================================

  INSERT INTO public.visit_reports (visit_id, responses, label, compliance_score, created_at)
  SELECT
    v.visit_id,
    ec.responses,
    'MIGRADO'::varchar,
    0,
    COALESCE(ec.completed_date, ec.created_at)
  FROM public.cycle_surveys ec
  JOIN public.visits v ON v.cycle_id = ec.cycle_id
  WHERE ec.completed = true
    AND NOT EXISTS (
      SELECT 1 FROM public.visit_reports vi WHERE vi.visit_id = v.visit_id
    )
  ON CONFLICT (visit_id) DO NOTHING;

  GET DIAGNOSTICS v_informes_created = ROW_COUNT;

  -- Vincular informes a tareas INFORME_VISITA
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'visit_reports',
    reference_id = (
      SELECT vi.report_id::text
      FROM public.visit_reports vi
      WHERE vi.visit_id = vt.visit_id
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_type = 'VISIT_REPORT'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.visit_reports vi WHERE vi.visit_id = vt.visit_id
    );

  RETURN QUERY SELECT v_visitas_consolidated, v_visitas_deleted, v_sagas_moved, v_tasks_moved, v_informes_created;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_count_unread_notifications()
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
  v_count INTEGER;
BEGIN
  SELECT u.user_id INTO v_user_id
  FROM public.users u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  SELECT COUNT(*)::integer INTO v_count
  FROM public.admin_notifications n
  WHERE n.read = false
  AND (n.for_user IS NULL OR n.for_user = v_user_id);

  RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_client_id character varying, p_type character varying DEFAULT 'VISIT_CUTOFF'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
  v_corte_number integer;
BEGIN
  SELECT u.user_id INTO v_id_usuario
  FROM public.users u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en table_name users';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.visits v
    WHERE v.client_id = p_client_id
      AND v.status IN ('PENDING', 'IN_PROGRESS', 'SCHEDULED')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Calculate corte_number: LEV = 0, CORTE = max + 1
  IF p_type = 'VISIT_INITIAL_PLACEMENT' THEN
    v_corte_number := 0;
  ELSE
    SELECT COALESCE(MAX(corte_number), -1) + 1
    INTO v_corte_number
    FROM public.visits
    WHERE client_id = p_client_id;
  END IF;

  INSERT INTO public.visits (
    client_id, user_id, cycle_id, type,
    status, created_at, due_at, last_activity_at, corte_number
  )
  VALUES (
    p_client_id, v_id_usuario, NULL, p_type::visit_type,
    'PENDING', now(), now() + interval '1 day', now(), v_corte_number
  )
  RETURNING visit_id INTO v_visit_id;

  INSERT INTO public.visit_reports (visit_id, responses, completed)
  VALUES (v_visit_id, '{}'::jsonb, false);

  IF p_type = 'VISIT_INITIAL_PLACEMENT' THEN
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'INITIAL_PLACEMENT', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_CABINET', 'PENDING', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'VISIT_REPORT', 'PENDING', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CUTOFF', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'SALE_ODV', 'PENDING', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'COLLECTION', 'PENDING', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'POST_CUTOFF_PLACEMENT', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_CABINET', 'PENDING', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'VISIT_REPORT', 'PENDING', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_client_id character varying, p_cycle_id integer, p_type character varying DEFAULT 'VISIT_CUTOFF'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
  v_corte_number integer;
BEGIN
  SELECT u.user_id INTO v_id_usuario
  FROM public.users u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en table_name users';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.visits v
    WHERE v.client_id = p_client_id
      AND v.status IN ('PENDING', 'IN_PROGRESS', 'SCHEDULED')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Calculate corte_number: LEV = 0, CORTE = max + 1
  IF p_type = 'VISIT_INITIAL_PLACEMENT' THEN
    v_corte_number := 0;
  ELSE
    SELECT COALESCE(MAX(corte_number), -1) + 1
    INTO v_corte_number
    FROM public.visits
    WHERE client_id = p_client_id;
  END IF;

  INSERT INTO public.visits (
    client_id, user_id, cycle_id, type,
    status, created_at, due_at, last_activity_at, corte_number
  )
  VALUES (
    p_client_id, v_id_usuario, p_cycle_id, p_type,
    'PENDING', now(), now() + interval '1 day', now(), v_corte_number
  )
  RETURNING visit_id INTO v_visit_id;

  INSERT INTO public.visit_reports (visit_id, responses, completed)
  VALUES (v_visit_id, '{}'::jsonb, false);

  IF p_type = 'VISIT_INITIAL_PLACEMENT' THEN
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'INITIAL_PLACEMENT', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_CABINET', 'PENDING', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'VISIT_REPORT', 'PENDING', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CUTOFF', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'SALE_ODV', 'PENDING', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'COLLECTION', 'PENDING', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'POST_CUTOFF_PLACEMENT', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_CABINET', 'PENDING', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'VISIT_REPORT', 'PENDING', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_client_status_history(p_client_id character varying)
 RETURNS TABLE(id uuid, previous_status client_status, new_status client_status, changed_by character varying, changed_by_nombre character varying, changed_at timestamp with time zone, reason text, days_in_previous_status integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT 
    cel.id,
    cel.previous_status,
    cel.new_status,
    cel.changed_by,
    u.name as changed_by_nombre,
    cel.changed_at,
    cel.reason,
    cel.days_in_previous_status
  FROM public.client_status_log cel
  LEFT JOIN public.users u ON u.user_id = cel.changed_by
  WHERE cel.client_id = p_client_id
  ORDER BY cel.changed_at DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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

  -- 3. Combinar items de sagas VENTA y RECOLECCION
  -- NOTA: Ahora parsea correctamente: VENTA usa 'quantity', RECOLECCION usa 'cantidad_salida'
  SELECT jsonb_agg(combined_item)
  INTO v_items
  FROM (
    -- Items de VENTA: quantity representa lo vendido
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'cantidad_actual', 0,
      'vendido', COALESCE((item->>'quantity')::int, (item->>'vendido')::int, 0),
      'recolectado', 0,
      'permanencia', false
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'SALE'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0

    UNION ALL

    -- Items de RECOLECCION: cantidad_salida representa lo recolectado
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'cantidad_actual', 0,
      'vendido', 0,
      'recolectado', COALESCE(
        (item->>'cantidad_salida')::int, 
        (item->>'recolectado')::int, 
        (item->>'quantity')::int, 
        0
      ),
      'permanencia', COALESCE((item->>'cantidad_permanencia')::int, 0) > 0
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0
  ) items
  WHERE (combined_item->>'vendido')::int > 0 
     OR (combined_item->>'recolectado')::int > 0;

  IF v_items IS NOT NULL AND jsonb_array_length(v_items) > 0 THEN
    RETURN v_items;
  END IF;

  -- 4. Buscar en inventory_movements (usando type semántico)
  SELECT jsonb_agg(item_data)
  INTO v_items
  FROM (
    SELECT jsonb_build_object(
      'sku', mi.sku,
      'cantidad_actual', 0,
      'vendido', COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0),
      'recolectado', COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0),
      'permanencia', CASE WHEN SUM(CASE WHEN mi.type::text = 'HOLDING' THEN 1 ELSE 0 END) > 0 THEN true ELSE false END
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
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_cutoff_holding_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
BEGIN
  -- Leer items de permanencia del CORTE
  -- Calcula la quantity restante: cantidad_actual - vendido - recolectado
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'product', COALESCE(m.product, item->>'product', item->>'sku'),
        'quantity', GREATEST(
          0,
          (COALESCE(item->>'cantidad_actual', item->>'quantity', '0'))::int
          - (COALESCE(item->>'vendido', '0'))::int
          - (COALESCE(item->>'recolectado', '0'))::int
        )
      )
    ) FILTER (WHERE 
      GREATEST(
        0,
        (COALESCE(item->>'cantidad_actual', item->>'quantity', '0'))::int
        - (COALESCE(item->>'vendido', '0'))::int
        - (COALESCE(item->>'recolectado', '0'))::int
      ) > 0
    ),
    '[]'::jsonb
  )
  INTO v_items
  FROM saga_transactions st
  CROSS JOIN LATERAL jsonb_array_elements(st.items) AS item
  LEFT JOIN medications m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.type = 'CUTOFF'
    AND (item->>'permanencia')::boolean = true;

  RETURN v_items;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_post_cutoff_placement_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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

  -- Obtener SKUs con permanencia del CORTE para esta visita
  SELECT ARRAY_AGG(DISTINCT item->>'sku')
  INTO v_permanencia_skus
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'CUTOFF'
    AND st.items IS NOT NULL
    AND (item->>'permanencia')::boolean = true;

  -- También buscar en inventory_movements con PERMANENCIA (usando type semántico)
  IF v_permanencia_skus IS NULL THEN
    SELECT ARRAY_AGG(DISTINCT mi.sku)
    INTO v_permanencia_skus
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text = 'HOLDING';
  END IF;

  -- Primero buscar en LEV_POST_CORTE (nuevo formato)
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_items IS NOT NULL THEN
    -- LEV_POST_CORTE: {sku, quantity, es_permanencia}
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'quantity', COALESCE((item->>'quantity')::int, 0),
        'es_permanencia', COALESCE(
          (item->>'es_permanencia')::boolean,
          (item->>'sku') = ANY(v_permanencia_skus)
        )
      )
    )
    INTO v_items
    FROM jsonb_array_elements(v_items) AS item
    WHERE COALESCE((item->>'quantity')::int, 0) > 0;

    RETURN COALESCE(v_items, '[]'::jsonb);
  END IF;

  -- Buscar en inventory_movements con PERMANENCIA (usando type semántico)
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
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_placement_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente varchar;
  v_items jsonb;
BEGIN
  SELECT v.client_id INTO v_id_cliente
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_client(v_id_cliente) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Buscar saga de LEVANTAMIENTO_INICIAL
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  -- Transformar items: cantidad_entrada es la quantity en el botiquín
  IF v_items IS NOT NULL THEN
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'quantity', COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)
      )
    )
    INTO v_items
    FROM jsonb_array_elements(v_items) AS item
    WHERE COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0) > 0;
  END IF;

  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_next_visit_type(p_client_id character varying)
 RETURNS character varying
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_has_completed_visit boolean;
BEGIN
  -- Check if client has any completed VISITA_LEVANTAMIENTO_INICIAL or any VISITA_CORTE
  SELECT EXISTS (
    SELECT 1
    FROM public.visits v
    WHERE v.client_id = p_client_id
      AND v.status = 'COMPLETED'
      AND (v.type = 'VISIT_INITIAL_PLACEMENT' OR v.type = 'VISIT_CUTOFF')
  ) INTO v_has_completed_visit;

  IF v_has_completed_visit THEN
    RETURN 'VISIT_CUTOFF';
  ELSE
    RETURN 'VISIT_INITIAL_PLACEMENT';
  END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_unread_notifications()
 RETURNS TABLE(id uuid, type notification_type, title character varying, message text, metadata jsonb, created_at timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
BEGIN
  -- Obtener usuario actual
  SELECT u.user_id INTO v_user_id
  FROM public.users u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  RETURN QUERY
  SELECT 
    n.id,
    n.type,
    n.title,
    n.message,
    n.metadata,
    n.created_at
  FROM public.admin_notifications n
  WHERE n.read = false
  AND (n.for_user IS NULL OR n.for_user = v_user_id)
  ORDER BY n.created_at DESC
  LIMIT 50;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_odv_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
  v_odv_ids jsonb;
  v_odv_id_list text[];
  v_sold_skus text[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.visits WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_visit(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Get all ODV IDs from saga_zoho_links for this visit
  SELECT ARRAY_AGG(szl.zoho_id)
  INTO v_odv_id_list
  FROM public.saga_zoho_links szl
  JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
  WHERE st.visit_id = p_visit_id
    AND szl.type IN ('SALE', 'CABINET');

  -- 1. Get items from inventory_movements with tipo_legacy = 'SALE'
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', items.sku,
      'cantidad_vendida', items.cantidad_vendida
    )
  ),
  ARRAY_AGG(items.sku)
  INTO v_items, v_sold_skus
  FROM (
    SELECT mi.sku, SUM(mi.quantity) as cantidad_vendida
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo_legacy::text = 'SALE'
      AND mi.quantity > 0
    GROUP BY mi.sku
  ) items;

  -- If found items from movimientos, get ODV IDs that have those SKUs
  IF v_items IS NOT NULL AND v_sold_skus IS NOT NULL THEN
    SELECT jsonb_agg(DISTINCT
      jsonb_build_object(
        'odv_id', odv_data.odv_id,
        'date', odv_data.fecha_odv,
        'total_piezas', odv_data.total_piezas
      )
    )
    INTO v_odv_ids
    FROM (
      SELECT vo.odv_id,
             MIN(vo.date) as fecha_odv,
             SUM(vo.quantity) as total_piezas
      FROM public.odv_sales vo
      WHERE vo.sku = ANY(v_sold_skus)
        AND vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.odv_id
    ) odv_data;
  END IF;

  -- 2. If no items from movimientos, try odv_sales using ODV IDs
  IF v_items IS NULL AND v_odv_id_list IS NOT NULL THEN
    -- Get items from odv_sales
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', items.sku,
        'cantidad_vendida', items.cantidad_vendida
      )
    )
    INTO v_items
    FROM (
      SELECT vo.sku, SUM(vo.quantity) as cantidad_vendida
      FROM public.odv_sales vo
      WHERE vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.sku
    ) items;

    -- Get ODV IDs with their actual totals from odv_sales
    SELECT jsonb_agg(
      jsonb_build_object(
        'odv_id', odv_data.odv_id,
        'date', odv_data.fecha_odv,
        'total_piezas', odv_data.total_piezas
      ) ORDER BY odv_data.fecha_odv
    )
    INTO v_odv_ids
    FROM (
      SELECT vo.odv_id,
             MIN(vo.date) as fecha_odv,
             SUM(vo.quantity) as total_piezas
      FROM public.odv_sales vo
      WHERE vo.odv_id = ANY(v_odv_id_list)
      GROUP BY vo.odv_id
    ) odv_data;
  END IF;

  -- 3. If still no items, try CORTE items with 'vendido'
  IF v_items IS NULL THEN
    SELECT st.items INTO v_items
    FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'CUTOFF'
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
          'date', szl.created_at::date,
          'total_piezas', 0
        ) ORDER BY szl.created_at
      )
      INTO v_odv_ids
      FROM public.saga_zoho_links szl
      JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
      WHERE st.visit_id = p_visit_id
        AND szl.type IN ('SALE', 'CABINET');
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'odv_ids', COALESCE(v_odv_ids, '[]'::jsonb),
    'odv_id', (
      SELECT szl.zoho_id
      FROM public.saga_zoho_links szl
      JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
      WHERE st.visit_id = p_visit_id
        AND szl.type IN ('SALE', 'CABINET')
      ORDER BY szl.created_at
      LIMIT 1
    ),
    'items', COALESCE(v_items, '[]'::jsonb)
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_client_tiers()
 RETURNS TABLE(tier character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT DISTINCT c.tier
  FROM clients c
  WHERE c.tier IS NOT NULL AND c.tier != ''
  ORDER BY c.tier;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_user_notifications(p_user_id character varying, p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_unread_only boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_notifications JSONB;
    v_unread_count INTEGER;
BEGIN
    -- Count unread
    SELECT COUNT(*) INTO v_unread_count
    FROM notifications
    WHERE user_id = p_user_id
      AND read_at IS NULL
      AND (expires_at IS NULL OR expires_at > NOW());

    -- Get notifications
    SELECT COALESCE(jsonb_agg(n ORDER BY n.created_at DESC), '[]')
    INTO v_notifications
    FROM (
        SELECT
            id, type, title, body, data,
            read_at, created_at,
            read_at IS NULL as is_unread
        FROM notifications
        WHERE user_id = p_user_id
          AND (expires_at IS NULL OR expires_at > NOW())
          AND (NOT p_unread_only OR read_at IS NULL)
        ORDER BY created_at DESC
        LIMIT p_limit OFFSET p_offset
    ) n;

    RETURN jsonb_build_object(
        'notifications', v_notifications,
        'unread_count', v_unread_count
    );
END;
$function$
;

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
$function$
;

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
  SELECT v.client_id, v.type::text
  INTO v_id_cliente, v_visit_tipo
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar acceso
  IF NOT public.can_access_visit(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Verificar si hay inventory_movements (fuente de verdad)
  SELECT EXISTS(
    SELECT 1 FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
  ) INTO v_has_movimientos;

  -- 1. CORTE: FIRST try to read from visit_tasks.metadata (most reliable source)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'vendido', COALESCE((item->>'vendido')::int, 0),
      'recolectado', COALESCE((item->>'recolectado')::int, 0),
      'permanencia', 0
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
    -- Calculate totals from the corte items
    SELECT
      COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0)::int,
      COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.visit_tasks vt,
         jsonb_array_elements(vt.metadata->'items') AS item
    WHERE vt.visit_id = p_visit_id
      AND vt.task_type = 'CUTOFF'
      AND vt.status = 'COMPLETED';
  ELSIF v_has_movimientos THEN
    -- FALLBACK: Use inventory_movements if visit_tasks.metadata is empty
    SELECT jsonb_agg(item_data)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'product', product,
        'vendido', vendido,
        'recolectado', recolectado,
        'permanencia', permanencia
      ) as item_data
      FROM (
        SELECT
          mi.sku,
          COALESCE(m.product, mi.sku) as product,
          COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0)::int as vendido,
          COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0)::int as recolectado,
          COALESCE(SUM(CASE WHEN mi.type::text = 'HOLDING' THEN mi.quantity ELSE 0 END), 0)::int as permanencia
        FROM public.inventory_movements mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medications m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.type::text IN ('SALE', 'COLLECTION', 'HOLDING')
        GROUP BY mi.sku, m.product
        -- FIX: Use mi.quantity for PERMANENCIA in HAVING clause
        HAVING SUM(CASE WHEN mi.type::text IN ('SALE', 'COLLECTION') THEN mi.quantity ELSE 0 END) > 0
           OR SUM(CASE WHEN mi.type::text = 'HOLDING' THEN mi.quantity ELSE 0 END) > 0
      ) grouped
    ) items;

    SELECT
      COALESCE(SUM(CASE WHEN mi.type::text = 'SALE' THEN mi.quantity ELSE 0 END), 0)::int,
      COALESCE(SUM(CASE WHEN mi.type::text = 'COLLECTION' THEN mi.quantity ELSE 0 END), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.type::text IN ('SALE', 'COLLECTION');
  ELSE
    -- FALLBACK: Use saga_transactions.items
    SELECT jsonb_agg(combined_item)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'product', COALESCE(m.product, item->>'sku'),
        'vendido', COALESCE((item->>'quantity')::int, (item->>'vendido')::int, 0),
        'recolectado', 0,
        'permanencia', 0
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
        'vendido', 0,
        'recolectado', COALESCE(
          (item->>'cantidad_salida')::int,
          (item->>'quantity')::int,
          0
        ),
        'permanencia', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medications m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.type::text = 'COLLECTION'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
    ) items
    WHERE (combined_item->>'vendido')::int > 0
       OR (combined_item->>'recolectado')::int > 0;

    SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
    INTO v_total_vendido
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'SALE'
      AND st.items IS NOT NULL;

    SELECT COALESCE(SUM(COALESCE((item->>'cantidad_salida')::int, (item->>'quantity')::int, 0)), 0)::int
    INTO v_total_recolectado
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL;
  END IF;

  -- 2. LEVANTAMIENTO_INICIAL
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)
    )
  )
  INTO v_levantamiento_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'quantity')::int, 0)), 0)::int
  INTO v_total_levantamiento
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'INITIAL_PLACEMENT'
    AND st.items IS NOT NULL;

  -- 3. LEV_POST_CORTE
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'quantity')::int, 0),
      'es_permanencia', COALESCE((item->>'es_permanencia')::boolean, false)
    )
  )
  INTO v_lev_post_corte_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'quantity')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
  INTO v_total_lev_post_corte
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.type::text = 'POST_CUTOFF_PLACEMENT'
    AND st.items IS NOT NULL;

  -- 4. ODVs de VENTA (usando saga_zoho_links.type = 'SALE')
  --    FIX: Use COALESCE(szl.items, st.items) for per-ODV items
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_venta
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'date', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'type', szl.type::text,
      'total_piezas', (
        SELECT COALESCE(SUM(COALESCE((item->>'quantity')::int, 0)), 0)::int
        FROM jsonb_array_elements(COALESCE(szl.items, st.items)) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'product', COALESCE(m.product, item->>'sku'),
            'cantidad_vendida', COALESCE((item->>'quantity')::int, 0)
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

  -- 5. ODVs de BOTIQUIN (usando saga_zoho_links.type = 'CABINET')
  --    FIX: Use COALESCE(szl.items, st.items) for per-ODV items
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_botiquin
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'date', szl.created_at,
      'status', COALESCE(szl.zoho_sync_status, 'pending'),
      'type', szl.type::text,
      'total_piezas', (
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

  -- 6. Resumen de movimientos
  SELECT
    COALESCE(COUNT(*)::int, 0),
    COALESCE(SUM(mi.quantity)::int, 0),
    COALESCE(COUNT(DISTINCT mi.sku)::int, 0)
  INTO v_mov_total_count, v_mov_total_cantidad, v_mov_unique_skus
  FROM public.inventory_movements mi
  JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
  WHERE st.visit_id = p_visit_id;

  SELECT COALESCE(jsonb_object_agg(tipo_text, suma), '{}'::jsonb)
  INTO v_mov_by_tipo
  FROM (
    SELECT mi.type::text as tipo_text, SUM(mi.quantity)::int as suma
    FROM public.inventory_movements mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    GROUP BY mi.type
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
      'product', COALESCE(m.product, item->>'sku'),
      'quantity', COALESCE((item->>'recolectado')::int, 0)
    )
  )
  INTO v_recoleccion_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medications m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_type = 'CUTOFF'
    AND vt.status = 'COMPLETED'
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

    v_total_recoleccion := v_total_recolectado;
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
    INTO v_recoleccion_items
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    LEFT JOIN public.medications m ON m.sku = item->>'sku'
    WHERE st.visit_id = p_visit_id
      AND st.type::text = 'COLLECTION'
      AND st.items IS NOT NULL
      AND COALESCE((item->>'cantidad_salida')::int, (item->>'quantity')::int, 0) > 0;

    v_total_recoleccion := v_total_recolectado;
  END IF;

  RETURN jsonb_build_object(
    'visit_id', p_visit_id,
    'visit_type', v_visit_tipo,
    'client_id', v_id_cliente,
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
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_mark_notification_read(p_notification_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
BEGIN
  -- Obtener usuario actual
  SELECT user_id INTO v_user_id
  FROM public.users
  WHERE auth_user_id = auth.uid()
  LIMIT 1;

  UPDATE public.admin_notifications
  SET 
    read = true,
    read_at = now(),
    read_by = v_user_id
  WHERE id = p_notification_id
  AND read = false;

  RETURN FOUND;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_mark_all_notifications_read(p_user_id character varying)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE notifications
    SET read_at = NOW()
    WHERE user_id = p_user_id
      AND read_at IS NULL;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_mark_notification_read(p_notification_id uuid, p_user_id character varying)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
    UPDATE notifications
    SET read_at = NOW()
    WHERE id = p_notification_id
      AND user_id = p_user_id
      AND read_at IS NULL;

    RETURN FOUND;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_dev_legacy()
 RETURNS TABLE(visitas_created integer, tasks_created integer, sagas_updated integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_updated integer := 0;
  v_row_count integer := 0;
  v_saga record;
  v_visit_id uuid;
  v_existing_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Migrar CORTE_RENOVACION → VISITA_CORTE
  -- =====================================================
  -- CORTE_RENOVACION representa un ciclo de corte completo

  FOR v_saga IN
    SELECT DISTINCT ON (st.client_id, st.cycle_id)
      st.id as saga_id,
      st.client_id,
      st.user_id,
      st.cycle_id,
      st.status,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type = 'CUTOFF_RENEWAL'
      AND st.visit_id IS NULL
    ORDER BY st.client_id, st.cycle_id, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de corte
      INSERT INTO public.visits (
        visit_id, client_id, user_id, cycle_id, type, status,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.client_id,
        v_saga.user_id,
        v_saga.cycle_id,
        'VISIT_CUTOFF'::public.visit_type,
        'COMPLETED'::public.visit_status,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'legacy_tipo', 'CUTOFF_RENEWAL', 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear todas las tareas para visita de corte
      -- CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'CUTOFF'::public.visit_task_type,
        'COMPLETED'::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- VENTA_ODV
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'SALE_ODV'::public.visit_task_type,
        'COMPLETED'::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- RECOLECCION
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'COLLECTION'::public.visit_task_type,
        'COMPLETED'::public.visit_task_status,
        false,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- LEV_POST_CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'POST_CUTOFF_PLACEMENT'::public.visit_task_type,
        'COMPLETED'::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'VISIT_REPORT'::public.visit_task_type,
        'COMPLETED'::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga con visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Migrar VENTA → Enlazar a VISITA_CORTE existente
  -- =====================================================

  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.client_id,
      st.cycle_id,
      st.status,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type = 'SALE'
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga con visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar tarea VENTA_ODV con referencia
      UPDATE public.visit_tasks
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga.saga_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id
        AND task_type = 'SALE_ODV'
        AND reference_id IS NULL;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 3: Migrar RECOLECCION (saga) → Enlazar a VISITA_CORTE
  -- =====================================================

  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.client_id,
      st.cycle_id,
      st.status,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type = 'COLLECTION'
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga con visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar tarea RECOLECCION con referencia
      UPDATE public.visit_tasks
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga.saga_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id
        AND task_type = 'COLLECTION'
        AND reference_id IS NULL;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 4: Enlazar sagas restantes a visits existentes
  -- =====================================================

  -- Actualizar cualquier saga sin visit_id que tenga un cycle_id coincidente
  UPDATE public.saga_transactions st
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visits v
      WHERE v.client_id = st.client_id
        AND v.cycle_id = st.cycle_id
      LIMIT 1
    ),
    updated_at = now()
  WHERE st.visit_id IS NULL
    AND st.cycle_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visits v
      WHERE v.client_id = st.client_id AND v.cycle_id = st.cycle_id
    );

  GET DIAGNOSTICS v_row_count = ROW_COUNT;
  v_sagas_updated := v_sagas_updated + v_row_count;

  -- =====================================================
  -- PASO 5: Enlazar collections (table_name) sin visit_id
  -- =====================================================

  UPDATE public.collections r
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visits v
      WHERE v.client_id = r.client_id
        AND v.cycle_id = r.cycle_id
        AND v.type = 'VISIT_CUTOFF'
      LIMIT 1
    ),
    updated_at = now()
  WHERE r.visit_id IS NULL
    AND r.cycle_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visits v
      WHERE v.client_id = r.client_id
        AND v.cycle_id = r.cycle_id
        AND v.type = 'VISIT_CUTOFF'
    );

  RETURN QUERY SELECT v_visitas_created, v_tasks_created, v_sagas_updated;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_full_history()
 RETURNS TABLE(ciclos_processed integer, visitas_created integer, tasks_created integer, sagas_linked integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_ciclos_processed integer := 0;
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_linked integer := 0;
  v_row_count integer := 0;
  v_ciclo record;
  v_visit_id uuid;
  v_visit_tipo public.visit_type;
  v_visit_estado public.visit_status;
  v_existing_visit_id uuid;
  v_saga_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Crear visits desde migration.cabinet_cycles
  -- =====================================================

  FOR v_ciclo IN
    SELECT
      cb.cycle_id,
      cb.client_id,
      cb.user_id,
      cb.type,
      cb.created_date,
      cb.previous_cycle_id
    FROM migration.cabinet_cycles cb
    ORDER BY cb.created_date ASC
  LOOP
    v_ciclos_processed := v_ciclos_processed + 1;

    -- Determinar type de visita
    IF v_ciclo.type::text = 'PLACEMENT' THEN
      v_visit_tipo := 'VISIT_INITIAL_PLACEMENT'::public.visit_type;
    ELSE
      v_visit_tipo := 'VISIT_CUTOFF'::public.visit_type;
    END IF;

    -- Verificar si ya existe una visita para este ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.cycle_id = v_ciclo.cycle_id
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- La visita se considera COMPLETADA porque viene del historial
      v_visit_estado := 'COMPLETED'::public.visit_status;

      -- Crear nueva visita
      INSERT INTO public.visits (
        visit_id, client_id, user_id, cycle_id, type, status,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_ciclo.client_id,
        v_ciclo.user_id,
        v_ciclo.cycle_id,
        v_visit_tipo,
        v_visit_estado,
        v_ciclo.created_date,
        v_ciclo.created_date,
        v_ciclo.created_date,
        v_ciclo.created_date,
        jsonb_build_object(
          'migrated_from_legacy', true,
          'migration_date', now(),
          'source_ciclo_id', v_ciclo.cycle_id,
          'previous_cycle_id', v_ciclo.previous_cycle_id
        )
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas según el type de visita
      IF v_visit_tipo = 'VISIT_INITIAL_PLACEMENT' THEN
        -- LEVANTAMIENTO_INICIAL
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'INITIAL_PLACEMENT'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- ODV_BOTIQUIN
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'ODV_CABINET'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- INFORME_VISITA
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'VISIT_REPORT'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

      ELSE -- VISITA_CORTE
        -- CORTE
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'CUTOFF'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- VENTA_ODV
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'SALE_ODV'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- RECOLECCION
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'COLLECTION'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- LEV_POST_CORTE
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'POST_CUTOFF_PLACEMENT'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- INFORME_VISITA
        INSERT INTO public.visit_tasks (
          visit_id, task_type, status, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'VISIT_REPORT'::public.visit_task_type,
          'COMPLETED'::public.visit_task_status,
          true,
          v_ciclo.created_date,
          v_ciclo.created_date,
          v_ciclo.created_date,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;
      END IF;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- =====================================================
    -- PASO 2: Enlazar saga_transactions al visit_id
    -- =====================================================

    -- Buscar saga_transactions que coincidan con este ciclo
    UPDATE public.saga_transactions st
    SET
      visit_id = v_visit_id,
      updated_at = now()
    WHERE st.cycle_id = v_ciclo.cycle_id
      AND st.visit_id IS NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_sagas_linked := v_sagas_linked + v_row_count;

    -- También intentar enlazar por client_id y date aproximada si no tiene cycle_id
    UPDATE public.saga_transactions st
    SET
      visit_id = v_visit_id,
      cycle_id = v_ciclo.cycle_id,
      updated_at = now()
    WHERE st.client_id = v_ciclo.client_id
      AND st.visit_id IS NULL
      AND st.cycle_id IS NULL
      AND st.created_at >= v_ciclo.created_date - interval '1 day'
      AND st.created_at <= v_ciclo.created_date + interval '1 day'
      AND (
        (st.type::text = 'INITIAL_PLACEMENT' AND v_ciclo.type::text = 'PLACEMENT')
        OR (st.type::text IN ('CUTOFF', 'POST_CUTOFF_PLACEMENT', 'SALE_ODV') AND v_ciclo.type::text = 'CUTOFF')
      );

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_sagas_linked := v_sagas_linked + v_row_count;

    -- Actualizar las tareas con referencia a saga_transactions si existe
    FOR v_saga_id IN
      SELECT st.id FROM public.saga_transactions st
      WHERE st.visit_id = v_visit_id
    LOOP
      -- Actualizar tarea correspondiente según type de saga
      UPDATE public.visit_tasks vt
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga_id::text,
        last_activity_at = now()
      WHERE vt.visit_id = v_visit_id
        AND vt.reference_id IS NULL
        AND (
          (vt.task_type::text = 'INITIAL_PLACEMENT' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.type::text = 'INITIAL_PLACEMENT'
          ))
          OR (vt.task_type::text = 'CUTOFF' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.type::text = 'CUTOFF'
          ))
          OR (vt.task_type::text = 'POST_CUTOFF_PLACEMENT' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.type::text = 'POST_CUTOFF_PLACEMENT'
          ))
          OR (vt.task_type::text = 'SALE_ODV' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.type::text = 'SALE_ODV'
          ))
        );
    END LOOP;

  END LOOP;

  -- =====================================================
  -- PASO 3: Enlazar saga_transactions restantes sin visit_id
  -- =====================================================

  -- Para sagas que aún no tienen visit_id, buscar visita por cliente y date
  UPDATE public.saga_transactions st
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visits v
      WHERE v.client_id = st.client_id
        AND v.cycle_id = st.cycle_id
      LIMIT 1
    ),
    updated_at = now()
  WHERE st.visit_id IS NULL
    AND st.cycle_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visits v
      WHERE v.client_id = st.client_id AND v.cycle_id = st.cycle_id
    );

  GET DIAGNOSTICS v_row_count = ROW_COUNT;
  v_sagas_linked := v_sagas_linked + v_row_count;

  -- =====================================================
  -- PASO 4: Enlazar collections sin visit_id
  -- =====================================================

  UPDATE public.collections r
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visits v
      WHERE v.client_id = r.client_id
        AND v.cycle_id = r.cycle_id
        AND v.type = 'VISIT_CUTOFF'
      LIMIT 1
    ),
    updated_at = now()
  WHERE r.visit_id IS NULL
    AND r.cycle_id IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visits v
      WHERE v.client_id = r.client_id
        AND v.cycle_id = r.cycle_id
        AND v.type = 'VISIT_CUTOFF'
    );

  RETURN QUERY SELECT v_ciclos_processed, v_visitas_created, v_tasks_created, v_sagas_linked;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_legacy_sagas()
 RETURNS TABLE(visitas_created integer, tasks_created integer, sagas_updated integer, recolecciones_updated integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_updated integer := 0;
  v_recolecciones_updated integer := 0;
  v_saga record;
  v_visit_id uuid;
  v_visit_tipo public.visit_type;
  v_existing_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Migrar LEVANTAMIENTO_INICIAL sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT DISTINCT ON (st.client_id, st.cycle_id)
      st.id as saga_id,
      st.client_id,
      st.user_id,
      st.cycle_id,
      st.status,
      st.items,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type = 'INITIAL_PLACEMENT'
      AND st.visit_id IS NULL
    ORDER BY st.client_id, st.cycle_id, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_INITIAL_PLACEMENT'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de levantamiento inicial
      INSERT INTO public.visits (
        visit_id, client_id, user_id, cycle_id, type, status,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.client_id,
        v_saga.user_id,
        v_saga.cycle_id,
        'VISIT_INITIAL_PLACEMENT'::public.visit_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'IN_PROGRESS' END)::public.visit_status,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE NULL END,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas para levantamiento inicial
      -- LEVANTAMIENTO_INICIAL
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'INITIAL_PLACEMENT'::public.visit_task_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'IN_PROGRESS' END)::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE NULL END,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- ODV_BOTIQUIN
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'ODV_CABINET'::public.visit_task_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'PENDING' END)::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'VISIT_REPORT'::public.visit_task_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'PENDING' END)::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga_transactions con el visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Migrar CORTE sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT DISTINCT ON (st.client_id, st.cycle_id)
      st.id as saga_id,
      st.client_id,
      st.user_id,
      st.cycle_id,
      st.status,
      st.items,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type = 'CUTOFF'
      AND st.visit_id IS NULL
    ORDER BY st.client_id, st.cycle_id, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de corte
      INSERT INTO public.visits (
        visit_id, client_id, user_id, cycle_id, type, status,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.client_id,
        v_saga.user_id,
        v_saga.cycle_id,
        'VISIT_CUTOFF'::public.visit_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'IN_PROGRESS' END)::public.visit_status,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE NULL END,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas para visita de corte
      -- CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'CUTOFF'::public.visit_task_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'IN_PROGRESS' END)::public.visit_task_status,
        true,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE NULL END,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- VENTA_ODV
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'SALE_ODV'::public.visit_task_type,
        (CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE 'PENDING' END)::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- RECOLECCION
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'COLLECTION'::public.visit_task_type,
        'PENDING'::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- LEV_POST_CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'POST_CUTOFF_PLACEMENT'::public.visit_task_type,
        'PENDING'::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_type, status, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'VISIT_REPORT'::public.visit_task_type,
        'PENDING'::public.visit_task_status,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga_transactions con el visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 3: Migrar LEV_POST_CORTE y VENTA_ODV sin visit_id
  -- (Estos deben asociarse a una VISITA_CORTE existente)
  -- =====================================================
  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.client_id,
      st.user_id,
      st.cycle_id,
      st.type,
      st.status,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.type IN ('POST_CUTOFF_PLACEMENT', 'SALE_ODV')
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte existente para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga_transactions con el visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar la tarea correspondiente si existe
      IF v_saga.type::text = 'POST_CUTOFF_PLACEMENT' THEN
        UPDATE public.visit_tasks
        SET
          status = CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE status END,
          completed_at = CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE completed_at END,
          reference_table = 'saga_transactions',
          reference_id = v_saga.saga_id::text,
          last_activity_at = now()
        WHERE visit_id = v_existing_visit_id AND task_type = 'POST_CUTOFF_PLACEMENT';
      ELSIF v_saga.type::text = 'SALE_ODV' THEN
        UPDATE public.visit_tasks
        SET
          status = CASE WHEN v_saga.status = 'CONFIRMED' THEN 'COMPLETED' ELSE status END,
          completed_at = CASE WHEN v_saga.status = 'CONFIRMED' THEN v_saga.created_at ELSE completed_at END,
          reference_table = 'saga_transactions',
          reference_id = v_saga.saga_id::text,
          last_activity_at = now()
        WHERE visit_id = v_existing_visit_id AND task_type = 'SALE_ODV';
      END IF;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 4: Migrar collections sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT
      r.recoleccion_id,
      r.client_id,
      r.user_id,
      r.cycle_id,
      r.status,
      r.created_at
    FROM public.collections r
    WHERE r.visit_id IS NULL
    ORDER BY r.created_at ASC
  LOOP
    -- Buscar visita de corte existente para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visits v
    WHERE v.client_id = v_saga.client_id
      AND (v.cycle_id = v_saga.cycle_id OR (v.cycle_id IS NULL AND v_saga.cycle_id IS NULL))
      AND v.type = 'VISIT_CUTOFF'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar recolección con el visit_id
      UPDATE public.collections
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE recoleccion_id = v_saga.recoleccion_id;

      v_recolecciones_updated := v_recolecciones_updated + 1;

      -- Actualizar la tarea de recolección
      UPDATE public.visit_tasks
      SET
        status = CASE WHEN v_saga.status = 'ENTREGADA' THEN 'COMPLETED' ELSE status END,
        completed_at = CASE WHEN v_saga.status = 'ENTREGADA' THEN v_saga.created_at ELSE completed_at END,
        reference_table = 'collections',
        reference_id = v_saga.recoleccion_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id AND task_type = 'COLLECTION';
    END IF;
  END LOOP;

  RETURN QUERY SELECT v_visitas_created, v_tasks_created, v_sagas_updated, v_recolecciones_updated;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_owner_delete_visit(p_visit_id uuid, p_user_id text, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_user_rol text;
  v_visit_estado text;
  v_id_cliente text;
  v_deleted_counts jsonb;
  v_count_visitas int := 0;
  v_count_tasks int := 0;
  v_count_sagas int := 0;
  v_count_task_odvs int := 0;
  v_count_visita_odvs int := 0;
  v_count_movimientos int := 0;
  v_count_recolecciones int := 0;
  v_count_recolecciones_items int := 0;
  v_count_recolecciones_firmas int := 0;
  v_count_recolecciones_evidencias int := 0;
  v_count_informes int := 0;
  v_count_compensation_log int := 0;
  v_count_inventario_restored int := 0;
  v_has_task_id boolean := false;
  v_has_saga_comp_log boolean := false;
  v_has_task_odvs boolean := false;
  -- Variables para restaurar inventario
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_current_visit_had_lev_post_corte boolean := false;
  v_restore_source text := NULL;
BEGIN
  -- Verificar role OWNER
  SELECT u.role::text
  INTO v_user_rol
  FROM public.users u
  WHERE u.user_id = p_user_id;

  IF v_user_rol IS NULL OR v_user_rol != 'OWNER' THEN
    RAISE EXCEPTION 'Solo users OWNER pueden eliminar visits permanentemente';
  END IF;

  -- Obtener info de la visita
  SELECT v.status::text, v.client_id
  INTO v_visit_estado, v_id_cliente
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_visit_estado IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Solo permitir eliminar visits CANCELADAS
  IF v_visit_estado != 'CANCELLED' THEN
    RAISE EXCEPTION 'Solo se pueden eliminar visits canceladas. Estado actual: %', v_visit_estado;
  END IF;

  -- Detectar qué esquema tenemos disponible
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'inventory_movements'
    AND column_name = 'task_id'
  ) INTO v_has_task_id;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name = 'saga_compensation_log'
  ) INTO v_has_saga_comp_log;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name = 'task_odvs'
  ) INTO v_has_task_odvs;

  -- ============ VERIFICAR SI ESTA VISITA MODIFICÓ EL INVENTARIO ============
  -- Verificar PRIMERO en visit_tasks (normal)
  -- Si no encuentra, verificar en saga_transactions (por si rollback ya borró tasks)
  SELECT EXISTS (
    SELECT 1 FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
      AND vt.task_type = 'POST_CUTOFF_PLACEMENT'
      AND vt.status IN ('COMPLETED', 'SKIPPED_M', 'SKIPPED')
  ) OR EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.type = 'POST_CUTOFF_PLACEMENT'
      AND st.status = 'CONFIRMED'
  ) INTO v_current_visit_had_lev_post_corte;

  -- ============ BUSCAR ÚLTIMA VISITA COMPLETADA DEL CLIENTE ============
  IF v_current_visit_had_lev_post_corte THEN
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visits v
    WHERE v.client_id = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.status = 'COMPLETED'
      AND v.type IN ('VISIT_CUTOFF', 'VISIT_INITIAL_PLACEMENT')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.type = 'POST_CUTOFF_PLACEMENT'
        AND st.status = 'CONFIRMED'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.type = 'INITIAL_PLACEMENT'
          AND st.status = 'CONFIRMED'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- ============ RESTAURAR INVENTARIO ============
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      DELETE FROM public.cabinet_inventory WHERE client_id = v_id_cliente;

      INSERT INTO public.cabinet_inventory (client_id, sku, available_quantity, last_updated)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'quantity')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      INSERT INTO public.cabinet_client_available_skus (client_id, sku, intake_date)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'quantity')::integer > 0
      ON CONFLICT (client_id, sku) DO NOTHING;
    ELSE
      v_restore_source := 'Sin visita completed anterior - inventario no modificado';
    END IF;
  END IF;

  -- Eliminar en orden para respetar foreign keys

  IF v_has_saga_comp_log THEN
    EXECUTE 'DELETE FROM public.saga_compensation_log WHERE visit_id = $1' USING p_visit_id;
    GET DIAGNOSTICS v_count_compensation_log = ROW_COUNT;
  END IF;

  IF v_has_task_id THEN
    DELETE FROM public.inventory_movements
    WHERE task_id IN (SELECT task_id FROM public.visit_tasks WHERE visit_id = p_visit_id);
    GET DIAGNOSTICS v_count_movimientos = ROW_COUNT;
  ELSE
    DELETE FROM public.inventory_movements
    WHERE id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id);
    GET DIAGNOSTICS v_count_movimientos = ROW_COUNT;
  END IF;

  IF v_has_task_odvs THEN
    EXECUTE 'DELETE FROM public.task_odvs WHERE task_id IN (SELECT task_id FROM public.visit_tasks WHERE visit_id = $1)' USING p_visit_id;
    GET DIAGNOSTICS v_count_task_odvs = ROW_COUNT;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'visit_odvs') THEN
    EXECUTE 'DELETE FROM public.visit_odvs WHERE visit_id = $1' USING p_visit_id;
    GET DIAGNOSTICS v_count_visita_odvs = ROW_COUNT;
  END IF;

  DELETE FROM public.collection_evidence
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.collections WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_evidencias = ROW_COUNT;

  DELETE FROM public.collection_signatures
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.collections WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_firmas = ROW_COUNT;

  DELETE FROM public.collection_items
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.collections WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_items = ROW_COUNT;

  DELETE FROM public.collections
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_recolecciones = ROW_COUNT;

  DELETE FROM public.visit_reports
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_informes = ROW_COUNT;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'saga_transactions') THEN
    DELETE FROM public.saga_transactions
    WHERE visit_id = p_visit_id;
    GET DIAGNOSTICS v_count_sagas = ROW_COUNT;
  END IF;

  DELETE FROM public.visit_tasks
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_tasks = ROW_COUNT;

  DELETE FROM public.visits
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_visitas = ROW_COUNT;

  v_deleted_counts := jsonb_build_object(
    'visits', v_count_visitas,
    'visit_tasks', v_count_tasks,
    'saga_transactions', v_count_sagas,
    'task_odvs', v_count_task_odvs,
    'visit_odvs', v_count_visita_odvs,
    'inventory_movements', v_count_movimientos,
    'collections', v_count_recolecciones,
    'collection_items', v_count_recolecciones_items,
    'collection_signatures', v_count_recolecciones_firmas,
    'collection_evidence', v_count_recolecciones_evidencias,
    'visit_reports', v_count_informes,
    'saga_compensation_log', v_count_compensation_log,
    'inventario_restored', v_count_inventario_restored
  );

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'deleted_counts', v_deleted_counts,
    'reason', p_reason,
    'inventory_reverted', v_current_visit_had_lev_post_corte AND v_lev_post_corte_items IS NOT NULL,
    'restore_source', v_restore_source,
    'last_completed_visit_id', v_last_completed_visit_id
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_save_draft_step(p_visit_id uuid, p_odv_id text, p_saga_type text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_zoho_link_tipo zoho_link_type;
  v_task_tipo text;
BEGIN
  RAISE NOTICE '[cotizacion:borrador] visit_id=%, odv_id=%, saga_type=%', p_visit_id, p_odv_id, p_saga_type;

  SELECT * INTO v_saga
  FROM saga_transactions
  WHERE visit_id = p_visit_id
    AND type::text = p_saga_type
    AND status != 'CANCELLED_F'
  ORDER BY created_at DESC
  LIMIT 1;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'No se encontró saga % para visita %', p_saga_type, p_visit_id;
  END IF;

  RAISE NOTICE '[cotizacion:borrador] found saga_id=%, status=%', v_saga.id, v_saga.status;

  CASE p_saga_type
    WHEN 'SALE' THEN
      v_zoho_link_tipo := 'SALE';
      v_task_tipo := 'SALE_ODV';
    WHEN 'INITIAL_PLACEMENT' THEN
      v_zoho_link_tipo := 'CABINET';
      v_task_tipo := 'ODV_CABINET';
    WHEN 'POST_CUTOFF_PLACEMENT' THEN
      v_zoho_link_tipo := 'CABINET';
      v_task_tipo := 'ODV_CABINET';
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado para borrador: %', p_saga_type;
  END CASE;

  INSERT INTO saga_zoho_links (
    id_saga_transaction, zoho_id, type, items, zoho_sync_status, created_at, updated_at
  )
  VALUES (
    v_saga.id, p_odv_id, v_zoho_link_tipo, v_saga.items, 'borrador_validado', now(), now()
  )
  ON CONFLICT (id_saga_transaction, zoho_id)
  DO UPDATE SET
    zoho_sync_status = 'borrador_validado',
    updated_at = now();

  UPDATE visit_tasks
  SET
    metadata = metadata || jsonb_build_object('cotizacion_step', 'borrador_validado', 'odv_id', p_odv_id),
    last_activity_at = now()
  WHERE visit_id = p_visit_id
    AND task_type = v_task_tipo::visit_task_type;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_set_manual_botiquin_odv_id(p_visit_id uuid, p_zoho_odv_id text, p_task_type text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente varchar;
  v_id_usuario varchar;
  v_saga_id uuid;
  v_result jsonb;
BEGIN
  -- Validar task_type
  IF p_task_type NOT IN ('INITIAL_PLACEMENT', 'POST_CUTOFF_PLACEMENT') THEN
    RAISE EXCEPTION 'task_type debe ser LEVANTAMIENTO_INICIAL o LEV_POST_CORTE';
  END IF;

  -- Obtener datos de la visita
  SELECT v.client_id, v.user_id
  INTO v_id_cliente, v_id_usuario
  FROM visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Buscar saga existente del type especificado
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND type::text = p_task_type
  ORDER BY created_at DESC LIMIT 1;

  IF v_saga_id IS NULL THEN
    -- Crear saga nueva (sin items, se asume que vienen de levantamiento)
    INSERT INTO saga_transactions (
      type, status, client_id, user_id,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      p_task_type::saga_transaction_type,
      'DRAFT'::saga_transaction_status,
      v_id_cliente,
      v_id_usuario,
      '[]'::jsonb,
      jsonb_build_object('manual_odv', true, 'zoho_odv_id', p_zoho_odv_id),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  SELECT rpc_confirm_saga_pivot(v_saga_id, p_zoho_odv_id, NULL) INTO v_result;

  -- Actualizar tarea ODV_BOTIQUIN
  UPDATE visit_tasks
  SET
    status = 'COMPLETED'::visit_task_status,
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'saga_id', v_saga_id,
      'zoho_odv_id', p_zoho_odv_id,
      'manual_odv', true,
      'saga_type', p_task_type
    )
  WHERE visit_id = p_visit_id 
  AND task_type = 'ODV_CABINET'::visit_task_type;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_set_manual_odv_id(p_visit_id uuid, p_zoho_odv_id text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga_id uuid;
  v_id_cliente varchar;
  v_id_usuario varchar;
  v_items jsonb;
  v_odv_id text;
  v_result jsonb;
BEGIN
  IF p_zoho_odv_id IS NULL THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Normalizar el formato del ODV ID
  IF p_zoho_odv_id ~ '^[0-9]{1,5}$' THEN
    v_odv_id := 'DCOdV-' || p_zoho_odv_id;
  ELSE
    v_odv_id := p_zoho_odv_id;
  END IF;

  IF v_odv_id !~ '^DCOdV-[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Buscar saga existente de type VENTA
  SELECT st.id INTO v_saga_id
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id AND st.type::text = 'SALE'
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_saga_id IS NULL THEN
    -- Obtener datos de la visita
    SELECT v.client_id, v.user_id
    INTO v_id_cliente, v_id_usuario
    FROM public.visits v
    WHERE v.visit_id = p_visit_id;

    IF v_id_cliente IS NULL THEN
      RAISE EXCEPTION 'Visita no encontrada';
    END IF;

    -- Obtener items del corte (saga VENTA que se creó en submit_corte)
    -- o de la metadata de la tarea CORTE
    SELECT COALESCE(
      (SELECT st.items FROM saga_transactions st
       WHERE st.visit_id = p_visit_id AND st.type::text = 'SALE'
       ORDER BY created_at DESC LIMIT 1),
      '[]'::jsonb
    ) INTO v_items;

    -- Si no hay items, intentar extraer del corte
    IF v_items = '[]'::jsonb THEN
      SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'quantity', (item->>'vendido')::int
          )
        ),
        '[]'::jsonb
      )
      INTO v_items
      FROM (
        SELECT item
        FROM saga_transactions st,
        LATERAL jsonb_array_elements(st.items) AS item
        WHERE st.visit_id = p_visit_id
        AND st.type::text = 'SALE'
        AND COALESCE((item->>'vendido')::int, (item->>'quantity')::int, 0) > 0
        ORDER BY st.created_at DESC
        LIMIT 100
      ) sub;
    END IF;

    -- Fix: Fallback to odv_sales when items are still empty
    IF v_items = '[]'::jsonb OR v_items IS NULL THEN
      SELECT jsonb_agg(jsonb_build_object('sku', vo.sku, 'quantity', vo.quantity))
      INTO v_items
      FROM public.odv_sales vo
      WHERE vo.odv_id = v_odv_id;
      v_items := COALESCE(v_items, '[]'::jsonb);
    END IF;

    -- Crear nueva saga de type VENTA
    INSERT INTO public.saga_transactions (
      type, status, client_id, user_id,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'SALE'::saga_transaction_type,
      'DRAFT'::saga_transaction_status,
      v_id_cliente,
      v_id_usuario,
      v_items,
      jsonb_build_object(
        'zoho_required', true,
        'zoho_manual', true,
        'zoho_odv_id', v_odv_id
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  SELECT rpc_confirm_saga_pivot(v_saga_id, v_odv_id, NULL) INTO v_result;

  -- Actualizar tarea VENTA_ODV
  UPDATE public.visit_tasks
  SET
    status = 'COMPLETED',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'saga_id', v_saga_id,
      'zoho_odv_id', v_odv_id,
      'manual_odv', true
    )
  WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_skip_collection(p_visit_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Verificar visita existe
  IF NOT EXISTS (SELECT 1 FROM visits WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar tarea existe y está pendiente
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'COLLECTION'
    AND status NOT IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M')
  ) THEN
    RAISE EXCEPTION 'Tarea RECOLECCION no encontrada o ya completed';
  END IF;

  -- Marcar como OMITIDA
  UPDATE visit_tasks
  SET
    status = 'SKIPPED',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'skipped', true,
      'skipped_at', now()
    )
  WHERE visit_id = p_visit_id AND task_type = 'COLLECTION';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_skip_sale_odv(p_visit_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Verificar visita existe
  IF NOT EXISTS (SELECT 1 FROM visits WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar tarea existe y está pendiente
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV'
    AND status NOT IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M')
  ) THEN
    RAISE EXCEPTION 'Tarea VENTA_ODV no encontrada o ya completed';
  END IF;

  -- Marcar como OMITIDA
  UPDATE visit_tasks
  SET
    status = 'SKIPPED',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'skipped', true,
      'skipped_at', now()
    )
  WHERE visit_id = p_visit_id AND task_type = 'SALE_ODV';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_start_task(p_visit_id uuid, p_task visit_task_type)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Actualizar la tarea a EN_CURSO
  UPDATE public.visit_tasks
  SET
    status = 'IN_PROGRESS',
    started_at = COALESCE(started_at, now()),
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = p_task;

  -- Actualizar la visita a EN_CURSO también
  UPDATE public.visits
  SET
    status = 'IN_PROGRESS',
    started_at = COALESCE(started_at, now()),
    last_activity_at = now()
  WHERE visit_id = p_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_cutoff(p_visit_id uuid, p_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_saga_venta_id uuid;
  v_saga_recoleccion_id uuid;
  v_recoleccion_id uuid;
  v_total_vendido integer := 0;
  v_total_recolectado integer := 0;
  v_items_venta jsonb;
  v_items_recoleccion jsonb;
BEGIN
  -- Calcular totales
  SELECT
    COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0),
    COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)
  INTO v_total_vendido, v_total_recolectado
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item;

  -- Filtrar items para VENTA
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'quantity', (item->>'vendido')::int
    )
  ) INTO v_items_venta
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'vendido')::int, 0) > 0;

  -- Filtrar items para RECOLECCION
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'quantity', (item->>'recolectado')::int
    )
  ) INTO v_items_recoleccion
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'recolectado')::int, 0) > 0;

  -- Obtener datos de la visita (sin cycle_id)
  SELECT v.user_id, v.client_id
  INTO v_id_usuario, v_id_cliente
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- CREAR SAGA VENTA
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

  -- CREAR SAGA RECOLECCION
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
    RETURNING id INTO v_saga_recoleccion_id;

    -- INSERT sin cycle_id (columna no existe en collections)
    INSERT INTO public.collections (
      visit_id, client_id, user_id, status
    )
    SELECT p_visit_id, v_id_cliente, v_id_usuario, 'PENDING'
    WHERE NOT EXISTS (
      SELECT 1 FROM public.collections r WHERE r.visit_id = p_visit_id
    )
    RETURNING recoleccion_id INTO v_recoleccion_id;

    IF v_recoleccion_id IS NULL THEN
      SELECT recoleccion_id INTO v_recoleccion_id
      FROM public.collections WHERE visit_id = p_visit_id LIMIT 1;
    END IF;

    -- Insertar items en collection_items
    INSERT INTO public.collection_items (recoleccion_id, sku, quantity)
    SELECT
      v_recoleccion_id,
      (item->>'sku')::varchar,
      (item->>'quantity')::int
    FROM jsonb_array_elements(COALESCE(v_items_recoleccion, '[]'::jsonb)) AS item
    ON CONFLICT (recoleccion_id, sku) DO UPDATE
    SET quantity = EXCLUDED.quantity;

    UPDATE public.visit_tasks
    SET
      reference_table = NULL,
      reference_id = NULL,
      metadata = jsonb_build_object('recoleccion_id', v_recoleccion_id),
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

  -- MARCAR CORTE COMPLETADO
  UPDATE public.visit_tasks
  SET
    status = 'COMPLETED',
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    metadata = jsonb_build_object(
      'items', p_items,
      'saga_venta_id', v_saga_venta_id,
      'saga_recoleccion_id', v_saga_recoleccion_id,
      'total_vendido', v_total_vendido,
      'total_recolectado', v_total_recolectado
    ),
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = 'CUTOFF';

  RETURN jsonb_build_object(
    'success', true,
    'saga_venta_id', v_saga_venta_id,
    'saga_recoleccion_id', v_saga_recoleccion_id,
    'total_vendido', v_total_vendido,
    'total_recolectado', v_total_recolectado
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_visit_report(p_visit_id uuid, p_responses jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_id_ciclo integer;
  v_tipo_visita visit_type;
  v_informe_id uuid;
  v_next_visit_id uuid;
  v_fecha_proxima date;
  v_etiqueta varchar;
  v_cumplimiento_score integer := 0;
  v_total_preguntas integer := 0;
  v_next_corte_number integer;
BEGIN
  -- Obtener datos de la visita actual
  SELECT v.user_id, v.client_id, v.cycle_id, v.type
  INTO v_id_usuario, v_id_cliente, v_id_ciclo, v_tipo_visita
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Validar que ODV_BOTIQUIN esté completed antes del informe
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_type = 'ODV_CABINET'
    AND status = 'COMPLETED'::visit_task_status
  ) THEN
    RAISE EXCEPTION 'Debe completar la confirmación ODV Botiquín antes de enviar el informe';
  END IF;

  -- Extraer date próxima visita
  v_fecha_proxima := (p_responses->>'fecha_proxima_visita')::date;

  -- Calcular score de cumplimiento
  SELECT
    COALESCE(SUM(CASE WHEN value::text = 'true' THEN 1 ELSE 0 END), 0),
    COUNT(*)
  INTO v_cumplimiento_score, v_total_preguntas
  FROM jsonb_each(p_responses)
  WHERE key NOT IN ('fecha_proxima_visita', 'imagen_visita', 'imagen_visita_local')
  AND jsonb_typeof(value) = 'boolean';

  -- Determinar label
  IF v_total_preguntas > 0 THEN
    IF v_cumplimiento_score = v_total_preguntas THEN
      v_etiqueta := 'EXCELENTE';
    ELSIF v_cumplimiento_score >= (v_total_preguntas * 0.8) THEN
      v_etiqueta := 'BUENO';
    ELSIF v_cumplimiento_score >= (v_total_preguntas * 0.6) THEN
      v_etiqueta := 'REGULAR';
    ELSE
      v_etiqueta := 'REQUIERE_ATENCION';
    END IF;
  ELSE
    v_etiqueta := 'SIN_EVALUAR';
  END IF;

  -- Crear o actualizar informe
  INSERT INTO public.visit_reports (
    visit_id, responses, label, compliance_score, completed, completed_date, created_at
  )
  VALUES (
    p_visit_id,
    p_responses,
    v_etiqueta,
    v_cumplimiento_score,
    true,
    now(),
    now()
  )
  ON CONFLICT (visit_id) DO UPDATE SET
    responses = EXCLUDED.responses,
    label = EXCLUDED.label,
    compliance_score = EXCLUDED.compliance_score,
    completed = true,
    completed_date = COALESCE(visit_reports.completed_date, now()),
    updated_at = now()
  RETURNING report_id INTO v_informe_id;

  -- Marcar tarea INFORME_VISITA como completed
  UPDATE public.visit_tasks
  SET
    status = 'COMPLETED'::visit_task_status,
    completed_at = now(),
    reference_table = 'visit_reports',
    reference_id = v_informe_id::text,
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = 'VISIT_REPORT';

  -- Actualizar label en la visita actual
  UPDATE public.visits
  SET
    label = v_etiqueta,
    updated_at = now()
  WHERE visit_id = p_visit_id;

  -- Crear próxima visita si se especificó date
  IF v_fecha_proxima IS NOT NULL THEN
    -- FIX: exclude current visit from duplicate check (visit_id != p_visit_id)
    IF NOT EXISTS (
      SELECT 1 FROM public.visits
      WHERE client_id = v_id_cliente
      AND DATE(due_at) = v_fecha_proxima
      AND status != 'CANCELLED'
      AND visit_id != p_visit_id
    ) THEN
      -- Calculate next corte_number for this client
      SELECT COALESCE(MAX(corte_number), -1) + 1
      INTO v_next_corte_number
      FROM public.visits
      WHERE client_id = v_id_cliente;

      INSERT INTO public.visits (
        client_id, user_id, cycle_id, type, status, due_at, created_at, corte_number
      )
      VALUES (
        v_id_cliente,
        v_id_usuario,
        v_id_ciclo,
        'VISIT_CUTOFF'::visit_type,
        'SCHEDULED'::visit_status,
        v_fecha_proxima,
        now(),
        v_next_corte_number
      )
      RETURNING visit_id INTO v_next_visit_id;

      -- Crear tareas para la próxima visita CON transaction_type y step_order
      INSERT INTO public.visit_tasks (visit_id, task_type, status, required, due_at, transaction_type, step_order, created_at)
      VALUES
        (v_next_visit_id, 'CUTOFF', 'PENDING'::visit_task_status, true, v_fecha_proxima, 'COMPENSABLE', 1, now()),
        (v_next_visit_id, 'SALE_ODV', 'PENDING'::visit_task_status, true, v_fecha_proxima, 'PIVOT', 2, now()),
        (v_next_visit_id, 'COLLECTION', 'PENDING'::visit_task_status, false, v_fecha_proxima, 'RETRYABLE', 3, now()),
        (v_next_visit_id, 'POST_CUTOFF_PLACEMENT', 'PENDING'::visit_task_status, true, v_fecha_proxima, 'COMPENSABLE', 4, now()),
        (v_next_visit_id, 'ODV_CABINET', 'PENDING'::visit_task_status, true, v_fecha_proxima, 'PIVOT', 5, now()),
        (v_next_visit_id, 'VISIT_REPORT', 'PENDING'::visit_task_status, true, v_fecha_proxima, 'RETRYABLE', 6, now());
    END IF;
  END IF;

  RETURN v_informe_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_post_cutoff_placement(p_visit_id uuid, p_items jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_saga_id uuid;
  v_estado_cliente public.client_status;
  v_items_count integer;
  v_venta_odv_estado text;
BEGIN
  -- Obtener datos de la visita
  SELECT v.user_id, v.client_id
  INTO v_id_usuario, v_id_cliente
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Obtener status del cliente
  SELECT c.status INTO v_estado_cliente
  FROM public.clients c
  WHERE c.client_id = v_id_cliente;

  -- Contar items en el array
  v_items_count := COALESCE(jsonb_array_length(p_items), 0);

  -- =========================================================================
  -- NUEVA VALIDACIÓN: Verificar que VENTA_ODV esté completed u omitida
  -- Esto asegura que los SKUs vendidos ya fueron eliminados de disponibles
  -- =========================================================================
  SELECT vt.status::text INTO v_venta_odv_estado
  FROM public.visit_tasks vt
  WHERE vt.visit_id = p_visit_id 
  AND vt.task_type = 'SALE_ODV'::visit_task_type;

  -- Si existe tarea VENTA_ODV y no está completed/omitida, bloquear
  IF v_venta_odv_estado IS NOT NULL 
     AND v_venta_odv_estado NOT IN ('COMPLETED', 'SKIPPED', 'SKIPPED_M') THEN
    RAISE EXCEPTION 'Debe confirmar la ODV de Venta antes de realizar el levantamiento post-corte. Estado actual de VENTA_ODV: %', v_venta_odv_estado;
  END IF;
  -- =========================================================================

  -- Verificar que la tarea CORTE está completed
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_type = 'CUTOFF'::visit_task_type
    AND status = 'COMPLETED'::visit_task_status
  ) THEN
    RAISE EXCEPTION 'Debe completar el CORTE antes del levantamiento post-corte';
  END IF;

  -- Validar items vacíos según status del cliente
  IF v_items_count = 0 THEN
    IF v_estado_cliente != 'DOWNGRADING' THEN
      RAISE EXCEPTION 'Inventario vacío solo permitido para clients EN_BAJA. Estado actual: %. Para dar de baja al cliente, primero cambie su status a EN_BAJA usando el panel de administración.', v_estado_cliente;
    END IF;
    
    -- Cliente EN_BAJA con items = 0: marcar como INACTIVO automáticamente
    UPDATE public.clients
    SET status = 'INACTIVE', updated_at = now()
    WHERE client_id = v_id_cliente;

    -- Registrar en auditoría
    INSERT INTO public.client_status_log (
      client_id, 
      previous_status, 
      new_status, 
      changed_by, 
      reason,
      metadata
    )
    VALUES (
      v_id_cliente, 
      'DOWNGRADING', 
      'INACTIVE', 
      v_id_usuario, 
      'Baja automática por LEV_POST_CORTE vacío',
      jsonb_build_object(
        'visit_id', p_visit_id,
        'automatico', true
      )
    );
  END IF;

  -- Buscar saga existente
  SELECT id INTO v_saga_id
  FROM public.saga_transactions
  WHERE visit_id = p_visit_id
  AND type = 'POST_CUTOFF_PLACEMENT'::saga_transaction_type
  LIMIT 1;

  -- Actualizar saga_transaction existente o crear nueva
  IF v_saga_id IS NOT NULL THEN
    UPDATE public.saga_transactions
    SET
      items = p_items,
      status = CASE 
        WHEN v_items_count = 0 THEN 'SKIPPED'::saga_transaction_status
        ELSE 'DRAFT'::saga_transaction_status
      END,
      updated_at = now(),
      metadata = jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'CABINET',
        'zoho_required', v_items_count > 0,
        'cliente_en_baja', v_estado_cliente = 'DOWNGRADING',
        'items_count', v_items_count
      )
    WHERE id = v_saga_id;
  ELSE
    INSERT INTO public.saga_transactions (
      type, status, client_id, user_id,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'POST_CUTOFF_PLACEMENT'::saga_transaction_type,
      CASE 
        WHEN v_items_count = 0 THEN 'SKIPPED'::saga_transaction_status
        ELSE 'DRAFT'::saga_transaction_status
      END,
      v_id_cliente,
      v_id_usuario,
      p_items,
      jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'CABINET',
        'zoho_required', v_items_count > 0,
        'cliente_en_baja', v_estado_cliente = 'DOWNGRADING',
        'items_count', v_items_count
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- Marcar tarea LEV_POST_CORTE como completed (u omitida si 0 items)
  UPDATE public.visit_tasks
  SET
    status = CASE 
      WHEN v_items_count = 0 THEN 'SKIPPED_M'::visit_task_status
      ELSE 'COMPLETED'::visit_task_status
    END,
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    last_activity_at = now(),
    metadata = jsonb_build_object(
      'items_count', v_items_count,
      'cliente_estado', v_estado_cliente
    )
  WHERE visit_id = p_visit_id AND task_type = 'POST_CUTOFF_PLACEMENT'::visit_task_type;

  RETURN v_saga_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_initial_placement(p_visit_id uuid, p_items jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_id_ciclo integer;
  v_saga_id uuid;
BEGIN
  -- Obtener datos de la visita
  SELECT v.user_id, v.client_id, v.cycle_id
  INTO v_id_usuario, v_id_cliente, v_id_ciclo
  FROM public.visits v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar que la tarea existe y no está completed
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_type = 'INITIAL_PLACEMENT'
    AND status NOT IN ('COMPLETED', 'SKIPPED', 'CANCELLED')
  ) THEN
    -- Si ya está completed, devolver el saga_id existente
    SELECT st.id INTO v_saga_id
    FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
    AND st.type = 'INITIAL_PLACEMENT'::saga_transaction_type
    LIMIT 1;

    RETURN v_saga_id;
  END IF;

  -- Crear saga transaction con status BORRADOR (idempotente)
  INSERT INTO public.saga_transactions (
    type, status, client_id, user_id,
    items, metadata, visit_id, created_at, updated_at
  )
  SELECT
    'INITIAL_PLACEMENT'::saga_transaction_type,
    'DRAFT'::saga_transaction_status,
    v_id_cliente,
    v_id_usuario,
    p_items,
    jsonb_build_object(
      'visit_id', p_visit_id,
      'zoho_account_mode', 'CABINET',
      'zoho_required', true
    ),
    p_visit_id,
    now(), now()
  WHERE NOT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
    AND st.type = 'INITIAL_PLACEMENT'::saga_transaction_type
  )
  RETURNING id INTO v_saga_id;

  -- Si ya existía, obtener el ID y actualizar items
  IF v_saga_id IS NULL THEN
    UPDATE public.saga_transactions
    SET items = p_items, updated_at = now()
    WHERE visit_id = p_visit_id
    AND type = 'INITIAL_PLACEMENT'::saga_transaction_type
    RETURNING id INTO v_saga_id;
  END IF;

  -- Marcar tarea LEVANTAMIENTO_INICIAL como completed
  -- reference_id = NULL según nuevo patrón (COMPENSABLE)
  UPDATE public.visit_tasks
  SET
    status = 'COMPLETED',
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_type = 'INITIAL_PLACEMENT';

  -- ❌ NO crear cabinet_inventory aquí
  -- ❌ NO crear inventory_movements aquí
  -- Eso se hace en rpc_confirm_saga_pivot cuando se confirma ODV_BOTIQUIN

  RETURN v_saga_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_sync_cabinet_available_skus(p_client_id character varying DEFAULT NULL::character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_cliente record;
  v_inserted integer := 0;
  v_deleted_inactive integer := 0;
  v_deleted_sold integer := 0;
  v_total_clients integer := 0;
  v_results jsonb := '[]'::jsonb;
BEGIN
  -- 1. Eliminar registros de clients inactivos o que ya no existen
  DELETE FROM public.cabinet_client_available_skus
  WHERE client_id IN (
    SELECT c.client_id FROM public.clients c WHERE c.active = false
  )
  OR client_id NOT IN (
    SELECT c.client_id FROM public.clients c
  );

  GET DIAGNOSTICS v_deleted_inactive = ROW_COUNT;

  -- 2. NUEVO: Eliminar SKUs que ya fueron vendidos según inventory_movements
  -- Esto asegura que si un SKU fue vendido (movimiento type VENTA), 
  -- no esté disponible para lev_post_corte
  DELETE FROM public.cabinet_client_available_skus bcs
  WHERE EXISTS (
    SELECT 1 FROM public.inventory_movements mi
    WHERE mi.client_id = bcs.client_id
      AND mi.sku = bcs.sku
      AND mi.type = 'SALE'
  );

  GET DIAGNOSTICS v_deleted_sold = ROW_COUNT;

  -- 3. También eliminar SKUs que fueron vendidos según odv_sales (legacy)
  DELETE FROM public.cabinet_client_available_skus bcs
  WHERE EXISTS (
    SELECT 1 FROM public.odv_sales vo
    WHERE vo.client_id = bcs.client_id
      AND vo.sku = bcs.sku
  );

  -- 4. Sincronizar SKUs para clients activos
  FOR v_cliente IN
    SELECT c.client_id
    FROM public.clients c
    WHERE c.active = true
      AND (p_client_id IS NULL OR c.client_id = p_client_id)
  LOOP
    v_total_clients := v_total_clients + 1;

    WITH inserted AS (
      INSERT INTO public.cabinet_client_available_skus (client_id, sku, intake_date)
      SELECT
        v_cliente.client_id,
        m.sku,
        now()
      FROM public.medications m
      WHERE 
        -- No fue vendido en odv_sales (legacy)
        NOT EXISTS (
          SELECT 1 FROM public.odv_sales vo
          WHERE vo.client_id = v_cliente.client_id
            AND vo.sku = m.sku
        )
        -- NUEVO: No fue vendido según inventory_movements
        AND NOT EXISTS (
          SELECT 1 FROM public.inventory_movements mi
          WHERE mi.client_id = v_cliente.client_id
            AND mi.sku = m.sku
            AND mi.type = 'SALE'
        )
        -- No existe ya en la table_name
        AND NOT EXISTS (
          SELECT 1 FROM public.cabinet_client_available_skus bcs
          WHERE bcs.client_id = v_cliente.client_id
            AND bcs.sku = m.sku
        )
      RETURNING sku
    )
    SELECT COUNT(*) INTO v_inserted FROM inserted;

    v_results := v_results || jsonb_build_object(
      'client_id', v_cliente.client_id,
      'skus_added', v_inserted
    );
  END LOOP;

  RETURN jsonb_build_object(
    'success', true,
    'deleted_inactive_records', v_deleted_inactive,
    'deleted_sold_records', v_deleted_sold,
    'total_clients_synced', v_total_clients,
    'details', v_results
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_try_complete_visit(p_visit_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visita_exists boolean;
  v_all_required_completed boolean;
  v_pending_required integer;
BEGIN
  -- Verificar que la visita existe y no está completed/cancelada
  SELECT EXISTS(
    SELECT 1 FROM public.visits
    WHERE visit_id = p_visit_id
    AND status NOT IN ('COMPLETED', 'CANCELLED')
  ) INTO v_visita_exists;

  IF NOT v_visita_exists THEN
    -- Si ya está completed, devolver true
    IF EXISTS (
      SELECT 1 FROM public.visits
      WHERE visit_id = p_visit_id
      AND status = 'COMPLETED'
    ) THEN
      RETURN true;
    END IF;
    RETURN false;
  END IF;

  -- Contar tareas requeridas NO finalizadas
  -- COMPLETADO y OMITIDA son estados finales válidos que permiten completar la visita
  SELECT COUNT(*)
  INTO v_pending_required
  FROM public.visit_tasks
  WHERE visit_id = p_visit_id
  AND required = true
  AND status NOT IN ('COMPLETED', 'SKIPPED');

  v_all_required_completed := (v_pending_required = 0);

  IF v_all_required_completed THEN
    -- Marcar visita como completed
    UPDATE public.visits
    SET
      status = 'COMPLETED',
      completed_at = now(),
      updated_at = now()
    WHERE visit_id = p_visit_id;

    RETURN true;
  END IF;

  RETURN false;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_consolidation()
 RETURNS TABLE(total_visitas integer, visitas_duplicadas integer, sagas_sin_visit integer, tareas_sin_referencia integer, informes_creados integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM public.visits),
    (
      SELECT COUNT(*)::integer FROM (
        SELECT client_id, user_id, DATE(created_at)
        FROM public.visits
        GROUP BY client_id, user_id, DATE(created_at)
        HAVING COUNT(*) > 1
      ) dups
    ),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.visit_tasks WHERE reference_id IS NULL AND status = 'COMPLETED'),
    (SELECT COUNT(*)::integer FROM public.visit_reports);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_dev_migration()
 RETURNS TABLE(total_visitas integer, visitas_migradas integer, total_sagas integer, sagas_sin_visit integer, total_recolecciones integer, recolecciones_sin_visit integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM public.visits),
    (SELECT COUNT(*)::integer FROM public.visits WHERE metadata->>'migrated_from_legacy' = 'true'),
    (SELECT COUNT(*)::integer FROM public.saga_transactions),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.collections),
    (SELECT COUNT(*)::integer FROM public.collections WHERE visit_id IS NULL);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_migration_consistency()
 RETURNS TABLE(total_ciclos_migration integer, total_visitas integer, visitas_migradas integer, sagas_sin_visit integer, recolecciones_sin_visit integer, ciclos_sin_visita integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM migration.cabinet_cycles),
    (SELECT COUNT(*)::integer FROM public.visits),
    (SELECT COUNT(*)::integer FROM public.visits WHERE metadata->>'migrated_from_legacy' = 'true'),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.collections WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM migration.cabinet_cycles cb
     WHERE NOT EXISTS (SELECT 1 FROM public.visits v WHERE v.cycle_id = cb.cycle_id));
END;
$function$
;

CREATE OR REPLACE FUNCTION public.saga_outbox_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Solo para estados que requieren procesamiento externo
  IF NEW.status IN ('PENDING_CONFIRMATION', 'PROCESSING_ZOHO') THEN
    
    -- Insertar evento en outbox según el type de saga
    INSERT INTO event_outbox (
      event_type,
      saga_transaction_id,
      payload,
      processed,
      next_attempt
    ) VALUES (
      CASE 
        WHEN NEW.type = 'SALE' THEN 'CREATE_SALE_ODV'::outbox_event_type
        WHEN NEW.type IN ('INITIAL_PLACEMENT', 'CUTOFF_RENEWAL') 
          THEN 'CREATE_CONSIGNMENT_ODV'::outbox_event_type
        WHEN NEW.type = 'COLLECTION' THEN 'CREATE_RETURN'::outbox_event_type
        ELSE 'SYNC_ZOHO'::outbox_event_type
      END,
      NEW.id,
      jsonb_build_object(
        'type', NEW.type,
        'client_id', NEW.client_id,
        'user_id', NEW.user_id,
        'items', NEW.items,
        'metadata', NEW.metadata
      ),
      FALSE,
      NOW()
    )
    ON CONFLICT DO NOTHING; -- Evitar duplicados
    
  END IF;
  
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.set_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
begin
  new.updated_at = now();
  return new;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_generate_movements_from_saga()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
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
      (item->>'quantity')::int as quantity,
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

    -- Freeze current catalog price for this SKU
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
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_notify_task_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    v_visit RECORD;
    v_task_name TEXT;
BEGIN
    -- Only when status changes to COMPLETADO
    IF NEW.status = 'COMPLETED' AND OLD.status != 'COMPLETED' THEN
        -- Get visit info
        SELECT v.*, c.client_name as cliente_nombre
        INTO v_visit
        FROM visits v
        JOIN clients c ON c.client_id = v.client_id
        WHERE v.visit_id = NEW.visit_id;

        -- Task name mapping
        v_task_name := CASE NEW.task_type::text
            WHEN 'INITIAL_PLACEMENT' THEN 'Levantamiento'
            WHEN 'CUTOFF' THEN 'Corte'
            WHEN 'COLLECTION' THEN 'Recolección'
            WHEN 'POST_CUTOFF_PLACEMENT' THEN 'Lev. Post Corte'
            WHEN 'SALE_ODV' THEN 'Venta ODV'
            WHEN 'ODV_CABINET' THEN 'ODV Botiquín'
            WHEN 'VISIT_REPORT' THEN 'Informe'
            ELSE NEW.task_type::text
        END;

        -- Notify the representative
        PERFORM create_notification(
            v_visit.user_id,
            'TASK_COMPLETED',
            format('%s completado', v_task_name),
            format('Cliente: %s', v_visit.cliente_nombre),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'task_id', NEW.task_id,
                'task_type', NEW.task_type::text
            ),
            format('task_%s_%s', NEW.task_id, 'completed')
        );
    END IF;

    -- Notify ERROR to admins
    IF NEW.status = 'ERROR' AND OLD.status != 'ERROR' THEN
        PERFORM notify_admins(
            'TASK_ERROR',
            format('Error en %s', NEW.task_type::text),
            format('Visita: %s, Tarea: %s', NEW.visit_id, NEW.task_type::text),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'task_id', NEW.task_id,
                'task_type', NEW.task_type::text,
                'error', NEW.metadata->>'error_message'
            )
        );
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_notify_visit_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    v_cliente TEXT;
BEGIN
    IF NEW.status = 'COMPLETED' AND OLD.status != 'COMPLETED' THEN
        SELECT client_name INTO v_cliente FROM clients WHERE client_id = NEW.client_id;

        -- Notify the representative
        PERFORM create_notification(
            NEW.user_id,
            'TASK_COMPLETED',
            'Visita completed',
            format('Cliente: %s', v_cliente),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'cliente', v_cliente
            ),
            format('visit_%s_completed', NEW.visit_id)
        );

        -- Notify admins
        PERFORM notify_admins(
            'ADMIN_ACTION',
            'Visita completed',
            format('Representante finalizó visita a %s', v_cliente),
            jsonb_build_object('visit_id', NEW.visit_id)
        );
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_refresh_stats()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  PERFORM refresh_all_materialized_views();
  RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.update_tier_and_current_billing()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  UPDATE clients c
  SET current_billing = sub.promedio_mensual,
      current_tier = CASE
        WHEN sub.promedio_mensual > 45000 THEN 'ALTO'
        WHEN sub.promedio_mensual >= 20000 THEN 'MEDIO'
        ELSE 'BAJO'
      END
  FROM (
    SELECT
      v.client_id,
      COALESCE(SUM(v.quantity * v.price), 0) / NULLIF(COUNT(DISTINCT date_trunc('month', v.date)), 0) AS promedio_mensual
    FROM odv_sales v
    GROUP BY v.client_id
  ) sub
  WHERE c.client_id = sub.client_id;

  -- Set NULL for clients with no odv_sales
  UPDATE clients c
  SET current_billing = 0,
      current_tier = NULL
  WHERE NOT EXISTS (
    SELECT 1 FROM odv_sales v WHERE v.client_id = c.client_id
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.upsert_push_token(p_user_id character varying, p_token text, p_platform text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_current_user_id VARCHAR;
BEGIN
  -- Obtener el user_id actual basado en auth.uid()
  SELECT user_id INTO v_current_user_id
  FROM users
  WHERE auth_user_id = auth.uid();
  
  -- Verificar que el usuario solo puede guardar tokens para sí mismo
  IF v_current_user_id IS NULL OR v_current_user_id != p_user_id THEN
    RAISE EXCEPTION 'Unauthorized: Cannot save push token for another user';
  END IF;
  
  -- Desactivar tokens anteriores de este usuario en esta plataforma
  UPDATE user_push_tokens
  SET is_active = false, updated_at = NOW()
  WHERE user_id = p_user_id 
    AND platform = p_platform
    AND token != p_token;
  
  -- Upsert el nuevo token
  INSERT INTO user_push_tokens (user_id, token, platform, is_active, created_at, updated_at)
  VALUES (p_user_id, p_token, p_platform, true, NOW(), NOW())
  ON CONFLICT (token) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    is_active = true,
    updated_at = NOW();
    
END;
$function$
;

CREATE OR REPLACE FUNCTION public.validate_unique_skus_in_items()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  sku_duplicados jsonb;
BEGIN
  -- Buscar SKUs que aparecen más de una vez en items
  SELECT jsonb_object_agg(sku, count)
  INTO sku_duplicados
  FROM (
    SELECT 
      item->>'sku' as sku,
      item->>'movement_type' as type,
      COUNT(*) as count
    FROM jsonb_array_elements(NEW.items) as item
    GROUP BY item->>'sku', item->>'movement_type'
    HAVING COUNT(*) > 1
  ) duplicates;

  -- Si hay duplicados, rechazar la transacción
  IF sku_duplicados IS NOT NULL THEN
    RAISE EXCEPTION 'ERROR: SKUs duplicados detectados en items: %. Cada SKU solo puede aparecer una vez por type de movimiento en una SAGA. Una SAGA representa una operación completa (1 SAGA = 1 ODV en Zoho).', 
      sku_duplicados::text
    USING HINT = 'Verifique que no esté agregando el mismo SKU múltiples veces al array items.';
  END IF;

  RETURN NEW;
END;
$function$
;

