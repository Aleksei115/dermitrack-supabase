-- Fix: rpc_submit_informe_visita duplicate check blocks next visit creation
-- Add: corte_number column to visitas for per-client cycle tracking
-- Add: corteProgress indicator in get_dashboard_static()
-- Revert: TEST_ exclusion filters (client deleted)

-- ============================================================
-- Part C1: Add corte_number column
-- ============================================================
ALTER TABLE visitas ADD COLUMN IF NOT EXISTS corte_number INT;

-- Backfill: 0 = LEV, 1 = first CORTE, 2 = second CORTE, etc. per client
WITH numbered AS (
  SELECT visit_id,
    ROW_NUMBER() OVER (PARTITION BY id_cliente ORDER BY created_at) - 1 AS cn
  FROM visitas
)
UPDATE visitas v SET corte_number = n.cn
FROM numbered n WHERE v.visit_id = n.visit_id;

-- ============================================================
-- Part B + C2: Fix rpc_submit_informe_visita
-- Bug: duplicate check matches current visit (due_at = fecha_proxima)
-- Fix: exclude current visit with AND visit_id != p_visit_id
-- Also: set corte_number on new visit
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_submit_informe_visita(p_visit_id uuid, p_respuestas jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_id_ciclo integer;
  v_tipo_visita visit_tipo;
  v_informe_id uuid;
  v_next_visit_id uuid;
  v_fecha_proxima date;
  v_etiqueta varchar;
  v_cumplimiento_score integer := 0;
  v_total_preguntas integer := 0;
  v_next_corte_number integer;
BEGIN
  -- Obtener datos de la visita actual
  SELECT v.id_usuario, v.id_cliente, v.id_ciclo, v.tipo
  INTO v_id_usuario, v_id_cliente, v_id_ciclo, v_tipo_visita
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Validar que ODV_BOTIQUIN esté completada antes del informe
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_tipo = 'ODV_BOTIQUIN'
    AND estado = 'COMPLETADO'::visit_task_estado
  ) THEN
    RAISE EXCEPTION 'Debe completar la confirmación ODV Botiquín antes de enviar el informe';
  END IF;

  -- Extraer fecha próxima visita
  v_fecha_proxima := (p_respuestas->>'fecha_proxima_visita')::date;

  -- Calcular score de cumplimiento
  SELECT
    COALESCE(SUM(CASE WHEN value::text = 'true' THEN 1 ELSE 0 END), 0),
    COUNT(*)
  INTO v_cumplimiento_score, v_total_preguntas
  FROM jsonb_each(p_respuestas)
  WHERE key NOT IN ('fecha_proxima_visita', 'imagen_visita', 'imagen_visita_local')
  AND jsonb_typeof(value) = 'boolean';

  -- Determinar etiqueta
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
  INSERT INTO public.visita_informes (
    visit_id, respuestas, etiqueta, cumplimiento_score, completada, fecha_completada, created_at
  )
  VALUES (
    p_visit_id,
    p_respuestas,
    v_etiqueta,
    v_cumplimiento_score,
    true,
    now(),
    now()
  )
  ON CONFLICT (visit_id) DO UPDATE SET
    respuestas = EXCLUDED.respuestas,
    etiqueta = EXCLUDED.etiqueta,
    cumplimiento_score = EXCLUDED.cumplimiento_score,
    completada = true,
    fecha_completada = COALESCE(visita_informes.fecha_completada, now()),
    updated_at = now()
  RETURNING informe_id INTO v_informe_id;

  -- Marcar tarea INFORME_VISITA como completada
  UPDATE public.visit_tasks
  SET
    estado = 'COMPLETADO'::visit_task_estado,
    completed_at = now(),
    reference_table = 'visita_informes',
    reference_id = v_informe_id::text,
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = 'INFORME_VISITA';

  -- Actualizar etiqueta en la visita actual
  UPDATE public.visitas
  SET
    etiqueta = v_etiqueta,
    updated_at = now()
  WHERE visit_id = p_visit_id;

  -- Crear próxima visita si se especificó fecha
  IF v_fecha_proxima IS NOT NULL THEN
    -- FIX: exclude current visit from duplicate check (visit_id != p_visit_id)
    IF NOT EXISTS (
      SELECT 1 FROM public.visitas
      WHERE id_cliente = v_id_cliente
      AND DATE(due_at) = v_fecha_proxima
      AND estado != 'CANCELADO'
      AND visit_id != p_visit_id
    ) THEN
      -- Calculate next corte_number for this client
      SELECT COALESCE(MAX(corte_number), -1) + 1
      INTO v_next_corte_number
      FROM public.visitas
      WHERE id_cliente = v_id_cliente;

      INSERT INTO public.visitas (
        id_cliente, id_usuario, id_ciclo, tipo, estado, due_at, created_at, corte_number
      )
      VALUES (
        v_id_cliente,
        v_id_usuario,
        v_id_ciclo,
        'VISITA_CORTE'::visit_tipo,
        'PROGRAMADO'::visit_estado,
        v_fecha_proxima,
        now(),
        v_next_corte_number
      )
      RETURNING visit_id INTO v_next_visit_id;

      -- Crear tareas para la próxima visita CON transaction_type y step_order
      INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, due_at, transaction_type, step_order, created_at)
      VALUES
        (v_next_visit_id, 'CORTE', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'COMPENSABLE', 1, now()),
        (v_next_visit_id, 'VENTA_ODV', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'PIVOT', 2, now()),
        (v_next_visit_id, 'RECOLECCION', 'PENDIENTE'::visit_task_estado, false, v_fecha_proxima, 'RETRYABLE', 3, now()),
        (v_next_visit_id, 'LEV_POST_CORTE', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'COMPENSABLE', 4, now()),
        (v_next_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'PIVOT', 5, now()),
        (v_next_visit_id, 'INFORME_VISITA', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'RETRYABLE', 6, now());
    END IF;
  END IF;

  RETURN v_informe_id;
END;
$function$;

-- ============================================================
-- Part C3: Update rpc_create_visit (overload with p_id_ciclo)
-- Add corte_number assignment
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_id_ciclo integer, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
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
  SELECT u.id_usuario INTO v_id_usuario
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en tabla usuarios';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.visitas v
    WHERE v.id_cliente = p_id_cliente
      AND v.estado IN ('PENDIENTE', 'EN_CURSO', 'PROGRAMADO')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Calculate corte_number: LEV = 0, CORTE = max + 1
  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    v_corte_number := 0;
  ELSE
    SELECT COALESCE(MAX(corte_number), -1) + 1
    INTO v_corte_number
    FROM public.visitas
    WHERE id_cliente = p_id_cliente;
  END IF;

  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at, corte_number
  )
  VALUES (
    p_id_cliente, v_id_usuario, p_id_ciclo, p_tipo,
    'PENDIENTE', now(), now() + interval '1 day', now(), v_corte_number
  )
  RETURNING visit_id INTO v_visit_id;

  INSERT INTO public.visita_informes (visit_id, respuestas, completada)
  VALUES (v_visit_id, '{}'::jsonb, false);

  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'LEVANTAMIENTO_INICIAL', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'VENTA_ODV', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'RECOLECCION', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'LEV_POST_CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$;

-- ============================================================
-- Part C4: Update rpc_create_visit (overload without p_id_ciclo)
-- Add corte_number assignment
-- ============================================================
CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
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
  SELECT u.id_usuario INTO v_id_usuario
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en tabla usuarios';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.visitas v
    WHERE v.id_cliente = p_id_cliente
      AND v.estado IN ('PENDIENTE', 'EN_CURSO', 'PROGRAMADO')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Calculate corte_number: LEV = 0, CORTE = max + 1
  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    v_corte_number := 0;
  ELSE
    SELECT COALESCE(MAX(corte_number), -1) + 1
    INTO v_corte_number
    FROM public.visitas
    WHERE id_cliente = p_id_cliente;
  END IF;

  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at, corte_number
  )
  VALUES (
    p_id_cliente, v_id_usuario, NULL, p_tipo::visit_tipo,
    'PENDIENTE', now(), now() + interval '1 day', now(), v_corte_number
  )
  RETURNING visit_id INTO v_visit_id;

  INSERT INTO public.visita_informes (visit_id, respuestas, completada)
  VALUES (v_visit_id, '{}'::jsonb, false);

  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'LEVANTAMIENTO_INICIAL', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'VENTA_ODV', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'RECOLECCION', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'LEV_POST_CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$;

-- ============================================================
-- Part C5 + Analytics: Update get_corte_actual_data()
-- Use corte_number instead of completed_at for ranking
-- Include LEV visits (remove tipo = 'VISITA_CORTE' filter)
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_corte_actual_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  -- Rank all completed visits per active doctor using corte_number
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.id_cliente,
      v.completed_at::date AS fecha_visita,
      v.corte_number,
      ROW_NUMBER() OVER (PARTITION BY v.id_cliente ORDER BY v.corte_number DESC) AS rn
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado = 'COMPLETADO'
      AND v.completed_at IS NOT NULL
      AND (p_medicos IS NULL OR v.id_cliente = ANY(p_medicos))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  ),
  -- All active clients for pending detection
  all_active_clients AS (
    SELECT c.id_cliente, c.nombre_cliente
    FROM clientes c
    WHERE c.activo = TRUE
      AND (p_medicos IS NULL OR c.id_cliente = ANY(p_medicos))
  ),
  -- Padecimiento dedup (1:1 per sku)
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  -- SKUs passing marca + padecimiento filters
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  -- Current corte movements via saga_transactions from current_visits
  current_mov AS (
    SELECT mov.id_cliente, ac.nombre_cliente, mov.sku, mov.tipo, mov.cantidad, COALESCE(mov.precio_unitario, 0) AS precio
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.id_cliente = mov.id_cliente
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- Previous corte movements via saga_transactions from prev_visits
  prev_mov AS (
    SELECT mov.id_cliente, mov.sku, mov.tipo, mov.cantidad, COALESCE(mov.precio_unitario, 0) AS precio
    FROM prev_visits pv
    JOIN saga_transactions st ON st.visit_id = pv.visit_id
    JOIN movimientos_inventario mov ON mov.id_saga_transaction = st.id
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  -- KPI aggregation (current corte)
  kpi_stats AS (
    SELECT
      COALESCE(COUNT(DISTINCT mov.id_cliente), 0)::int AS total_medicos_visitados,
      COALESCE(COUNT(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.id_cliente END), 0)::int AS medicos_con_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM current_mov mov
  ),
  -- Previous corte totals for % change (consistent filters)
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion
    FROM prev_mov mov
  ),
  -- Previous per-médico venta (for VS Anterior column)
  prev_medico_stats AS (
    SELECT mov.id_cliente,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta
    FROM prev_mov mov
    GROUP BY mov.id_cliente
  ),
  -- Visited doctors with movement data
  visited_medico_rows AS (
    SELECT
      mov.id_cliente,
      mov.nombre_cliente,
      cv.fecha_visita::text AS fecha_visita,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END), 0)::int AS piezas_recoleccion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_venta,
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_creacion,
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * mov.precio ELSE 0 END), 0)::numeric AS valor_recoleccion,
      STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.sku END, ', ') AS skus_vendidos,
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END), 0) > 0 AS tiene_venta,
      pms.valor_venta AS valor_venta_anterior,
      CASE
        WHEN pms.valor_venta IS NOT NULL AND pms.valor_venta > 0
          THEN ROUND(((COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * mov.precio ELSE 0 END), 0) - pms.valor_venta) / pms.valor_venta * 100)::numeric, 1)
        WHEN (pms.valor_venta IS NULL OR pms.valor_venta = 0)
          AND COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END), 0) > 0
          THEN 100.0
        ELSE NULL
      END AS porcentaje_cambio
    FROM current_mov mov
    JOIN current_visits cv ON cv.id_cliente = mov.id_cliente
    LEFT JOIN prev_medico_stats pms ON mov.id_cliente = pms.id_cliente
    GROUP BY mov.id_cliente, mov.nombre_cliente, cv.fecha_visita, pms.valor_venta
  ),
  -- Pending doctors: active clients without a current completed visit
  pending_medico_rows AS (
    SELECT
      ac.id_cliente,
      ac.nombre_cliente,
      NULL::text AS fecha_visita,
      0::int AS piezas_venta,
      0::int AS piezas_creacion,
      0::int AS piezas_recoleccion,
      0::numeric AS valor_venta,
      0::numeric AS valor_creacion,
      0::numeric AS valor_recoleccion,
      NULL::text AS skus_vendidos,
      false AS tiene_venta,
      NULL::numeric AS valor_venta_anterior,
      NULL::numeric AS porcentaje_cambio
    FROM all_active_clients ac
    WHERE NOT EXISTS (SELECT 1 FROM current_visits cv WHERE cv.id_cliente = ac.id_cliente)
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'fecha_inicio', (SELECT MIN(cv.fecha_visita) FROM current_visits cv),
      'fecha_fin', (SELECT MAX(cv.fecha_visita) FROM current_visits cv),
      'dias_corte', COALESCE((SELECT MAX(cv.fecha_visita) - MIN(cv.fecha_visita) + 1 FROM current_visits cv), 0),
      'total_medicos_visitados', k.total_medicos_visitados,
      'medicos_con_venta', k.medicos_con_venta,
      'medicos_sin_venta', k.total_medicos_visitados - k.medicos_con_venta,
      'piezas_venta', k.piezas_venta,
      'valor_venta', k.valor_venta,
      'piezas_creacion', k.piezas_creacion,
      'valor_creacion', k.valor_creacion,
      'piezas_recoleccion', k.piezas_recoleccion,
      'valor_recoleccion', k.valor_recoleccion,
      'porcentaje_cambio_venta',
        CASE WHEN p.valor_venta > 0
          THEN ROUND(((k.valor_venta - p.valor_venta) / p.valor_venta * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_creacion',
        CASE WHEN p.valor_creacion > 0
          THEN ROUND(((k.valor_creacion - p.valor_creacion) / p.valor_creacion * 100)::numeric, 1)
          ELSE NULL END,
      'porcentaje_cambio_recoleccion',
        CASE WHEN p.valor_recoleccion > 0
          THEN ROUND(((k.valor_recoleccion - p.valor_recoleccion) / p.valor_recoleccion * 100)::numeric, 1)
          ELSE NULL END
    ),
    'medicos', COALESCE(
      (SELECT json_agg(row_to_json(sub) ORDER BY sub.fecha_visita IS NULL ASC, sub.valor_venta DESC, sub.nombre_cliente)
       FROM (
         SELECT * FROM visited_medico_rows
         UNION ALL
         SELECT * FROM pending_medico_rows
       ) sub),
      '[]'::json)
  ) INTO v_result
  FROM kpi_stats k
  CROSS JOIN prev_stats p;

  RETURN v_result;
END;
$function$;

-- Public wrapper for get_corte_actual_data
CREATE OR REPLACE FUNCTION public.get_corte_actual_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL
)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT analytics.get_corte_actual_data(p_medicos, p_marcas, p_padecimientos);
$function$;

-- ============================================================
-- Part D + E: Update get_dashboard_static() with corteProgress
-- and revert TEST_ exclusion filter
-- ============================================================
CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filtros json;
  v_stats json;
  v_progress json;
BEGIN
  SELECT row_to_json(f) INTO v_filtros
  FROM (
    SELECT
      (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
       FROM medicamentos m WHERE m.marca IS NOT NULL) AS marcas,
      (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
       FROM clientes c WHERE c.activo = true) AS medicos,
      (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
       FROM padecimientos p) AS padecimientos,
      (SELECT MIN(fecha_movimiento)::date
       FROM movimientos_inventario WHERE tipo = 'CREACION') AS "fechaPrimerLevantamiento"
  ) f;

  SELECT row_to_json(s) INTO v_stats
  FROM (
    SELECT
      r.fecha_inicio AS "fechaInicio",
      r.fecha_fin AS "fechaFin",
      r.dias_corte AS "diasCorte",
      r.total_medicos_visitados AS "totalMedicosVisitados",
      r.total_movimientos AS "totalMovimientos",
      r.piezas_venta AS "piezasVenta",
      r.piezas_creacion AS "piezasCreacion",
      r.piezas_recoleccion AS "piezasRecoleccion",
      r.valor_venta AS "valorVenta",
      r.valor_creacion AS "valorCreacion",
      r.valor_recoleccion AS "valorRecoleccion",
      r.medicos_con_venta AS "medicosConVenta",
      r.medicos_sin_venta AS "medicosSinVenta",
      r.valor_venta_anterior AS "valorVentaAnterior",
      r.valor_creacion_anterior AS "valorCreacionAnterior",
      r.valor_recoleccion_anterior AS "valorRecoleccionAnterior",
      r.promedio_por_medico_anterior AS "promedioPorMedicoAnterior",
      r.porcentaje_cambio_venta AS "porcentajeCambioVenta",
      r.porcentaje_cambio_creacion AS "porcentajeCambioCreacion",
      r.porcentaje_cambio_recoleccion AS "porcentajeCambioRecoleccion",
      r.porcentaje_cambio_promedio AS "porcentajeCambioPromedio"
    FROM get_corte_stats_generales_con_comparacion() r
    LIMIT 1
  ) s;

  -- Progress: skip PROGRAMADO (future) and CANCELADO (abandoned) to get actual cycle status
  SELECT json_build_object(
    'completaron', COUNT(*) FILTER (WHERE lv.estado = 'COMPLETADO'),
    'pendientes', COUNT(*) FILTER (WHERE lv.estado IN ('EN_CURSO','PENDIENTE')),
    'total', (SELECT COUNT(*) FROM clientes WHERE activo = TRUE)
  ) INTO v_progress
  FROM (
    SELECT DISTINCT ON (v.id_cliente) v.estado
    FROM visitas v
    JOIN clientes c ON c.id_cliente = v.id_cliente AND c.activo = TRUE
    WHERE v.estado NOT IN ('PROGRAMADO', 'CANCELADO')
    ORDER BY v.id_cliente, v.corte_number DESC
  ) lv;

  RETURN json_build_object(
    'corteFiltros', v_filtros,
    'corteStatsGenerales', v_stats,
    'corteProgress', v_progress
  );
END;
$function$;

-- Re-create public wrapper for get_dashboard_static
CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$;

-- ============================================================
-- Part E: Revert TEST_ exclusion from get_filtros_disponibles()
-- ============================================================
CREATE OR REPLACE FUNCTION public.get_filtros_disponibles()
 RETURNS TABLE(marcas character varying[], medicos jsonb, padecimientos character varying[], fecha_primer_levantamiento date)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.marca ORDER BY m.marca)
     FROM medicamentos m WHERE m.marca IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente) ORDER BY c.nombre_cliente)
     FROM clientes c WHERE c.activo = true),
    (SELECT ARRAY_AGG(DISTINCT p.nombre ORDER BY p.nombre)
     FROM padecimientos p)::varchar[],
    (SELECT MIN(fecha_movimiento)::date
     FROM movimientos_inventario WHERE tipo = 'CREACION');
END;
$function$;

-- ============================================================
-- Permissions
-- ============================================================
GRANT EXECUTE ON FUNCTION public.rpc_submit_informe_visita(uuid, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.rpc_create_visit(character varying, integer, character varying) TO authenticated;
GRANT EXECUTE ON FUNCTION public.rpc_create_visit(character varying, character varying) TO authenticated;
GRANT EXECUTE ON FUNCTION analytics.get_corte_actual_data(character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_actual_data(character varying[], character varying[], character varying[]) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION analytics.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_dashboard_static() TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_filtros_disponibles() TO authenticated, anon;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
