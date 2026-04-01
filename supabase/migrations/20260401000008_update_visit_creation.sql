-- ============================================================================
-- Migration 9: Update visit creation — PIVOT → VALIDATION for ODV tasks
-- Fase 2: New visits use VALIDATION transaction_type; existing visits keep PIVOT
-- ============================================================================

-- ── Update rpc_create_visit (2 params) ──────────────────────────────────────

CREATE OR REPLACE FUNCTION public.rpc_create_visit(
  p_client_id character varying,
  p_type character varying DEFAULT 'VISIT_CUTOFF'::character varying
)
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
    -- 3 tasks: COMPENSABLE → VALIDATION → RETRYABLE
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'INITIAL_PLACEMENT', 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_CABINET',       'PENDING', now() + interval '7 days', 'VALIDATION', 2),
      (v_visit_id, 'VISIT_REPORT',      'PENDING', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    -- 6 tasks: COMPENSABLE → VALIDATION → COMPENSABLE → COMPENSABLE → VALIDATION → RETRYABLE
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CUTOFF',                 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'SALE_ODV',               'PENDING', now() + interval '7 days', 'VALIDATION', 2),
      (v_visit_id, 'COLLECTION',             'PENDING', now() + interval '7 days', 'COMPENSABLE', 3),
      (v_visit_id, 'POST_CUTOFF_PLACEMENT',  'PENDING', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_CABINET',            'PENDING', now() + interval '7 days', 'VALIDATION', 5),
      (v_visit_id, 'VISIT_REPORT',           'PENDING', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$;

-- ── Update rpc_create_visit (3 params — with cycle_id) ─────────────────────

CREATE OR REPLACE FUNCTION public.rpc_create_visit(
  p_client_id character varying,
  p_cycle_id integer,
  p_type character varying DEFAULT 'VISIT_CUTOFF'::character varying
)
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
      (v_visit_id, 'ODV_CABINET',       'PENDING', now() + interval '7 days', 'VALIDATION', 2),
      (v_visit_id, 'VISIT_REPORT',      'PENDING', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    INSERT INTO public.visit_tasks (visit_id, task_type, status, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CUTOFF',                 'PENDING', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'SALE_ODV',               'PENDING', now() + interval '7 days', 'VALIDATION', 2),
      (v_visit_id, 'COLLECTION',             'PENDING', now() + interval '7 days', 'COMPENSABLE', 3),
      (v_visit_id, 'POST_CUTOFF_PLACEMENT',  'PENDING', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_CABINET',            'PENDING', now() + interval '7 days', 'VALIDATION', 5),
      (v_visit_id, 'VISIT_REPORT',           'PENDING', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$;

NOTIFY pgrst, 'reload schema';
