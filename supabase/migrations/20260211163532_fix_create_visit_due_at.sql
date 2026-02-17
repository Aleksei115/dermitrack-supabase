-- Fix: rpc_create_visit sets due_at = now() causing visits to immediately show as RETRASADO
-- Change due_at to now() + interval '1 day' so manually-created visits have a reasonable deadline

-- Overload 1: with p_id_ciclo
CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_id_ciclo integer, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
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

  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at
  )
  VALUES (
    p_id_cliente, v_id_usuario, p_id_ciclo, p_tipo,
    'PENDIENTE', now(), now() + interval '1 day', now()
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

-- Overload 2: without p_id_ciclo
CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
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

  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at
  )
  VALUES (
    p_id_cliente, v_id_usuario, NULL, p_tipo::visit_tipo,
    'PENDIENTE', now(), now() + interval '1 day', now()
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
