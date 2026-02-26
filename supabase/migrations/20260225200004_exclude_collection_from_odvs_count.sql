-- Count ODVs from saga_zoho_links excluding RETURN (collection ODVs)
-- RETURN is tracked separately via collections_count

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
      'odvs_count', (
        SELECT COUNT(*) FROM saga_zoho_links szl
        JOIN saga_transactions st ON st.id = szl.id_saga_transaction
        WHERE st.visit_id = v.visit_id
          AND szl.type::text != 'RETURN'
      ),
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

NOTIFY pgrst, 'reload schema';
