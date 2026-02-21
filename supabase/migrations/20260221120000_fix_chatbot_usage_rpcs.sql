-- Fix chatbot usage RPCs: column names don't match actual table schema
-- Table has: id_usuario, fecha, queries_used, queries_limit, updated_at
-- RPCs were using: queries_today, last_query_date (old column names)

CREATE OR REPLACE FUNCTION chatbot.get_remaining_queries(
  p_id_usuario VARCHAR,
  p_rol VARCHAR
)
RETURNS TABLE(queries_used INTEGER, queries_limit INTEGER, remaining INTEGER)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  -- OWNER/ADMIN get unlimited (999)
  IF p_rol IN ('OWNER', 'ADMINISTRADOR') THEN
    v_limit := 999;
  ELSE
    v_limit := 20;
  END IF;

  SELECT COALESCE(ul.queries_used, 0)
  INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.id_usuario = p_id_usuario
    AND ul.fecha = CURRENT_DATE;

  IF NOT FOUND THEN
    v_used := 0;
  END IF;

  RETURN QUERY SELECT v_used, v_limit, GREATEST(v_limit - v_used, 0);
END;
$$;

CREATE OR REPLACE FUNCTION chatbot.check_and_increment_usage(
  p_id_usuario VARCHAR,
  p_rol VARCHAR
)
RETURNS TABLE(allowed BOOLEAN, queries_used INTEGER, queries_limit INTEGER, remaining INTEGER)
LANGUAGE plpgsql SECURITY DEFINER
AS $$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  IF p_rol IN ('OWNER', 'ADMINISTRADOR') THEN
    v_limit := 999;
  ELSE
    v_limit := 20;
  END IF;

  -- Upsert usage record
  INSERT INTO chatbot.usage_limits (id_usuario, queries_used, fecha)
  VALUES (p_id_usuario, 0, CURRENT_DATE)
  ON CONFLICT (id_usuario) DO UPDATE
  SET queries_used = CASE
    WHEN chatbot.usage_limits.fecha < CURRENT_DATE THEN 0
    ELSE chatbot.usage_limits.queries_used
  END,
  fecha = CURRENT_DATE,
  updated_at = now();

  -- Get current count
  SELECT ul.queries_used INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.id_usuario = p_id_usuario;

  IF v_used >= v_limit THEN
    RETURN QUERY SELECT false, v_used, v_limit, 0;
    RETURN;
  END IF;

  -- Increment
  UPDATE chatbot.usage_limits
  SET queries_used = queries_used + 1,
      updated_at = now()
  WHERE id_usuario = p_id_usuario;

  v_used := v_used + 1;
  RETURN QUERY SELECT true, v_used, v_limit, GREATEST(v_limit - v_used, 0);
END;
$$;

-- Also fix rollback_usage if it exists with old column names
CREATE OR REPLACE FUNCTION chatbot.rollback_usage(
  p_id_usuario VARCHAR
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
  UPDATE chatbot.usage_limits
  SET queries_used = GREATEST(queries_used - 1, 0),
      updated_at = now()
  WHERE id_usuario = p_id_usuario
    AND fecha = CURRENT_DATE;
END;
$$;

-- Notify PostgREST to reload schema
NOTIFY pgrst, 'reload schema';
