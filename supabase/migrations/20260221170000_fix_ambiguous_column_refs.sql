-- Fix ambiguous column references in RETURN QUERY SELECT
-- Variable names (v_used, v_limit) conflict with output column names

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
  IF p_rol = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_rol = 'ADMINISTRADOR' THEN
    v_limit := 10;
  ELSE
    v_limit := 5;
  END IF;

  INSERT INTO chatbot.usage_limits (id_usuario, fecha, queries_used, queries_limit)
  VALUES (p_id_usuario, CURRENT_DATE, 0, v_limit)
  ON CONFLICT (id_usuario, fecha) DO UPDATE
  SET updated_at = now();

  SELECT ul.queries_used INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.id_usuario = p_id_usuario
    AND ul.fecha = CURRENT_DATE;

  IF v_used >= v_limit THEN
    RETURN QUERY SELECT false::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, 0::INTEGER;
    RETURN;
  END IF;

  UPDATE chatbot.usage_limits
  SET queries_used = chatbot.usage_limits.queries_used + 1,
      updated_at = now()
  WHERE id_usuario = p_id_usuario
    AND fecha = CURRENT_DATE;

  v_used := v_used + 1;
  RETURN QUERY SELECT true::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$$;

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
  IF p_rol = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_rol = 'ADMINISTRADOR' THEN
    v_limit := 10;
  ELSE
    v_limit := 5;
  END IF;

  SELECT COALESCE(ul.queries_used, 0)
  INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.id_usuario = p_id_usuario
    AND ul.fecha = CURRENT_DATE;

  IF NOT FOUND THEN
    v_used := 0;
  END IF;

  RETURN QUERY SELECT v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$$;

NOTIFY pgrst, 'reload schema';
