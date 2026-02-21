-- Fix ON CONFLICT to match actual PK (id_usuario, fecha) instead of just (id_usuario)

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

  -- Upsert usage record (PK is id_usuario + fecha)
  INSERT INTO chatbot.usage_limits (id_usuario, fecha, queries_used)
  VALUES (p_id_usuario, CURRENT_DATE, 0)
  ON CONFLICT (id_usuario, fecha) DO UPDATE
  SET updated_at = now();

  -- Get current count
  SELECT ul.queries_used INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.id_usuario = p_id_usuario
    AND ul.fecha = CURRENT_DATE;

  IF v_used >= v_limit THEN
    RETURN QUERY SELECT false, v_used, v_limit, 0;
    RETURN;
  END IF;

  -- Increment
  UPDATE chatbot.usage_limits
  SET queries_used = queries_used + 1,
      updated_at = now()
  WHERE id_usuario = p_id_usuario
    AND fecha = CURRENT_DATE;

  v_used := v_used + 1;
  RETURN QUERY SELECT true, v_used, v_limit, GREATEST(v_limit - v_used, 0);
END;
$$;

NOTIFY pgrst, 'reload schema';
