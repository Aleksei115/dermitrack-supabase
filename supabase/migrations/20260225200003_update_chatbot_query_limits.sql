-- Update chatbot query limits: REP 5→7, ADMIN stays 10

-- 1. Recreate check_and_increment_usage with new limits
CREATE OR REPLACE FUNCTION chatbot.check_and_increment_usage(
  p_user_id TEXT,
  p_role TEXT
)
RETURNS TABLE(allowed BOOLEAN, queries_used INTEGER, queries_limit INTEGER, remaining INTEGER)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  IF p_role = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_role = 'ADMIN' THEN
    v_limit := 10;
  ELSE
    v_limit := 7;
  END IF;

  INSERT INTO chatbot.usage_limits (user_id, date, queries_used, queries_limit)
  VALUES (p_user_id, CURRENT_DATE, 0, v_limit)
  ON CONFLICT (user_id, date) DO UPDATE
  SET updated_at = now();

  SELECT ul.queries_used INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.user_id = p_user_id
    AND ul.date = CURRENT_DATE;

  IF v_used >= v_limit THEN
    RETURN QUERY SELECT false::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, 0::INTEGER;
    RETURN;
  END IF;

  UPDATE chatbot.usage_limits
  SET queries_used = chatbot.usage_limits.queries_used + 1,
      updated_at = now()
  WHERE user_id = p_user_id
    AND date = CURRENT_DATE;

  v_used := v_used + 1;
  RETURN QUERY SELECT true::BOOLEAN, v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$$;

-- 2. Recreate get_remaining_queries with new limits
CREATE OR REPLACE FUNCTION chatbot.get_remaining_queries(
  p_user_id TEXT,
  p_role TEXT
)
RETURNS TABLE(queries_used INTEGER, queries_limit INTEGER, remaining INTEGER)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_limit INTEGER;
  v_used INTEGER;
BEGIN
  IF p_role = 'OWNER' THEN
    v_limit := 30;
  ELSIF p_role = 'ADMIN' THEN
    v_limit := 10;
  ELSE
    v_limit := 7;
  END IF;

  SELECT COALESCE(ul.queries_used, 0)
  INTO v_used
  FROM chatbot.usage_limits ul
  WHERE ul.user_id = p_user_id
    AND ul.date = CURRENT_DATE;

  IF NOT FOUND THEN
    v_used := 0;
  END IF;

  RETURN QUERY SELECT v_used::INTEGER, v_limit::INTEGER, GREATEST(v_limit - v_used, 0)::INTEGER;
END;
$$;
