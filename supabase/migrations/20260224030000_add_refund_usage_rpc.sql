-- 20260224030000_add_refund_usage_rpc.sql
-- Refund a query when the response hits MAX_TOKENS and is discarded.
-- Decrements queries_used by 1 for today's row (floor at 0).

CREATE OR REPLACE FUNCTION chatbot.refund_usage(
  p_id_usuario VARCHAR
)
RETURNS void
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

GRANT EXECUTE ON FUNCTION chatbot.refund_usage(VARCHAR) TO service_role;

NOTIFY pgrst, 'reload schema';
