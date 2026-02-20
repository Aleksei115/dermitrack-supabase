-- Grant service_role access to chatbot schema (used by Edge Function via SERVICE_ROLE_KEY)
-- Also grant authenticated minimal access for future RLS-based direct queries

-- Schema USAGE
GRANT USAGE ON SCHEMA chatbot TO service_role;
GRANT USAGE ON SCHEMA chatbot TO authenticated;

-- Tables: service_role needs full CRUD for Edge Function operations
GRANT SELECT ON TABLE chatbot.config TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE chatbot.conversations TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE chatbot.messages TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE chatbot.usage_limits TO service_role;

-- Tables: authenticated needs access for RLS-protected operations
GRANT SELECT ON TABLE chatbot.config TO authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE chatbot.conversations TO authenticated;
GRANT SELECT, INSERT, UPDATE ON TABLE chatbot.messages TO authenticated;
GRANT SELECT ON TABLE chatbot.usage_limits TO authenticated;

-- Functions: service_role (Edge Function calls these via .rpc())
GRANT EXECUTE ON FUNCTION chatbot.check_and_increment_usage(character varying, text) TO service_role;
GRANT EXECUTE ON FUNCTION chatbot.get_remaining_queries(character varying, text) TO service_role;
GRANT EXECUTE ON FUNCTION chatbot.rollback_usage(character varying) TO service_role;

-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
