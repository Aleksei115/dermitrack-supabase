-- Drop duplicate chatbot usage functions (old versions with text param type)
-- Keep only the varchar versions created in the previous migration

DROP FUNCTION IF EXISTS chatbot.get_remaining_queries(character varying, text);
DROP FUNCTION IF EXISTS chatbot.check_and_increment_usage(character varying, text);

NOTIFY pgrst, 'reload schema';
