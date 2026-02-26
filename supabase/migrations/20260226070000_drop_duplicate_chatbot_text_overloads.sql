-- Fix: PostgREST returns HTTP 300 because both TEXT and CHARACTER VARYING
-- overloads exist for these functions. Drop the TEXT ones so only VARCHAR remains.

DROP FUNCTION IF EXISTS chatbot.check_and_increment_usage(text, text);
DROP FUNCTION IF EXISTS chatbot.get_remaining_queries(text, text);
