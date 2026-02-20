-- Expose chatbot schema to PostgREST so the Edge Function can use
-- admin.schema("chatbot").from(...) and admin.schema("chatbot").rpc(...)
ALTER ROLE authenticator SET pgrst.db_schemas TO 'public, graphql_public, analytics, chatbot';
NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';
