-- Improve fuzzy_search_clients: use word_similarity instead of similarity
-- word_similarity compares the query against individual words in the target,
-- so "beatriz" matches "BEATRIZ REYES JUAREZ" at 100% instead of 38%.
CREATE OR REPLACE FUNCTION chatbot.fuzzy_search_clients(
  p_search text,
  p_user_id character varying DEFAULT NULL::character varying,
  p_limit integer DEFAULT 5
)
RETURNS TABLE(client_id character varying, name text, similarity real)
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT c.client_id, c.client_name::TEXT as name,
    extensions.word_similarity(unaccent(lower(p_search)), unaccent(lower(c.client_name))) as similarity
  FROM clients c
  WHERE (p_user_id IS NULL OR c.user_id = p_user_id)
    AND extensions.word_similarity(unaccent(lower(p_search)), unaccent(lower(c.client_name))) > 0.2
  ORDER BY extensions.word_similarity(unaccent(lower(p_search)), unaccent(lower(c.client_name))) DESC
  LIMIT p_limit;
$$;

-- Notify PostgREST to reload schema cache
NOTIFY pgrst, 'reload schema';
