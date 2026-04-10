-- RPC to fetch data sheet chunks by SKU list (replaces direct table query)
-- Needed because anon key does not have SELECT on chatbot.data_sheet_chunks
CREATE OR REPLACE FUNCTION chatbot.get_data_sheets_by_skus(
  p_skus text[]
)
RETURNS TABLE(sku varchar, content text, chunk_index int)
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
  SELECT d.sku, d.content, d.chunk_index
  FROM chatbot.data_sheet_chunks d
  WHERE d.sku = ANY(p_skus)
  ORDER BY d.sku, d.chunk_index;
$$;

NOTIFY pgrst, 'reload schema';
