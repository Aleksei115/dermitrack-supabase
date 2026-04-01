-- SECURITY DEFINER RPC for ingesting data sheet chunks.
-- Accepts a SKU, JSONB array of chunks, and full-text embedding.
-- Runs as postgres so the anon key can call it via PostgREST.
CREATE OR REPLACE FUNCTION chatbot.upsert_data_sheet(
  p_sku text,
  p_chunks jsonb,
  p_full_text text,
  p_full_embedding text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
AS $$
BEGIN
  -- Delete old chunks for this SKU
  DELETE FROM chatbot.data_sheet_chunks WHERE sku = p_sku;

  -- Insert new chunks from JSONB array
  INSERT INTO chatbot.data_sheet_chunks (sku, chunk_index, content, embedding)
  SELECT
    p_sku,
    (elem->>'chunk_index')::int,
    elem->>'content',
    CASE WHEN elem->>'embedding' IS NOT NULL
         THEN (elem->>'embedding')::vector(768)
         ELSE NULL
    END
  FROM jsonb_array_elements(p_chunks) AS elem;

  -- Upsert full-text embedding
  INSERT INTO chatbot.medication_embeddings (sku, embedding_text, embedding, updated_at)
  VALUES (
    p_sku,
    p_full_text,
    CASE WHEN p_full_embedding IS NOT NULL
         THEN p_full_embedding::vector(768)
         ELSE NULL
    END,
    now()
  )
  ON CONFLICT (sku) DO UPDATE SET
    embedding_text = EXCLUDED.embedding_text,
    embedding = EXCLUDED.embedding,
    updated_at = now();
END;
$$;

NOTIFY pgrst, 'reload schema';
