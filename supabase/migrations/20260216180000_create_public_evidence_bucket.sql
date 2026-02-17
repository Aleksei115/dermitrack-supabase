-- Bucket público para evidencias descargables
-- Las fotos subidas aquí son accesibles via URL pública sin autenticación
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'public-evidence',
  'public-evidence',
  true,
  5242880,  -- 5MB
  ARRAY['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic']
)
ON CONFLICT (id) DO NOTHING;

-- Lectura pública (cualquiera puede descargar)
CREATE POLICY "public_evidence_select" ON storage.objects
  FOR SELECT USING (bucket_id = 'public-evidence');

-- Solo usuarios autenticados pueden subir
CREATE POLICY "public_evidence_insert" ON storage.objects
  FOR INSERT WITH CHECK (
    bucket_id = 'public-evidence'
    AND auth.role() = 'authenticated'
  );
