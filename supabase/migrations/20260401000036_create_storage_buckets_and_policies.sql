-- Create storage buckets and RLS policies for evidence/signature uploads.
-- These were created manually on other projects but missing from migrations.

-- ═══ Create buckets (skip if already exist) ═══

INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'survey-evidence',
  'survey-evidence',
  false,
  10485760, -- 10MB
  ARRAY['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic', 'image/heif']
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES (
  'public-evidence',
  'public-evidence',
  true,
  10485760, -- 10MB
  ARRAY['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic', 'image/heif']
)
ON CONFLICT (id) DO NOTHING;

-- ═══ RLS policies for survey-evidence (private bucket) ═══

-- Authenticated users can upload
DO $$ BEGIN
  CREATE POLICY "authenticated_insert" ON storage.objects
    FOR INSERT TO authenticated
    WITH CHECK (bucket_id = 'survey-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Authenticated users can read their uploads
DO $$ BEGIN
  CREATE POLICY "authenticated_select_survey" ON storage.objects
    FOR SELECT TO authenticated
    USING (bucket_id = 'survey-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Authenticated users can overwrite (upsert)
DO $$ BEGIN
  CREATE POLICY "authenticated_update_survey" ON storage.objects
    FOR UPDATE TO authenticated
    USING (bucket_id = 'survey-evidence')
    WITH CHECK (bucket_id = 'survey-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ═══ RLS policies for public-evidence (public bucket) ═══

-- Authenticated users can upload
DO $$ BEGIN
  CREATE POLICY "authenticated_insert_public" ON storage.objects
    FOR INSERT TO authenticated
    WITH CHECK (bucket_id = 'public-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Anyone can read (public bucket)
DO $$ BEGIN
  CREATE POLICY "public_select_evidence" ON storage.objects
    FOR SELECT TO public
    USING (bucket_id = 'public-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Authenticated users can overwrite (upsert)
DO $$ BEGIN
  CREATE POLICY "authenticated_update_public" ON storage.objects
    FOR UPDATE TO authenticated
    USING (bucket_id = 'public-evidence')
    WITH CHECK (bucket_id = 'public-evidence');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

NOTIFY pgrst, 'reload schema';
