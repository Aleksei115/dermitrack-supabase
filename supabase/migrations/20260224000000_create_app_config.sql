-- ============================================================
-- app_config: key-value store for app configuration
-- Used by the mobile app to check min version, maintenance mode, etc.
-- Accessible via anon key (no auth required) for pre-auth checks.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.app_config (
  key   VARCHAR(100) PRIMARY KEY,
  value TEXT         NOT NULL DEFAULT '',
  description TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.app_config IS 'Key-value configuration for mobile app version gating and maintenance mode';

-- Auto-update updated_at on changes
CREATE OR REPLACE FUNCTION fn_app_config_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_app_config_updated_at
  BEFORE UPDATE ON public.app_config
  FOR EACH ROW
  EXECUTE FUNCTION fn_app_config_updated_at();

-- RLS: anon and authenticated can read, only service_role can write
ALTER TABLE public.app_config ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read app_config"
  ON public.app_config
  FOR SELECT
  USING (true);

-- Seed initial configuration
INSERT INTO public.app_config (key, value, description) VALUES
  ('min_version',           '1.0.0',                                    'Versión mínima requerida (bloquea versiones anteriores)'),
  ('recommended_version',   '1.0.0',                                    'Versión recomendada (muestra prompt suave)'),
  ('maintenance_mode',      'false',                                    'Modo mantenimiento (bloquea toda la app)'),
  ('maintenance_message',   'Estamos realizando mejoras. Vuelve pronto.', 'Mensaje mostrado durante mantenimiento'),
  ('android_update_url',    '',                                         'URL de descarga del APK de Android'),
  ('ios_update_url',        '',                                         'URL de descarga para iOS')
ON CONFLICT (key) DO NOTHING;

-- Notify PostgREST to pick up schema changes
NOTIFY pgrst, 'reload schema';
