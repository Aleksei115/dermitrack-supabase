-- ============================================================================
-- staging schema + prospects table for Botiquín Dérmico v2 analysis
-- ============================================================================
-- Origin: vtas sept2025_marzo2026.csv (Luna Labs export, sep 2025 - mar 2026)
-- Purpose: Replace local CSV file with canonical Supabase source for the v2
-- look-alike analysis. Holds long-form monthly qty/val per prospective doctor.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

GRANT USAGE ON SCHEMA staging TO postgres, anon, authenticated, service_role;

CREATE TABLE IF NOT EXISTS staging.prospects_metropolitana (
  id           BIGSERIAL PRIMARY KEY,
  cliente      TEXT NOT NULL,
  estado       TEXT NOT NULL,
  asesor       TEXT NOT NULL,
  mes          DATE NOT NULL,                       -- first day of the month
  qty          NUMERIC(16,4) NOT NULL DEFAULT 0,
  val          NUMERIC(16,4) NOT NULL DEFAULT 0,
  source_file  TEXT NOT NULL DEFAULT 'vtas_sept2025_marzo2026.csv',
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Same doctor name can appear under multiple asesores in the source pivot;
  -- the natural key is (cliente, estado, asesor, mes). Aggregation by (cliente,
  -- estado) is performed downstream by the notebook loader.
  CONSTRAINT prospects_metro_uniq UNIQUE (cliente, estado, asesor, mes)
);

CREATE INDEX IF NOT EXISTS idx_prospects_metro_estado  ON staging.prospects_metropolitana(estado);
CREATE INDEX IF NOT EXISTS idx_prospects_metro_cliente ON staging.prospects_metropolitana(cliente);
CREATE INDEX IF NOT EXISTS idx_prospects_metro_mes     ON staging.prospects_metropolitana(mes);

-- ----------------------------------------------------------------------------
-- Grants
-- ----------------------------------------------------------------------------
GRANT SELECT ON staging.prospects_metropolitana TO authenticated, anon, service_role;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON staging.prospects_metropolitana TO service_role, postgres;
GRANT USAGE, SELECT ON SEQUENCE staging.prospects_metropolitana_id_seq TO service_role, postgres;

-- ----------------------------------------------------------------------------
-- RLS (project policy: postgres + service_role full access; authenticated read)
-- ----------------------------------------------------------------------------
ALTER TABLE staging.prospects_metropolitana ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS postgres_all       ON staging.prospects_metropolitana;
DROP POLICY IF EXISTS service_role_all   ON staging.prospects_metropolitana;
DROP POLICY IF EXISTS authenticated_read ON staging.prospects_metropolitana;

CREATE POLICY postgres_all
  ON staging.prospects_metropolitana
  FOR ALL TO postgres
  USING (true) WITH CHECK (true);

CREATE POLICY service_role_all
  ON staging.prospects_metropolitana
  FOR ALL TO service_role
  USING (true) WITH CHECK (true);

CREATE POLICY authenticated_read
  ON staging.prospects_metropolitana
  FOR SELECT TO authenticated
  USING (true);

-- ----------------------------------------------------------------------------
-- Public view wrapper so PostgREST/supabase-py can query without schema config
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW public.prospects_metropolitana AS
SELECT id, cliente, estado, asesor, mes, qty, val
FROM staging.prospects_metropolitana;

GRANT SELECT ON public.prospects_metropolitana TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
