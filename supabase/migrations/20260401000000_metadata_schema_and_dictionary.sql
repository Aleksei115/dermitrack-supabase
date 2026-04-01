-- ============================================================================
-- Migration 1: Metadata schema + data dictionary + business glossary
-- Fase 0: Governance Base — AI-first schema introspection layer
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS metadata;
COMMENT ON SCHEMA metadata IS 'Data governance layer: dictionary, glossary, classification, lineage. AI-first — designed for chatbot/agent introspection.';

-- ── Data Dictionary View ────────────────────────────────────────────────────
-- Gives AI agents a complete view of the schema with descriptions

CREATE OR REPLACE VIEW metadata.data_dictionary AS
SELECT
  c.table_schema,
  c.table_name,
  obj_description((c.table_schema || '.' || c.table_name)::regclass) AS table_description,
  c.column_name,
  c.data_type,
  c.udt_name,
  c.is_nullable,
  col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position) AS column_description,
  c.column_default
FROM information_schema.columns c
WHERE c.table_schema IN ('public', 'analytics', 'metadata')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;

COMMENT ON VIEW metadata.data_dictionary IS
'Complete schema introspection: every column in public/analytics/metadata with table and column descriptions. AI agents should query this to understand the data model.';

-- ── Function Catalog View ───────────────────────────────────────────────────

CREATE OR REPLACE VIEW metadata.function_catalog AS
SELECT
  n.nspname AS schema_name,
  p.proname AS function_name,
  pg_catalog.obj_description(p.oid) AS description,
  CASE p.provolatile
    WHEN 'v' THEN 'VOLATILE'
    WHEN 's' THEN 'STABLE'
    WHEN 'i' THEN 'IMMUTABLE'
  END AS volatility,
  pg_get_function_arguments(p.oid) AS arguments,
  pg_get_function_result(p.oid) AS return_type,
  p.prosecdef AS is_security_definer
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname IN ('public', 'analytics', 'chatbot', 'metadata')
ORDER BY n.nspname, p.proname;

COMMENT ON VIEW metadata.function_catalog IS
'All RPC functions with descriptions, arguments, return types, and volatility. AI agents should query this to understand available operations.';

-- ── Business Glossary ───────────────────────────────────────────────────────

CREATE TABLE metadata.business_glossary (
  term text PRIMARY KEY,
  definition text NOT NULL,
  category text CHECK (category IN ('metric', 'dimension', 'process', 'entity', 'enum')),
  related_tables text[],
  related_functions text[],
  examples text,
  created_at timestamptz DEFAULT now()
);

COMMENT ON TABLE metadata.business_glossary IS
'Domain-specific terminology for DermiTrack. AI agents should search here when encountering unknown terms (M1, M2, cutoff, etc).';

-- ── Glossary Search RPC ─────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION metadata.search_glossary(p_query text)
RETURNS SETOF metadata.business_glossary
LANGUAGE sql STABLE
AS $$
  SELECT * FROM metadata.business_glossary
  WHERE term ILIKE '%' || p_query || '%'
     OR definition ILIKE '%' || p_query || '%';
$$;

COMMENT ON FUNCTION metadata.search_glossary(text) IS
'Full-text search on business glossary. Use for AI queries: "what is M2?", "explain cutoff", "define FEFO".';

-- ── Data Lineage ────────────────────────────────────────────────────────────

CREATE TABLE metadata.data_lineage (
  table_name text PRIMARY KEY,
  source_system text NOT NULL,  -- 'mobile_app', 'csv_import', 'trigger', 'rpc', 'manual', 'edge_function'
  update_frequency text,        -- 'real-time', 'daily', 'weekly', 'monthly', 'immutable'
  is_derived boolean DEFAULT false,
  derived_from text[],
  notes text
);

COMMENT ON TABLE metadata.data_lineage IS
'Where each table''s data comes from (mobile app, CSV import, triggers, etc). AI agents use this to explain data provenance.';

-- ── Column Classification ───────────────────────────────────────────────────

CREATE TABLE metadata.column_classification (
  schema_name text,
  table_name text,
  column_name text,
  classification text CHECK (classification IN ('public', 'internal', 'confidential', 'pii')),
  pii_type text,  -- 'name', 'phone', 'email', 'location', null
  PRIMARY KEY (schema_name, table_name, column_name)
);

COMMENT ON TABLE metadata.column_classification IS
'Data sensitivity classification per column. PII columns should never be exposed in AI query results without filtering.';

-- ── Permissions ─────────────────────────────────────────────────────────────

GRANT USAGE ON SCHEMA metadata TO authenticated, anon;
GRANT SELECT ON ALL TABLES IN SCHEMA metadata TO authenticated, anon;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA metadata TO authenticated, anon;

-- Default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT SELECT ON TABLES TO authenticated, anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT EXECUTE ON FUNCTIONS TO authenticated, anon;

NOTIFY pgrst, 'reload schema';
