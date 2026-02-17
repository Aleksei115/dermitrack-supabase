-- ============================================================
-- Part 1: Schema changes for MEXEG032 reconciliation
-- Must be separate migration because ALTER TYPE ADD VALUE
-- cannot be used in the same transaction as DML using the value
-- ============================================================

-- Add PERMANENCIA to saga transaction tipo enum
ALTER TYPE tipo_saga_transaction ADD VALUE IF NOT EXISTS 'PERMANENCIA';

-- Allow NULL zoho_id in saga_zoho_links (for RECOLECCION links)
ALTER TABLE public.saga_zoho_links ALTER COLUMN zoho_id DROP NOT NULL;
