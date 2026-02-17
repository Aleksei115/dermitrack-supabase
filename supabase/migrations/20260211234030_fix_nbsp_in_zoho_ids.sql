-- =============================================================
-- Fix non-breaking spaces (U+00A0) in saga_zoho_links.zoho_id
-- TRIM() only removes ASCII spaces; these have \u00A0 (0xC2A0 in UTF-8)
-- =============================================================

-- link_id 14: "\u00A0DCOdV-34391" → "DCOdV-34391" (MEXAP10933, visita 2025-11-15)
-- link_id 39: "\u00A0DCOdV-32555" → "DCOdV-32555" (MEXBR172, visita 2025-10-15)
UPDATE saga_zoho_links
SET zoho_id = REGEXP_REPLACE(zoho_id, '^\s+', '')
WHERE id IN (14, 39);
