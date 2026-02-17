-- =============================================================
-- Migración: Corrección de vínculos ODV ↔ Visitas (Fase 1 + 2)
-- Motivo: La investigación de matching ODV reveló:
--   - 2 saga_zoho_links con espacios al inicio en zoho_id
--   - 2 saga_zoho_links vinculando ODVs a visitas SIN movimientos VENTA
--   - Fase 2 (DCOdV-36315): Ya correcto, no requiere cambio
-- Restricción: Solo visitas pre-2026
-- =============================================================

-- Fase 1a: Corregir leading spaces en zoho_id
-- link_id 14: " DCOdV-34391" → "DCOdV-34391" (MEXAP10933, visita 2025-11-15)
-- link_id 39: " DCOdV-32555" → "DCOdV-32555" (MEXBR172, visita 2025-10-15)
UPDATE saga_zoho_links SET zoho_id = TRIM(zoho_id) WHERE id IN (14, 39);

-- Fase 1b: Eliminar links de visitas SIN movimientos VENTA
-- link_id 11: DCOdV-33465, MEXJG20850, 2025-10-15 (0 movimientos VENTA)
-- link_id 46: DCOdV-34390, MEXPF13496, 2025-11-28 (0 movimientos VENTA)
DELETE FROM saga_zoho_links WHERE id IN (11, 46);

-- Fase 2: DCOdV-36315 (MEXBR172, 2025-11-28)
-- Verificado: P592:2, P632:1, X952:1, Y810:2 ya son tipo='VENTA'
-- No se requiere cambio. Link_id 56 permanece.
