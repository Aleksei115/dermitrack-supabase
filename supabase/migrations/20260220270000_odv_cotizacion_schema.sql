-- Phase 2a: Schema changes for 2-step ODV confirmation via PDF cotizacion
--
-- 1. Expand zoho_sync_status CHECK to include 'borrador_validado'
-- 2. Add unique constraint to botiquin_odv(odv_id, id_cliente, sku) for idempotency

BEGIN;

-- 1. Expand zoho_sync_status CHECK constraint to include borrador_validado
ALTER TABLE saga_zoho_links
  DROP CONSTRAINT IF EXISTS saga_odv_links_zoho_sync_status_check;

ALTER TABLE saga_zoho_links
  ADD CONSTRAINT saga_odv_links_zoho_sync_status_check
  CHECK (zoho_sync_status = ANY (ARRAY[
    'pending'::text,
    'synced'::text,
    'error'::text,
    'borrador_validado'::text
  ]));

-- 2. Add unique constraint to botiquin_odv (matches ventas_odv pattern)
CREATE UNIQUE INDEX IF NOT EXISTS uq_botiquin_odv_cliente_sku
  ON botiquin_odv (odv_id, id_cliente, sku);

NOTIFY pgrst, 'reload schema';

COMMIT;
