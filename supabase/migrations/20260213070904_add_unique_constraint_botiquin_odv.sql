-- Add unique constraint on (odv_id, id_cliente, sku) to botiquin_odv
-- Matches the existing constraint on ventas_odv (uq_venta_odv_cliente_sku)
-- Required for ON CONFLICT DO NOTHING in the import-odv edge function

-- Step 1: Remove duplicate rows, keeping the one with lowest id_venta
DELETE FROM public.botiquin_odv
WHERE id_venta NOT IN (
  SELECT min(id_venta)
  FROM public.botiquin_odv
  GROUP BY odv_id, id_cliente, sku
);

-- Step 2: Add unique constraint
ALTER TABLE public.botiquin_odv
  ADD CONSTRAINT uq_botiquin_odv_cliente_sku UNIQUE (odv_id, id_cliente, sku);
