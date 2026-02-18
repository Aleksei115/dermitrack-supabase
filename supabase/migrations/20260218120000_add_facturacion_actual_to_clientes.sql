-- Ensure rango_actual and facturacion_actual columns exist on clientes table
-- (already present in DEV via manual DDL; this migration ensures consistency)
ALTER TABLE public.clientes ADD COLUMN IF NOT EXISTS rango_actual varchar(50);
ALTER TABLE public.clientes ADD COLUMN IF NOT EXISTS facturacion_actual numeric(14,2);
