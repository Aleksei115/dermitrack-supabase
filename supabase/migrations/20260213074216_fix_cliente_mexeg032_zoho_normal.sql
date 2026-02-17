-- Set id_cliente_zoho_normal for client MEXEG032 (Edna González)
-- Previously null, causing ODV import to skip rows with Código Cliente = MEXEG032
UPDATE public.clientes
SET id_cliente_zoho_normal = 'MEXEG032'
WHERE id_cliente = 'MEXEG032'
  AND id_cliente_zoho_normal IS NULL;
