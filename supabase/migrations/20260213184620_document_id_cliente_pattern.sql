-- Documentar el patrón dual de ID cliente con SQL COMMENTs
-- Contexto: Zoho usa dos IDs por cliente (normal para ventas, botiquín para consignaciones).
-- El schema usa id_cliente (= zoho_normal) como PK canónico en todas las tablas.
-- La traducción de IDs ocurre en el boundary (edge function import-odv), no en el schema.

COMMENT ON COLUMN public.clientes.id_cliente IS
  'PK canónico = id_cliente_zoho_normal. Usado como FK en todas las tablas del sistema.';

COMMENT ON COLUMN public.clientes.id_cliente_zoho_botiquin IS
  'ID alternativo de Zoho para consignaciones. Solo usado por import-odv para traducción al ingestar datos.';

COMMENT ON COLUMN public.botiquin_odv.id_cliente IS
  'FK a clientes.id_cliente (= zoho_normal). Traducción desde zoho_botiquin ocurre en import-odv.';

COMMENT ON COLUMN public.ventas_odv.id_cliente IS
  'FK a clientes.id_cliente (= zoho_normal). Coincide directamente con el ID de venta de Zoho.';
