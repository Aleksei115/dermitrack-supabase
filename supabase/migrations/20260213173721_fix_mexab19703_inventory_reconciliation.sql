-- Fix data reconciliation for client MEXAB19703 (Araceli Barrera Jacome)
-- Ensures CREACION + PERMANENCIA = RECOLECCION + VENTA for all 2025 visits
-- 2026 data (Visit 6) is NOT touched

-- Part 1: Move LEV_POST_CORTE saga from Visit 2 to Visit 3
UPDATE public.saga_transactions
SET visit_id = '4cb3e5d9-99c8-400d-bc26-c64a269d9152'
WHERE id = '317e0871-b1a3-4fa1-a5b6-c4280d032c63';

-- Part 2: Link orphan CREACION movements to LEV_POST_CORTE sagas

-- Visit 3 orphans (11 rows) -> LEV_POST_CORTE 317e0871
UPDATE public.movimientos_inventario
SET id_saga_transaction = '317e0871-b1a3-4fa1-a5b6-c4280d032c63'
WHERE id IN (1640,1642,1644,1646,1648,1650,1652,1654,1656,1658,1660);

-- Visit 4 orphan (1 row) -> LEV_POST_CORTE daa30369
UPDATE public.movimientos_inventario
SET id_saga_transaction = 'daa30369-a87d-4e40-a47a-1c15b4b8b3e3'
WHERE id = 1663;

-- Visit 5 orphans (10 rows) -> LEV_POST_CORTE 1f82f37d
UPDATE public.movimientos_inventario
SET id_saga_transaction = '1f82f37d-667f-45fa-b131-607c3429661c'
WHERE id IN (1677,1679,1681,1683,1685,1687,1689,1691,1693,1695);

-- Part 3: Fix Visit 2 PERMANENCIA (delete wrong, insert correct)

-- Delete wrong PERMANENCIA (qty=0, under VENTA saga 6e02715c)
DELETE FROM public.movimientos_inventario
WHERE id IN (1716,1717,1718,1719,1720,1721,1722,1723,1724);

-- Create correct PERMANENCIA (NULL saga, quantities matching Visit 1 stock)
INSERT INTO public.movimientos_inventario
  (id_saga_transaction, id_cliente, sku, cantidad, cantidad_antes, cantidad_despues, fecha_movimiento, tipo)
VALUES
  (NULL,'MEXAB19703','P005',4,4,4,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','P031',2,2,2,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','P040',4,4,4,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','P070',4,4,4,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','P156',4,4,4,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','P294',4,4,4,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','R846',3,3,3,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','S531',2,2,2,'2025-10-15 18:09:40+00','PERMANENCIA'),
  (NULL,'MEXAB19703','V160',3,3,3,'2025-10-15 18:09:40+00','PERMANENCIA');

-- Part 4: Fix Visit 4 PERMANENCIA (unlink from VENTA saga)
UPDATE public.movimientos_inventario
SET id_saga_transaction = NULL
WHERE id IN (1915,1916,1917,1918,1919,1920,1921,1922,1923,1924,1925);

-- Part 5: Add DEVOLUCION saga_zoho_links for RECOLECCION sagas
INSERT INTO public.saga_zoho_links
  (id_saga_transaction, zoho_id, tipo, zoho_sync_status, zoho_synced_at)
VALUES
  ('d57da800-4987-4a71-8ec4-bf3e8aca3ad2', NULL, 'DEVOLUCION', 'synced', now()),
  ('cb86f2ea-164d-4801-8be0-918f29bde752', NULL, 'DEVOLUCION', 'synced', now());

-- Part 6: Update existing saga_zoho_links to synced
UPDATE public.saga_zoho_links
SET zoho_sync_status = 'synced', zoho_synced_at = now()
WHERE id IN (60, 61, 62, 63, 111, 114);

-- Part 7: Update saga estados

-- 2025 sagas with all links synced -> COMPLETADO
UPDATE public.saga_transactions
SET estado = 'COMPLETADO'
WHERE id IN (
  '0371e403-9ef4-4afe-aee0-36f61f578a00',
  '317e0871-b1a3-4fa1-a5b6-c4280d032c63',
  'd57da800-4987-4a71-8ec4-bf3e8aca3ad2',
  'ade1daf2-493c-4b16-b1a0-b93c3e7699ba',
  'daa30369-a87d-4e40-a47a-1c15b4b8b3e3',
  'cb86f2ea-164d-4801-8be0-918f29bde752',
  '1f82f37d-667f-45fa-b131-607c3429661c'
)
AND estado = 'CONFIRMADO';

-- Empty VENTA saga (Visit 2) -> CANCELADA
UPDATE public.saga_transactions
SET estado = 'CANCELADA'
WHERE id = '6e02715c-6f7d-4fc4-9c22-48851fb303ac'
AND estado = 'CONFIRMADO';
