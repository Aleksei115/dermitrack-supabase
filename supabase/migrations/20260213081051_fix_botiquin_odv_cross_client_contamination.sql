-- Fix botiquin_odv cross-client ODV contamination for MEXPF13496
-- and remove rows with leading spaces in odv_id.
--
-- A bulk import on 2026-01-25 inserted 242 botiquin_odv rows for MEXPF13496,
-- of which 29 ODV IDs (174 rows) belong to sagas of 8 other clients.
-- Additionally, 6 ODV IDs have leading spaces — all 6 also belong to other
-- clients' sagas, so they are deleted rather than trimmed.

-- Part 1: Delete rows with leading spaces in odv_id (all belong to other clients)
DELETE FROM public.botiquin_odv
WHERE odv_id LIKE ' %';

-- Part 2: Delete rows where MEXPF13496 has ODVs that belong to other clients' sagas
DELETE FROM public.botiquin_odv bo
WHERE bo.id_cliente = 'MEXPF13496'
AND EXISTS (
  SELECT 1
  FROM public.saga_zoho_links szl
  JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
  WHERE szl.zoho_id = bo.odv_id
    AND st.id_cliente != 'MEXPF13496'
);
