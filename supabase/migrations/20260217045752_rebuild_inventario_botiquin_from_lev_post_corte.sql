-- Rebuild inventario_botiquin from LEV_POST_CORTE sagas (28 Jan - 7 Feb 2026)
-- Fix: previous sync incorrectly included PERMANENCIA movements, inflating quantities.
-- Correct source of truth is the physical count from levantamientos post-corte.

DELETE FROM inventario_botiquin;

INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
SELECT
  st.id_cliente,
  (item->>'sku')::varchar,
  (item->>'cantidad')::integer,
  st.created_at
FROM saga_transactions st,
  jsonb_array_elements(st.items) AS item
WHERE st.tipo::text = 'LEV_POST_CORTE'
  AND st.created_at >= '2026-01-28'
  AND st.created_at < '2026-02-08'
ON CONFLICT (id_cliente, sku) DO UPDATE
SET cantidad_disponible = EXCLUDED.cantidad_disponible,
    ultima_actualizacion = EXCLUDED.ultima_actualizacion;
