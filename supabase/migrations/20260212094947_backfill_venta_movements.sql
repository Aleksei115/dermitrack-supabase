-- Backfill missing VENTA movements for 10 ODV items ($5,175 total)
-- and update empty saga.items from ventas_odv data

-- Part A: Insert missing VENTA movements
-- For each VENTA saga linked to an ODV via saga_zoho_links,
-- check if movements exist for each ventas_odv item.
-- If not, insert the missing movement.
INSERT INTO public.movimientos_inventario (
  id_saga_transaction,
  id_cliente,
  sku,
  tipo,
  cantidad,
  cantidad_antes,
  cantidad_despues,
  fecha_movimiento,
  precio_unitario
)
SELECT
  szl.id_saga_transaction,
  st.id_cliente,
  vo.sku,
  'VENTA'::tipo_movimiento_botiquin,
  vo.cantidad,
  0,  -- historical, not recalculable
  0,  -- historical, not recalculable
  st.created_at,
  vo.precio
FROM public.saga_zoho_links szl
JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
JOIN public.ventas_odv vo ON vo.odv_id = szl.zoho_id
JOIN public.medicamentos m ON m.sku = vo.sku  -- skip SKUs not in catalog (e.g. P181)
WHERE szl.tipo = 'VENTA'
  AND NOT EXISTS (
    SELECT 1 FROM public.movimientos_inventario mi
    WHERE mi.id_cliente = st.id_cliente
      AND mi.sku = vo.sku
      AND mi.tipo = 'VENTA'
      AND mi.fecha_movimiento::date BETWEEN (st.created_at::date - 1) AND (st.created_at::date + 1)
  );

-- Part B: Update empty saga.items with data from ventas_odv
-- Aggregate by SKU to avoid duplicate-SKU trigger validation
UPDATE public.saga_transactions st
SET items = sub.items_from_odv
FROM (
  SELECT
    agg.id_saga_transaction,
    jsonb_agg(jsonb_build_object('sku', agg.sku, 'cantidad', agg.total_cantidad)) as items_from_odv
  FROM (
    SELECT szl.id_saga_transaction, vo.sku, SUM(vo.cantidad) as total_cantidad
    FROM public.saga_zoho_links szl
    JOIN public.ventas_odv vo ON vo.odv_id = szl.zoho_id
    WHERE szl.tipo = 'VENTA'
    GROUP BY szl.id_saga_transaction, vo.sku
  ) agg
  GROUP BY agg.id_saga_transaction
) sub
WHERE sub.id_saga_transaction = st.id
  AND jsonb_array_length(COALESCE(st.items, '[]'::jsonb)) = 0;
