-- clasificacion_base(): Single source of truth for M1/M2/M3 classification
-- Returns one row per (id_cliente, sku, m_type) with all metadata needed for downstream RPCs
-- M4 (cross-sell) eliminated from the system
--
-- M1: Adopciones — distinct (id_cliente, sku) with VENTA in movimientos_inventario
-- M2: Conversiones — M1 pairs with ODV AFTER first VENTA (strict >), excluding saga ODVs
-- M3: Exposiciones — CREACION sin VENTA, ODV AFTER first CREACION, no prior ODV, excluding saga

CREATE OR REPLACE FUNCTION analytics.clasificacion_base()
RETURNS TABLE(
  id_cliente         varchar,
  nombre_cliente     varchar,
  sku                varchar,
  producto           varchar,
  padecimiento       varchar,
  marca              varchar,
  es_top             boolean,
  m_type             text,
  first_event_date   date,
  revenue_botiquin   numeric,
  revenue_odv        numeric,
  cantidad_odv       numeric,
  num_transacciones_odv bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  WITH
  -- Saga-linked ODV IDs to exclude (billing ODVs created by the visit saga)
  saga_odv_ids AS (
    SELECT DISTINCT szl.zoho_id
    FROM saga_zoho_links szl
    WHERE szl.tipo = 'VENTA'
      AND szl.zoho_id IS NOT NULL
  ),
  -- Padecimiento lookup (1:1 in practice — pick first by id)
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku)
      mp.sku,
      p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  -- M1: All (cliente, sku) pairs with VENTA in botiquín
  m1_pairs AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_venta,
           SUM(mi.cantidad * m.precio) AS revenue_botiquin
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, mi.sku
  ),
  -- M2: M1 pairs that have ODV purchases AFTER first_venta (excluding saga)
  m2_agg AS (
    SELECT mp.id_cliente, mp.sku,
           COALESCE(SUM(v.cantidad * v.precio), 0) AS revenue_odv,
           COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m1_pairs mp
    JOIN ventas_odv v ON v.id_cliente = mp.id_cliente
                      AND v.sku = mp.sku
                      AND v.fecha > mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    GROUP BY mp.id_cliente, mp.sku
    HAVING COALESCE(SUM(v.cantidad * v.precio), 0) > 0
  ),
  -- M3 candidates: CREACION without VENTA for same (cliente, sku)
  m3_candidates AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento::date) AS first_creacion
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'CREACION'
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = mi.id_cliente
          AND mi2.sku = mi.sku
          AND mi2.tipo = 'VENTA'
      )
    GROUP BY mi.id_cliente, mi.sku
  ),
  -- M3: Candidates with ODV AFTER first_creacion, no prior ODV purchases, excluding saga
  m3_agg AS (
    SELECT mc.id_cliente, mc.sku, mc.first_creacion,
           COALESCE(SUM(v.cantidad * v.precio), 0) AS revenue_odv,
           COALESCE(SUM(v.cantidad), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc
    JOIN ventas_odv v ON v.id_cliente = mc.id_cliente
                      AND v.sku = mc.sku
                      AND v.fecha > mc.first_creacion
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
      AND NOT EXISTS (
        SELECT 1 FROM ventas_odv v2
        WHERE v2.id_cliente = mc.id_cliente
          AND v2.sku = mc.sku
          AND v2.fecha <= mc.first_creacion
      )
    GROUP BY mc.id_cliente, mc.sku, mc.first_creacion
    HAVING COALESCE(SUM(v.cantidad * v.precio), 0) > 0
  )

  -- M1 rows
  SELECT
    mp.id_cliente::varchar,
    c.nombre_cliente::varchar,
    mp.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top AS es_top,
    'M1'::text AS m_type,
    mp.first_venta AS first_event_date,
    mp.revenue_botiquin,
    0::numeric AS revenue_odv,
    0::numeric AS cantidad_odv,
    0::bigint AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN clientes c ON c.id_cliente = mp.id_cliente
  JOIN medicamentos m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku

  UNION ALL

  -- M2 rows
  SELECT
    m2.id_cliente::varchar,
    c.nombre_cliente::varchar,
    m2.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top,
    'M2'::text,
    mp.first_venta,
    0::numeric,
    m2.revenue_odv,
    m2.cantidad_odv,
    m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.id_cliente = m2.id_cliente AND mp.sku = m2.sku
  JOIN clientes c ON c.id_cliente = m2.id_cliente
  JOIN medicamentos m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku

  UNION ALL

  -- M3 rows
  SELECT
    m3.id_cliente::varchar,
    c.nombre_cliente::varchar,
    m3.sku::varchar,
    m.producto::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar,
    m.marca::varchar,
    m.top,
    'M3'::text,
    m3.first_creacion,
    0::numeric,
    m3.revenue_odv,
    m3.cantidad_odv,
    m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clientes c ON c.id_cliente = m3.id_cliente
  JOIN medicamentos m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$$;

-- Public wrapper (exposed via PostgREST)
CREATE OR REPLACE FUNCTION public.clasificacion_base()
RETURNS TABLE(
  id_cliente         varchar,
  nombre_cliente     varchar,
  sku                varchar,
  producto           varchar,
  padecimiento       varchar,
  marca              varchar,
  es_top             boolean,
  m_type             text,
  first_event_date   date,
  revenue_botiquin   numeric,
  revenue_odv        numeric,
  cantidad_odv       numeric,
  num_transacciones_odv bigint
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT * FROM analytics.clasificacion_base();
$$;

NOTIFY pgrst, 'reload schema';
