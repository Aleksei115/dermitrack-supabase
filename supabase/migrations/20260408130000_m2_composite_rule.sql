-- ============================================================
-- M2 compuesto: M2a (post-placement, pre-first_venta) + M2b (post-first_venta)
--
-- Contexto:
--   La regla anterior de M2 era estricta: ODV.date >= first_cabinet_sale.
--   Esto dejaba en limbo los ODVs que ocurrían cuando el cabinet ya
--   estaba físicamente instalado en el consultorio (post-placement) pero
--   antes de que se registrara la primera cash-sale del par (pre-first_venta).
--
--   Caso real: Dra. Beatriz Reyes (MEXBR172), SKU V160. Cabinet instalado
--   2025-11-28 (corte 4), primera cabinet SALE registrada 2026-02-04 (corte 5).
--   En medio, la doctora generó 3 ODVs de V160 por ~$19,720 entre enero y
--   marzo, producto del cabinet estando físicamente ahí. Esos ODVs caían
--   en ningún bucket (ni M2 porque pre-first_venta, ni M3 porque el par
--   eventualmente sí tuvo cabinet SALE).
--
-- Nuevo modelo:
--   M2 = M2a ∪ M2b, donde
--     M2a: pares con cabinet SALE, ODV en [first_placement, first_venta)
--     M2b: pares con cabinet SALE, ODV en [first_venta, ∞)
--   Ambos excluyen ODVs linkedadas vía cabinet_sale_odv_ids (no doble-cuenta
--   con M1). La composición se calcula internamente vía dos CTEs explícitos
--   (m2a_agg, m2b_agg) combinados con FULL OUTER JOIN y sumados por par.
--   El output público solo expone m_type='M2' — NO distingue M2a/M2b.
--
--   M3 queda igual: pares con PLACEMENT pero sin cabinet SALE, ODVs
--   post-placement sin ODV prior al placement.
--
--   M2 y M3 siguen siendo mutuamente excluyentes (M2 requiere cabinet SALE,
--   M3 requiere ausencia de cabinet SALE).
--
-- Funciones actualizadas (3 lugares — la lógica estaba duplicada):
--   1. analytics.clasificacion_base()                         — lógica canónica
--   2. analytics.get_dashboard_data() — copia 1 (clasificacion) — dashboard principal
--   3. analytics.get_dashboard_data() — copia 2 (categorized)   — billing composition
-- ============================================================

-- ============================================================
-- 1. analytics.clasificacion_base()
-- ============================================================

CREATE OR REPLACE FUNCTION analytics.clasificacion_base(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(
  client_id character varying,
  client_name character varying,
  sku character varying,
  product character varying,
  condition character varying,
  brand character varying,
  is_top boolean,
  m_type text,
  first_event_date date,
  cabinet_revenue numeric,
  odv_revenue numeric,
  odv_quantity numeric,
  odv_transaction_count bigint
)
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'analytics', 'public'
AS $function$
  WITH
  sale_odv_ids AS (
    SELECT DISTINCT csoi.odv_id AS zoho_id
    FROM cabinet_sale_odv_ids csoi
    WHERE csoi.odv_type = 'SALE'
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp
    JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),

  -- ===================================================================
  -- M1: pares (client, sku) con cabinet SALE en la ventana
  -- ===================================================================
  m1_pairs AS (
    SELECT mi.client_id, mi.sku,
           MIN(mi.movement_date::date) AS first_venta,
           SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS revenue_botiquin
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id, mi.sku
  ),

  -- ===================================================================
  -- first_placement: primera instalación del cabinet para el par.
  -- Se usa para definir la ventana [first_placement, first_venta) de M2a.
  -- Si no hay placement registrado (edge case), se cae a first_venta
  -- → M2a queda vacío para ese par (solo aplica M2b).
  -- ===================================================================
  first_placement AS (
    SELECT mi.client_id, mi.sku,
           MIN(mi.movement_date::date) AS first_placement
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id, mi.sku
  ),
  m1_with_placement AS (
    SELECT mp.client_id, mp.sku, mp.first_venta, mp.revenue_botiquin,
           COALESCE(fp.first_placement, mp.first_venta) AS first_placement
    FROM m1_pairs mp
    LEFT JOIN first_placement fp
      ON fp.client_id = mp.client_id AND fp.sku = mp.sku
  ),

  -- ===================================================================
  -- M2a: ODVs para pares con cabinet SALE, en [first_placement, first_venta)
  -- Conceptualmente: "el cabinet está físicamente ahí, aún no hay
  -- primera cash-sale, pero la doctora ya está generando ventas vía ODV".
  -- ===================================================================
  m2a_agg AS (
    SELECT mp.client_id, mp.sku,
           COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m1_with_placement mp
    JOIN odv_sales v ON v.client_id = mp.client_id AND v.sku = mp.sku
      AND v.date >= mp.first_placement
      AND v.date <  mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM sale_odv_ids)
    GROUP BY mp.client_id, mp.sku
  ),

  -- ===================================================================
  -- M2b: ODVs para pares con cabinet SALE, en [first_venta, ∞)
  -- Conceptualmente: "venta recurrente post-activación del cabinet".
  -- Esta es la definición estricta tradicional de M2.
  -- ===================================================================
  m2b_agg AS (
    SELECT mp.client_id, mp.sku,
           COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m1_with_placement mp
    JOIN odv_sales v ON v.client_id = mp.client_id AND v.sku = mp.sku
      AND v.date >= mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM sale_odv_ids)
    GROUP BY mp.client_id, mp.sku
  ),

  -- ===================================================================
  -- M2 compuesto: suma de M2a + M2b por par (client, sku).
  -- FULL OUTER JOIN captura pares que aparecen en uno, otro, o ambos.
  -- El output solo expone m_type='M2' — los sub-buckets son internos.
  -- ===================================================================
  m2_agg AS (
    SELECT
      COALESCE(a.client_id, b.client_id) AS client_id,
      COALESCE(a.sku, b.sku) AS sku,
      COALESCE(a.revenue_odv, 0) + COALESCE(b.revenue_odv, 0) AS revenue_odv,
      COALESCE(a.cantidad_odv, 0) + COALESCE(b.cantidad_odv, 0) AS cantidad_odv,
      COALESCE(a.num_transacciones_odv, 0) + COALESCE(b.num_transacciones_odv, 0) AS num_transacciones_odv
    FROM m2a_agg a
    FULL OUTER JOIN m2b_agg b
      ON a.client_id = b.client_id AND a.sku = b.sku
    WHERE (COALESCE(a.revenue_odv, 0) + COALESCE(b.revenue_odv, 0)) > 0
  ),

  -- ===================================================================
  -- M3: pares con PLACEMENT pero SIN cabinet SALE (exposición pura).
  -- Excluye pares con ODV previa al placement (ventas pre-programa).
  -- ===================================================================
  m3_candidates AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi
    WHERE mi.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
      AND NOT EXISTS (
        SELECT 1 FROM inventory_movements mi2
        WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
      )
    GROUP BY mi.client_id, mi.sku
  ),
  m3_agg AS (
    SELECT mc.client_id, mc.sku, mc.first_creacion,
           COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
           COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc
    JOIN odv_sales v ON v.client_id = mc.client_id AND v.sku = mc.sku
      AND v.date >= mc.first_creacion
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM sale_odv_ids)
      AND NOT EXISTS (
        SELECT 1 FROM odv_sales v2
        WHERE v2.client_id = mc.client_id AND v2.sku = mc.sku AND v2.date <= mc.first_creacion
      )
    GROUP BY mc.client_id, mc.sku, mc.first_creacion
    HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  )

  -- ===================================================================
  -- UNION: M1 + M2 (compuesto) + M3
  -- ===================================================================
  SELECT mp.client_id::varchar, c.client_name::varchar, mp.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M1'::text,
    mp.first_venta,
    mp.revenue_botiquin, 0::numeric, 0::numeric, 0::bigint
  FROM m1_pairs mp
  JOIN clients c ON c.client_id = mp.client_id
  JOIN medications m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku

  UNION ALL

  SELECT m2.client_id::varchar, c.client_name::varchar, m2.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M2'::text,
    mp.first_venta,
    mp.revenue_botiquin, m2.revenue_odv, m2.cantidad_odv, m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id
  JOIN medications m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku

  UNION ALL

  SELECT m3.client_id::varchar, c.client_name::varchar, m3.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M3'::text,
    m3.first_creacion,
    0::numeric, m3.revenue_odv, m3.cantidad_odv, m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clients c ON c.client_id = m3.client_id
  JOIN medications m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$function$;

COMMENT ON FUNCTION analytics.clasificacion_base(varchar[], varchar[], varchar[], date, date) IS
'Clasifica cada par (client, sku) en M1 (cabinet SALE directo), M2 compuesto, o M3 (exposición). M2 = M2a (ODVs post-placement pre-first_venta) + M2b (ODVs post-first_venta). Los sub-buckets se calculan internamente vía CTEs explícitos pero el output solo expone m_type=M2. M2 y M3 son mutuamente excluyentes. Ver migración 20260408130000.';

-- ============================================================
-- 2. analytics.get_dashboard_data()
-- ============================================================

CREATE OR REPLACE FUNCTION analytics.get_dashboard_data(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS json
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
WITH
sku_padecimiento AS (
  SELECT DISTINCT ON (mp.sku)
    mp.sku, p.name AS padecimiento
  FROM medication_conditions mp
  JOIN conditions p ON p.condition_id = mp.condition_id
  ORDER BY mp.sku, p.condition_id
),
filtered_skus AS (
  SELECT m.sku
  FROM medications m
  LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
  WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
    AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
),
linked_odv_ids AS (
  SELECT DISTINCT szl.odv_id
  FROM cabinet_sale_odv_ids szl
  WHERE szl.odv_type = 'SALE'
    AND szl.odv_id IS NOT NULL
),

-- ===================================================================
-- M1: cabinet SALE pairs
-- ===================================================================
m1_pairs AS (
  SELECT mi.client_id, mi.sku,
         MIN(mi.movement_date::date) AS first_venta,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS revenue_botiquin
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id, mi.sku
),

-- ===================================================================
-- first_placement: usado para definir ventana [first_placement, first_venta)
-- de M2a. NO filtra por el window del dashboard — necesitamos la fecha
-- real de la primera instalación del cabinet, aunque sea previa al window.
-- ===================================================================
first_placement AS (
  SELECT mi.client_id, mi.sku,
         MIN(mi.movement_date::date) AS first_placement
  FROM inventory_movements mi
  WHERE mi.type = 'PLACEMENT'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
  GROUP BY mi.client_id, mi.sku
),
m1_with_placement AS (
  SELECT mp.client_id, mp.sku, mp.first_venta, mp.revenue_botiquin,
         COALESCE(fp.first_placement, mp.first_venta) AS first_placement
  FROM m1_pairs mp
  LEFT JOIN first_placement fp
    ON fp.client_id = mp.client_id AND fp.sku = mp.sku
),

-- ===================================================================
-- M2 compuesto: M2a (pre-first_venta) + M2b (post-first_venta)
-- ===================================================================
m2a_agg AS (
  SELECT mp.client_id, mp.sku,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m1_with_placement mp
  JOIN odv_sales v ON v.client_id = mp.client_id
                   AND v.sku = mp.sku
                   AND v.date >= mp.first_placement
                   AND v.date <  mp.first_venta
  WHERE v.odv_id NOT IN (SELECT odv_id FROM linked_odv_ids)
  GROUP BY mp.client_id, mp.sku
),
m2b_agg AS (
  SELECT mp.client_id, mp.sku,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m1_with_placement mp
  JOIN odv_sales v ON v.client_id = mp.client_id
                   AND v.sku = mp.sku
                   AND v.date >= mp.first_venta
  WHERE v.odv_id NOT IN (SELECT odv_id FROM linked_odv_ids)
  GROUP BY mp.client_id, mp.sku
),
m2_agg AS (
  SELECT
    COALESCE(a.client_id, b.client_id) AS client_id,
    COALESCE(a.sku, b.sku) AS sku,
    COALESCE(a.revenue_odv, 0) + COALESCE(b.revenue_odv, 0) AS revenue_odv,
    COALESCE(a.cantidad_odv, 0) + COALESCE(b.cantidad_odv, 0) AS cantidad_odv,
    COALESCE(a.num_transacciones_odv, 0) + COALESCE(b.num_transacciones_odv, 0) AS num_transacciones_odv
  FROM m2a_agg a
  FULL OUTER JOIN m2b_agg b
    ON a.client_id = b.client_id AND a.sku = b.sku
  WHERE (COALESCE(a.revenue_odv, 0) + COALESCE(b.revenue_odv, 0)) > 0
),

-- ===================================================================
-- M3: exposición sin cabinet SALE
-- ===================================================================
m3_candidates AS (
  SELECT mi.client_id, mi.sku,
         MIN(mi.movement_date::date) AS first_creacion
  FROM inventory_movements mi
  WHERE mi.type = 'PLACEMENT'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    AND NOT EXISTS (
      SELECT 1 FROM inventory_movements mi2
      WHERE mi2.client_id = mi.client_id
        AND mi2.sku = mi.sku
        AND mi2.type = 'SALE'
    )
  GROUP BY mi.client_id, mi.sku
),
m3_agg AS (
  SELECT mc.client_id, mc.sku, mc.first_creacion,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m3_candidates mc
  JOIN odv_sales v ON v.client_id = mc.client_id
                    AND v.sku = mc.sku
                    AND v.date >= mc.first_creacion
  WHERE v.odv_id NOT IN (SELECT odv_id FROM linked_odv_ids)
    AND NOT EXISTS (
      SELECT 1 FROM odv_sales v2
      WHERE v2.client_id = mc.client_id
        AND v2.sku = mc.sku
        AND v2.date <= mc.first_creacion
    )
  GROUP BY mc.client_id, mc.sku, mc.first_creacion
  HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
),

clasificacion AS (
  SELECT
    mp.client_id, c.client_name, mp.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS') AS padecimiento,
    m.brand, m.top AS es_top,
    'M1'::text AS m_type,
    mp.first_venta AS first_event_date,
    mp.revenue_botiquin,
    0::numeric AS revenue_odv,
    0::numeric AS cantidad_odv,
    0::bigint AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN clients c ON c.client_id = mp.client_id
  JOIN medications m ON m.sku = mp.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku

  UNION ALL

  SELECT
    m2.client_id, c.client_name, m2.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.brand, m.top,
    'M2'::text,
    mp.first_venta,
    mp.revenue_botiquin,
    m2.revenue_odv,
    m2.cantidad_odv,
    m2.num_transacciones_odv
  FROM m2_agg m2
  JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id
  JOIN medications m ON m.sku = m2.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku

  UNION ALL

  SELECT
    m3.client_id, c.client_name, m3.sku, m.product,
    COALESCE(sp.padecimiento, 'OTROS'),
    m.brand, m.top,
    'M3'::text,
    m3.first_creacion,
    0::numeric,
    m3.revenue_odv,
    m3.cantidad_odv,
    m3.num_transacciones_odv
  FROM m3_agg m3
  JOIN clients c ON c.client_id = m3.client_id
  JOIN medications m ON m.sku = m3.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku
),
clasificacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT client_id, client_name, sku, product,
           padecimiento AS condition,
           brand, es_top AS is_top, m_type, first_event_date,
           revenue_botiquin AS cabinet_revenue,
           revenue_odv AS odv_revenue,
           cantidad_odv AS odv_quantity,
           num_transacciones_odv AS odv_transaction_count
    FROM clasificacion
  ) r
),
impacto_m1 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_botiquin), 0) AS rev
  FROM clasificacion WHERE m_type = 'M1'
),
impacto_m2 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M2'
),
impacto_m3 AS (
  SELECT COUNT(*)::int AS cnt, COALESCE(SUM(revenue_odv), 0) AS rev
  FROM clasificacion WHERE m_type = 'M3'
),
total_odv AS (
  SELECT COALESCE(SUM(v.quantity * v.price), 0) AS rev
  FROM odv_sales v
  WHERE (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR v.date >= p_start_date)
    AND (p_end_date IS NULL OR v.date <= p_end_date)
),
impacto_json AS (
  SELECT row_to_json(r) AS val
  FROM (
    SELECT m1.cnt AS adoptions, m1.rev AS revenue_adoptions,
           m2.cnt AS conversions, m2.rev AS revenue_conversions,
           m3.cnt AS exposures, m3.rev AS revenue_exposures,
           0::int AS crosssell_pairs, 0::numeric AS revenue_crosssell,
           (m1.rev + m2.rev + m3.rev) AS total_impact_revenue,
           t.rev AS total_odv_revenue,
           CASE WHEN t.rev > 0
             THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1)
             ELSE 0 END AS impact_percentage
    FROM impacto_m1 m1, impacto_m2 m2, impacto_m3 m3, total_odv t
  ) r
),
movements AS (
  SELECT
    mi.client_id, mi.sku,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::bigint           AS venta_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0)       AS venta_valor,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::bigint         AS creacion_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'PLACEMENT'), 0)    AS creacion_valor,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::bigint      AS recoleccion_pz,
    COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'COLLECTION'), 0) AS recoleccion_valor
  FROM inventory_movements mi
  JOIN medications med ON med.sku = mi.sku
  WHERE (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id, mi.sku
),
m2_counts AS (
  SELECT cb.client_id, cb.sku,
    COUNT(*)::bigint AS conversiones_m2,
    COALESCE(SUM(cb.revenue_odv), 0) AS revenue_m2
  FROM clasificacion cb
  WHERE cb.m_type = 'M2'
  GROUP BY cb.client_id, cb.sku
),
market_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      mv.client_id, mv.sku, med.product, med.brand,
      COALESCE(sp.padecimiento, 'OTROS') AS condition, med.top AS is_top,
      mv.venta_pz AS sale_pieces, mv.venta_valor AS sale_value,
      mv.creacion_pz AS placement_pieces, mv.creacion_valor AS placement_value,
      mv.recoleccion_pz AS collection_pieces, mv.recoleccion_valor AS collection_value,
      COALESCE(ib.available_quantity, 0)::bigint AS active_stock_pieces,
      COALESCE(m2c.conversiones_m2, 0)::bigint AS m2_conversions,
      COALESCE(m2c.revenue_m2, 0)::numeric AS m2_revenue
    FROM movements mv
    JOIN medications med ON med.sku = mv.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
    LEFT JOIN m2_counts m2c ON m2c.client_id = mv.client_id AND m2c.sku = mv.sku
    LEFT JOIN cabinet_inventory ib ON ib.client_id = mv.client_id AND ib.sku = mv.sku
  ) r
),
conversion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r."generatedValue" DESC), '[]'::json) AS val
  FROM (
    SELECT
      b.m_type AS "mType",
      b.client_id AS "clientId",
      b.client_name AS "clientName",
      b.sku,
      b.product,
      b.first_event_date AS "cabinetDate",
      odv_first.first_odv AS "firstOdvDate",
      (odv_first.first_odv - b.first_event_date)::int AS "conversionDays",
      b.num_transacciones_odv AS "odvSaleCount",
      b.cantidad_odv::bigint AS "totalPieces",
      b.revenue_odv AS "generatedValue",
      b.revenue_botiquin AS "cabinetValue"
    FROM clasificacion b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_odv
      FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku
        AND v.date >= b.first_event_date
        AND v.odv_id NOT IN (SELECT odv_id FROM linked_odv_ids)
    ) odv_first ON true
    WHERE b.m_type IN ('M2', 'M3')
  ) r
),
m1_odv_ids AS (
  SELECT DISTINCT szl.odv_id AS odv_id, szl.client_id
  FROM cabinet_sale_odv_ids szl
  WHERE szl.odv_type = 'SALE'
    AND szl.odv_id IS NOT NULL
),
m1_impacto AS (
  SELECT mi.client_id,
    SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
    SUM(mi.quantity) AS m1_piezas,
    COUNT(DISTINCT mi.sku) AS m1_skus
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY mi.client_id
),
first_venta AS (
  SELECT client_id, sku, MIN(movement_date::date) AS first_venta
  FROM inventory_movements
  WHERE type = 'SALE'
  GROUP BY client_id, sku
),
-- first_placement_fact: usado en categorized para clasificación de ODVs sueltas.
-- Captura la fecha de primer placement por par (sin filtro de window).
first_placement_fact AS (
  SELECT client_id, sku, MIN(movement_date::date) AS first_placement
  FROM inventory_movements
  WHERE type = 'PLACEMENT'
  GROUP BY client_id, sku
),
first_creacion AS (
  SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
  FROM inventory_movements mi
  WHERE mi.type = 'PLACEMENT'
    AND NOT EXISTS (
      SELECT 1 FROM inventory_movements mi2
      WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE'
    )
  GROUP BY mi.client_id, mi.sku
),
prior_odv AS (
  SELECT DISTINCT v.client_id, v.sku
  FROM odv_sales v
  JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
  WHERE v.date <= fc.first_creacion
),

-- ===================================================================
-- categorized (billing composition): clasifica ODVs individuales para
-- la composición de facturación por cliente. M2 aquí también compuesto:
-- cualquier ODV post-placement (≥ first_placement) para un par que
-- eventualmente tiene cabinet SALE cuenta como M2, sin importar si es
-- pre o post first_venta.
-- ===================================================================
categorized AS (
  SELECT
    v.client_id, v.sku, v.date, v.quantity,
    v.quantity * v.price AS line_total,
    CASE
      WHEN m1o.odv_id IS NOT NULL THEN 'M1'
      WHEN fv.sku IS NOT NULL
           AND fp.first_placement IS NOT NULL
           AND v.date >= fp.first_placement THEN 'M2'
      WHEN fv.sku IS NOT NULL
           AND fp.first_placement IS NULL
           AND v.date >= fv.first_venta THEN 'M2'
      WHEN fc.sku IS NOT NULL AND v.date >= fc.first_creacion AND po.sku IS NULL THEN 'M3'
      ELSE 'UNLINKED'
    END AS categoria
  FROM odv_sales v
  LEFT JOIN m1_odv_ids m1o ON v.odv_id = m1o.odv_id AND v.client_id = m1o.client_id
  LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
  LEFT JOIN first_placement_fact fp ON v.client_id = fp.client_id AND v.sku = fp.sku
  LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
  LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
  WHERE v.price > 0
    AND v.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR v.date >= p_start_date)
    AND (p_end_date IS NULL OR v.date <= p_end_date)
),
fact_totals AS (
  SELECT
    client_id,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M1'), 0) AS m1_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M2'), 0) AS m2_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'M3'), 0) AS m3_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria = 'UNLINKED'), 0) AS unlinked_total,
    COALESCE(SUM(line_total), 0) AS grand_total,
    COALESCE(SUM(line_total) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_valor,
    COALESCE(SUM(quantity) FILTER (WHERE categoria IN ('M2','M3')), 0) AS m2m3_piezas,
    COUNT(DISTINCT sku) FILTER (WHERE categoria IN ('M2','M3')) AS m2m3_skus
  FROM categorized
  GROUP BY client_id
),
facturacion_json AS (
  SELECT COALESCE(json_agg(row_to_json(r) ORDER BY r.diff DESC), '[]'::json) AS val
  FROM (
    SELECT
      c.client_id, c.client_name, c.current_tier,
      c.tier AS previous_tier,
      c.active,
      COALESCE(c.avg_billing, 0)::numeric AS baseline,
      COALESCE(c.current_billing, 0)::numeric AS current_billing,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m1_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m1,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m2_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m2,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.m3_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_m3,
      CASE WHEN t.grand_total > 0
        THEN ROUND((COALESCE(c.current_billing, 0) * t.unlinked_total / t.grand_total)::numeric, 2)
        ELSE 0 END AS current_unlinked,
      CASE WHEN COALESCE(c.avg_billing, 0) > 0
        THEN ROUND(((COALESCE(c.current_billing, 0) - c.avg_billing) / c.avg_billing * 100)::numeric, 1)
        ELSE NULL END AS growth_pct,
      CASE WHEN t.grand_total > 0
        THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
        ELSE 0 END AS linked_pct,
      (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS linked_value,
      (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS linked_pieces,
      (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS linked_skus,
      (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) AS diff
    FROM clients c
    LEFT JOIN fact_totals t ON c.client_id = t.client_id
    LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
    WHERE c.current_tier IS NOT NULL
      AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ) r
),
sankey_json AS (
  SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) AS val
  FROM (
    SELECT
      b.client_id, b.client_name, b.sku, b.product,
      b.m_type AS category,
      b.revenue_odv AS odv_value,
      b.cantidad_odv AS odv_quantity,
      b.num_transacciones_odv AS transaction_count
    FROM clasificacion b
    WHERE b.m_type IN ('M2', 'M3')
  ) r
)

SELECT json_build_object(
  'classificationBase', c.val,
  'impactSummary', i.val,
  'marketAnalysis', m.val,
  'conversionDetails', cv.val,
  'billingComposition', f.val,
  'sankeyFlows', s.val
)
FROM clasificacion_json c, impacto_json i, market_json m,
     conversion_json cv, facturacion_json f, sankey_json s;
$function$;

COMMENT ON FUNCTION analytics.get_dashboard_data(varchar[], varchar[], varchar[], date, date) IS
'RPC consolidado del dashboard. Aplica el modelo M2 compuesto (M2a=post-placement/pre-first_venta + M2b=post-first_venta) en los tres lugares donde se clasifica: (1) clasificacion CTE para classificationBase/impactSummary/sankey/conversion/market, (2) categorized CTE para billingComposition. Ver migración 20260408130000.';

NOTIFY pgrst, 'reload schema';
