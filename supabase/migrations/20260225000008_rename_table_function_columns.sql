-- ============================================================================
-- PHASE 8: Rename RETURNS TABLE columns from Spanish to English
-- ============================================================================
-- For TABLE-returning functions: only change RETURNS TABLE column names,
-- keeping function bodies exactly the same (PostgreSQL matches by position).
-- For JSON-returning functions: update references to renamed columns.
-- ============================================================================

BEGIN;

-- ============================================================================
-- PART A: TABLE-returning functions - rename columns
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. clasificacion_base() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.clasificacion_base(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.clasificacion_base(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, "condition" character varying, brand character varying, is_top boolean, m_type text, first_event_date date, cabinet_revenue numeric, odv_revenue numeric, odv_quantity numeric, odv_transaction_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  saga_odv_ids AS (
    SELECT DISTINCT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
    FROM medication_conditions mp JOIN conditions p ON p.condition_id = mp.condition_id
    ORDER BY mp.sku, p.condition_id
  ),
  filtered_skus AS (
    SELECT m.sku FROM medications m LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_brands IS NULL OR m.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  m1_pairs AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_venta,
           SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS revenue_botiquin
    FROM inventory_movements mi JOIN medications m ON m.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY mi.client_id, mi.sku
  ),
  m2_agg AS (
    SELECT mp.client_id, mp.sku, COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv, COUNT(v.*) AS num_transacciones_odv
    FROM m1_pairs mp JOIN odv_sales v ON v.client_id = mp.client_id AND v.sku = mp.sku AND v.date >= mp.first_venta
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    GROUP BY mp.client_id, mp.sku HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  ),
  m3_candidates AS (
    SELECT mi.client_id, mi.sku, MIN(mi.movement_date::date) AS first_creacion
    FROM inventory_movements mi WHERE mi.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
      AND NOT EXISTS (SELECT 1 FROM inventory_movements mi2 WHERE mi2.client_id = mi.client_id AND mi2.sku = mi.sku AND mi2.type = 'SALE')
    GROUP BY mi.client_id, mi.sku
  ),
  m3_agg AS (
    SELECT mc.client_id, mc.sku, mc.first_creacion, COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
           COALESCE(SUM(v.quantity), 0) AS cantidad_odv, COUNT(v.*) AS num_transacciones_odv
    FROM m3_candidates mc JOIN odv_sales v ON v.client_id = mc.client_id AND v.sku = mc.sku AND v.date >= mc.first_creacion
    WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
      AND NOT EXISTS (SELECT 1 FROM odv_sales v2 WHERE v2.client_id = mc.client_id AND v2.sku = mc.sku AND v2.date <= mc.first_creacion)
    GROUP BY mc.client_id, mc.sku, mc.first_creacion HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
  )
  SELECT mp.client_id::varchar, c.client_name::varchar, mp.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M1'::text, mp.first_venta,
    mp.revenue_botiquin, 0::numeric, 0::numeric, 0::bigint
  FROM m1_pairs mp JOIN clients c ON c.client_id = mp.client_id JOIN medications m ON m.sku = mp.sku LEFT JOIN sku_padecimiento sp ON sp.sku = mp.sku
  UNION ALL
  SELECT m2.client_id::varchar, c.client_name::varchar, m2.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M2'::text, mp.first_venta,
    mp.revenue_botiquin, m2.revenue_odv, m2.cantidad_odv, m2.num_transacciones_odv
  FROM m2_agg m2 JOIN m1_pairs mp ON mp.client_id = m2.client_id AND mp.sku = m2.sku
  JOIN clients c ON c.client_id = m2.client_id JOIN medications m ON m.sku = m2.sku LEFT JOIN sku_padecimiento sp ON sp.sku = m2.sku
  UNION ALL
  SELECT m3.client_id::varchar, c.client_name::varchar, m3.sku::varchar, m.product::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, m.brand::varchar, m.top, 'M3'::text, m3.first_creacion,
    0::numeric, m3.revenue_odv, m3.cantidad_odv, m3.num_transacciones_odv
  FROM m3_agg m3 JOIN clients c ON c.client_id = m3.client_id JOIN medications m ON m.sku = m3.sku LEFT JOIN sku_padecimiento sp ON sp.sku = m3.sku;
$function$;

DROP FUNCTION IF EXISTS public.clasificacion_base(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.clasificacion_base(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, "condition" character varying, brand character varying, is_top boolean, m_type text, first_event_date date, cabinet_revenue numeric, odv_revenue numeric, odv_quantity numeric, odv_transaction_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 2. get_cabinet_impact_summary() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cabinet_impact_summary(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cabinet_impact_summary(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(adoptions integer, revenue_adoptions numeric, conversions integer, revenue_conversions numeric, exposures integer, revenue_exposures numeric, crosssell_pairs integer, revenue_crosssell numeric, total_impact_revenue numeric, total_odv_revenue numeric, impact_percentage numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
  sku_padecimiento AS (SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento FROM medication_conditions mp JOIN conditions p ON p.condition_id = mp.condition_id ORDER BY mp.sku, p.condition_id),
  filtered_skus AS (SELECT m.sku FROM medications m LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku WHERE (p_brands IS NULL OR m.brand = ANY(p_brands)) AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))),
  base AS (SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date)),
  m1 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(cabinet_revenue), 0) AS rev FROM base WHERE m_type = 'M1'),
  m2 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(odv_revenue), 0) AS rev FROM base WHERE m_type = 'M2'),
  m3 AS (SELECT COUNT(*)::int AS cnt, COALESCE(SUM(odv_revenue), 0) AS rev FROM base WHERE m_type = 'M3'),
  total_odv AS (SELECT COALESCE(SUM(v.quantity * v.price), 0) AS rev FROM odv_sales v WHERE (p_doctors IS NULL OR v.client_id = ANY(p_doctors)) AND v.sku IN (SELECT sku FROM filtered_skus) AND (p_start_date IS NULL OR v.date >= p_start_date) AND (p_end_date IS NULL OR v.date <= p_end_date))
  SELECT m1.cnt, m1.rev, m2.cnt, m2.rev, m3.cnt, m3.rev, 0::int, 0::numeric,
    (m1.rev + m2.rev + m3.rev), t.rev,
    CASE WHEN t.rev > 0 THEN ROUND(((m1.rev + m2.rev + m3.rev) / t.rev) * 100, 1) ELSE 0 END
  FROM m1, m2, m3, total_odv t;
$function$;

DROP FUNCTION IF EXISTS public.get_cabinet_impact_summary(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_cabinet_impact_summary(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(adoptions integer, revenue_adoptions numeric, conversions integer, revenue_conversions numeric, exposures integer, revenue_exposures numeric, crosssell_pairs integer, revenue_crosssell numeric, total_impact_revenue numeric, total_odv_revenue numeric, impact_percentage numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_cabinet_impact_summary(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 3. get_impact_detail() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_impact_detail(text, character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_impact_detail(p_metric text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, quantity integer, price numeric, value numeric, date date, detail text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_metric = 'M1' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           CASE WHEN avg_pu.avg_precio > 0
             THEN ROUND(b.cabinet_revenue / avg_pu.avg_precio)::int
             ELSE 0
           END AS quantity,
           COALESCE(avg_pu.avg_precio, 0) AS price,
           b.cabinet_revenue AS valor,
           b.first_event_date AS date,
           'Adopcion en botiquin'::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    LEFT JOIN LATERAL (
      SELECT CASE WHEN SUM(mi.quantity) > 0
        THEN SUM(mi.quantity * COALESCE(mi.unit_price, 0)) / SUM(mi.quantity)
        ELSE 0 END AS avg_precio
      FROM inventory_movements mi
      WHERE mi.client_id = b.client_id AND mi.sku = b.sku AND mi.type = 'SALE'
    ) avg_pu ON true
    WHERE b.m_type = 'M1'
    ORDER BY b.cabinet_revenue DESC;

  ELSIF p_metric = 'M2' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           b.odv_quantity::int AS quantity,
           ROUND(b.odv_revenue / NULLIF(b.odv_quantity, 0), 2) AS price,
           b.odv_revenue AS valor,
           odv_first.first_fecha AS date,
           ('ODV despues de botiquin (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_fecha FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date > b.first_event_date
        AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
    ) odv_first ON true
    WHERE b.m_type = 'M2'
    ORDER BY b.odv_revenue DESC;

  ELSIF p_metric = 'M3' THEN
    RETURN QUERY
    SELECT b.client_id, b.client_name, b.sku, b.product,
           b.odv_quantity::int AS quantity,
           ROUND(b.odv_revenue / NULLIF(b.odv_quantity, 0), 2) AS price,
           b.odv_revenue AS valor,
           odv_first.first_fecha AS date,
           ('Exposicion post-botiquin (' || b.first_event_date::text || ')')::text AS detalle
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    JOIN LATERAL (
      SELECT MIN(v.date) AS first_fecha FROM odv_sales v
      WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date > b.first_event_date
        AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
    ) odv_first ON true
    WHERE b.m_type = 'M3'
    ORDER BY b.odv_revenue DESC;

  ELSIF p_metric = 'M4' THEN
    RETURN;

  ELSE
    RAISE EXCEPTION 'Invalid metric: %. Use M1, M2, M3 or M4.', p_metric;
  END IF;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_impact_detail(text, character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_impact_detail(p_metric text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku character varying, product character varying, quantity integer, price numeric, value numeric, date date, detail text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_impact_detail(p_metric, p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 4. get_brand_performance() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_brand_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_brand_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(brand character varying, value numeric, pieces integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
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
  )
  SELECT m.brand,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor,
         SUM(mi.quantity)::int AS piezas
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY m.brand
  ORDER BY valor DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_brand_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_brand_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(brand character varying, value numeric, pieces integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_brand_performance(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 5. get_condition_performance() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_condition_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_condition_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, value numeric, pieces integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
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
  )
  SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
         SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor,
         SUM(mi.quantity)::int AS piezas
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
  WHERE mi.type = 'SALE'
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ORDER BY valor DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_condition_performance(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_condition_performance(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, value numeric, pieces integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_condition_performance(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 6. get_product_interest() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_product_interest(integer, character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_product_interest(p_limit integer DEFAULT 15, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(product character varying, sale integer, placement integer, collection integer, active_stock integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
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
  )
  SELECT
    CASE WHEN LENGTH(m.product) > 20 THEN LEFT(m.product, 20) || '...' ELSE m.product END AS product,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::int AS venta,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::int AS creacion,
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::int AS recoleccion,
    GREATEST(0,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)
    )::int AS stock_activo
  FROM inventory_movements mi
  JOIN medications m ON m.sku = mi.sku
  WHERE mi.type IN ('SALE', 'PLACEMENT', 'COLLECTION')
    AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
    AND mi.sku IN (SELECT sku FROM filtered_skus)
    AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
    AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
  GROUP BY m.product
  ORDER BY (
    COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0) +
    GREATEST(0,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)
      - COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)
    )
  ) DESC
  LIMIT p_limit;
$function$;

DROP FUNCTION IF EXISTS public.get_product_interest(integer, character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_product_interest(p_limit integer DEFAULT 15, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(product character varying, sale integer, placement integer, collection integer, active_stock integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_product_interest(p_limit, p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 7. get_opportunity_matrix() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_opportunity_matrix(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, sale integer, collection integer, valor numeric, converted_qty integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
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
  botiquin_by_pad AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::int AS venta,
           COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::int AS recoleccion,
           COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0) AS valor
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.type IN ('SALE', 'COLLECTION')
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  ),
  converted AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           COALESCE(SUM(v.quantity), 0)::int AS converted_qty
    FROM (
      SELECT mi.client_id, mi.sku,
             MIN(mi.movement_date::date) AS first_venta
      FROM inventory_movements mi
      WHERE mi.type = 'SALE'
        AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
        AND mi.sku IN (SELECT sku FROM filtered_skus)
      GROUP BY mi.client_id, mi.sku
    ) bv
    JOIN odv_sales v ON v.client_id = bv.client_id AND v.sku = bv.sku AND v.date > bv.first_venta
    LEFT JOIN sku_padecimiento sp ON sp.sku = bv.sku
    GROUP BY COALESCE(sp.padecimiento, 'OTROS')
  )
  SELECT bp.padecimiento, bp.venta, bp.recoleccion, bp.valor,
         COALESCE(cv.converted_qty, 0)::int AS converted_qty
  FROM botiquin_by_pad bp
  LEFT JOIN converted cv ON cv.padecimiento = bp.padecimiento
  ORDER BY bp.valor DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_opportunity_matrix(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_opportunity_matrix(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, sale integer, collection integer, valor numeric, converted_qty integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_opportunity_matrix(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 8. get_yoy_padecimiento() - analytics + public (rename columns only)
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_yoy_padecimiento(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, "year" integer, valor numeric, growth numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH
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
  yearly AS (
    SELECT COALESCE(sp.padecimiento, 'OTROS')::varchar AS padecimiento,
           EXTRACT(YEAR FROM mi.movement_date)::int AS anio,
           SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS valor
    FROM inventory_movements mi
    JOIN medications m ON m.sku = mi.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = mi.sku
    WHERE mi.type = 'SALE'
      AND (p_doctors IS NULL OR mi.client_id = ANY(p_doctors))
      AND mi.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR mi.movement_date::date >= p_start_date)
      AND (p_end_date IS NULL OR mi.movement_date::date <= p_end_date)
    GROUP BY COALESCE(sp.padecimiento, 'OTROS'), EXTRACT(YEAR FROM mi.movement_date)::int
  ),
  with_prev AS (
    SELECT y.padecimiento, y.anio, y.valor,
           LAG(y.valor) OVER (PARTITION BY y.padecimiento ORDER BY y.anio) AS prev_valor
    FROM yearly y
  )
  SELECT wp.padecimiento, wp.anio, wp.valor,
         CASE WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor > 0
              THEN ROUND(((wp.valor - wp.prev_valor) / wp.prev_valor) * 100)
              WHEN wp.prev_valor IS NOT NULL AND wp.prev_valor = 0 AND wp.valor > 0
              THEN 100::numeric
              ELSE NULL
         END AS crecimiento
  FROM with_prev wp
  ORDER BY wp.padecimiento, wp.anio;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_yoy_padecimiento(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE("condition" character varying, "year" integer, valor numeric, growth numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_yoy_padecimiento(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 9. get_market_analysis() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_market_analysis(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_market_analysis(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, sku character varying, product character varying, brand character varying, "condition" character varying, is_top boolean, sale_pieces bigint, sale_value numeric, placement_pieces bigint, placement_value numeric, collection_pieces bigint, collection_value numeric, active_stock_pieces bigint, m2_conversions bigint, m2_revenue numeric)
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
  movements AS (
    SELECT
      mi.client_id, mi.sku,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'SALE'), 0)::bigint             AS venta_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'SALE'), 0)         AS venta_valor,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'PLACEMENT'), 0)::bigint           AS creacion_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'PLACEMENT'), 0)      AS creacion_valor,
      COALESCE(SUM(mi.quantity) FILTER (WHERE mi.type = 'COLLECTION'), 0)::bigint        AS recoleccion_pz,
      COALESCE(SUM(mi.quantity * COALESCE(mi.unit_price, 0)) FILTER (WHERE mi.type = 'COLLECTION'), 0)   AS recoleccion_valor
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
      COALESCE(SUM(cb.odv_revenue), 0) AS revenue_m2
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) cb
    WHERE cb.m_type = 'M2'
    GROUP BY cb.client_id, cb.sku
  )
  SELECT
    mv.client_id::varchar, mv.sku::varchar, med.product::varchar, med.brand::varchar,
    COALESCE(sp.padecimiento, 'OTROS')::varchar, med.top AS es_top,
    mv.venta_pz, mv.venta_valor,
    mv.creacion_pz, mv.creacion_valor,
    mv.recoleccion_pz, mv.recoleccion_valor,
    COALESCE(ib.available_quantity, 0)::bigint AS stock_activo_pz,
    COALESCE(m2.conversiones_m2, 0)::bigint,
    COALESCE(m2.revenue_m2, 0)::numeric
  FROM movements mv
  JOIN medications med ON med.sku = mv.sku
  LEFT JOIN sku_padecimiento sp ON sp.sku = mv.sku
  LEFT JOIN m2_counts m2 ON m2.client_id = mv.client_id AND m2.sku = mv.sku
  LEFT JOIN cabinet_inventory ib ON ib.client_id = mv.client_id AND ib.sku = mv.sku;
$function$;

DROP FUNCTION IF EXISTS public.get_market_analysis(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_market_analysis(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, sku character varying, product character varying, brand character varying, "condition" character varying, is_top boolean, sale_pieces bigint, sale_value numeric, placement_pieces bigint, placement_value numeric, collection_pieces bigint, collection_value numeric, active_stock_pieces bigint, m2_conversions bigint, m2_revenue numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_market_analysis(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 10. get_billing_composition() - filtered + legacy + public wrappers
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_billing_composition(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_billing_composition(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, previous_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, growth_pct numeric, linked_pct numeric, linked_value numeric, linked_pieces bigint, linked_skus bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH
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
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
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
  categorized AS (
    SELECT
      v.client_id,
      v.sku,
      v.date,
      v.quantity,
      v.quantity * v.price AS line_total,
      CASE
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM odv_sales v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.client_id = m1.client_id
    LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
    WHERE v.price > 0
      AND v.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR v.date >= p_start_date)
      AND (p_end_date IS NULL OR v.date <= p_end_date)
  ),
  totals AS (
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
  )
  SELECT
    c.client_id,
    c.client_name,
    c.current_tier,
    c.tier AS rango_anterior,
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
      ELSE NULL END AS pct_crecimiento,
    CASE WHEN t.grand_total > 0
      THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
      ELSE 0 END AS pct_vinculado,
    (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
    (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clients c
  LEFT JOIN totals t ON c.client_id = t.client_id
  LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
  WHERE c.current_tier IS NOT NULL
    AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ORDER BY (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_billing_composition(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_billing_composition(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, previous_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, growth_pct numeric, linked_pct numeric, linked_value numeric, linked_pieces bigint, linked_skus bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_billing_composition(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$;

-- Legacy (no-args) version
DROP FUNCTION IF EXISTS analytics.get_billing_composition_legacy() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_billing_composition_legacy()
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, growth_pct numeric, linked_pct numeric, linked_value numeric, linked_pieces bigint, linked_skus bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
  WITH
  m1_odv_ids AS (
    SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON szl.id_saga_transaction = st.id
    WHERE szl.type = 'SALE'
      AND szl.zoho_id IS NOT NULL
  ),
  m1_impacto AS (
    SELECT mi.client_id,
      SUM(mi.quantity * COALESCE(mi.unit_price, 0)) AS m1_valor,
      SUM(mi.quantity) AS m1_piezas,
      COUNT(DISTINCT mi.sku) AS m1_skus
    FROM inventory_movements mi
    WHERE mi.type = 'SALE'
    GROUP BY mi.client_id
  ),
  first_venta AS (
    SELECT client_id, sku, MIN(movement_date::date) AS first_venta
    FROM inventory_movements
    WHERE type = 'SALE'
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
  categorized AS (
    SELECT
      v.client_id,
      v.sku,
      v.date,
      v.quantity,
      v.quantity * v.price AS line_total,
      CASE
        WHEN m1.odv_id IS NOT NULL THEN 'M1'
        WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
        WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
        ELSE 'UNLINKED'
      END AS categoria
    FROM odv_sales v
    LEFT JOIN m1_odv_ids m1 ON v.odv_id = m1.odv_id AND v.client_id = m1.client_id
    LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
    LEFT JOIN first_creacion fc ON v.client_id = fc.client_id AND v.sku = fc.sku
    LEFT JOIN prior_odv po ON v.client_id = po.client_id AND v.sku = po.sku
    WHERE v.price > 0
  ),
  totals AS (
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
  )
  SELECT
    c.client_id,
    c.client_name,
    c.current_tier,
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
      ELSE NULL END AS pct_crecimiento,
    CASE WHEN t.grand_total > 0
      THEN ROUND(((t.m1_total + t.m2_total + t.m3_total) / t.grand_total * 100)::numeric, 1)
      ELSE 0 END AS pct_vinculado,
    (COALESCE(m1i.m1_valor, 0) + COALESCE(t.m2m3_valor, 0))::numeric AS valor_vinculado,
    (COALESCE(m1i.m1_piezas, 0) + COALESCE(t.m2m3_piezas, 0))::bigint AS piezas_vinculadas,
    (COALESCE(m1i.m1_skus, 0) + COALESCE(t.m2m3_skus, 0))::bigint AS skus_vinculados
  FROM clients c
  LEFT JOIN totals t ON c.client_id = t.client_id
  LEFT JOIN m1_impacto m1i ON c.client_id = m1i.client_id
  WHERE c.current_tier IS NOT NULL
  ORDER BY (COALESCE(c.current_billing, 0) - COALESCE(c.avg_billing, 0)) DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_billing_composition() CASCADE;
CREATE OR REPLACE FUNCTION public.get_billing_composition()
 RETURNS TABLE(client_id character varying, client_name character varying, current_tier character varying, active boolean, baseline numeric, current_billing numeric, current_m1 numeric, current_m2 numeric, current_m3 numeric, current_unlinked numeric, growth_pct numeric, linked_pct numeric, linked_value numeric, linked_pieces bigint, linked_skus bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_billing_composition_legacy(); $function$;

-- ---------------------------------------------------------------------------
-- 11. get_sankey_conversion_flows() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_sankey_conversion_flows(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_sankey_conversion_flows(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku text, product text, category text, odv_value numeric, odv_quantity numeric, transaction_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT b.client_id::varchar, b.client_name::varchar, b.sku::text, b.product::text,
    b.m_type::text AS categoria, b.odv_revenue AS valor_odv, b.odv_quantity, b.odv_transaction_count AS num_transacciones
  FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
  WHERE b.m_type IN ('M2', 'M3');
$function$;

DROP FUNCTION IF EXISTS public.get_sankey_conversion_flows(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_sankey_conversion_flows(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(client_id character varying, client_name character varying, sku text, product text, category text, odv_value numeric, odv_quantity numeric, transaction_count bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_sankey_conversion_flows(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- ---------------------------------------------------------------------------
-- 12. get_conversion_details() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_conversion_details(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_conversion_details(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(m_type text, client_id character varying, client_name character varying, sku character varying, product character varying, cabinet_date date, first_odv_date date, conversion_days integer, odv_sale_count bigint, total_pieces bigint, generated_value numeric, cabinet_value numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT b.m_type::text, b.client_id, b.client_name, b.sku, b.product,
    b.first_event_date, odv_first.first_odv, (odv_first.first_odv - b.first_event_date)::int,
    b.odv_transaction_count, b.odv_quantity::bigint, b.odv_revenue, b.cabinet_revenue
  FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
  JOIN LATERAL (
    SELECT MIN(v.date) AS first_odv FROM odv_sales v
    WHERE v.client_id = b.client_id AND v.sku = b.sku AND v.date >= b.first_event_date
      AND v.odv_id NOT IN (SELECT szl.zoho_id FROM saga_zoho_links szl WHERE szl.type = 'SALE' AND szl.zoho_id IS NOT NULL)
  ) odv_first ON true
  WHERE b.m_type IN ('M2', 'M3')
  ORDER BY b.odv_revenue DESC;
$function$;

DROP FUNCTION IF EXISTS public.get_conversion_details(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_conversion_details(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(m_type text, client_id character varying, client_name character varying, sku character varying, product character varying, cabinet_date date, first_odv_date date, conversion_days integer, odv_sale_count bigint, total_pieces bigint, generated_value numeric, cabinet_value numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_conversion_details(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$;

-- ---------------------------------------------------------------------------
-- 13. get_conversion_metrics() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_conversion_metrics(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(total_adoptions bigint, total_conversions bigint, generated_value numeric, cabinet_value numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH base AS (SELECT * FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date))
  SELECT (SELECT COUNT(*) FROM base WHERE m_type = 'M1')::bigint,
         (SELECT COUNT(*) FROM base WHERE m_type = 'M2')::bigint,
         COALESCE((SELECT SUM(odv_revenue) FROM base WHERE m_type = 'M2'), 0)::numeric,
         COALESCE((SELECT SUM(cabinet_revenue) FROM base WHERE m_type = 'M2'), 0)::numeric;
$function$;

DROP FUNCTION IF EXISTS public.get_conversion_metrics(character varying[], character varying[], character varying[], date, date) CASCADE;
CREATE OR REPLACE FUNCTION public.get_conversion_metrics(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS TABLE(total_adoptions bigint, total_conversions bigint, generated_value numeric, cabinet_value numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_conversion_metrics(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$;

-- ---------------------------------------------------------------------------
-- 14. get_cutoff_general_stats_with_comparison() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cutoff_general_stats_with_comparison() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_general_stats_with_comparison()
 RETURNS TABLE(start_date date, end_date date, cutoff_days integer, total_doctors_visited integer, total_movements integer, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, doctors_with_sales integer, doctors_without_sales integer, previous_sale_value numeric, previous_placement_value numeric, previous_collection_value numeric, previous_avg_per_doctor numeric, sale_change_pct numeric, placement_change_pct numeric, collection_change_pct numeric, avg_change_pct numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_dias_corte int;
  v_prev_inicio date;
  v_prev_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin, r.dias_corte
  INTO v_fecha_inicio, v_fecha_fin, v_dias_corte
  FROM analytics.get_current_cutoff_range() r;

  SELECT MIN(sq.created_date), MAX(sq.fecha_completado)
  INTO v_prev_inicio, v_prev_fin
  FROM (
    SELECT DISTINCT ON (v.client_id)
      v.created_at::date AS created_date,
      v.completed_at::date AS fecha_completado
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at::date < v_fecha_inicio
    ORDER BY v.client_id, v.completed_at DESC
  ) sq;

  IF v_prev_inicio IS NOT NULL THEN
    SELECT
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_prev_inicio AND v_prev_fin;

    SELECT COUNT(DISTINCT mov.client_id)
    INTO v_ant_medicos_con_venta
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_prev_inicio AND v_prev_fin
      AND mov.type = 'SALE';
  END IF;

  RETURN QUERY
  WITH
  current_movements AS (
    SELECT mov.*
    FROM inventory_movements mov
    JOIN clients c ON mov.client_id = c.client_id AND c.active = TRUE
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_visitados AS (
    SELECT DISTINCT cm.client_id FROM current_movements cm
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT cm.client_id FROM current_movements cm WHERE cm.type = 'SALE'
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int AS total_mov,
      SUM(CASE WHEN cm.type = 'SALE' THEN cm.quantity ELSE 0 END)::int AS pz_venta,
      SUM(CASE WHEN cm.type = 'PLACEMENT' THEN cm.quantity ELSE 0 END)::int AS pz_creacion,
      SUM(CASE WHEN cm.type = 'COLLECTION' THEN cm.quantity ELSE 0 END)::int AS pz_recoleccion,
      SUM(CASE WHEN cm.type = 'SALE' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_venta,
      SUM(CASE WHEN cm.type = 'PLACEMENT' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_creacion,
      SUM(CASE WHEN cm.type = 'COLLECTION' THEN cm.quantity * COALESCE(cm.unit_price, 0) ELSE 0 END) AS val_recoleccion
    FROM current_movements cm
  )
  SELECT
    v_fecha_inicio,
    v_fecha_fin,
    v_dias_corte,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    v_ant_val_venta,
    v_ant_val_creacion,
    v_ant_val_recoleccion,
    CASE WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
      THEN v_ant_val_venta / v_ant_medicos_con_venta
      ELSE NULL
    END,
    CASE WHEN v_ant_val_venta IS NOT NULL AND v_ant_val_venta > 0
      THEN ROUND(((COALESCE(s.val_venta, 0) - v_ant_val_venta) / v_ant_val_venta * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_creacion IS NOT NULL AND v_ant_val_creacion > 0
      THEN ROUND(((COALESCE(s.val_creacion, 0) - v_ant_val_creacion) / v_ant_val_creacion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE WHEN v_ant_val_recoleccion IS NOT NULL AND v_ant_val_recoleccion > 0
      THEN ROUND(((COALESCE(s.val_recoleccion, 0) - v_ant_val_recoleccion) / v_ant_val_recoleccion * 100)::numeric, 1)
      ELSE NULL
    END,
    CASE
      WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0
           AND (SELECT COUNT(*)::int FROM medicos_con_venta_actual) > 0
           AND v_ant_val_venta > 0 THEN
        ROUND((
          (COALESCE(s.val_venta, 0) / (SELECT COUNT(*)::int FROM medicos_con_venta_actual)) -
          (v_ant_val_venta / v_ant_medicos_con_venta)
        ) / (v_ant_val_venta / v_ant_medicos_con_venta) * 100, 1)
      ELSE NULL
    END
  FROM stats_actual s;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_general_stats_with_comparison() CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_general_stats_with_comparison()
 RETURNS TABLE(start_date date, end_date date, cutoff_days integer, total_doctors_visited integer, total_movements integer, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, doctors_with_sales integer, doctors_without_sales integer, previous_sale_value numeric, previous_placement_value numeric, previous_collection_value numeric, previous_avg_per_doctor numeric, sale_change_pct numeric, placement_change_pct numeric, collection_change_pct numeric, avg_change_pct numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_general_stats_with_comparison(); $function$;

-- ---------------------------------------------------------------------------
-- 15. get_cutoff_stats_by_doctor() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cutoff_stats_by_doctor() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_stats_by_doctor()
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, sold_skus text, placed_skus text, collected_skus text, has_sale boolean)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH visitas_en_corte AS (
    SELECT DISTINCT
      mov.client_id,
      mov.id_saga_transaction,
      MIN(mov.movement_date::date) as fecha_saga
    FROM inventory_movements mov
    WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.id_saga_transaction IS NOT NULL
    GROUP BY mov.client_id, mov.id_saga_transaction
  )
  SELECT
    c.client_id,
    c.client_name,
    MAX(v.fecha_saga) as fecha_visita,
    SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END)::int,
    SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END)::int,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'PLACEMENT' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.type = 'COLLECTION' THEN mov.sku END, ', '),
    SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END) > 0
  FROM visitas_en_corte v
  JOIN inventory_movements mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON v.client_id = c.client_id
  GROUP BY c.client_id, c.client_name
  ORDER BY SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) DESC,
           c.client_name;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_stats_by_doctor() CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_stats_by_doctor()
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, sold_skus text, placed_skus text, collected_skus text, has_sale boolean)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_stats_by_doctor(); $function$;

-- ---------------------------------------------------------------------------
-- 16. get_cutoff_stats_by_doctor_with_comparison() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cutoff_stats_by_doctor_with_comparison() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_stats_by_doctor_with_comparison()
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, sold_skus text, has_sale boolean, previous_sale_value numeric, change_pct numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH corte_actual AS (
    SELECT * FROM analytics.get_cutoff_stats_by_doctor()
  ),
  corte_anterior AS (
    SELECT * FROM analytics.get_previous_cutoff_stats()
  )
  SELECT
    ca.client_id,
    ca.client_name,
    ca.visit_date,
    ca.sale_pieces,
    ca.placement_pieces,
    ca.collection_pieces,
    ca.sale_value,
    ca.placement_value,
    ca.collection_value,
    ca.sold_skus,
    ca.has_sale,
    COALESCE(cp.valor_venta, 0) as valor_venta_anterior,
    CASE
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.sale_value > 0 THEN 100.00
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.sale_value = 0 THEN 0.00
      ELSE ROUND(((ca.sale_value - COALESCE(cp.valor_venta, 0)) / cp.valor_venta * 100), 1)
    END as porcentaje_cambio
  FROM corte_actual ca
  LEFT JOIN corte_anterior cp ON ca.client_id = cp.client_id
  ORDER BY ca.sale_value DESC;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_stats_by_doctor_with_comparison() CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_stats_by_doctor_with_comparison()
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, sale_pieces integer, placement_pieces integer, collection_pieces integer, sale_value numeric, placement_value numeric, collection_value numeric, sold_skus text, has_sale boolean, previous_sale_value numeric, change_pct numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_stats_by_doctor_with_comparison(); $function$;

-- ---------------------------------------------------------------------------
-- 17. get_cutoff_skus_value_per_visit() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cutoff_skus_value_per_visit(character varying, character varying) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_skus_value_per_visit(p_client_id character varying DEFAULT NULL::character varying, p_brand character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, unique_skus integer, sale_value numeric, brand character varying)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  SELECT
    c.client_id,
    c.client_name,
    mov.movement_date::date as fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END), 0) as valor_venta,
    med.brand
  FROM inventory_movements mov
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON mov.client_id = c.client_id
  WHERE mov.movement_date::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.type = 'SALE'
    AND (p_client_id IS NULL OR c.client_id = p_client_id)
    AND (p_brand IS NULL OR med.brand = p_brand)
  GROUP BY c.client_id, c.client_name, mov.movement_date::date, med.brand
  ORDER BY valor_venta DESC;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_skus_value_per_visit(character varying, character varying) CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_skus_value_per_visit(p_client_id character varying DEFAULT NULL::character varying, p_brand character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, unique_skus integer, sale_value numeric, brand character varying)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_skus_value_per_visit(p_client_id, p_brand); $function$;

-- ---------------------------------------------------------------------------
-- 18. get_available_filters() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_available_filters() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_available_filters()
 RETURNS TABLE(brands character varying[], doctors jsonb, conditions character varying[], first_placement_date date)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT ARRAY_AGG(DISTINCT m.brand ORDER BY m.brand)
     FROM medications m WHERE m.brand IS NOT NULL)::varchar[],
    (SELECT jsonb_agg(jsonb_build_object('id', c.client_id, 'name', c.client_name) ORDER BY c.client_name)
     FROM clients c WHERE c.active = true),
    (SELECT ARRAY_AGG(DISTINCT p.name ORDER BY p.name)
     FROM conditions p)::varchar[],
    (SELECT MIN(movement_date)::date
     FROM inventory_movements WHERE type = 'PLACEMENT');
END;
$function$;

DROP FUNCTION IF EXISTS public.get_available_filters() CASCADE;
CREATE OR REPLACE FUNCTION public.get_available_filters()
 RETURNS TABLE(brands character varying[], doctors jsonb, conditions character varying[], first_placement_date date)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_available_filters(); $function$;

-- ---------------------------------------------------------------------------
-- 19. get_historical_conversions_evolution() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_historical_conversions_evolution(date, date, text, character varying[], character varying[], character varying[]) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_historical_conversions_evolution(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_grouping text DEFAULT 'day'::text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(date_group date, date_label text, total_pairs integer, cabinet_pairs integer, direct_pairs integer, total_value numeric, cabinet_value numeric, direct_value numeric, transaction_count integer, client_count integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
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
  botiquin_linked AS (
    SELECT DISTINCT b.client_id, b.sku, b.first_event_date
    FROM analytics.clasificacion_base(p_doctors, p_brands, p_conditions, p_start_date, p_end_date) b
    WHERE b.m_type IN ('M1', 'M2', 'M3')
      AND (p_doctors IS NULL OR b.client_id = ANY(p_doctors))
      AND b.sku IN (SELECT fs.sku FROM filtered_skus fs)
  ),
  ventas_clasificadas AS (
    SELECT
      v.client_id, v.sku, v.date, v.quantity, v.price,
      (v.quantity * COALESCE(v.price, 0)) as valor_venta,
      CASE
        WHEN bl.client_id IS NOT NULL
             AND v.date >= bl.first_event_date THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_grouping = 'week' THEN date_trunc('week', v.date)::DATE
        ELSE v.date::DATE
      END as fecha_agrupada
    FROM odv_sales v
    LEFT JOIN botiquin_linked bl ON v.client_id = bl.client_id AND v.sku = bl.sku
    WHERE (p_start_date IS NULL OR v.date >= p_start_date)
      AND (p_end_date IS NULL OR v.date <= p_end_date)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
      AND v.sku IN (SELECT fs.sku FROM filtered_skus fs)
  )
  SELECT
    vc.fecha_agrupada as fecha_grupo,
    CASE
      WHEN p_grouping = 'week' THEN 'Sem ' || to_char(vc.fecha_agrupada, 'DD/MM')
      ELSE to_char(vc.fecha_agrupada, 'DD Mon')
    END as fecha_label,
    COUNT(DISTINCT vc.client_id || '-' || vc.sku)::INT as pares_total,
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.client_id || '-' || vc.sku END)::INT as pares_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.client_id || '-' || vc.sku END)::INT as pares_directo,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC as valor_total,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_botiquin,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_directo,
    COUNT(*)::INT as num_transacciones,
    COUNT(DISTINCT vc.client_id)::INT as num_clientes
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_historical_conversions_evolution(date, date, text, character varying[], character varying[], character varying[]) CASCADE;
CREATE OR REPLACE FUNCTION public.get_historical_conversions_evolution(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_grouping text DEFAULT 'day'::text, p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(date_group date, date_label text, total_pairs integer, cabinet_pairs integer, direct_pairs integer, total_value numeric, cabinet_value numeric, direct_value numeric, transaction_count integer, client_count integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_historical_conversions_evolution(p_start_date, p_end_date, p_grouping, p_doctors, p_brands, p_conditions); $function$;

-- ---------------------------------------------------------------------------
-- 20. get_historical_skus_value_per_visit() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_historical_skus_value_per_visit(date, date, character varying) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_historical_skus_value_per_visit(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_client_id character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, unique_skus integer, sale_value numeric, sale_pieces integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH visits AS (
    SELECT
      mov.id_saga_transaction,
      mov.client_id,
      MIN(mov.movement_date::date) as fecha_visita
    FROM inventory_movements mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.type = 'SALE'
    GROUP BY mov.id_saga_transaction, mov.client_id
  )
  SELECT
    c.client_id,
    c.client_name,
    v.fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0) as valor_venta,
    SUM(mov.quantity)::int as piezas_venta
  FROM visits v
  JOIN inventory_movements mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medications med ON mov.sku = med.sku
  JOIN clients c ON v.client_id = c.client_id
  WHERE mov.type = 'SALE'
    AND (p_start_date IS NULL OR v.fecha_visita >= p_start_date)
    AND (p_end_date IS NULL OR v.fecha_visita <= p_end_date)
    AND (p_client_id IS NULL OR v.client_id = p_client_id)
  GROUP BY c.client_id, c.client_name, v.fecha_visita
  ORDER BY v.fecha_visita ASC, c.client_name;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_historical_skus_value_per_visit(date, date, character varying) CASCADE;
CREATE OR REPLACE FUNCTION public.get_historical_skus_value_per_visit(p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_client_id character varying DEFAULT NULL::character varying)
 RETURNS TABLE(client_id character varying, client_name character varying, visit_date date, unique_skus integer, sale_value numeric, sale_pieces integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_historical_skus_value_per_visit(p_start_date, p_end_date, p_client_id); $function$;

-- ---------------------------------------------------------------------------
-- 21. get_cutoff_logistics_detail() - analytics + public
-- ---------------------------------------------------------------------------
-- Note: get_cutoff_logistics_data() is a large function; only renaming RETURNS TABLE columns
DROP FUNCTION IF EXISTS analytics.get_cutoff_logistics_data(character varying[], character varying[], character varying[]) CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_logistics_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(advisor_name text, client_name character varying, client_id character varying, visit_date text, sku character varying, product character varying, placed_quantity integer, sale_qty integer, collection_qty integer, total_cutoff integer, destino text, saga_status text, odv_cabinet text, odv_sale text, collection_id uuid, collection_status text, evidence_paths text[], signature_path text, observations text, received_by text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.client_id,
      v.user_id,
      v.completed_at::date AS fecha_visita,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.type = 'VISIT_CUTOFF'
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_placement_visits AS (
    SELECT DISTINCT ON (cv.client_id)
      cv.client_id,
      v.visit_id
    FROM current_visits cv
    JOIN visits v_cur ON v_cur.visit_id = cv.visit_id
    JOIN visits v ON v.client_id = cv.client_id
      AND v.visit_id != cv.visit_id
      AND v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.completed_at < v_cur.completed_at
    WHERE EXISTS (
      SELECT 1 FROM saga_transactions st
      JOIN inventory_movements mi ON mi.id_saga_transaction = st.id
      WHERE st.visit_id = v.visit_id
        AND mi.type = 'PLACEMENT'
        AND mi.client_id = cv.client_id
    )
    ORDER BY cv.client_id, v.completed_at DESC
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
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
  )
  SELECT
    u.name::text                                                          AS nombre_asesor,
    c.client_name,
    mov.client_id,
    TO_CHAR(cv.fecha_visita, 'YYYY-MM-DD')                                 AS fecha_visita,
    mov.sku,
    med.product,
    (SELECT COALESCE(SUM(m_cre.quantity), 0)
     FROM inventory_movements m_cre
     JOIN saga_transactions st_cre ON m_cre.id_saga_transaction = st_cre.id
     WHERE st_cre.visit_id = ppv.visit_id
       AND m_cre.client_id = mov.client_id
       AND m_cre.sku = mov.sku
       AND m_cre.type = 'PLACEMENT')::int                                    AS cantidad_colocada,
    CASE WHEN mov.type = 'SALE'       THEN mov.quantity ELSE 0 END        AS qty_venta,
    CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END        AS qty_recoleccion,
    mov.quantity                                                           AS total_corte,
    mov.type::text                                                         AS destino,
    st.status::text                                                        AS saga_status,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM (
       SELECT m_cre.id_saga_transaction
       FROM inventory_movements m_cre
       WHERE m_cre.client_id = mov.client_id
         AND m_cre.sku = mov.sku
         AND m_cre.type = 'PLACEMENT'
         AND m_cre.movement_date <= mov.movement_date
         AND EXISTS (
           SELECT 1 FROM saga_zoho_links szl_chk
           WHERE szl_chk.id_saga_transaction = m_cre.id_saga_transaction
             AND szl_chk.type = 'CABINET'
             AND szl_chk.zoho_id IS NOT NULL
         )
       ORDER BY m_cre.movement_date DESC
       LIMIT 1
     ) latest_cre
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = latest_cre.id_saga_transaction
       AND szl.type = 'CABINET'
       AND szl.zoho_id IS NOT NULL
       AND EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       ))                                                                   AS odv_botiquin,
    (SELECT STRING_AGG(DISTINCT szl.zoho_id, ', ' ORDER BY szl.zoho_id)
     FROM saga_transactions st_mov
     JOIN saga_transactions st_ven ON st_ven.visit_id = st_mov.visit_id
       AND st_ven.type = 'SALE'
     JOIN saga_zoho_links szl ON szl.id_saga_transaction = st_ven.id
       AND szl.type = 'SALE'
       AND szl.zoho_id IS NOT NULL
     WHERE st_mov.id = mov.id_saga_transaction
       AND (szl.items IS NULL OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(szl.items) elem
         WHERE elem->>'sku' = mov.sku::text
       )))                                                                  AS odv_venta,
    rcl.recoleccion_id,
    rcl.status::text                                                       AS recoleccion_estado,
    (SELECT ARRAY_AGG(re.storage_path)
     FROM collection_evidence re
     WHERE re.recoleccion_id = rcl.recoleccion_id)                         AS evidencia_paths,
    (SELECT rf.storage_path
     FROM collection_signatures rf
     WHERE rf.recoleccion_id = rcl.recoleccion_id
     LIMIT 1)                                                              AS firma_path,
    rcl.cedis_observations                                                AS observaciones,
    rcl.cedis_responsible_name                                           AS quien_recibio
  FROM current_visits cv
  LEFT JOIN prev_placement_visits ppv ON ppv.client_id = cv.client_id
  JOIN saga_transactions st ON st.visit_id = cv.visit_id
  JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
  JOIN clients c        ON mov.client_id = c.client_id
  JOIN medications med  ON mov.sku = med.sku
  LEFT JOIN users u   ON cv.user_id = u.user_id
  LEFT JOIN collections rcl ON cv.visit_id = rcl.visit_id AND mov.client_id = rcl.client_id
  WHERE mov.type IN ('SALE', 'COLLECTION')
    AND mov.sku IN (SELECT sku FROM filtered_skus)
  ORDER BY c.client_name, mov.sku;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_logistics_data(character varying[], character varying[], character varying[]) CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_logistics_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[])
 RETURNS TABLE(advisor_name text, client_name character varying, client_id character varying, visit_date text, sku character varying, product character varying, placed_quantity integer, sale_qty integer, collection_qty integer, total_cutoff integer, destino text, saga_status text, odv_cabinet text, odv_sale text, collection_id uuid, collection_status text, evidence_paths text[], signature_path text, observations text, received_by text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_cutoff_logistics_data(p_doctors, p_brands, p_conditions);
$function$;

-- get_cutoff_logistics_detail() - rename columns only (same body)
DROP FUNCTION IF EXISTS analytics.get_cutoff_logistics_detail() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cutoff_logistics_detail()
 RETURNS TABLE(advisor_name text, client_name text, client_id text, visit_date date, sku text, product text, placed_quantity integer, sale_qty integer, collection_qty integer, total_cutoff integer, destino text, saga_status text, odv_cabinet text, odv_sale text, collection_id text, collection_status text, evidence_paths text[], signature_path text, observations text, received_by text)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM analytics.get_current_cutoff_range() r;

  RETURN QUERY
  WITH
  latest_visits AS (
    SELECT DISTINCT ON (v.client_id)
      v.visit_id, v.client_id, v.user_id,
      v.created_at::date AS fecha_visita
    FROM visits v
    JOIN clients cl ON cl.client_id = v.client_id AND cl.active = TRUE
    WHERE v.created_at::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND v.status NOT IN ('CANCELLED')
    ORDER BY v.client_id, v.created_at DESC
  ),
  visit_sagas AS (
    SELECT lv.visit_id, lv.client_id, st.id AS saga_id, st.type AS saga_type, st.status AS saga_status
    FROM latest_visits lv
    JOIN saga_transactions st ON st.visit_id = lv.visit_id
  ),
  creaciones AS (
    SELECT vs.client_id, mi.sku, SUM(mi.quantity)::int AS cantidad_colocada, vs.saga_id, vs.saga_status
    FROM visit_sagas vs
    JOIN inventory_movements mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'POST_CUTOFF_PLACEMENT' AND mi.type = 'PLACEMENT'
    GROUP BY vs.client_id, mi.sku, vs.saga_id, vs.saga_status
  ),
  ventas AS (
    SELECT vs.client_id, mi.sku, SUM(mi.quantity)::int AS qty_venta, vs.saga_id
    FROM visit_sagas vs
    JOIN inventory_movements mi ON mi.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'SALE' AND mi.type = 'SALE'
    GROUP BY vs.client_id, mi.sku, vs.saga_id
  ),
  recol AS (
    SELECT r.client_id, r.recoleccion_id, r.status AS recoleccion_estado, r.cedis_observations, r.cedis_responsible_name
    FROM latest_visits lv
    JOIN collections r ON r.visit_id = lv.visit_id
  ),
  recol_items AS (
    SELECT rec.client_id, ri.sku, SUM(ri.quantity)::int AS qty_recoleccion, rec.recoleccion_id, rec.recoleccion_estado, rec.cedis_observations, rec.cedis_responsible_name
    FROM recol rec
    JOIN collection_items ri ON ri.recoleccion_id = rec.recoleccion_id
    GROUP BY rec.client_id, ri.sku, rec.recoleccion_id, rec.recoleccion_estado, rec.cedis_observations, rec.cedis_responsible_name
  ),
  zoho_botiquin AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'POST_CUTOFF_PLACEMENT' AND szl.type = 'CABINET'
    GROUP BY vs.saga_id
  ),
  zoho_venta AS (
    SELECT vs.saga_id, string_agg(DISTINCT szl.zoho_id, ', ') AS odv
    FROM visit_sagas vs
    JOIN saga_zoho_links szl ON szl.id_saga_transaction = vs.saga_id
    WHERE vs.saga_type = 'SALE' AND szl.type = 'SALE'
    GROUP BY vs.saga_id
  ),
  evidencias AS (
    SELECT re.recoleccion_id, array_agg(re.storage_path) AS paths
    FROM collection_evidence re GROUP BY re.recoleccion_id
  ),
  firmas AS (
    SELECT rf.recoleccion_id, rf.storage_path FROM collection_signatures rf
  ),
  combined AS (
    SELECT c.client_id, c.sku, c.cantidad_colocada, c.saga_id AS saga_creacion, c.saga_status,
      COALESCE(v.qty_venta, 0) AS qty_venta, v.saga_id AS saga_venta,
      COALESCE(ri.qty_recoleccion, 0) AS qty_recoleccion, ri.recoleccion_id, ri.recoleccion_estado, ri.cedis_observations, ri.cedis_responsible_name
    FROM creaciones c
    LEFT JOIN ventas v ON v.client_id = c.client_id AND v.sku = c.sku
    LEFT JOIN recol_items ri ON ri.client_id = c.client_id AND ri.sku = c.sku
  )
  SELECT
    u.name::text AS nombre_asesor, cl.client_name::text AS client_name, cl.client_id::text AS client_id,
    lv.fecha_visita AS fecha_visita, cb.sku::text AS sku, med.product::text AS product,
    cb.cantidad_colocada AS cantidad_colocada, cb.qty_venta AS qty_venta, cb.qty_recoleccion AS qty_recoleccion,
    cb.qty_venta + cb.qty_recoleccion AS total_corte,
    CASE
      WHEN cb.qty_venta > 0 AND cb.qty_recoleccion > 0 THEN 'VENTA+RECOLECCION'
      WHEN cb.qty_venta > 0 THEN 'SALE'
      WHEN cb.qty_recoleccion > 0 THEN 'COLLECTION'
      ELSE 'PENDING'
    END AS destino,
    cb.saga_status::text AS saga_status, zb.odv AS odv_botiquin, zv.odv AS odv_venta,
    cb.recoleccion_id::text AS recoleccion_id, cb.recoleccion_estado::text AS recoleccion_estado,
    ev.paths AS evidencia_paths, fi.storage_path AS firma_path,
    cb.cedis_observations::text AS observaciones, cb.cedis_responsible_name::text AS quien_recibio
  FROM combined cb
  JOIN latest_visits lv ON lv.client_id = cb.client_id
  JOIN clients cl ON cl.client_id = cb.client_id
  JOIN medications med ON med.sku = cb.sku
  LEFT JOIN users u ON u.user_id = lv.user_id
  LEFT JOIN zoho_botiquin zb ON zb.saga_id = cb.saga_creacion
  LEFT JOIN zoho_venta zv ON zv.saga_id = cb.saga_venta
  LEFT JOIN evidencias ev ON ev.recoleccion_id = cb.recoleccion_id
  LEFT JOIN firmas fi ON fi.recoleccion_id = cb.recoleccion_id
  ORDER BY cl.client_name, cb.sku;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cutoff_logistics_detail() CASCADE;
CREATE OR REPLACE FUNCTION public.get_cutoff_logistics_detail()
 RETURNS TABLE(advisor_name text, client_name text, client_id text, visit_date date, sku text, product text, placed_quantity integer, sale_qty integer, collection_qty integer, total_cutoff integer, destino text, saga_status text, odv_cabinet text, odv_sale text, collection_id text, collection_status text, evidence_paths text[], signature_path text, observations text, received_by text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cutoff_logistics_detail(); $function$;

-- ---------------------------------------------------------------------------
-- 22. get_cabinet_data() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_cabinet_data() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_cabinet_data()
 RETURNS TABLE(sku character varying, movement_id bigint, movement_type text, quantity integer, movement_date text, batch_id text, intake_date text, initial_quantity integer, available_quantity integer, client_id character varying, client_name character varying, tier character varying, avg_billing numeric, total_billing numeric, product character varying, price numeric, brand character varying, top boolean, "condition" character varying)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    med.sku,
    mov.id as id_movimiento,
    CAST(mov.type AS TEXT) AS movement_type,
    mov.quantity,
    TO_CHAR(mov.movement_date, 'DD/MM/YYYY') AS movement_date,
    mov.id::TEXT as id_lote,
    TO_CHAR(mov.movement_date, 'DD/MM/YYYY') AS intake_date,
    COALESCE(inv.available_quantity, 0)::INTEGER as cantidad_inicial,
    COALESCE(inv.available_quantity, 0)::INTEGER as available_quantity,
    mov.client_id,
    c.client_name,
    c.tier,
    c.avg_billing,
    c.total_billing,
    med.product,
    mov.unit_price AS price,
    med.brand,
    med.top,
    p.name as padecimiento
  FROM inventory_movements mov
  JOIN clients c ON mov.client_id = c.client_id
  JOIN medications med ON mov.sku = med.sku
  LEFT JOIN cabinet_inventory inv
    ON mov.client_id = inv.client_id AND mov.sku = inv.sku
  LEFT JOIN medication_conditions mp ON mov.sku = mp.sku
  LEFT JOIN conditions p ON mp.condition_id = p.condition_id;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_cabinet_data() CASCADE;
CREATE OR REPLACE FUNCTION public.get_cabinet_data()
 RETURNS TABLE(sku character varying, movement_id bigint, movement_type text, quantity integer, movement_date text, batch_id text, intake_date text, initial_quantity integer, available_quantity integer, client_id character varying, client_name character varying, tier character varying, avg_billing numeric, total_billing numeric, product character varying, price numeric, brand character varying, top boolean, "condition" character varying)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_cabinet_data(); $function$;

-- ---------------------------------------------------------------------------
-- 23. get_balance_metrics() - analytics + public
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS analytics.get_balance_metrics() CASCADE;
CREATE OR REPLACE FUNCTION analytics.get_balance_metrics()
 RETURNS TABLE(concept text, created_value numeric, sales_value numeric, collection_value numeric, holding_inbound_value numeric, holding_virtual_value numeric, calculated_total_value numeric, difference numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  WITH metricas_inventario AS (
    SELECT
      SUM(inv.available_quantity * COALESCE(inv.unit_price, 0)) as total_stock_vivo
    FROM cabinet_inventory inv
    JOIN medications med ON inv.sku = med.sku
    JOIN clients c ON inv.client_id = c.client_id
    WHERE c.active = TRUE
  ),
  metricas_movimientos AS (
    SELECT
      SUM(CASE WHEN mov.type = 'PLACEMENT'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_creado_historico,
      SUM(CASE WHEN mov.type = 'HOLDING'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_permanencia_entrada,
      SUM(CASE WHEN mov.type = 'SALE'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_ventas,
      SUM(CASE WHEN mov.type = 'COLLECTION'
        THEN mov.quantity * COALESCE(mov.unit_price, 0) ELSE 0 END) as total_recoleccion
    FROM inventory_movements mov
    JOIN medications med ON mov.sku = med.sku
  )
  SELECT
    'BALANCE_GLOBAL_SISTEMA'::TEXT as concepto,
    COALESCE(M.total_creado_historico, 0) as valor_creado,
    COALESCE(M.total_ventas, 0) as valor_ventas,
    COALESCE(M.total_recoleccion, 0) as valor_recoleccion,
    COALESCE(M.total_permanencia_entrada, 0) as valor_permanencia_entrada,
    COALESCE(I.total_stock_vivo, 0) as valor_permanencia_virtual,
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as valor_calculado_total,
    COALESCE(M.total_creado_historico, 0) -
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as diferencia
  FROM metricas_inventario I
  CROSS JOIN metricas_movimientos M;
END;
$function$;

DROP FUNCTION IF EXISTS public.get_balance_metrics() CASCADE;
CREATE OR REPLACE FUNCTION public.get_balance_metrics()
 RETURNS TABLE(concept text, created_value numeric, sales_value numeric, collection_value numeric, holding_inbound_value numeric, holding_virtual_value numeric, calculated_total_value numeric, difference numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$ SELECT * FROM analytics.get_balance_metrics(); $function$;

-- ============================================================================
-- PART B: JSON-returning functions - update references to renamed columns
-- ============================================================================

-- ---------------------------------------------------------------------------
-- B1. analytics.get_dashboard_static() - references get_cutoff_general_stats_with_comparison()
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_dashboard_static()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filtros json;
  v_stats json;
  v_progress json;
BEGIN
  SELECT row_to_json(f) INTO v_filtros
  FROM (
    SELECT
      (SELECT ARRAY_AGG(DISTINCT m.brand ORDER BY m.brand)
       FROM medications m WHERE m.brand IS NOT NULL) AS brands,
      (SELECT jsonb_agg(jsonb_build_object('id', c.client_id, 'name', c.client_name) ORDER BY c.client_name)
       FROM clients c WHERE c.active = true) AS doctors,
      (SELECT ARRAY_AGG(DISTINCT p.name ORDER BY p.name)
       FROM conditions p) AS conditions,
      (SELECT MIN(movement_date)::date
       FROM inventory_movements WHERE type = 'PLACEMENT') AS "firstPlacementDate"
  ) f;

  SELECT row_to_json(s) INTO v_stats
  FROM (
    SELECT
      r.start_date AS "startDate",
      r.end_date AS "endDate",
      r.cutoff_days AS "cutoffDays",
      r.total_doctors_visited AS "totalDoctorsVisited",
      r.total_movements AS "totalMovements",
      r.sale_pieces AS "salePieces",
      r.placement_pieces AS "placementPieces",
      r.collection_pieces AS "collectionPieces",
      r.sale_value AS "saleValue",
      r.placement_value AS "placementValue",
      r.collection_value AS "collectionValue",
      r.doctors_with_sales AS "doctorsWithSales",
      r.doctors_without_sales AS "doctorsWithoutSales",
      r.previous_sale_value AS "previousSaleValue",
      r.previous_placement_value AS "previousPlacementValue",
      r.previous_collection_value AS "previousCollectionValue",
      r.previous_avg_per_doctor AS "previousAvgPerDoctor",
      r.sale_change_pct AS "saleChangePct",
      r.placement_change_pct AS "placementChangePct",
      r.collection_change_pct AS "collectionChangePct",
      r.avg_change_pct AS "avgChangePct"
    FROM analytics.get_cutoff_general_stats_with_comparison() r
    LIMIT 1
  ) s;

  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_completados AS (
    SELECT v.client_id,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
  )
  SELECT json_build_object(
    'completed', (SELECT COUNT(DISTINCT client_id) FROM ranked_completados WHERE rn = 1),
    'pending', (SELECT COUNT(*) FROM clients WHERE active = TRUE)
      - (SELECT COUNT(DISTINCT client_id) FROM ranked_completados WHERE rn = 1)
      - (SELECT COUNT(*) FROM voided_clients),
    'cancelled', (SELECT COUNT(*) FROM voided_clients),
    'total', (SELECT COUNT(*) FROM clients WHERE active = TRUE)
  ) INTO v_progress;

  RETURN json_build_object(
    'cutoffFilters', v_filtros,
    'cutoffStatsGeneral', v_stats,
    'cutoffProgress', v_progress
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.get_dashboard_static()
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_static(); $function$;

-- ---------------------------------------------------------------------------
-- B2. analytics.get_active_collection() - already has English JSON keys, no changes needed
-- (included for completeness since Phase 7 had it)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_active_collection()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
DECLARE
  result json;
BEGIN
  WITH borrador_items AS (
    SELECT
      st.client_id,
      (item->>'sku')::text as sku,
      (item->>'quantity')::int as quantity
    FROM saga_transactions st,
    jsonb_array_elements(st.items) as item
    WHERE st.type = 'COLLECTION'
      AND st.status = 'DRAFT'
  )
  SELECT json_build_object(
    'total_pieces', COALESCE(SUM(bi.quantity), 0)::bigint,
    'total_value', COALESCE(SUM(bi.quantity * COALESCE(inv.unit_price, 0)), 0),
    'client_count', COALESCE(COUNT(DISTINCT bi.client_id), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN clients c ON bi.client_id = c.client_id
  LEFT JOIN cabinet_inventory inv ON bi.client_id = inv.client_id AND bi.sku = inv.sku
  WHERE c.active = TRUE;

  RETURN COALESCE(result, json_build_object('total_pieces', 0, 'total_value', 0, 'client_count', 0));
END;
$function$;

-- ---------------------------------------------------------------------------
-- B3. analytics.get_current_cutoff_data() - references renamed columns
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_current_cutoff_data(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[]
)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  voided_clients AS (
    SELECT sub.client_id
    FROM (
      SELECT DISTINCT ON (v.client_id) v.client_id, v.status
      FROM visits v
      JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
      WHERE v.status NOT IN ('SCHEDULED')
        AND NOT (v.status = 'CANCELLED' AND v.completed_at IS NULL)
      ORDER BY v.client_id, v.corte_number DESC
    ) sub
    WHERE sub.status = 'CANCELLED'
  ),
  ranked_visits AS (
    SELECT
      v.visit_id,
      v.client_id,
      v.completed_at::date AS visit_date,
      v.corte_number,
      ROW_NUMBER() OVER (PARTITION BY v.client_id ORDER BY v.corte_number DESC) AS rn
    FROM visits v
    JOIN clients c ON c.client_id = v.client_id AND c.active = TRUE
    WHERE v.status = 'COMPLETED'
      AND v.completed_at IS NOT NULL
      AND v.client_id NOT IN (SELECT client_id FROM voided_clients)
      AND (p_doctors IS NULL OR v.client_id = ANY(p_doctors))
  ),
  current_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 1
  ),
  prev_visits AS (
    SELECT * FROM ranked_visits WHERE rn = 2
  ),
  all_active_clients AS (
    SELECT c.client_id, c.client_name
    FROM clients c
    WHERE c.active = TRUE
      AND (p_doctors IS NULL OR c.client_id = ANY(p_doctors))
  ),
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
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
  current_mov AS (
    SELECT mov.client_id, ac.client_name, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM current_visits cv
    JOIN saga_transactions st ON st.visit_id = cv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    JOIN all_active_clients ac ON ac.client_id = mov.client_id
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  prev_mov AS (
    SELECT mov.client_id, mov.sku, mov.type, mov.quantity, COALESCE(mov.unit_price, 0) AS price
    FROM prev_visits pv
    JOIN saga_transactions st ON st.visit_id = pv.visit_id
    JOIN inventory_movements mov ON mov.id_saga_transaction = st.id
    JOIN medications med ON mov.sku = med.sku
    WHERE mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stats AS (
    SELECT
      COALESCE(COUNT(DISTINCT mov.client_id), 0)::int AS total_doctors_visited,
      COALESCE(COUNT(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.client_id END), 0)::int AS doctors_with_sales,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS sale_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS placement_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS collection_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value
    FROM current_mov mov
  ),
  prev_stats AS (
    SELECT
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value
    FROM prev_mov mov
  ),
  prev_medico_stats AS (
    SELECT mov.client_id,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value
    FROM prev_mov mov
    GROUP BY mov.client_id
  ),
  visited_medico_rows AS (
    SELECT
      mov.client_id,
      mov.client_name,
      cv.visit_date::text AS visit_date,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity ELSE 0 END), 0)::int AS sale_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity ELSE 0 END), 0)::int AS placement_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity ELSE 0 END), 0)::int AS collection_pieces,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS sale_value,
      COALESCE(SUM(CASE WHEN mov.type = 'PLACEMENT' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS placement_value,
      COALESCE(SUM(CASE WHEN mov.type = 'COLLECTION' THEN mov.quantity * mov.price ELSE 0 END), 0)::numeric AS collection_value,
      STRING_AGG(DISTINCT CASE WHEN mov.type = 'SALE' THEN mov.sku END, ', ') AS sold_skus,
      COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0 AS has_sale,
      pms.sale_value AS previous_sale_value,
      CASE
        WHEN pms.sale_value IS NOT NULL AND pms.sale_value > 0
          THEN ROUND(((COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN mov.quantity * mov.price ELSE 0 END), 0) - pms.sale_value) / pms.sale_value * 100)::numeric, 1)
        WHEN (pms.sale_value IS NULL OR pms.sale_value = 0)
          AND COALESCE(SUM(CASE WHEN mov.type = 'SALE' THEN 1 ELSE 0 END), 0) > 0
          THEN 100.0
        ELSE NULL
      END AS change_pct
    FROM current_mov mov
    JOIN current_visits cv ON cv.client_id = mov.client_id
    LEFT JOIN prev_medico_stats pms ON mov.client_id = pms.client_id
    GROUP BY mov.client_id, mov.client_name, cv.visit_date, pms.sale_value
  ),
  pending_medico_rows AS (
    SELECT
      ac.client_id,
      ac.client_name,
      NULL::text AS visit_date,
      0::int AS sale_pieces,
      0::int AS placement_pieces,
      0::int AS collection_pieces,
      0::numeric AS sale_value,
      0::numeric AS placement_value,
      0::numeric AS collection_value,
      NULL::text AS sold_skus,
      false AS has_sale,
      NULL::numeric AS previous_sale_value,
      NULL::numeric AS change_pct
    FROM all_active_clients ac
    WHERE NOT EXISTS (SELECT 1 FROM current_visits cv WHERE cv.client_id = ac.client_id)
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'start_date', (SELECT MIN(cv.visit_date) FROM current_visits cv),
      'end_date', (SELECT MAX(cv.visit_date) FROM current_visits cv),
      'cutoff_days', COALESCE((SELECT MAX(cv.visit_date) - MIN(cv.visit_date) + 1 FROM current_visits cv), 0),
      'total_doctors_visited', k.total_doctors_visited,
      'doctors_with_sales', k.doctors_with_sales,
      'doctors_without_sales', k.total_doctors_visited - k.doctors_with_sales,
      'sale_pieces', k.sale_pieces,
      'sale_value', k.sale_value,
      'placement_pieces', k.placement_pieces,
      'placement_value', k.placement_value,
      'collection_pieces', k.collection_pieces,
      'collection_value', k.collection_value,
      'sale_change_pct',
        CASE WHEN p.sale_value > 0
          THEN ROUND(((k.sale_value - p.sale_value) / p.sale_value * 100)::numeric, 1)
          ELSE NULL END,
      'placement_change_pct',
        CASE WHEN p.placement_value > 0
          THEN ROUND(((k.placement_value - p.placement_value) / p.placement_value * 100)::numeric, 1)
          ELSE NULL END,
      'collection_change_pct',
        CASE WHEN p.collection_value > 0
          THEN ROUND(((k.collection_value - p.collection_value) / p.collection_value * 100)::numeric, 1)
          ELSE NULL END
    ),
    'doctors', COALESCE(
      (SELECT json_agg(row_to_json(sub) ORDER BY sub.visit_date IS NULL ASC, sub.sale_value DESC, sub.client_name)
       FROM (
         SELECT * FROM visited_medico_rows
         UNION ALL
         SELECT * FROM pending_medico_rows
       ) sub),
      '[]'::json)
  ) INTO v_result
  FROM kpi_stats k
  CROSS JOIN prev_stats p;

  RETURN v_result;
END;
$function$;

-- ---------------------------------------------------------------------------
-- B4. analytics.get_historical_cutoff_data() - already has English JSON keys from Phase 7
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_historical_cutoff_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_result json;
BEGIN
  WITH
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.name AS padecimiento
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
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'SALE'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'PLACEMENT'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.available_quantity * COALESCE(inv.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_count
    FROM cabinet_inventory inv
    JOIN medications med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE inv.available_quantity > 0
      AND (p_doctors IS NULL OR inv.client_id = ANY(p_doctors))
      AND (p_brands IS NULL OR med.brand = ANY(p_brands))
      AND (p_conditions IS NULL OR sp.padecimiento = ANY(p_conditions))
  ),
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_count
    FROM inventory_movements mov
    WHERE mov.type = 'COLLECTION'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.client_id,
      MIN(mov.movement_date::date) AS visit_date
    FROM inventory_movements mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.type = 'SALE'
      AND (p_doctors IS NULL OR mov.client_id = ANY(p_doctors))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mov.id_saga_transaction, mov.client_id
  ),
  visita_rows AS (
    SELECT
      c.client_id,
      c.client_name,
      vb.visit_date::text AS visit_date,
      COUNT(DISTINCT mov.sku)::int AS unique_skus,
      COALESCE(SUM(mov.quantity * COALESCE(mov.unit_price, 0)), 0)::numeric AS sale_value,
      COALESCE(SUM(mov.quantity), 0)::int AS sale_pieces
    FROM visitas_base vb
    JOIN inventory_movements mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN clients c ON vb.client_id = c.client_id
    WHERE mov.type = 'SALE'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_start_date IS NULL OR vb.visit_date >= p_start_date)
      AND (p_end_date IS NULL OR vb.visit_date <= p_end_date)
    GROUP BY c.client_id, c.client_name, vb.visit_date
    ORDER BY vb.visit_date ASC, c.client_name
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'sale_value_m1', (SELECT valor FROM kpi_venta_m1),
      'sale_skus_m1', (SELECT skus_count FROM kpi_venta_m1),
      'placement_value', (SELECT valor FROM kpi_creacion),
      'placement_skus', (SELECT skus_count FROM kpi_creacion),
      'active_stock', (SELECT valor FROM kpi_stock),
      'stock_skus', (SELECT skus_count FROM kpi_stock),
      'collection_value', (SELECT valor FROM kpi_recoleccion),
      'collection_skus', (SELECT skus_count FROM kpi_recoleccion)
    ),
    'visits', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$function$;

-- ---------------------------------------------------------------------------
-- B5. analytics.get_dashboard_data() - references clasificacion columns
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics.get_dashboard_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
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
saga_odv_ids AS (
  SELECT DISTINCT szl.zoho_id
  FROM saga_zoho_links szl
  WHERE szl.type = 'SALE'
    AND szl.zoho_id IS NOT NULL
),
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
m2_agg AS (
  SELECT mp.client_id, mp.sku,
         COALESCE(SUM(v.quantity * v.price), 0) AS revenue_odv,
         COALESCE(SUM(v.quantity), 0) AS cantidad_odv,
         COUNT(v.*) AS num_transacciones_odv
  FROM m1_pairs mp
  JOIN odv_sales v ON v.client_id = mp.client_id
                    AND v.sku = mp.sku
                    AND v.date >= mp.first_venta
  WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
  GROUP BY mp.client_id, mp.sku
  HAVING COALESCE(SUM(v.quantity * v.price), 0) > 0
),
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
  WHERE v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
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
-- JSON builders with ENGLISH keys
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
        AND v.odv_id NOT IN (SELECT zoho_id FROM saga_odv_ids)
    ) odv_first ON true
    WHERE b.m_type IN ('M2', 'M3')
  ) r
),
m1_odv_ids AS (
  SELECT DISTINCT szl.zoho_id AS odv_id, st.client_id
  FROM saga_zoho_links szl
  JOIN saga_transactions st ON szl.id_saga_transaction = st.id
  WHERE szl.type = 'SALE'
    AND szl.zoho_id IS NOT NULL
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
categorized AS (
  SELECT
    v.client_id, v.sku, v.date, v.quantity,
    v.quantity * v.price AS line_total,
    CASE
      WHEN m1o.odv_id IS NOT NULL THEN 'M1'
      WHEN fv.sku IS NOT NULL AND v.date > fv.first_venta THEN 'M2'
      WHEN fc.sku IS NOT NULL AND v.date > fc.first_creacion AND po.sku IS NULL THEN 'M3'
      ELSE 'UNLINKED'
    END AS categoria
  FROM odv_sales v
  LEFT JOIN m1_odv_ids m1o ON v.odv_id = m1o.odv_id AND v.client_id = m1o.client_id
  LEFT JOIN first_venta fv ON v.client_id = fv.client_id AND v.sku = fv.sku
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

CREATE OR REPLACE FUNCTION public.get_dashboard_data(p_doctors character varying[] DEFAULT NULL::character varying[], p_brands character varying[] DEFAULT NULL::character varying[], p_conditions character varying[] DEFAULT NULL::character varying[], p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date)
 RETURNS json
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'analytics'
AS $function$ SELECT analytics.get_dashboard_data(p_doctors, p_brands, p_conditions, p_start_date, p_end_date); $function$;

-- ============================================================================
-- NOTIFY and COMMIT
-- ============================================================================
NOTIFY pgrst, 'reload schema';

COMMIT;
