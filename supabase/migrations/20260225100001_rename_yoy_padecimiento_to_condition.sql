-- =============================================================================
-- Rename get_yoy_padecimiento → get_yoy_by_condition (English schema rename)
-- =============================================================================

-- 1. Create new analytics function with English name
CREATE OR REPLACE FUNCTION analytics.get_yoy_by_condition(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(condition character varying, year integer, valor numeric, growth numeric)
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

-- 2. Create new public wrapper
CREATE OR REPLACE FUNCTION public.get_yoy_by_condition(
  p_doctors character varying[] DEFAULT NULL::character varying[],
  p_brands character varying[] DEFAULT NULL::character varying[],
  p_conditions character varying[] DEFAULT NULL::character varying[],
  p_start_date date DEFAULT NULL::date,
  p_end_date date DEFAULT NULL::date
)
RETURNS TABLE(condition character varying, year integer, valor numeric, growth numeric)
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
  SELECT * FROM analytics.get_yoy_by_condition(p_doctors, p_brands, p_conditions, p_start_date, p_end_date);
$function$;

-- 3. Grant access to new functions
GRANT EXECUTE ON FUNCTION public.get_yoy_by_condition(character varying[], character varying[], character varying[], date, date) TO authenticated;

-- 4. Drop old functions
DROP FUNCTION IF EXISTS public.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date);
DROP FUNCTION IF EXISTS analytics.get_yoy_padecimiento(character varying[], character varying[], character varying[], date, date);

-- 5. Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
