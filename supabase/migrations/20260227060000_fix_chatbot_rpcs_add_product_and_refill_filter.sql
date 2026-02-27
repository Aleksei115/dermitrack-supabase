-- Migration: Add `product` column to chatbot RPCs + defensive filter in refill recommendations
-- Fixes: hallucinated product names (long description -> short product name) + refill recommending already-purchased products

-- Must DROP first because RETURNS TABLE signature changed (added `product` column)
DROP FUNCTION IF EXISTS public.get_complete_sales_ranking(INTEGER);
DROP FUNCTION IF EXISTS chatbot.get_complete_sales_ranking(INTEGER);
DROP FUNCTION IF EXISTS public.get_doctor_inventory(VARCHAR, VARCHAR, BOOLEAN);
DROP FUNCTION IF EXISTS chatbot.get_doctor_inventory(VARCHAR, VARCHAR, BOOLEAN);
DROP FUNCTION IF EXISTS public.get_refill_recommendations(VARCHAR);
DROP FUNCTION IF EXISTS chatbot.get_refill_recommendations(VARCHAR);

-- ============================================================================
-- Fix E: Add `product` to chatbot.get_complete_sales_ranking
-- ============================================================================

CREATE OR REPLACE FUNCTION chatbot.get_complete_sales_ranking(p_limit_count INTEGER DEFAULT 20)
RETURNS TABLE(
  sku VARCHAR, product VARCHAR, description TEXT, brand VARCHAR,
  piezas_botiquin INTEGER, piezas_conversion INTEGER, piezas_exposicion INTEGER, piezas_totales INTEGER,
  ventas_botiquin NUMERIC, ventas_conversion NUMERIC, ventas_exposicion NUMERIC, ventas_totales NUMERIC
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  RETURN QUERY
  WITH impacts AS (
    SELECT d.sku, d.quantity, d.value, 'M1'::text AS type
    FROM analytics.get_impact_detail('M1') d
    UNION ALL
    SELECT d.sku, d.quantity, d.value, 'M2'::text
    FROM analytics.get_impact_detail('M2') d
    UNION ALL
    SELECT d.sku, d.quantity, d.value, 'M3'::text
    FROM analytics.get_impact_detail('M3') d
  )
  SELECT
    i.sku::VARCHAR,
    m.product::VARCHAR,
    m.description::TEXT,
    m.brand::VARCHAR,
    SUM(CASE WHEN i.type = 'M1' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_botiquin,
    SUM(CASE WHEN i.type = 'M2' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_conversion,
    SUM(CASE WHEN i.type = 'M3' THEN i.quantity ELSE 0 END)::INTEGER AS piezas_exposicion,
    SUM(i.quantity)::INTEGER AS piezas_totales,
    ROUND(SUM(CASE WHEN i.type = 'M1' THEN i.value ELSE 0 END), 2) AS ventas_botiquin,
    ROUND(SUM(CASE WHEN i.type = 'M2' THEN i.value ELSE 0 END), 2) AS ventas_conversion,
    ROUND(SUM(CASE WHEN i.type = 'M3' THEN i.value ELSE 0 END), 2) AS ventas_exposicion,
    ROUND(SUM(i.value), 2) AS ventas_totales
  FROM impacts i
  JOIN medications m ON m.sku = i.sku
  GROUP BY i.sku, m.product, m.description, m.brand
  ORDER BY SUM(i.value) DESC
  LIMIT p_limit_count;
END;
$$;

-- ============================================================================
-- Fix F: Add `product` to chatbot.get_doctor_inventory
-- ============================================================================

CREATE OR REPLACE FUNCTION chatbot.get_doctor_inventory(
  p_client_id VARCHAR,
  p_user_id VARCHAR,
  p_is_admin BOOLEAN DEFAULT false
)
RETURNS TABLE(
  sku VARCHAR, product VARCHAR, description TEXT, brand VARCHAR,
  content VARCHAR, available_quantity INTEGER, price NUMERIC
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF NOT p_is_admin THEN
    IF NOT EXISTS (
      SELECT 1 FROM clients c
      WHERE c.client_id = p_client_id AND c.user_id = p_user_id
    ) THEN
      RETURN;
    END IF;
  END IF;

  RETURN QUERY
  SELECT
    m.sku,
    m.product,
    m.description,
    m.brand,
    m.content,
    ib.available_quantity,
    m.price
  FROM cabinet_inventory ib
  JOIN medications m ON m.sku = ib.sku
  WHERE ib.client_id = p_client_id
    AND ib.available_quantity > 0
  ORDER BY m.brand, m.sku;
END;
$$;

-- ============================================================================
-- Fix I: Add `product` + defensive filters to chatbot.get_refill_recommendations
-- Excludes products the doctor already buys via ODV or has SALE movements
-- ============================================================================

CREATE OR REPLACE FUNCTION chatbot.get_refill_recommendations(p_client_id VARCHAR)
RETURNS TABLE(
  sku VARCHAR, product VARCHAR, description TEXT, brand VARCHAR, price NUMERIC,
  was_in_cabinet BOOLEAN, global_sales_pieces INTEGER, global_sales_value NUMERIC,
  recommendation TEXT
)
LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE
  v_top_threshold NUMERIC;
  v_mid_threshold NUMERIC;
BEGIN
  -- Calculate sales thresholds for recommendation labels
  SELECT
    PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY COALESCE(sales.total_qty, 0)),
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY COALESCE(sales.total_qty, 0))
  INTO v_top_threshold, v_mid_threshold
  FROM cabinet_client_available_skus cas2
  LEFT JOIN (
    SELECT im.sku AS s, SUM(im.quantity) AS total_qty
    FROM inventory_movements im
    WHERE im.type = 'SALE'
    GROUP BY im.sku
  ) sales ON sales.s = cas2.sku
  WHERE cas2.client_id = p_client_id;

  -- Default thresholds if no data
  IF v_top_threshold IS NULL THEN v_top_threshold := 10; END IF;
  IF v_mid_threshold IS NULL THEN v_mid_threshold := 5; END IF;

  RETURN QUERY
  SELECT
    cas.sku::VARCHAR,
    m.product::VARCHAR,
    m.description::TEXT,
    m.brand::VARCHAR,
    m.price,
    -- was_in_cabinet: TRUE if there's any historical inventory movement for this client+sku
    EXISTS(
      SELECT 1 FROM inventory_movements im2
      WHERE im2.client_id = p_client_id AND im2.sku = cas.sku
    ) AS was_in_cabinet,
    COALESCE(global_sales.total_pieces, 0)::INTEGER AS global_sales_pieces,
    COALESCE(global_sales.total_value, 0)::NUMERIC AS global_sales_value,
    CASE
      WHEN COALESCE(global_sales.total_pieces, 0) >= v_top_threshold THEN 'Alta demanda - recomendado'
      WHEN COALESCE(global_sales.total_pieces, 0) >= v_mid_threshold THEN 'Demanda media'
      ELSE 'Baja rotacion'
    END::TEXT AS recommendation
  FROM cabinet_client_available_skus cas
  JOIN medications m ON m.sku = cas.sku
  -- Exclude SKUs already in stock
  LEFT JOIN cabinet_inventory ci ON ci.client_id = cas.client_id AND ci.sku = cas.sku
  -- Global sales ranking
  LEFT JOIN (
    SELECT
      im.sku AS s,
      SUM(im.quantity) AS total_pieces,
      SUM(im.quantity * COALESCE(im.unit_price, 0)) AS total_value
    FROM inventory_movements im
    WHERE im.type = 'SALE'
    GROUP BY im.sku
  ) global_sales ON global_sales.s = cas.sku
  WHERE cas.client_id = p_client_id
    AND (ci.available_quantity IS NULL OR ci.available_quantity = 0)
    -- Fix I: Exclude products the doctor already buys via ODV
    AND NOT EXISTS (
      SELECT 1 FROM odv_sales os
      WHERE os.client_id = p_client_id AND os.sku = cas.sku
    )
    -- Fix I: Exclude products with SALE movements for this client (double safety)
    AND NOT EXISTS (
      SELECT 1 FROM inventory_movements im
      WHERE im.client_id = p_client_id AND im.sku = cas.sku AND im.type = 'SALE'
    )
  ORDER BY COALESCE(global_sales.total_pieces, 0) DESC
  LIMIT 15;
END;
$$;

-- Notify PostgREST to pick up new function signatures
NOTIFY pgrst, 'reload schema';
