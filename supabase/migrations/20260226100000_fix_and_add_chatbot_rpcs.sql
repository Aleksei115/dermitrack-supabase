-- ============================================================================
-- Fix broken chatbot RPCs (d.valor → d.value) + add 2 new RPCs
-- ============================================================================

-- 1a. Fix chatbot.get_complete_sales_ranking — d.valor → d.value
CREATE OR REPLACE FUNCTION chatbot.get_complete_sales_ranking(p_limit_count integer DEFAULT 20)
RETURNS TABLE(
  sku character varying,
  description text,
  brand character varying,
  piezas_botiquin integer,
  piezas_conversion integer,
  piezas_exposicion integer,
  piezas_totales integer,
  ventas_botiquin numeric,
  ventas_conversion numeric,
  ventas_exposicion numeric,
  ventas_totales numeric
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
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
  GROUP BY i.sku, m.description, m.brand
  ORDER BY SUM(i.value) DESC
  LIMIT p_limit_count;
END;
$$;

-- 1b. Fix chatbot.get_complete_brand_performance — d.valor → d.value
CREATE OR REPLACE FUNCTION chatbot.get_complete_brand_performance()
RETURNS TABLE(
  brand character varying,
  piezas_botiquin integer,
  piezas_conversion integer,
  piezas_exposicion integer,
  piezas_totales integer,
  ventas_botiquin numeric,
  ventas_conversion numeric,
  ventas_exposicion numeric,
  ventas_totales numeric
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
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
  GROUP BY m.brand
  ORDER BY SUM(i.value) DESC;
END;
$$;

-- ============================================================================
-- 1c. New RPC: chatbot.get_visit_status
-- Returns visit status for current cutoff period
-- ============================================================================
CREATE OR REPLACE FUNCTION chatbot.get_visit_status(
  p_user_id VARCHAR DEFAULT NULL,
  p_is_admin BOOLEAN DEFAULT FALSE
)
RETURNS TABLE(
  client_id VARCHAR,
  client_name VARCHAR,
  visit_type TEXT,
  visit_status TEXT,
  saga_status TEXT,
  tasks_completed INTEGER,
  tasks_total INTEGER,
  created_at TIMESTAMPTZ
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_cutoff_start DATE;
  v_cutoff_end DATE;
BEGIN
  -- Get current cutoff range
  SELECT r.fecha_inicio, r.fecha_fin
  INTO v_cutoff_start, v_cutoff_end
  FROM analytics.get_current_cutoff_range() r;

  -- Fallback to last 30 days if no cutoff detected
  IF v_cutoff_start IS NULL THEN
    v_cutoff_start := CURRENT_DATE - INTERVAL '30 days';
    v_cutoff_end := CURRENT_DATE;
  END IF;

  RETURN QUERY
  SELECT
    v.client_id::VARCHAR,
    c.client_name::VARCHAR,
    v.type::TEXT AS visit_type,
    v.status::TEXT AS visit_status,
    v.saga_status::TEXT,
    COALESCE(task_counts.completed, 0)::INTEGER AS tasks_completed,
    COALESCE(task_counts.total, 0)::INTEGER AS tasks_total,
    v.created_at
  FROM visits v
  JOIN clients c ON c.client_id = v.client_id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*) FILTER (WHERE vt.status = 'COMPLETED') AS completed,
      COUNT(*) AS total
    FROM visit_tasks vt
    WHERE vt.visit_id = v.visit_id
  ) task_counts ON TRUE
  WHERE v.created_at::date >= v_cutoff_start
    AND v.created_at::date <= v_cutoff_end
    AND (p_is_admin OR v.user_id = p_user_id)
  ORDER BY
    CASE v.status
      WHEN 'IN_PROGRESS' THEN 1
      WHEN 'SCHEDULED' THEN 2
      WHEN 'PENDING' THEN 3
      WHEN 'DELAYED' THEN 4
      WHEN 'COMPLETED' THEN 5
      WHEN 'CANCELLED' THEN 6
      ELSE 7
    END,
    v.created_at DESC;
END;
$$;

-- ============================================================================
-- 1d. New RPC: chatbot.get_refill_recommendations
-- Recommends SKUs to refill for a doctor based on assigned SKUs vs current inventory
-- ============================================================================
CREATE OR REPLACE FUNCTION chatbot.get_refill_recommendations(
  p_client_id VARCHAR
)
RETURNS TABLE(
  sku VARCHAR,
  description TEXT,
  brand VARCHAR,
  price NUMERIC,
  was_in_cabinet BOOLEAN,
  global_sales_pieces INTEGER,
  global_sales_value NUMERIC,
  recommendation TEXT
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
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
  ORDER BY COALESCE(global_sales.total_pieces, 0) DESC
  LIMIT 15;
END;
$$;

-- Grant access through chatbot schema
GRANT USAGE ON SCHEMA chatbot TO authenticated, anon;
