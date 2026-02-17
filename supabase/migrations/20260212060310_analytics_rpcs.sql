-- Analytics RPCs: move client-side useMemo calculations to server-side
-- This migration creates the analytics schema and 12 RPC functions

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant usage to authenticated and anon roles (needed for PostgREST)
GRANT USAGE ON SCHEMA analytics TO authenticated, anon;

--------------------------------------------------------------------------------
-- HELPER: Get first padecimiento name for a SKU (deterministic pick)
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytics._padecimiento_for_sku(p_sku varchar)
RETURNS varchar
LANGUAGE sql STABLE
SET search_path TO 'public'
AS $$
  SELECT COALESCE(p.nombre, 'OTROS')
  FROM medicamento_padecimientos mp
  JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
  WHERE mp.sku = p_sku
  ORDER BY p.id_padecimiento
  LIMIT 1;
$$;

--------------------------------------------------------------------------------
-- A) IMPACTO DEL BOTIQUIN
--------------------------------------------------------------------------------

-- A1) get_impacto_botiquin_resumen: Summary of all 4 impact metrics
CREATE OR REPLACE FUNCTION analytics.get_impacto_botiquin_resumen()
RETURNS TABLE (
  adopciones int,
  revenue_adopciones numeric,
  conversiones int,
  revenue_conversiones numeric,
  exposiciones int,
  revenue_exposiciones numeric,
  crosssell_pares int,
  revenue_crosssell numeric,
  revenue_total_impacto numeric,
  revenue_total_odv numeric,
  porcentaje_impacto numeric
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_adopciones int := 0;
  v_rev_adopciones numeric := 0;
  v_conversiones int := 0;
  v_rev_conversiones numeric := 0;
  v_exposiciones int := 0;
  v_rev_exposiciones numeric := 0;
  v_crosssell int := 0;
  v_rev_crosssell numeric := 0;
  v_total_odv numeric := 0;
BEGIN
  -- M1: Adopciones = distinct (id_cliente, sku) pairs with VENTA in movimientos_inventario
  -- Revenue = sum(cantidad * medicamentos.precio) for those VENTA movements
  SELECT COUNT(*), COALESCE(SUM(total_val), 0)
  INTO v_adopciones, v_rev_adopciones
  FROM (
    SELECT mi.id_cliente, mi.sku,
           SUM(mi.cantidad * m.precio) AS total_val
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, mi.sku
  ) sub;

  -- M2: Conversiones = adopciones that also have a ventas_odv record after the first VENTA date
  SELECT COUNT(*), COALESCE(SUM(odv_val), 0)
  INTO v_conversiones, v_rev_conversiones
  FROM (
    SELECT a.id_cliente, a.sku,
           (SELECT SUM(v.cantidad * v.precio)
            FROM ventas_odv v
            WHERE v.id_cliente = a.id_cliente
              AND v.sku = a.sku
              AND v.fecha > a.first_venta
           ) AS odv_val
    FROM (
      SELECT mi.id_cliente, mi.sku,
             MIN(mi.fecha_movimiento::date) AS first_venta
      FROM movimientos_inventario mi
      WHERE mi.tipo = 'VENTA'
      GROUP BY mi.id_cliente, mi.sku
    ) a
    WHERE EXISTS (
      SELECT 1 FROM ventas_odv v
      WHERE v.id_cliente = a.id_cliente
        AND v.sku = a.sku
        AND v.fecha > a.first_venta
    )
  ) sub;

  -- M3: Exposiciones = (id_cliente, sku) with CREACION but NO VENTA in botiquin,
  --     that have purchases in ventas_odv AFTER the first CREACION date,
  --     excluding clients who already bought that SKU before the botiquin existed
  SELECT COUNT(*), COALESCE(SUM(odv_val), 0)
  INTO v_exposiciones, v_rev_exposiciones
  FROM (
    SELECT cs.id_cliente, cs.sku,
           (SELECT SUM(v.cantidad * v.precio)
            FROM ventas_odv v
            WHERE v.id_cliente = cs.id_cliente
              AND v.sku = cs.sku
              AND v.fecha > cs.first_creacion
           ) AS odv_val
    FROM (
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
    ) cs
    WHERE -- Must have ODV after botiquin creation
          EXISTS (
            SELECT 1 FROM ventas_odv v
            WHERE v.id_cliente = cs.id_cliente
              AND v.sku = cs.sku
              AND v.fecha > cs.first_creacion
          )
          -- Exclude clients who already bought this SKU before botiquin
          AND NOT EXISTS (
            SELECT 1 FROM ventas_odv v2
            WHERE v2.id_cliente = cs.id_cliente
              AND v2.sku = cs.sku
              AND v2.fecha <= cs.first_creacion
          )
  ) sub;

  -- M4: Cross-sell = new (id_cliente, sku) pairs by padecimiento
  -- Client buys via botiquin one padecimiento, then buys a DIFFERENT sku
  -- from the same padecimiento in ventas_odv (that was never in their botiquin)
  SELECT COUNT(*), COALESCE(SUM(odv_val), 0)
  INTO v_crosssell, v_rev_crosssell
  FROM (
    SELECT v.id_cliente, v.sku,
           SUM(v.cantidad * v.precio) AS odv_val
    FROM ventas_odv v
    JOIN medicamento_padecimientos mp_v ON mp_v.sku = v.sku
    WHERE EXISTS (
      -- The client has a botiquin VENTA for a different SKU in the same padecimiento
      SELECT 1
      FROM movimientos_inventario mi
      JOIN medicamento_padecimientos mp_mi ON mp_mi.sku = mi.sku
      WHERE mi.id_cliente = v.id_cliente
        AND mi.tipo = 'VENTA'
        AND mp_mi.id_padecimiento = mp_v.id_padecimiento
        AND mi.sku <> v.sku
    )
    AND NOT EXISTS (
      -- Exclude if this exact (client, sku) was already in botiquin
      SELECT 1
      FROM movimientos_inventario mi2
      WHERE mi2.id_cliente = v.id_cliente AND mi2.sku = v.sku
    )
    GROUP BY v.id_cliente, v.sku
  ) sub;

  -- Total ODV revenue
  SELECT COALESCE(SUM(cantidad * precio), 0)
  INTO v_total_odv
  FROM ventas_odv;

  RETURN QUERY SELECT
    v_adopciones,
    v_rev_adopciones,
    v_conversiones,
    v_rev_conversiones,
    v_exposiciones,
    v_rev_exposiciones,
    v_crosssell,
    v_rev_crosssell,
    (v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones + v_rev_crosssell),
    v_total_odv,
    CASE WHEN v_total_odv > 0
      THEN ROUND(((v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones + v_rev_crosssell) / v_total_odv) * 100, 1)
      ELSE 0
    END;
END;
$$;

-- A2) get_impacto_detalle: Detail for any metric (M1/M2/M3/M4)
CREATE OR REPLACE FUNCTION analytics.get_impacto_detalle(p_metrica text)
RETURNS TABLE (
  id_cliente varchar,
  nombre_cliente varchar,
  sku varchar,
  producto varchar,
  cantidad int,
  precio numeric,
  valor numeric,
  fecha date,
  detalle text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  IF p_metrica = 'M1' THEN
    -- Adopciones: VENTA pairs with their revenue (catalog price)
    RETURN QUERY
    SELECT mi.id_cliente, c.nombre_cliente, mi.sku, m.producto,
           SUM(mi.cantidad)::int AS cantidad,
           m.precio,
           SUM(mi.cantidad * m.precio) AS valor,
           MIN(mi.fecha_movimiento::date) AS fecha,
           'Adopción en botiquín'::text AS detalle
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    JOIN clientes c ON c.id_cliente = mi.id_cliente
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, c.nombre_cliente, mi.sku, m.producto, m.precio
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M2' THEN
    -- Conversiones: adopciones that also have ODV
    RETURN QUERY
    SELECT a.id_cliente, c.nombre_cliente, a.sku, m.producto,
           COALESCE(odv.total_qty, 0)::int AS cantidad,
           COALESCE(odv.avg_price, 0) AS precio,
           COALESCE(odv.total_val, 0) AS valor,
           odv.first_fecha AS fecha,
           ('ODV después de botiquín (' || a.first_venta::text || ')')::text AS detalle
    FROM (
      SELECT mi.id_cliente, mi.sku,
             MIN(mi.fecha_movimiento::date) AS first_venta
      FROM movimientos_inventario mi
      WHERE mi.tipo = 'VENTA'
      GROUP BY mi.id_cliente, mi.sku
    ) a
    JOIN clientes c ON c.id_cliente = a.id_cliente
    JOIN medicamentos m ON m.sku = a.sku
    JOIN LATERAL (
      SELECT SUM(v.cantidad) AS total_qty,
             ROUND(AVG(v.precio), 2) AS avg_price,
             SUM(v.cantidad * v.precio) AS total_val,
             MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = a.id_cliente
        AND v.sku = a.sku
        AND v.fecha > a.first_venta
    ) odv ON odv.total_qty > 0
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M3' THEN
    -- Exposiciones: CREACION sin VENTA pero con ODV post-botiquín,
    -- excluyendo clientes que ya compraban ese SKU antes del botiquín
    RETURN QUERY
    SELECT cs.id_cliente, c.nombre_cliente, cs.sku, m.producto,
           COALESCE(odv.total_qty, 0)::int AS cantidad,
           COALESCE(odv.avg_price, 0) AS precio,
           COALESCE(odv.total_val, 0) AS valor,
           odv.first_fecha AS fecha,
           ('Exposición post-botiquín (' || cs.first_creacion::text || ')')::text AS detalle
    FROM (
      SELECT mi.id_cliente, mi.sku,
             MIN(mi.fecha_movimiento::date) AS first_creacion
      FROM movimientos_inventario mi
      WHERE mi.tipo = 'CREACION'
        AND NOT EXISTS (
          SELECT 1 FROM movimientos_inventario mi2
          WHERE mi2.id_cliente = mi.id_cliente AND mi2.sku = mi.sku AND mi2.tipo = 'VENTA'
        )
      GROUP BY mi.id_cliente, mi.sku
    ) cs
    JOIN clientes c ON c.id_cliente = cs.id_cliente
    JOIN medicamentos m ON m.sku = cs.sku
    JOIN LATERAL (
      SELECT SUM(v.cantidad) AS total_qty,
             ROUND(AVG(v.precio), 2) AS avg_price,
             SUM(v.cantidad * v.precio) AS total_val,
             MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      WHERE v.id_cliente = cs.id_cliente
        AND v.sku = cs.sku
        AND v.fecha > cs.first_creacion
    ) odv ON odv.total_qty > 0
    WHERE -- Exclude clients who already bought this SKU before botiquin
          NOT EXISTS (
            SELECT 1 FROM ventas_odv v2
            WHERE v2.id_cliente = cs.id_cliente
              AND v2.sku = cs.sku
              AND v2.fecha <= cs.first_creacion
          )
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M4' THEN
    -- Cross-sell: new SKU from same padecimiento
    RETURN QUERY
    SELECT v_agg.id_cliente, c.nombre_cliente, v_agg.sku, m.producto,
           v_agg.total_qty::int AS cantidad,
           v_agg.avg_price AS precio,
           v_agg.total_val AS valor,
           v_agg.first_fecha AS fecha,
           ('Cross-sell padecimiento: ' || analytics._padecimiento_for_sku(v_agg.sku))::text AS detalle
    FROM (
      SELECT v.id_cliente, v.sku,
             SUM(v.cantidad) AS total_qty,
             ROUND(AVG(v.precio), 2) AS avg_price,
             SUM(v.cantidad * v.precio) AS total_val,
             MIN(v.fecha) AS first_fecha
      FROM ventas_odv v
      JOIN medicamento_padecimientos mp_v ON mp_v.sku = v.sku
      WHERE EXISTS (
        SELECT 1
        FROM movimientos_inventario mi
        JOIN medicamento_padecimientos mp_mi ON mp_mi.sku = mi.sku
        WHERE mi.id_cliente = v.id_cliente
          AND mi.tipo = 'VENTA'
          AND mp_mi.id_padecimiento = mp_v.id_padecimiento
          AND mi.sku <> v.sku
      )
      AND NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = v.id_cliente AND mi2.sku = v.sku
      )
      GROUP BY v.id_cliente, v.sku
    ) v_agg
    JOIN clientes c ON c.id_cliente = v_agg.id_cliente
    JOIN medicamentos m ON m.sku = v_agg.sku
    ORDER BY valor DESC;

  ELSE
    RAISE EXCEPTION 'Métrica inválida: %. Use M1, M2, M3 o M4.', p_metrica;
  END IF;
END;
$$;

--------------------------------------------------------------------------------
-- B) DASHBOARD COMPUTATIONS (replace client-side useMemo)
--------------------------------------------------------------------------------

-- B1) get_brand_performance: Sales by brand from movimientos_inventario
CREATE OR REPLACE FUNCTION analytics.get_brand_performance()
RETURNS TABLE (
  marca varchar,
  valor numeric,
  piezas int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT m.marca,
         SUM(mi.cantidad * m.precio) AS valor,
         SUM(mi.cantidad)::int AS piezas
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  WHERE mi.tipo = 'VENTA'
  GROUP BY m.marca
  ORDER BY valor DESC;
$$;

-- B2) get_padecimiento_performance: Sales by padecimiento
CREATE OR REPLACE FUNCTION analytics.get_padecimiento_performance()
RETURNS TABLE (
  padecimiento varchar,
  valor numeric,
  piezas int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT COALESCE(p.nombre, 'OTROS') AS padecimiento,
         SUM(mi.cantidad * m.precio) AS valor,
         SUM(mi.cantidad)::int AS piezas
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  LEFT JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
  LEFT JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
  WHERE mi.tipo = 'VENTA'
  GROUP BY COALESCE(p.nombre, 'OTROS')
  ORDER BY valor DESC;
$$;

-- B3) get_adoption_metrics: Adoption speed and timeline
CREATE OR REPLACE FUNCTION analytics.get_adoption_metrics()
RETURNS TABLE (
  avg_dias_adopcion int,
  avg_periodos_adopcion numeric,
  total_adopciones int,
  timeline jsonb
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_avg_dias int;
  v_total int;
  v_timeline jsonb;
BEGIN
  -- Calculate adoption speed: days from first CREACION to first VENTA per (id_cliente, sku)
  WITH pairs AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(CASE WHEN mi.tipo = 'CREACION' THEN mi.fecha_movimiento END) AS first_creacion,
           MIN(CASE WHEN mi.tipo = 'VENTA' THEN mi.fecha_movimiento END) AS first_venta
    FROM movimientos_inventario mi
    WHERE mi.tipo IN ('CREACION', 'VENTA')
    GROUP BY mi.id_cliente, mi.sku
    HAVING MIN(CASE WHEN mi.tipo = 'VENTA' THEN mi.fecha_movimiento END) IS NOT NULL
       AND MIN(CASE WHEN mi.tipo = 'CREACION' THEN mi.fecha_movimiento END) IS NOT NULL
  ),
  speeds AS (
    SELECT id_cliente, sku,
           GREATEST(0, EXTRACT(EPOCH FROM (first_venta - first_creacion)) / 86400)::int AS dias
    FROM pairs
  )
  SELECT COALESCE(ROUND(AVG(dias)), 0)::int, COUNT(*)::int
  INTO v_avg_dias, v_total
  FROM speeds;

  -- Timeline: adoptions (first VENTA) by month
  WITH first_ventas AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(mi.fecha_movimiento) AS first_venta
    FROM movimientos_inventario mi
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, mi.sku
  )
  SELECT COALESCE(jsonb_agg(row_to_json(sub)::jsonb ORDER BY sub.mes), '[]'::jsonb)
  INTO v_timeline
  FROM (
    SELECT TO_CHAR(first_venta, 'YYYY-MM') AS mes,
           COUNT(*)::int AS adopciones
    FROM first_ventas
    GROUP BY TO_CHAR(first_venta, 'YYYY-MM')
  ) sub;

  RETURN QUERY SELECT v_avg_dias,
    ROUND(v_avg_dias / 15.0, 1),
    v_total,
    v_timeline;
END;
$$;

-- B4) get_product_interest: Movement types per product
CREATE OR REPLACE FUNCTION analytics.get_product_interest(p_limit int DEFAULT 15)
RETURNS TABLE (
  producto varchar,
  venta int,
  creacion int,
  recoleccion int,
  stock_activo int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT
    CASE WHEN LENGTH(m.producto) > 20 THEN LEFT(m.producto, 20) || '...' ELSE m.producto END AS producto,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::int AS venta,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)::int AS creacion,
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::int AS recoleccion,
    GREATEST(0,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)
    )::int AS stock_activo
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  WHERE mi.tipo IN ('VENTA', 'CREACION', 'RECOLECCION')
  GROUP BY m.producto
  ORDER BY (
    COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0) +
    GREATEST(0,
      COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'CREACION'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)
      - COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)
    )
  ) DESC
  LIMIT p_limit;
$$;

-- B5) get_opportunity_matrix: Opportunity by padecimiento
CREATE OR REPLACE FUNCTION analytics.get_opportunity_matrix()
RETURNS TABLE (
  padecimiento varchar,
  venta int,
  recoleccion int,
  valor numeric,
  converted_qty int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  WITH botiquin_by_pad AS (
    SELECT COALESCE(p.nombre, 'OTROS') AS padecimiento,
           COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'VENTA'), 0)::int AS venta,
           COALESCE(SUM(mi.cantidad) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0)::int AS recoleccion,
           COALESCE(SUM(mi.cantidad * m.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0) AS valor
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    LEFT JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
    LEFT JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    WHERE mi.tipo IN ('VENTA', 'RECOLECCION')
    GROUP BY COALESCE(p.nombre, 'OTROS')
  ),
  converted AS (
    -- Count ODV qty for pairs that have both botiquin VENTA and ODV
    SELECT COALESCE(p.nombre, 'OTROS') AS padecimiento,
           COALESCE(SUM(v.cantidad), 0)::int AS converted_qty
    FROM (
      SELECT mi.id_cliente, mi.sku,
             MIN(mi.fecha_movimiento::date) AS first_venta
      FROM movimientos_inventario mi
      WHERE mi.tipo = 'VENTA'
      GROUP BY mi.id_cliente, mi.sku
    ) bv
    JOIN ventas_odv v ON v.id_cliente = bv.id_cliente AND v.sku = bv.sku AND v.fecha > bv.first_venta
    JOIN medicamento_padecimientos mp ON mp.sku = bv.sku
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    GROUP BY COALESCE(p.nombre, 'OTROS')
  )
  SELECT bp.padecimiento, bp.venta, bp.recoleccion, bp.valor,
         COALESCE(cv.converted_qty, 0)::int AS converted_qty
  FROM botiquin_by_pad bp
  LEFT JOIN converted cv ON cv.padecimiento = bp.padecimiento
  ORDER BY bp.valor DESC;
$$;

-- B6) get_yoy_padecimiento: Year-over-Year by padecimiento
CREATE OR REPLACE FUNCTION analytics.get_yoy_padecimiento()
RETURNS TABLE (
  padecimiento varchar,
  anio int,
  valor numeric,
  crecimiento numeric
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
BEGIN
  RETURN QUERY
  WITH yearly AS (
    SELECT COALESCE(p.nombre, 'OTROS') AS padecimiento,
           EXTRACT(YEAR FROM mi.fecha_movimiento)::int AS anio,
           SUM(mi.cantidad * m.precio) AS valor
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    LEFT JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
    LEFT JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    WHERE mi.tipo = 'VENTA'
    GROUP BY COALESCE(p.nombre, 'OTROS'), EXTRACT(YEAR FROM mi.fecha_movimiento)::int
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
$$;

-- B7) get_top_converting_skus: Top SKUs by conversion performance
CREATE OR REPLACE FUNCTION analytics.get_top_converting_skus(p_limit int DEFAULT 10)
RETURNS TABLE (
  sku varchar,
  producto varchar,
  conversiones int,
  avg_dias int,
  roi numeric,
  valor_generado numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  WITH botiquin_pairs AS (
    SELECT mi.id_cliente, mi.sku,
           MIN(CASE WHEN mi.tipo = 'CREACION' THEN mi.fecha_movimiento END) AS first_creacion,
           MIN(CASE WHEN mi.tipo = 'VENTA' THEN mi.fecha_movimiento END) AS first_venta,
           SUM(CASE WHEN mi.tipo = 'CREACION' THEN mi.cantidad * m.precio ELSE 0 END) AS invest
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo IN ('CREACION', 'VENTA')
    GROUP BY mi.id_cliente, mi.sku
    HAVING MIN(CASE WHEN mi.tipo = 'VENTA' THEN mi.fecha_movimiento END) IS NOT NULL
  ),
  with_odv AS (
    SELECT bp.id_cliente, bp.sku, bp.first_venta, bp.invest,
           GREATEST(0, EXTRACT(EPOCH FROM (bp.first_venta - bp.first_creacion)) / 86400)::int AS dias,
           COALESCE(SUM(v.cantidad * v.precio), 0) AS odv_val
    FROM botiquin_pairs bp
    LEFT JOIN ventas_odv v ON v.id_cliente = bp.id_cliente
                          AND v.sku = bp.sku
                          AND v.fecha > bp.first_venta::date
    GROUP BY bp.id_cliente, bp.sku, bp.first_venta, bp.first_creacion, bp.invest
    HAVING COALESCE(SUM(v.cantidad * v.precio), 0) > 0
  )
  SELECT wo.sku, m.producto,
         COUNT(*)::int AS conversiones,
         ROUND(AVG(wo.dias))::int AS avg_dias,
         CASE WHEN SUM(wo.invest) > 0
              THEN ROUND(SUM(wo.odv_val) / SUM(wo.invest), 1)
              ELSE 0
         END AS roi,
         SUM(wo.odv_val) AS valor_generado
  FROM with_odv wo
  JOIN medicamentos m ON m.sku = wo.sku
  GROUP BY wo.sku, m.producto
  ORDER BY valor_generado DESC
  LIMIT p_limit;
$$;

-- B8) get_doctor_performance: Doctor performance (all-time)
CREATE OR REPLACE FUNCTION analytics.get_doctor_performance(p_limit int DEFAULT 30)
RETURNS TABLE (
  nombre_cliente varchar,
  piezas int,
  valor numeric,
  unique_skus int,
  rango varchar,
  facturacion numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT c.nombre_cliente,
         SUM(mi.cantidad)::int AS piezas,
         SUM(mi.cantidad * m.precio) AS valor,
         COUNT(DISTINCT mi.sku)::int AS unique_skus,
         COALESCE(c.rango, 'N/A') AS rango,
         COALESCE(c.facturacion_promedio, 0) AS facturacion
  FROM movimientos_inventario mi
  JOIN medicamentos m ON m.sku = mi.sku
  JOIN clientes c ON c.id_cliente = mi.id_cliente
  WHERE mi.tipo = 'VENTA'
  GROUP BY c.nombre_cliente, c.rango, c.facturacion_promedio
  ORDER BY valor DESC
  LIMIT p_limit;
$$;

-- B9) get_cumulative_movements: Cumulative movements over time
CREATE OR REPLACE FUNCTION analytics.get_cumulative_movements()
RETURNS TABLE (
  fecha date,
  venta_valor numeric,
  creacion_valor numeric,
  recoleccion_valor numeric,
  stock_activo_valor numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  WITH daily AS (
    SELECT mi.fecha_movimiento::date AS fecha,
           COALESCE(SUM(mi.cantidad * m.precio) FILTER (WHERE mi.tipo = 'VENTA'), 0) AS venta,
           COALESCE(SUM(mi.cantidad * m.precio) FILTER (WHERE mi.tipo = 'CREACION'), 0) AS creacion,
           COALESCE(SUM(mi.cantidad * m.precio) FILTER (WHERE mi.tipo = 'RECOLECCION'), 0) AS recoleccion
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo IN ('VENTA', 'CREACION', 'RECOLECCION')
    GROUP BY mi.fecha_movimiento::date
  )
  SELECT d.fecha,
         SUM(d.venta) OVER w AS venta_valor,
         SUM(d.creacion) OVER w AS creacion_valor,
         SUM(d.recoleccion) OVER w AS recoleccion_valor,
         (SUM(d.creacion) OVER w - SUM(d.venta) OVER w - SUM(d.recoleccion) OVER w) AS stock_activo_valor
  FROM daily d
  WINDOW w AS (ORDER BY d.fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ORDER BY d.fecha;
$$;

-- Grant execute on all analytics functions to authenticated and anon
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO authenticated, anon;
