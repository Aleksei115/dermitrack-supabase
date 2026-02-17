-- 1. Backfill ventas_odv.precio from medicamentos where NULL (39 rows)
UPDATE ventas_odv v
SET precio = m.precio
FROM medicamentos m
WHERE m.sku = v.sku
AND v.precio IS NULL;

-- 2. Fix M4 cross-sell in resumen function
-- New M4 logic:
--   Exposure = CREACION in botiquin (doctor saw product in padecimiento P)
--   Cross-sell = doctor buys DIFFERENT SKU in same padecimiento P via ODV
--   Date filter: only ODV purchases AFTER first exposure to P
--   First-time only: doctor was NOT buying that SKU before exposure
--   Excludes saga billing ODVs
CREATE OR REPLACE FUNCTION public.get_impacto_botiquin_resumen()
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
  -- M1: Adopciones
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

  -- M2: Conversiones (excluding saga-linked billing ODVs)
  SELECT COUNT(*), COALESCE(SUM(odv_val), 0)
  INTO v_conversiones, v_rev_conversiones
  FROM (
    SELECT a.id_cliente, a.sku,
           (SELECT SUM(v.cantidad * v.precio)
            FROM ventas_odv v
            WHERE v.id_cliente = a.id_cliente
              AND v.sku = a.sku
              AND v.fecha > a.first_venta
              AND v.odv_id NOT IN (
                SELECT szl.zoho_id FROM saga_zoho_links szl
                WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
              )
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
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    )
  ) sub;

  -- M3: Exposiciones
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
    WHERE EXISTS (
            SELECT 1 FROM ventas_odv v
            WHERE v.id_cliente = cs.id_cliente
              AND v.sku = cs.sku
              AND v.fecha > cs.first_creacion
          )
          AND NOT EXISTS (
            SELECT 1 FROM ventas_odv v2
            WHERE v2.id_cliente = cs.id_cliente
              AND v2.sku = cs.sku
              AND v2.fecha <= cs.first_creacion
          )
  ) sub;

  -- M4: Cross-sell (exposure-based, date-filtered, first-time only)
  SELECT COUNT(*), COALESCE(SUM(total_val), 0)
  INTO v_crosssell, v_rev_crosssell
  FROM (
    SELECT cs.id_cliente, cs.sku,
           (SELECT SUM(v.cantidad * v.precio)
            FROM ventas_odv v
            WHERE v.id_cliente = cs.id_cliente
              AND v.sku = cs.sku
              AND v.fecha > cs.earliest_exposure
              AND v.odv_id NOT IN (
                SELECT szl.zoho_id FROM saga_zoho_links szl
                WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
              )
           ) AS total_val
    FROM (
      -- Unique cross-sell candidates with earliest padecimiento exposure
      SELECT v_d.id_cliente, v_d.sku,
             MIN(exp.first_exposure) AS earliest_exposure
      FROM (SELECT DISTINCT id_cliente, sku FROM ventas_odv) v_d
      JOIN medicamento_padecimientos mp_v ON mp_v.sku = v_d.sku
      JOIN (
        SELECT mi.id_cliente, mp.id_padecimiento,
               MIN(mi.fecha_movimiento::date) AS first_exposure
        FROM movimientos_inventario mi
        JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
        WHERE mi.tipo = 'CREACION'
        GROUP BY mi.id_cliente, mp.id_padecimiento
      ) exp ON exp.id_cliente = v_d.id_cliente
           AND exp.id_padecimiento = mp_v.id_padecimiento
      -- SKU not in botiquin
      WHERE NOT EXISTS (
        SELECT 1 FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = v_d.id_cliente AND mi2.sku = v_d.sku
      )
      GROUP BY v_d.id_cliente, v_d.sku
    ) cs
    -- Has purchases after exposure
    WHERE EXISTS (
      SELECT 1 FROM ventas_odv v
      WHERE v.id_cliente = cs.id_cliente
        AND v.sku = cs.sku
        AND v.fecha > cs.earliest_exposure
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    )
    -- First-time purchase only
    AND NOT EXISTS (
      SELECT 1 FROM ventas_odv v2
      WHERE v2.id_cliente = cs.id_cliente
        AND v2.sku = cs.sku
        AND v2.fecha <= cs.earliest_exposure
    )
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
    (v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones),
    v_total_odv,
    CASE WHEN v_total_odv > 0
      THEN ROUND(((v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones) / v_total_odv) * 100, 1)
      ELSE 0
    END;
END;
$$;

-- 3. Fix M4 cross-sell in detail function
DROP FUNCTION IF EXISTS public.get_impacto_detalle(text);

CREATE OR REPLACE FUNCTION public.get_impacto_detalle(p_metrica text)
RETURNS TABLE (
  id_cliente character varying,
  nombre_cliente character varying,
  sku character varying,
  producto character varying,
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
    RETURN QUERY
    SELECT mi.id_cliente, c.nombre_cliente, mi.sku, m.producto,
           SUM(mi.cantidad)::int AS cantidad,
           m.precio,
           SUM(mi.cantidad * m.precio) AS valor,
           MIN(mi.fecha_movimiento::date) AS fecha,
           'Adopcion en botiquin'::text AS detalle
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    JOIN clientes c ON c.id_cliente = mi.id_cliente
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente, c.nombre_cliente, mi.sku, m.producto, m.precio
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M2' THEN
    RETURN QUERY
    SELECT a.id_cliente, c.nombre_cliente, a.sku, m.producto,
           COALESCE(odv.total_qty, 0)::int AS cantidad,
           COALESCE(odv.avg_price, 0) AS precio,
           COALESCE(odv.total_val, 0) AS valor,
           odv.first_fecha AS fecha,
           ('ODV despues de botiquin (' || a.first_venta::text || ')')::text AS detalle
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
        AND v.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
    ) odv ON odv.total_qty > 0
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M3' THEN
    RETURN QUERY
    SELECT cs.id_cliente, c.nombre_cliente, cs.sku, m.producto,
           COALESCE(odv.total_qty, 0)::int AS cantidad,
           COALESCE(odv.avg_price, 0) AS precio,
           COALESCE(odv.total_val, 0) AS valor,
           odv.first_fecha AS fecha,
           ('Exposicion post-botiquin (' || cs.first_creacion::text || ')')::text AS detalle
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
    WHERE NOT EXISTS (
            SELECT 1 FROM ventas_odv v2
            WHERE v2.id_cliente = cs.id_cliente
              AND v2.sku = cs.sku
              AND v2.fecha <= cs.first_creacion
          )
    ORDER BY valor DESC;

  ELSIF p_metrica = 'M4' THEN
    RETURN QUERY
    SELECT v_agg.id_cliente, c.nombre_cliente, v_agg.sku, m.producto,
           v_agg.total_qty::int AS cantidad,
           v_agg.avg_price AS precio,
           v_agg.total_val AS valor,
           v_agg.first_fecha AS fecha,
           ('Cross-sell ' || COALESCE(v_agg.padecimiento_link, '') || ' (exposicion ' || v_agg.earliest_exposure::text || ')')::text AS detalle
    FROM (
      SELECT cs.id_cliente, cs.sku, cs.earliest_exposure,
             SUM(vo.cantidad) AS total_qty,
             ROUND(AVG(vo.precio), 2) AS avg_price,
             SUM(vo.cantidad * vo.precio) AS total_val,
             MIN(vo.fecha) AS first_fecha,
             (SELECT string_agg(DISTINCT pa.nombre, ', ' ORDER BY pa.nombre)
              FROM medicamento_padecimientos mpa
              JOIN padecimientos pa ON pa.id_padecimiento = mpa.id_padecimiento
              WHERE mpa.sku = cs.sku
                AND EXISTS (
                  SELECT 1 FROM movimientos_inventario mii
                  JOIN medicamento_padecimientos mp2 ON mp2.sku = mii.sku
                  WHERE mii.id_cliente = cs.id_cliente
                    AND mii.tipo = 'CREACION'
                    AND mp2.id_padecimiento = mpa.id_padecimiento
                )
             ) AS padecimiento_link
      FROM (
        -- Unique cross-sell candidates
        SELECT vd.id_cliente AS id_cliente, vd.sku AS sku,
               MIN(expos.first_exposure) AS earliest_exposure
        FROM (SELECT DISTINCT ventas_odv.id_cliente, ventas_odv.sku FROM ventas_odv) vd
        JOIN medicamento_padecimientos mpv ON mpv.sku = vd.sku
        JOIN (
          SELECT mie.id_cliente, mpe.id_padecimiento,
                 MIN(mie.fecha_movimiento::date) AS first_exposure
          FROM movimientos_inventario mie
          JOIN medicamento_padecimientos mpe ON mpe.sku = mie.sku
          WHERE mie.tipo = 'CREACION'
          GROUP BY mie.id_cliente, mpe.id_padecimiento
        ) expos ON expos.id_cliente = vd.id_cliente
             AND expos.id_padecimiento = mpv.id_padecimiento
        WHERE NOT EXISTS (
          SELECT 1 FROM movimientos_inventario mi2
          WHERE mi2.id_cliente = vd.id_cliente AND mi2.sku = vd.sku
        )
        GROUP BY vd.id_cliente, vd.sku
      ) cs
      JOIN ventas_odv vo ON vo.id_cliente = cs.id_cliente
        AND vo.sku = cs.sku
        AND vo.fecha > cs.earliest_exposure
        AND vo.odv_id NOT IN (
          SELECT szl.zoho_id FROM saga_zoho_links szl
          WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
        )
      WHERE NOT EXISTS (
        SELECT 1 FROM ventas_odv v2
        WHERE v2.id_cliente = cs.id_cliente
          AND v2.sku = cs.sku
          AND v2.fecha <= cs.earliest_exposure
      )
      GROUP BY cs.id_cliente, cs.sku, cs.earliest_exposure
    ) v_agg
    JOIN clientes c ON c.id_cliente = v_agg.id_cliente
    JOIN medicamentos m ON m.sku = v_agg.sku
    ORDER BY valor DESC;

  END IF;
END;
$$;

-- 4. Cross-sell significance function
CREATE OR REPLACE FUNCTION public.get_crosssell_significancia()
RETURNS TABLE (
  exposed_total int,
  exposed_with_crosssell int,
  exposed_conversion_pct numeric,
  unexposed_total int,
  unexposed_with_crosssell int,
  unexposed_conversion_pct numeric,
  chi_squared numeric,
  significancia text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
DECLARE
  v_a int; -- exposed + cross-sell
  v_b int; -- exposed + no cross-sell
  v_c int; -- unexposed + cross-sell
  v_d int; -- unexposed + no cross-sell
  v_n int;
  v_chi numeric;
BEGIN
  -- Build contingency table: (doctor, padecimiento) exposed vs unexposed
  WITH exposure AS (
    SELECT mi.id_cliente, mp.id_padecimiento,
           MIN(mi.fecha_movimiento::date) AS first_exposure
    FROM movimientos_inventario mi
    JOIN medicamento_padecimientos mp ON mp.sku = mi.sku
    WHERE mi.tipo = 'CREACION'
    GROUP BY mi.id_cliente, mp.id_padecimiento
  ),
  all_combos AS (
    SELECT d.id_cliente, p.id_padecimiento
    FROM (SELECT DISTINCT id_cliente FROM ventas_odv) d
    CROSS JOIN (SELECT DISTINCT id_padecimiento FROM medicamento_padecimientos) p
  ),
  analysis AS (
    SELECT ac.id_cliente, ac.id_padecimiento,
           e.first_exposure IS NOT NULL AS is_exposed,
           EXISTS (
             SELECT 1 FROM ventas_odv v
             JOIN medicamento_padecimientos mp ON mp.sku = v.sku
               AND mp.id_padecimiento = ac.id_padecimiento
             WHERE v.id_cliente = ac.id_cliente
               AND e.first_exposure IS NOT NULL
               AND v.fecha > e.first_exposure
               AND NOT EXISTS (
                 SELECT 1 FROM movimientos_inventario mi2
                 WHERE mi2.id_cliente = ac.id_cliente AND mi2.sku = v.sku
               )
               AND NOT EXISTS (
                 SELECT 1 FROM ventas_odv v2
                 WHERE v2.id_cliente = ac.id_cliente AND v2.sku = v.sku
                   AND v2.fecha <= e.first_exposure
               )
               AND v.odv_id NOT IN (
                 SELECT szl.zoho_id FROM saga_zoho_links szl
                 WHERE szl.tipo = 'VENTA' AND szl.zoho_id IS NOT NULL
               )
           ) AS has_cross_sell
    FROM all_combos ac
    LEFT JOIN exposure e ON e.id_cliente = ac.id_cliente
      AND e.id_padecimiento = ac.id_padecimiento
  )
  SELECT
    COUNT(*) FILTER (WHERE is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE is_exposed AND NOT has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND has_cross_sell),
    COUNT(*) FILTER (WHERE NOT is_exposed AND NOT has_cross_sell)
  INTO v_a, v_b, v_c, v_d
  FROM analysis;

  v_n := v_a + v_b + v_c + v_d;

  -- Chi-squared with Yates correction
  IF (v_a + v_b) > 0 AND (v_c + v_d) > 0 AND (v_a + v_c) > 0 AND (v_b + v_d) > 0 THEN
    v_chi := v_n::numeric
      * POWER(GREATEST(ABS(v_a::numeric * v_d - v_b::numeric * v_c) - v_n::numeric / 2, 0), 2)
      / ((v_a + v_b)::numeric * (v_c + v_d) * (v_a + v_c) * (v_b + v_d));
  ELSE
    v_chi := 0;
  END IF;

  RETURN QUERY SELECT
    (v_a + v_b)::int,
    v_a,
    CASE WHEN (v_a + v_b) > 0
      THEN ROUND(v_a::numeric / (v_a + v_b) * 100, 1)
      ELSE 0::numeric END,
    (v_c + v_d)::int,
    v_c,
    CASE WHEN (v_c + v_d) > 0
      THEN ROUND(v_c::numeric / (v_c + v_d) * 100, 1)
      ELSE 0::numeric END,
    ROUND(v_chi, 2),
    CASE
      WHEN v_chi > 10.83 THEN 'ALTA (p < 0.001)'
      WHEN v_chi > 6.64 THEN 'MEDIA (p < 0.01)'
      WHEN v_chi > 3.84 THEN 'BAJA (p < 0.05)'
      ELSE 'NO SIGNIFICATIVA'
    END;
END;
$$;
