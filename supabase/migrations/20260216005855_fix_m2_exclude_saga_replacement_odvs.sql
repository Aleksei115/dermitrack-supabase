-- Fix M2 (Conversiones): exclude ODVs linked to VENTA sagas
-- These are replenishment orders, not recurrent doctor purchases.
-- The bug: movimiento fecha_movimiento can be 1-2 days before the linked
-- ODV fecha, so v.fecha > first_venta incorrectly includes replacement ODVs.

-- 1. Fix summary function
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

  -- M2: Conversiones (excluding saga-linked replacement ODVs)
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

  -- M4: Cross-sell
  SELECT COUNT(*), COALESCE(SUM(odv_val), 0)
  INTO v_crosssell, v_rev_crosssell
  FROM (
    SELECT v.id_cliente, v.sku,
           SUM(v.cantidad * v.precio) AS odv_val
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
    (v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones),
    v_total_odv,
    CASE WHEN v_total_odv > 0
      THEN ROUND(((v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones) / v_total_odv) * 100, 1)
      ELSE 0
    END;
END;
$$;

-- 2. Fix detail function (M2 section only)
-- Must DROP first because return types use varchar, not text
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
           'Adopción en botiquín'::text AS detalle
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
        SELECT 1
        FROM movimientos_inventario mi2
        WHERE mi2.id_cliente = v.id_cliente AND mi2.sku = v.sku
      )
      GROUP BY v.id_cliente, v.sku
    ) v_agg
    JOIN clientes c ON c.id_cliente = v_agg.id_cliente
    JOIN medicamentos m ON m.sku = v_agg.sku
    ORDER BY valor DESC;

  END IF;
END;
$$;
