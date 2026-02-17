-- Fix porcentaje_impacto: calculate (M1+M2+M3)/total_odv without crosssell
-- The "Dinero generado por Botiquín" card shows M1+M2+M3, so the percentage
-- should match that numerator, not include crosssell.

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

  -- M2: Conversiones = adopciones that also have a ventas_odv record AFTER the first VENTA date
  -- Using > (not >=) to exclude the original botiquin ODV
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
    (v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones),
    v_total_odv,
    CASE WHEN v_total_odv > 0
      THEN ROUND(((v_rev_adopciones + v_rev_conversiones + v_rev_exposiciones) / v_total_odv) * 100, 1)
      ELSE 0
    END;
END;
$$;
