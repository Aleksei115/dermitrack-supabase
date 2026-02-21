-- Fix: kpi_stock in get_corte_historico_data uses COALESCE(inv.precio_unitario, 0)
-- which values 11 inventory rows at $0 because their precio_unitario is NULL.
-- Fix: add med.precio as fallback → COALESCE(inv.precio_unitario, med.precio, 0)
-- Result: stock_activo goes from 95,244 → 107,703

CREATE OR REPLACE FUNCTION analytics.get_corte_historico_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
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
  sku_padecimiento AS (
    SELECT DISTINCT ON (mp.sku) mp.sku, p.nombre AS padecimiento
    FROM medicamento_padecimientos mp
    JOIN padecimientos p ON p.id_padecimiento = mp.id_padecimiento
    ORDER BY mp.sku, p.id_padecimiento
  ),
  filtered_skus AS (
    SELECT m.sku
    FROM medicamentos m
    LEFT JOIN sku_padecimiento sp ON sp.sku = m.sku
    WHERE (p_marcas IS NULL OR m.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  kpi_venta_m1 AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_creacion AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'CREACION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  kpi_stock AS (
    SELECT COALESCE(SUM(inv.cantidad_disponible * COALESCE(inv.precio_unitario, med.precio, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT inv.sku)::int AS skus_unicos
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    LEFT JOIN sku_padecimiento sp ON sp.sku = inv.sku
    WHERE inv.cantidad_disponible > 0
      AND (p_medicos IS NULL OR inv.id_cliente = ANY(p_medicos))
      AND (p_marcas IS NULL OR med.marca = ANY(p_marcas))
      AND (p_padecimientos IS NULL OR sp.padecimiento = ANY(p_padecimientos))
  ),
  kpi_recoleccion AS (
    SELECT COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor,
           COUNT(DISTINCT mov.sku)::int AS skus_unicos
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.tipo = 'RECOLECCION'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
  ),
  visitas_base AS (
    SELECT
      mov.id_saga_transaction,
      mov.id_cliente,
      MIN(mov.fecha_movimiento::date) AS fecha_visita
    FROM movimientos_inventario mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.tipo = 'VENTA'
      AND (p_medicos IS NULL OR mov.id_cliente = ANY(p_medicos))
      AND mov.sku IN (SELECT sku FROM filtered_skus)
    GROUP BY mov.id_saga_transaction, mov.id_cliente
  ),
  visita_rows AS (
    SELECT
      c.id_cliente,
      c.nombre_cliente,
      vb.fecha_visita::text AS fecha_visita,
      COUNT(DISTINCT mov.sku)::int AS skus_unicos,
      COALESCE(SUM(mov.cantidad * COALESCE(mov.precio_unitario, 0)), 0)::numeric AS valor_venta,
      COALESCE(SUM(mov.cantidad), 0)::int AS piezas_venta
    FROM visitas_base vb
    JOIN movimientos_inventario mov ON vb.id_saga_transaction = mov.id_saga_transaction
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON vb.id_cliente = c.id_cliente
    WHERE mov.tipo = 'VENTA'
      AND mov.sku IN (SELECT sku FROM filtered_skus)
      AND (p_fecha_inicio IS NULL OR vb.fecha_visita >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR vb.fecha_visita <= p_fecha_fin)
    GROUP BY c.id_cliente, c.nombre_cliente, vb.fecha_visita
    ORDER BY vb.fecha_visita ASC, c.nombre_cliente
  )
  SELECT json_build_object(
    'kpis', json_build_object(
      'valor_venta_m1', (SELECT valor FROM kpi_venta_m1),
      'skus_venta_m1', (SELECT skus_unicos FROM kpi_venta_m1),
      'valor_creacion', (SELECT valor FROM kpi_creacion),
      'skus_creacion', (SELECT skus_unicos FROM kpi_creacion),
      'stock_activo', (SELECT valor FROM kpi_stock),
      'skus_stock', (SELECT skus_unicos FROM kpi_stock),
      'valor_recoleccion', (SELECT valor FROM kpi_recoleccion),
      'skus_recoleccion', (SELECT skus_unicos FROM kpi_recoleccion)
    ),
    'visitas', COALESCE((SELECT json_agg(row_to_json(vr)) FROM visita_rows vr), '[]'::json)
  ) INTO v_result;

  RETURN v_result;
END;
$function$;

-- Public wrapper
CREATE OR REPLACE FUNCTION public.get_corte_historico_data(
  p_medicos character varying[] DEFAULT NULL,
  p_marcas character varying[] DEFAULT NULL,
  p_padecimientos character varying[] DEFAULT NULL,
  p_fecha_inicio date DEFAULT NULL,
  p_fecha_fin date DEFAULT NULL
)
RETURNS json
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'public', 'analytics'
AS $function$
  SELECT analytics.get_corte_historico_data(p_medicos, p_marcas, p_padecimientos, p_fecha_inicio, p_fecha_fin);
$function$;

GRANT EXECUTE ON FUNCTION analytics.get_corte_historico_data(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
GRANT EXECUTE ON FUNCTION public.get_corte_historico_data(character varying[], character varying[], character varying[], date, date) TO authenticated, anon;
NOTIFY pgrst, 'reload schema';
