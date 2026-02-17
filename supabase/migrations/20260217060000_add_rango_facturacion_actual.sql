-- Add rango_actual and facturacion_actual columns to clientes
ALTER TABLE public.clientes ADD COLUMN IF NOT EXISTS rango_actual VARCHAR(50);
ALTER TABLE public.clientes ADD COLUMN IF NOT EXISTS facturacion_actual NUMERIC(14,2) DEFAULT 0;

-- Function: update rango and facturacion_actual based on ventas_odv
CREATE OR REPLACE FUNCTION public.update_rango_y_facturacion_actual()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  UPDATE clientes c
  SET facturacion_actual = sub.promedio_mensual,
      rango_actual = CASE
        WHEN sub.promedio_mensual > 45000 THEN 'ALTO'
        WHEN sub.promedio_mensual >= 20000 THEN 'MEDIO'
        ELSE 'BAJO'
      END
  FROM (
    SELECT
      v.id_cliente,
      COALESCE(SUM(v.cantidad * v.precio), 0) / NULLIF(COUNT(DISTINCT date_trunc('month', v.fecha)), 0) AS promedio_mensual
    FROM ventas_odv v
    GROUP BY v.id_cliente
  ) sub
  WHERE c.id_cliente = sub.id_cliente;

  -- Set NULL for clients with no ventas_odv
  UPDATE clientes c
  SET facturacion_actual = 0,
      rango_actual = NULL
  WHERE NOT EXISTS (
    SELECT 1 FROM ventas_odv v WHERE v.id_cliente = c.id_cliente
  );
END;
$$;

-- Schedule cron job: 1st of every month at 6am UTC
SELECT cron.schedule(
  'update-rango-facturacion',
  '0 6 1 * *',
  'SELECT public.update_rango_y_facturacion_actual()'
);

-- New RPC: get_ranking_medicos_completo
-- Returns ALL clients that have any movimiento OR any LEVANTAMIENTO_INICIAL saga
CREATE OR REPLACE FUNCTION analytics.get_ranking_medicos_completo()
RETURNS TABLE(
  nombre_cliente VARCHAR,
  id_cliente VARCHAR,
  activo BOOLEAN,
  rango VARCHAR,
  rango_actual VARCHAR,
  facturacion_actual NUMERIC,
  facturacion NUMERIC,
  piezas INT,
  valor NUMERIC,
  unique_skus INT
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  RETURN QUERY
  WITH clientes_con_botiquin AS (
    -- Clients with any movimiento
    SELECT DISTINCT mi.id_cliente
    FROM movimientos_inventario mi
    UNION
    -- Clients with LEVANTAMIENTO_INICIAL saga
    SELECT DISTINCT st.id_cliente
    FROM saga_transactions st
    WHERE st.tipo = 'LEVANTAMIENTO_INICIAL'
  ),
  ventas AS (
    SELECT
      mi.id_cliente,
      SUM(mi.cantidad)::int AS piezas,
      SUM(mi.cantidad * m.precio) AS valor,
      COUNT(DISTINCT mi.sku)::int AS unique_skus
    FROM movimientos_inventario mi
    JOIN medicamentos m ON m.sku = mi.sku
    WHERE mi.tipo = 'VENTA'
    GROUP BY mi.id_cliente
  )
  SELECT
    c.nombre_cliente,
    c.id_cliente,
    c.activo,
    COALESCE(c.rango, 'N/A')::VARCHAR AS rango,
    c.rango_actual,
    COALESCE(c.facturacion_actual, 0) AS facturacion_actual,
    COALESCE(c.facturacion_promedio, 0) AS facturacion,
    COALESCE(v.piezas, 0) AS piezas,
    COALESCE(v.valor, 0) AS valor,
    COALESCE(v.unique_skus, 0) AS unique_skus
  FROM clientes_con_botiquin cb
  JOIN clientes c ON c.id_cliente = cb.id_cliente
  LEFT JOIN ventas v ON v.id_cliente = cb.id_cliente
  ORDER BY COALESCE(v.valor, 0) DESC;
END;
$$;

-- Public wrapper for the RPC
CREATE OR REPLACE FUNCTION public.get_ranking_medicos_completo()
RETURNS TABLE(
  nombre_cliente VARCHAR,
  id_cliente VARCHAR,
  activo BOOLEAN,
  rango VARCHAR,
  rango_actual VARCHAR,
  facturacion_actual NUMERIC,
  facturacion NUMERIC,
  piezas INT,
  valor NUMERIC,
  unique_skus INT
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT * FROM analytics.get_ranking_medicos_completo();
$$;
