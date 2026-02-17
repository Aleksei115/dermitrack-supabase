-- Create public schema wrapper functions that delegate to analytics schema.
-- PostgREST has permission issues with the analytics schema directly,
-- so these wrappers (SECURITY DEFINER, owned by postgres) bypass that.

-- Ensure analytics schema permissions are in place
GRANT USAGE ON SCHEMA analytics TO anon, authenticated, service_role, authenticator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO anon, authenticated, service_role;

--------------------------------------------------------------------------------
-- A1) get_impacto_botiquin_resumen
--------------------------------------------------------------------------------
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
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_impacto_botiquin_resumen();
$$;

--------------------------------------------------------------------------------
-- A2) get_impacto_detalle
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_impacto_detalle(p_metrica text)
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
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_impacto_detalle(p_metrica);
$$;

--------------------------------------------------------------------------------
-- B1) get_brand_performance
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_brand_performance()
RETURNS TABLE (
  marca varchar,
  valor numeric,
  piezas int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_brand_performance();
$$;

--------------------------------------------------------------------------------
-- B2) get_padecimiento_performance
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_padecimiento_performance()
RETURNS TABLE (
  padecimiento varchar,
  valor numeric,
  piezas int
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_padecimiento_performance();
$$;

--------------------------------------------------------------------------------
-- B3) get_adoption_metrics
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_adoption_metrics()
RETURNS TABLE (
  avg_dias_adopcion int,
  avg_periodos_adopcion numeric,
  total_adopciones int,
  timeline jsonb
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_adoption_metrics();
$$;

--------------------------------------------------------------------------------
-- B4) get_product_interest
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_product_interest(p_limit int DEFAULT 15)
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
  SELECT * FROM analytics.get_product_interest(p_limit);
$$;

--------------------------------------------------------------------------------
-- B5) get_opportunity_matrix
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_opportunity_matrix()
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
  SELECT * FROM analytics.get_opportunity_matrix();
$$;

--------------------------------------------------------------------------------
-- B6) get_yoy_padecimiento
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_yoy_padecimiento()
RETURNS TABLE (
  padecimiento varchar,
  anio int,
  valor numeric,
  crecimiento numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_yoy_padecimiento();
$$;

--------------------------------------------------------------------------------
-- B7) get_top_converting_skus
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_top_converting_skus(p_limit int DEFAULT 10)
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
  SELECT * FROM analytics.get_top_converting_skus(p_limit);
$$;

--------------------------------------------------------------------------------
-- B8) get_doctor_performance
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_doctor_performance(p_limit int DEFAULT 30)
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
  SELECT * FROM analytics.get_doctor_performance(p_limit);
$$;

--------------------------------------------------------------------------------
-- B9) get_cumulative_movements
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_cumulative_movements()
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
  SELECT * FROM analytics.get_cumulative_movements();
$$;

--------------------------------------------------------------------------------
-- B10) get_top_conversion_mix
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_top_conversion_mix()
RETURNS TABLE (
  top_conversiones int,
  top_adopciones int,
  top_pct numeric,
  no_top_conversiones int,
  no_top_adopciones int,
  no_top_pct numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $$
  SELECT * FROM analytics.get_top_conversion_mix();
$$;

-- Grant execute on all new public wrappers
GRANT EXECUTE ON FUNCTION public.get_impacto_botiquin_resumen() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_impacto_detalle(text) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_brand_performance() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_padecimiento_performance() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_adoption_metrics() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_product_interest(int) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_opportunity_matrix() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_yoy_padecimiento() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_top_converting_skus(int) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_doctor_performance(int) TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_cumulative_movements() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.get_top_conversion_mix() TO anon, authenticated, service_role;
