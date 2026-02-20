-- Drop unused RPCs: doctor_performance and top_converting_skus
-- These views are replaced by client-side derivations from clasificacion_base

DROP FUNCTION IF EXISTS public.get_doctor_performance(int);
DROP FUNCTION IF EXISTS analytics.get_doctor_performance(int);
DROP FUNCTION IF EXISTS public.get_top_converting_skus(int);
DROP FUNCTION IF EXISTS analytics.get_top_converting_skus(int);

NOTIFY pgrst, 'reload schema';
