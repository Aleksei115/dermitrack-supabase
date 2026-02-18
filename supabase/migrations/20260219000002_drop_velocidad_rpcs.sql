-----------------------------------------------------------------------
-- Drop RPCs that were only used by the Velocidad tab (removed)
-----------------------------------------------------------------------

DROP FUNCTION IF EXISTS public.get_adoption_metrics();
DROP FUNCTION IF EXISTS analytics.get_adoption_metrics();

DROP FUNCTION IF EXISTS public.get_top_conversion_mix();
DROP FUNCTION IF EXISTS analytics.get_top_conversion_mix();

DROP FUNCTION IF EXISTS public.get_cumulative_movements();
DROP FUNCTION IF EXISTS analytics.get_cumulative_movements();

NOTIFY pgrst, 'reload schema';
