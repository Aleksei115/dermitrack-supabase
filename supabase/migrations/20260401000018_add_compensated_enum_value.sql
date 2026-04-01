-- Add COMPENSATED to visit_task_status enum
ALTER TYPE public.visit_task_status ADD VALUE IF NOT EXISTS 'COMPENSATED';

NOTIFY pgrst, 'reload schema';
