-- Expand audit_log action CHECK to include admin/compensation actions
ALTER TABLE public.audit_log DROP CONSTRAINT audit_log_accion_check;

ALTER TABLE public.audit_log ADD CONSTRAINT audit_log_accion_check
  CHECK (action::text = ANY (ARRAY[
    'INSERT', 'UPDATE', 'DELETE',
    'COMPENSATE', 'DELETE_VISIT',
    'ADJUSTMENT', 'RETRY', 'FORCE_COMPLETE', 'REVERT',
    'COMPENSATE_TASK', 'RETRY_VALIDATION'
  ]));

NOTIFY pgrst, 'reload schema';
