-- Fix is_admin() to include OWNER role (PROD drift from migration 20260211145225)
CREATE OR REPLACE FUNCTION public.is_admin()
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select exists (
    select 1
    from public.usuarios u
    where u.auth_user_id = auth.uid()
      and u.rol IN ('ADMINISTRADOR', 'OWNER')
      and u.activo = true
  );
$function$;
