-- RPC to list all clients for auditoría dropdown.
-- Bypasses RLS so any authenticated dashboard user (ADMIN/OWNER/ADVISOR)
-- can see the full client list for audit purposes.

CREATE OR REPLACE FUNCTION public.get_auditoria_clientes()
RETURNS TABLE(client_id varchar, client_name varchar, active boolean)
LANGUAGE sql
SECURITY DEFINER
SET search_path TO 'public'
SET row_security TO 'off'
AS $$
  SELECT c.client_id, c.client_name, c.active
  FROM clients c
  ORDER BY c.client_name;
$$;
