-- Sincronizar saga_status column con el estado real
-- La columna fisica quedaba siempre en 'RUNNING' aunque el estado ya era COMPLETADO/CANCELADO
UPDATE visitas
SET saga_status = CASE
  WHEN estado = 'COMPLETADO' THEN 'COMPLETED'
  WHEN estado = 'CANCELADO' THEN 'COMPENSATED'
  ELSE 'RUNNING'
END
WHERE saga_status != CASE
  WHEN estado = 'COMPLETADO' THEN 'COMPLETED'
  WHEN estado = 'CANCELADO' THEN 'COMPENSATED'
  ELSE 'RUNNING'
END;

-- Trigger para mantener sincronizado en adelante
CREATE OR REPLACE FUNCTION fn_sync_saga_status()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  NEW.saga_status := CASE
    WHEN NEW.estado = 'COMPLETADO' THEN 'COMPLETED'
    WHEN NEW.estado = 'CANCELADO' THEN 'COMPENSATED'
    ELSE 'RUNNING'
  END;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sync_saga_status
  BEFORE INSERT OR UPDATE OF estado ON public.visitas
  FOR EACH ROW
  EXECUTE FUNCTION fn_sync_saga_status();
