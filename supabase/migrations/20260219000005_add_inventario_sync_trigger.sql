-- Trigger que sincroniza inventario_botiquin despues de cada INSERT en movimientos_inventario
-- Previene desincronizacion entre movimientos_inventario y inventario_botiquin
CREATE OR REPLACE FUNCTION fn_sync_inventario_from_movements()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
BEGIN
  IF NEW.cantidad_despues IS NOT NULL THEN
    INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
    VALUES (NEW.id_cliente, NEW.sku, NEW.cantidad_despues, now())
    ON CONFLICT (id_cliente, sku)
    DO UPDATE SET
      cantidad_disponible = EXCLUDED.cantidad_disponible,
      ultima_actualizacion = EXCLUDED.ultima_actualizacion;
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_sync_inventario
  AFTER INSERT OR UPDATE ON public.movimientos_inventario
  FOR EACH ROW
  EXECUTE FUNCTION fn_sync_inventario_from_movements();
