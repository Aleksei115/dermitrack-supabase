-- Fix: 3 trigger functions still reference audit_log.user_id which was renamed
-- to audit_user_id during the English schema rename. This causes errors on
-- saga_transactions INSERT (cutoff submission), general audit triggers, and
-- inventory sale movements.

-- 1. audit_saga_transactions — fires on saga_transactions INSERT/UPDATE/DELETE
CREATE OR REPLACE FUNCTION public.audit_saga_transactions()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  INSERT INTO audit_log (
    table_name, record_id, action, audit_user_id,
    values_before, values_after, timestamp
  ) VALUES (
    TG_TABLE_NAME,
    COALESCE(NEW.id::text, OLD.id::text),
    TG_OP,
    COALESCE(NEW.user_id, OLD.user_id),
    CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END,
    NOW()
  );
  RETURN COALESCE(NEW, OLD);
END;
$function$;

-- 2. audit_trigger_func — general audit trigger (INSERT/UPDATE/DELETE)
CREATE OR REPLACE FUNCTION public.audit_trigger_func()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  usuario_actual VARCHAR;
BEGIN
  -- Prefer explicit app user id, then map from auth.uid()
  usuario_actual := current_setting('app.current_user_id', TRUE);
  IF usuario_actual IS NULL THEN
    usuario_actual := public.current_user_id();
  END IF;

  -- INSERT
  IF TG_OP = 'INSERT' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      audit_user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      NEW.id::text,
      'INSERT',
      usuario_actual,
      NULL,
      to_jsonb(NEW)
    );
    RETURN NEW;

  -- UPDATE
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      audit_user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      NEW.id::text,
      'UPDATE',
      usuario_actual,
      to_jsonb(OLD),
      to_jsonb(NEW)
    );
    RETURN NEW;

  -- DELETE
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO audit_log (
      table_name,
      record_id,
      action,
      audit_user_id,
      values_before,
      values_after
    ) VALUES (
      TG_TABLE_NAME,
      OLD.id::text,
      'DELETE',
      usuario_actual,
      to_jsonb(OLD),
      NULL
    );
    RETURN OLD;
  END IF;

  RETURN NULL;
END;
$function$;

-- 3. fn_remove_available_sku_on_sale — fires on inventory_movements INSERT
CREATE OR REPLACE FUNCTION public.fn_remove_available_sku_on_sale()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Solo actuar en movimientos type VENTA
  IF NEW.type = 'SALE' THEN
    DELETE FROM public.cabinet_client_available_skus
    WHERE client_id = NEW.client_id
      AND sku = NEW.sku;

    -- Log en audit_log si existe la infraestructura
    BEGIN
      INSERT INTO public.audit_log (
        table_name,
        record_id,
        action,
        audit_user_id,
        values_before,
        values_after
      )
      VALUES (
        'cabinet_client_available_skus',
        NEW.client_id || ':' || NEW.sku,
        'DELETE',
        NULL,  -- Sistema automático
        jsonb_build_object(
          'client_id', NEW.client_id,
          'sku', NEW.sku,
          'motivo', 'movimiento_venta',
          'movimiento_id', NEW.id
        ),
        NULL
      );
    EXCEPTION WHEN OTHERS THEN
      -- Si falla el log, no interrumpir la operación
      NULL;
    END;
  END IF;

  RETURN NEW;
END;
$function$;
