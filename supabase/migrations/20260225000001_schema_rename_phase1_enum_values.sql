-- ============================================================================
-- PHASE 1: Rename Enum Values (Spanish → English)
-- ============================================================================
-- PostgreSQL 10+ supports ALTER TYPE ... RENAME VALUE which updates stored
-- values in-place without needing to recreate the column.
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. estado_cliente → (values only, type renamed in Phase 3)
-- ---------------------------------------------------------------------------
ALTER TYPE estado_cliente RENAME VALUE 'ACTIVO' TO 'ACTIVE';
ALTER TYPE estado_cliente RENAME VALUE 'EN_BAJA' TO 'DOWNGRADING';
ALTER TYPE estado_cliente RENAME VALUE 'INACTIVO' TO 'INACTIVE';
ALTER TYPE estado_cliente RENAME VALUE 'SUSPENDIDO' TO 'SUSPENDED';

-- ---------------------------------------------------------------------------
-- 2. rol_usuario
-- ---------------------------------------------------------------------------
ALTER TYPE rol_usuario RENAME VALUE 'ADMINISTRADOR' TO 'ADMIN';
ALTER TYPE rol_usuario RENAME VALUE 'ASESOR' TO 'ADVISOR';
-- OWNER stays as OWNER

-- ---------------------------------------------------------------------------
-- 3. estado_saga_transaction
-- ---------------------------------------------------------------------------
ALTER TYPE estado_saga_transaction RENAME VALUE 'BORRADOR' TO 'DRAFT';
ALTER TYPE estado_saga_transaction RENAME VALUE 'PENDIENTE_CONFIRMACION' TO 'PENDING_CONFIRMATION';
ALTER TYPE estado_saga_transaction RENAME VALUE 'PROCESANDO_ZOHO' TO 'PROCESSING_ZOHO';
ALTER TYPE estado_saga_transaction RENAME VALUE 'COMPLETADA' TO 'COMPLETED_F';
ALTER TYPE estado_saga_transaction RENAME VALUE 'CANCELADA' TO 'CANCELLED_F';
ALTER TYPE estado_saga_transaction RENAME VALUE 'FALLIDA' TO 'FAILED';
ALTER TYPE estado_saga_transaction RENAME VALUE 'CONFIRMADO' TO 'CONFIRMED';
ALTER TYPE estado_saga_transaction RENAME VALUE 'PENDIENTE_SYNC' TO 'PENDING_SYNC';
ALTER TYPE estado_saga_transaction RENAME VALUE 'COMPLETADO' TO 'COMPLETED';
ALTER TYPE estado_saga_transaction RENAME VALUE 'OMITIDA' TO 'SKIPPED';

-- ---------------------------------------------------------------------------
-- 4. tipo_saga_transaction
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_saga_transaction RENAME VALUE 'LEVANTAMIENTO_INICIAL' TO 'INITIAL_PLACEMENT';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'CORTE_RENOVACION' TO 'CUTOFF_RENEWAL';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'VENTA' TO 'SALE';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'RECOLECCION' TO 'COLLECTION';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'CORTE' TO 'CUTOFF';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'LEV_POST_CORTE' TO 'POST_CUTOFF_PLACEMENT';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'DEVOLUCION_ODV' TO 'ODV_RETURN';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'VENTA_ODV' TO 'SALE_ODV';
ALTER TYPE tipo_saga_transaction RENAME VALUE 'PERMANENCIA' TO 'HOLDING';

-- ---------------------------------------------------------------------------
-- 5. visit_estado
-- ---------------------------------------------------------------------------
ALTER TYPE visit_estado RENAME VALUE 'PENDIENTE' TO 'PENDING';
ALTER TYPE visit_estado RENAME VALUE 'EN_CURSO' TO 'IN_PROGRESS';
ALTER TYPE visit_estado RENAME VALUE 'RETRASADO' TO 'DELAYED';
ALTER TYPE visit_estado RENAME VALUE 'COMPLETADO' TO 'COMPLETED';
ALTER TYPE visit_estado RENAME VALUE 'PROGRAMADO' TO 'SCHEDULED';
ALTER TYPE visit_estado RENAME VALUE 'CANCELADO' TO 'CANCELLED';

-- ---------------------------------------------------------------------------
-- 6. visit_task_estado
-- ---------------------------------------------------------------------------
ALTER TYPE visit_task_estado RENAME VALUE 'PENDIENTE' TO 'PENDING';
ALTER TYPE visit_task_estado RENAME VALUE 'EN_CURSO' TO 'IN_PROGRESS';
ALTER TYPE visit_task_estado RENAME VALUE 'RETRASADO' TO 'DELAYED';
ALTER TYPE visit_task_estado RENAME VALUE 'COMPLETADO' TO 'COMPLETED';
-- ERROR stays as ERROR
ALTER TYPE visit_task_estado RENAME VALUE 'PENDIENTE_SYNC' TO 'PENDING_SYNC';
ALTER TYPE visit_task_estado RENAME VALUE 'OMITIDO' TO 'SKIPPED_M';
ALTER TYPE visit_task_estado RENAME VALUE 'OMITIDA' TO 'SKIPPED';
ALTER TYPE visit_task_estado RENAME VALUE 'CANCELADO' TO 'CANCELLED';

-- ---------------------------------------------------------------------------
-- 7. visit_task_tipo
-- ---------------------------------------------------------------------------
ALTER TYPE visit_task_tipo RENAME VALUE 'LEVANTAMIENTO_INICIAL' TO 'INITIAL_PLACEMENT';
ALTER TYPE visit_task_tipo RENAME VALUE 'ODV_BOTIQUIN' TO 'ODV_CABINET';
ALTER TYPE visit_task_tipo RENAME VALUE 'CORTE' TO 'CUTOFF';
ALTER TYPE visit_task_tipo RENAME VALUE 'VENTA_ODV' TO 'SALE_ODV';
ALTER TYPE visit_task_tipo RENAME VALUE 'RECOLECCION' TO 'COLLECTION';
ALTER TYPE visit_task_tipo RENAME VALUE 'LEV_POST_CORTE' TO 'POST_CUTOFF_PLACEMENT';
ALTER TYPE visit_task_tipo RENAME VALUE 'INFORME_VISITA' TO 'VISIT_REPORT';

-- ---------------------------------------------------------------------------
-- 8. visit_tipo
-- ---------------------------------------------------------------------------
ALTER TYPE visit_tipo RENAME VALUE 'VISITA_LEVANTAMIENTO_INICIAL' TO 'VISIT_INITIAL_PLACEMENT';
ALTER TYPE visit_tipo RENAME VALUE 'VISITA_CORTE' TO 'VISIT_CUTOFF';

-- ---------------------------------------------------------------------------
-- 9. tipo_movimiento_botiquin
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_movimiento_botiquin RENAME VALUE 'VENTA' TO 'SALE';
ALTER TYPE tipo_movimiento_botiquin RENAME VALUE 'RECOLECCION' TO 'COLLECTION';
ALTER TYPE tipo_movimiento_botiquin RENAME VALUE 'PERMANENCIA' TO 'HOLDING';
ALTER TYPE tipo_movimiento_botiquin RENAME VALUE 'CREACION' TO 'PLACEMENT';

-- ---------------------------------------------------------------------------
-- 10. tipo_movimiento_inventario
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_movimiento_inventario RENAME VALUE 'ENTRADA' TO 'INBOUND';
ALTER TYPE tipo_movimiento_inventario RENAME VALUE 'SALIDA' TO 'OUTBOUND';

-- ---------------------------------------------------------------------------
-- 11. tipo_notificacion
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_notificacion RENAME VALUE 'CLIENTE_EN_BAJA' TO 'CLIENT_DOWNGRADING';
ALTER TYPE tipo_notificacion RENAME VALUE 'CLIENTE_INACTIVO' TO 'CLIENT_INACTIVE';
ALTER TYPE tipo_notificacion RENAME VALUE 'CLIENTE_REACTIVADO' TO 'CLIENT_REACTIVATED';
ALTER TYPE tipo_notificacion RENAME VALUE 'CLIENTE_SUSPENDIDO' TO 'CLIENT_SUSPENDED';
ALTER TYPE tipo_notificacion RENAME VALUE 'VISITA_SIN_ODV' TO 'VISIT_WITHOUT_ODV';
-- ERROR_ZOHO_SYNC stays as ERROR_ZOHO_SYNC
ALTER TYPE tipo_notificacion RENAME VALUE 'VISITA_CANCELADA' TO 'VISIT_CANCELLED';
ALTER TYPE tipo_notificacion RENAME VALUE 'SAGA_FALLIDA' TO 'SAGA_FAILED';

-- ---------------------------------------------------------------------------
-- 12. tipo_odv
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_odv RENAME VALUE 'BOTIQUIN' TO 'CABINET';
ALTER TYPE tipo_odv RENAME VALUE 'VENTA' TO 'SALE';

-- ---------------------------------------------------------------------------
-- 13. tipo_zoho_link
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_zoho_link RENAME VALUE 'VENTA' TO 'SALE';
ALTER TYPE tipo_zoho_link RENAME VALUE 'BOTIQUIN' TO 'CABINET';
ALTER TYPE tipo_zoho_link RENAME VALUE 'DEVOLUCION' TO 'RETURN';

-- ---------------------------------------------------------------------------
-- 14. tipo_ciclo_botiquin
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_ciclo_botiquin RENAME VALUE 'LEVANTAMIENTO' TO 'PLACEMENT';
ALTER TYPE tipo_ciclo_botiquin RENAME VALUE 'CORTE' TO 'CUTOFF';

-- ---------------------------------------------------------------------------
-- 15. tipo_evento_outbox
-- ---------------------------------------------------------------------------
ALTER TYPE tipo_evento_outbox RENAME VALUE 'CREAR_ODV_VENTA' TO 'CREATE_SALE_ODV';
ALTER TYPE tipo_evento_outbox RENAME VALUE 'CREAR_ODV_CONSIGNACION' TO 'CREATE_CONSIGNMENT_ODV';
ALTER TYPE tipo_evento_outbox RENAME VALUE 'CREAR_DEVOLUCION' TO 'CREATE_RETURN';
ALTER TYPE tipo_evento_outbox RENAME VALUE 'ACTUALIZAR_INVENTARIO' TO 'UPDATE_INVENTORY';
ALTER TYPE tipo_evento_outbox RENAME VALUE 'SINCRONIZAR_ZOHO' TO 'SYNC_ZOHO';
-- ZOHO_CREATE_ODV stays as ZOHO_CREATE_ODV

COMMIT;
