create extension if not exists "pg_cron" with schema "pg_catalog";

create schema if not exists "pgmq";

create extension if not exists "pgmq" with schema "pgmq";

create schema if not exists "analytics";

create schema if not exists "archive";

create schema if not exists "audit";

create schema if not exists "migration";

create type "migration"."tipo_movimiento_botiquin" as enum ('VENTA', 'RECOLECCION', 'PERMANENCIA', 'CREACION');

create type "public"."estado_cliente" as enum ('ACTIVO', 'EN_BAJA', 'INACTIVO', 'SUSPENDIDO');

create type "public"."estado_saga_transaction" as enum ('BORRADOR', 'PENDIENTE_CONFIRMACION', 'PROCESANDO_ZOHO', 'COMPLETADA', 'CANCELADA', 'FALLIDA', 'CONFIRMADO', 'PENDIENTE_SYNC', 'COMPLETADO', 'OMITIDA');

create type "public"."rol_usuario" as enum ('ADMINISTRADOR', 'ASESOR', 'OWNER');

create type "public"."tipo_ciclo_botiquin" as enum ('LEVANTAMIENTO', 'CORTE');

create type "public"."tipo_evento_outbox" as enum ('CREAR_ODV_VENTA', 'CREAR_ODV_CONSIGNACION', 'CREAR_DEVOLUCION', 'ACTUALIZAR_INVENTARIO', 'SINCRONIZAR_ZOHO', 'ZOHO_CREATE_ODV');

create type "public"."tipo_movimiento_botiquin" as enum ('VENTA', 'RECOLECCION', 'PERMANENCIA', 'CREACION');

create type "public"."tipo_movimiento_inventario" as enum ('ENTRADA', 'SALIDA');

create type "public"."tipo_notificacion" as enum ('CLIENTE_EN_BAJA', 'CLIENTE_INACTIVO', 'CLIENTE_REACTIVADO', 'CLIENTE_SUSPENDIDO', 'VISITA_SIN_ODV', 'ERROR_ZOHO_SYNC', 'VISITA_CANCELADA', 'SAGA_FALLIDA');

create type "public"."tipo_odv" as enum ('BOTIQUIN', 'VENTA');

create type "public"."tipo_saga_transaction" as enum ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION', 'VENTA', 'RECOLECCION', 'CORTE', 'LEV_POST_CORTE', 'DEVOLUCION_ODV', 'VENTA_ODV');

create type "public"."tipo_zoho_link" as enum ('VENTA', 'BOTIQUIN', 'DEVOLUCION');

create type "public"."transaction_type" as enum ('COMPENSABLE', 'PIVOT', 'RETRYABLE');

create type "public"."visit_estado" as enum ('PENDIENTE', 'EN_CURSO', 'RETRASADO', 'COMPLETADO', 'PROGRAMADO', 'CANCELADO');

create type "public"."visit_task_estado" as enum ('PENDIENTE', 'EN_CURSO', 'RETRASADO', 'COMPLETADO', 'ERROR', 'PENDIENTE_SYNC', 'OMITIDO', 'OMITIDA', 'CANCELADO');

create type "public"."visit_task_tipo" as enum ('LEVANTAMIENTO_INICIAL', 'ODV_BOTIQUIN', 'CORTE', 'VENTA_ODV', 'RECOLECCION', 'LEV_POST_CORTE', 'INFORME_VISITA');

create type "public"."visit_tipo" as enum ('VISITA_LEVANTAMIENTO_INICIAL', 'VISITA_CORTE');

create sequence "archive"."ciclos_botiquin_id_ciclo_seq";

create sequence "public"."audit_log_id_seq";

create sequence "public"."botiquin_odv_id_venta_seq";

create sequence "public"."movimientos_inventario_id_seq";

create sequence "public"."padecimientos_id_padecimiento_seq";

create sequence "public"."saga_odv_links_id_seq";

create sequence "public"."ventas_odv_id_venta_seq";

create sequence "public"."visita_odvs_id_seq";


  create table "archive"."ciclos_botiquin" (
    "id_ciclo" integer not null default nextval('archive.ciclos_botiquin_id_ciclo_seq'::regclass),
    "id_cliente" character varying(150) not null,
    "id_usuario" character varying(150) not null,
    "tipo" public.tipo_ciclo_botiquin not null,
    "fecha_creacion" timestamp without time zone not null default CURRENT_TIMESTAMP,
    "id_ciclo_anterior" integer,
    "latitud" numeric(9,6),
    "longitud" numeric(9,6)
      );


alter table "archive"."ciclos_botiquin" enable row level security;


  create table "archive"."encuestas_ciclo" (
    "id_ciclo" integer not null,
    "respuestas" jsonb not null default '{}'::jsonb,
    "completada" boolean default false,
    "fecha_completada" timestamp with time zone,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now()
      );


alter table "archive"."encuestas_ciclo" enable row level security;


  create table "public"."audit_log" (
    "id" bigint not null default nextval('public.audit_log_id_seq'::regclass),
    "tabla" character varying not null,
    "registro_id" character varying not null,
    "accion" character varying not null,
    "usuario_id" character varying,
    "timestamp" timestamp with time zone default now(),
    "valores_antes" jsonb,
    "valores_despues" jsonb,
    "ip_address" inet,
    "user_agent" text
      );


alter table "public"."audit_log" enable row level security;


  create table "public"."botiquin_clientes_sku_disponibles" (
    "id_cliente" character varying(150) not null,
    "sku" character varying(50) not null,
    "fecha_ingreso" timestamp without time zone not null default CURRENT_TIMESTAMP
      );


alter table "public"."botiquin_clientes_sku_disponibles" enable row level security;


  create table "public"."botiquin_odv" (
    "id_venta" integer not null default nextval('public.botiquin_odv_id_venta_seq'::regclass),
    "id_cliente" character varying(150) not null,
    "sku" character varying(50) not null,
    "odv_id" character varying(50) not null,
    "fecha" date not null,
    "cantidad" integer not null,
    "estado_factura" character varying(50),
    "created_at" timestamp without time zone default CURRENT_TIMESTAMP
      );


alter table "public"."botiquin_odv" enable row level security;


  create table "public"."cliente_estado_log" (
    "id" uuid not null default gen_random_uuid(),
    "id_cliente" character varying(150) not null,
    "estado_anterior" public.estado_cliente,
    "estado_nuevo" public.estado_cliente not null,
    "changed_by" character varying(150) not null,
    "changed_at" timestamp with time zone not null default now(),
    "razon" text,
    "metadata" jsonb default '{}'::jsonb,
    "dias_en_estado_anterior" integer
      );


alter table "public"."cliente_estado_log" enable row level security;


  create table "public"."clientes" (
    "id_cliente" character varying(150) not null,
    "nombre_cliente" character varying(150) not null,
    "id_zona" character varying(150),
    "id_usuario" character varying(150),
    "activo" boolean not null default true,
    "facturacion_promedio" numeric(14,2),
    "facturacion_total" numeric(14,2),
    "meses_con_venta" integer,
    "rango" character varying(50),
    "id_cliente_zoho_botiquin" character varying,
    "id_cliente_zoho_normal" character varying,
    "estado" public.estado_cliente not null default 'ACTIVO'::public.estado_cliente,
    "updated_at" timestamp with time zone default now()
      );


alter table "public"."clientes" enable row level security;


  create table "public"."event_outbox" (
    "id" uuid not null default gen_random_uuid(),
    "evento_tipo" public.tipo_evento_outbox not null,
    "saga_transaction_id" uuid not null,
    "payload" jsonb not null,
    "procesado" boolean default false,
    "procesado_en" timestamp with time zone,
    "intentos" integer default 0,
    "proximo_intento" timestamp with time zone default now(),
    "error_mensaje" text,
    "created_at" timestamp with time zone default now()
      );


alter table "public"."event_outbox" enable row level security;


  create table "public"."inventario_botiquin" (
    "id_cliente" character varying not null,
    "sku" character varying not null,
    "cantidad_disponible" integer not null default 0,
    "ultima_actualizacion" timestamp with time zone default now()
      );


alter table "public"."inventario_botiquin" enable row level security;


  create table "public"."medicamento_padecimientos" (
    "sku" character varying(50) not null,
    "id_padecimiento" integer not null
      );


alter table "public"."medicamento_padecimientos" enable row level security;


  create table "public"."medicamentos" (
    "sku" character varying(50) not null,
    "marca" character varying(100) not null,
    "fabricante" character varying(100) not null,
    "producto" character varying(150) not null,
    "descripcion" text,
    "contenido" character varying(50),
    "precio" numeric(10,2),
    "ficha_tecnica_url" text,
    "top" boolean not null default false,
    "codigo_barras" character varying(128),
    "imagen_barcode_url" text,
    "ultima_actualizacion" timestamp with time zone default now()
      );


alter table "public"."medicamentos" enable row level security;


  create table "public"."movimientos_inventario" (
    "id" bigint not null default nextval('public.movimientos_inventario_id_seq'::regclass),
    "id_saga_transaction" uuid,
    "id_cliente" character varying not null,
    "sku" character varying not null,
    "cantidad" integer not null,
    "cantidad_antes" integer not null,
    "cantidad_despues" integer not null,
    "fecha_movimiento" timestamp with time zone default now(),
    "tipo" public.tipo_movimiento_botiquin,
    "precio_unitario" numeric(10,2),
    "task_id" uuid
      );


alter table "public"."movimientos_inventario" enable row level security;


  create table "public"."notificaciones_admin" (
    "id" uuid not null default gen_random_uuid(),
    "tipo" public.tipo_notificacion not null,
    "titulo" character varying(200) not null,
    "mensaje" text,
    "metadata" jsonb default '{}'::jsonb,
    "leida" boolean default false,
    "created_at" timestamp with time zone default now(),
    "para_usuario" character varying(150),
    "leida_at" timestamp with time zone,
    "leida_por" character varying(150)
      );


alter table "public"."notificaciones_admin" enable row level security;


  create table "public"."notifications" (
    "id" uuid not null default gen_random_uuid(),
    "user_id" character varying not null,
    "type" text not null,
    "title" text not null,
    "body" text not null,
    "data" jsonb default '{}'::jsonb,
    "read_at" timestamp with time zone,
    "push_sent_at" timestamp with time zone,
    "expires_at" timestamp with time zone,
    "dedup_key" text,
    "created_at" timestamp with time zone default now()
      );


alter table "public"."notifications" enable row level security;


  create table "public"."padecimientos" (
    "id_padecimiento" integer not null default nextval('public.padecimientos_id_padecimiento_seq'::regclass),
    "nombre" character varying(100) not null
      );


alter table "public"."padecimientos" enable row level security;


  create table "public"."recolecciones" (
    "recoleccion_id" uuid not null default gen_random_uuid(),
    "visit_id" uuid not null,
    "id_cliente" character varying not null,
    "id_usuario" character varying not null,
    "estado" text not null default 'PENDIENTE'::text,
    "created_at" timestamp with time zone not null default now(),
    "updated_at" timestamp with time zone not null default now(),
    "entregada_at" timestamp with time zone,
    "cedis_responsable_nombre" text,
    "cedis_observaciones" text,
    "latitud" numeric,
    "longitud" numeric,
    "metadata" jsonb not null default '{}'::jsonb
      );


alter table "public"."recolecciones" enable row level security;


  create table "public"."recolecciones_evidencias" (
    "evidencia_id" uuid not null default gen_random_uuid(),
    "recoleccion_id" uuid not null,
    "storage_path" text not null,
    "mime_type" text,
    "created_at" timestamp with time zone not null default now(),
    "metadata" jsonb not null default '{}'::jsonb
      );


alter table "public"."recolecciones_evidencias" enable row level security;


  create table "public"."recolecciones_firmas" (
    "recoleccion_id" uuid not null,
    "storage_path" text not null,
    "signed_at" timestamp with time zone not null default now(),
    "device_info" jsonb not null default '{}'::jsonb
      );


alter table "public"."recolecciones_firmas" enable row level security;


  create table "public"."recolecciones_items" (
    "recoleccion_id" uuid not null,
    "sku" character varying not null,
    "cantidad" integer not null
      );


alter table "public"."recolecciones_items" enable row level security;


  create table "public"."saga_adjustments" (
    "id" uuid not null default gen_random_uuid(),
    "compensation_id" uuid not null,
    "saga_transaction_id" uuid not null,
    "item_sku" text not null,
    "old_quantity" integer not null,
    "new_quantity" integer not null,
    "adjustment_reason" text,
    "created_at" timestamp with time zone default now()
      );


alter table "public"."saga_adjustments" enable row level security;


  create table "public"."saga_compensations" (
    "id" uuid not null default gen_random_uuid(),
    "saga_transaction_id" uuid not null,
    "compensated_by" character varying not null,
    "reason" text not null,
    "compensation_type" text not null,
    "old_state" jsonb not null,
    "new_state" jsonb not null,
    "zoho_sync_status" text default 'PENDING'::text,
    "created_at" timestamp with time zone default now()
      );


alter table "public"."saga_compensations" enable row level security;


  create table "public"."saga_transactions" (
    "id" uuid not null default gen_random_uuid(),
    "tipo" public.tipo_saga_transaction not null,
    "id_cliente" character varying not null,
    "id_usuario" character varying not null,
    "items" jsonb not null,
    "metadata" jsonb default '{}'::jsonb,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    "visit_id" uuid,
    "estado" public.estado_saga_transaction default 'BORRADOR'::public.estado_saga_transaction
      );


alter table "public"."saga_transactions" enable row level security;


  create table "public"."saga_zoho_links" (
    "id" integer not null default nextval('public.saga_odv_links_id_seq'::regclass),
    "id_saga_transaction" uuid not null,
    "zoho_id" text not null,
    "zoho_sync_status" text default 'pending'::text,
    "zoho_synced_at" timestamp with time zone,
    "zoho_error_message" text,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now(),
    "tipo" public.tipo_zoho_link not null,
    "items" jsonb
      );


alter table "public"."saga_zoho_links" enable row level security;


  create table "public"."user_notification_preferences" (
    "id" uuid not null default gen_random_uuid(),
    "user_id" character varying not null,
    "push_enabled" boolean default true,
    "email_enabled" boolean default false,
    "quiet_hours_start" time without time zone,
    "quiet_hours_end" time without time zone,
    "disabled_types" text[] default '{}'::text[],
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now()
      );


alter table "public"."user_notification_preferences" enable row level security;


  create table "public"."user_push_tokens" (
    "id" uuid not null default gen_random_uuid(),
    "user_id" character varying not null,
    "token" text not null,
    "platform" text not null,
    "is_active" boolean default true,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now()
      );


alter table "public"."user_push_tokens" enable row level security;


  create table "public"."usuarios" (
    "id_usuario" character varying(150) not null,
    "nombre" character varying(150) not null,
    "email" character varying(150) not null,
    "password" character varying(255) not null,
    "rol" public.rol_usuario not null default 'ASESOR'::public.rol_usuario,
    "activo" boolean not null default true,
    "fecha_creacion" timestamp without time zone default CURRENT_TIMESTAMP,
    "auth_user_id" uuid,
    "id_zoho" character varying(100)
      );


alter table "public"."usuarios" enable row level security;


  create table "public"."ventas_odv" (
    "id_venta" integer not null default nextval('public.ventas_odv_id_venta_seq'::regclass),
    "id_cliente" character varying(150) not null,
    "sku" character varying(50) not null,
    "odv_id" character varying(50) not null,
    "fecha" date not null,
    "cantidad" integer not null,
    "estado_factura" character varying(50),
    "created_at" timestamp without time zone default CURRENT_TIMESTAMP,
    "precio" numeric(10,2)
      );


alter table "public"."ventas_odv" enable row level security;


  create table "public"."visit_tasks" (
    "visit_id" uuid not null,
    "task_tipo" public.visit_task_tipo not null,
    "estado" public.visit_task_estado not null default 'PENDIENTE'::public.visit_task_estado,
    "required" boolean not null default true,
    "created_at" timestamp with time zone not null default now(),
    "started_at" timestamp with time zone,
    "completed_at" timestamp with time zone,
    "due_at" timestamp with time zone,
    "last_activity_at" timestamp with time zone,
    "reference_table" text,
    "reference_id" text,
    "metadata" jsonb not null default '{}'::jsonb,
    "task_id" uuid not null default gen_random_uuid(),
    "transaction_type" public.transaction_type not null,
    "step_order" integer not null
      );


alter table "public"."visit_tasks" enable row level security;


  create table "public"."visita_informes" (
    "visit_id" uuid not null,
    "respuestas" jsonb not null default '{}'::jsonb,
    "completada" boolean not null default false,
    "fecha_completada" timestamp with time zone,
    "created_at" timestamp with time zone not null default now(),
    "updated_at" timestamp with time zone not null default now(),
    "informe_id" uuid default gen_random_uuid(),
    "etiqueta" character varying(50),
    "cumplimiento_score" integer default 0
      );


alter table "public"."visita_informes" enable row level security;


  create table "public"."visita_odvs" (
    "id" integer not null default nextval('public.visita_odvs_id_seq'::regclass),
    "visit_id" uuid not null,
    "odv_id" character varying(50) not null,
    "fecha_odv" date,
    "total_piezas" integer default 0,
    "created_at" timestamp with time zone default now(),
    "tipo" public.tipo_odv,
    "zoho_cliente_id" text
      );


alter table "public"."visita_odvs" enable row level security;


  create table "public"."visitas" (
    "visit_id" uuid not null default gen_random_uuid(),
    "id_cliente" character varying not null,
    "id_usuario" character varying not null,
    "id_ciclo" integer,
    "tipo" public.visit_tipo not null,
    "estado" public.visit_estado not null default 'PENDIENTE'::public.visit_estado,
    "created_at" timestamp with time zone not null default now(),
    "started_at" timestamp with time zone,
    "completed_at" timestamp with time zone,
    "due_at" timestamp with time zone,
    "last_activity_at" timestamp with time zone,
    "metadata" jsonb not null default '{}'::jsonb,
    "etiqueta" character varying(50),
    "updated_at" timestamp with time zone default now(),
    "saga_status" text default 'RUNNING'::text
      );


alter table "public"."visitas" enable row level security;


  create table "public"."zoho_health_status" (
    "id" integer not null default 1,
    "is_healthy" boolean default true,
    "last_check" timestamp with time zone default now(),
    "last_error" text,
    "consecutive_failures" integer default 0,
    "updated_at" timestamp with time zone default now()
      );


alter table "public"."zoho_health_status" enable row level security;


  create table "public"."zoho_tokens" (
    "auth_user_id" uuid not null,
    "zoho_user_id" text,
    "org_id" text,
    "refresh_token" text not null,
    "access_token" text,
    "expires_at" timestamp with time zone,
    "created_at" timestamp with time zone default now(),
    "updated_at" timestamp with time zone default now()
      );


alter table "public"."zoho_tokens" enable row level security;


  create table "public"."zonas" (
    "id_zona" character varying(150) not null,
    "nombre" character varying(150) not null,
    "activo" boolean not null default true
      );


alter table "public"."zonas" enable row level security;

alter sequence "archive"."ciclos_botiquin_id_ciclo_seq" owned by "archive"."ciclos_botiquin"."id_ciclo";

alter sequence "public"."audit_log_id_seq" owned by "public"."audit_log"."id";

alter sequence "public"."botiquin_odv_id_venta_seq" owned by "public"."botiquin_odv"."id_venta";

alter sequence "public"."movimientos_inventario_id_seq" owned by "public"."movimientos_inventario"."id";

alter sequence "public"."padecimientos_id_padecimiento_seq" owned by "public"."padecimientos"."id_padecimiento";

alter sequence "public"."saga_odv_links_id_seq" owned by "public"."saga_zoho_links"."id";

alter sequence "public"."ventas_odv_id_venta_seq" owned by "public"."ventas_odv"."id_venta";

alter sequence "public"."visita_odvs_id_seq" owned by "public"."visita_odvs"."id";

CREATE UNIQUE INDEX ciclos_botiquin_pkey ON archive.ciclos_botiquin USING btree (id_ciclo);

CREATE UNIQUE INDEX encuestas_ciclo_pkey ON archive.encuestas_ciclo USING btree (id_ciclo);

CREATE INDEX idx_ciclos_id_ciclo_anterior ON archive.ciclos_botiquin USING btree (id_ciclo_anterior);

CREATE INDEX idx_ciclos_id_cliente ON archive.ciclos_botiquin USING btree (id_cliente);

CREATE INDEX idx_ciclos_id_usuario ON archive.ciclos_botiquin USING btree (id_usuario);

CREATE UNIQUE INDEX audit_log_pkey ON public.audit_log USING btree (id);

CREATE UNIQUE INDEX botiquin_clientes_sku_disponibles_pkey ON public.botiquin_clientes_sku_disponibles USING btree (id_cliente, sku);

CREATE UNIQUE INDEX botiquin_odv_pkey ON public.botiquin_odv USING btree (id_venta);

CREATE UNIQUE INDEX cliente_estado_log_pkey ON public.cliente_estado_log USING btree (id);

CREATE UNIQUE INDEX clientes_pkey ON public.clientes USING btree (id_cliente);

CREATE UNIQUE INDEX event_outbox_pkey ON public.event_outbox USING btree (id);

CREATE INDEX idx_audit_tabla_registro ON public.audit_log USING btree (tabla, registro_id);

CREATE INDEX idx_audit_timestamp ON public.audit_log USING btree ("timestamp" DESC);

CREATE INDEX idx_audit_usuario ON public.audit_log USING btree (usuario_id);

CREATE INDEX idx_botiquin_clientes_sku ON public.botiquin_clientes_sku_disponibles USING btree (sku);

CREATE INDEX idx_botiquin_odv_fecha ON public.botiquin_odv USING btree (fecha);

CREATE INDEX idx_botiquin_odv_id_cliente ON public.botiquin_odv USING btree (id_cliente);

CREATE INDEX idx_botiquin_odv_odv_id ON public.botiquin_odv USING btree (odv_id);

CREATE INDEX idx_botiquin_odv_sku ON public.botiquin_odv USING btree (sku);

CREATE INDEX idx_cliente_estado_log_cliente ON public.cliente_estado_log USING btree (id_cliente);

CREATE INDEX idx_cliente_estado_log_estado_nuevo ON public.cliente_estado_log USING btree (estado_nuevo);

CREATE INDEX idx_cliente_estado_log_fecha ON public.cliente_estado_log USING btree (changed_at DESC);

CREATE INDEX idx_clientes_estado ON public.clientes USING btree (estado);

CREATE INDEX idx_clientes_id_usuario ON public.clientes USING btree (id_usuario);

CREATE INDEX idx_clientes_id_zona ON public.clientes USING btree (id_zona);

CREATE INDEX idx_inventario_cliente ON public.inventario_botiquin USING btree (id_cliente);

CREATE INDEX idx_inventario_sku ON public.inventario_botiquin USING btree (sku);

CREATE INDEX idx_med_padec_id_padecimiento ON public.medicamento_padecimientos USING btree (id_padecimiento);

CREATE INDEX idx_mov_inv_task_id ON public.movimientos_inventario USING btree (task_id);

CREATE INDEX idx_movimientos_cliente ON public.movimientos_inventario USING btree (id_cliente);

CREATE INDEX idx_movimientos_fecha ON public.movimientos_inventario USING btree (fecha_movimiento DESC);

CREATE INDEX idx_movimientos_inv_cliente_sku ON public.movimientos_inventario USING btree (id_cliente, sku);

CREATE INDEX idx_movimientos_inv_tipo ON public.movimientos_inventario USING btree (tipo);

CREATE INDEX idx_movimientos_saga ON public.movimientos_inventario USING btree (id_saga_transaction);

CREATE INDEX idx_movimientos_sku ON public.movimientos_inventario USING btree (sku);

CREATE INDEX idx_movimientos_task_id ON public.movimientos_inventario USING btree (task_id);

CREATE INDEX idx_notificaciones_no_leidas ON public.notificaciones_admin USING btree (leida, created_at DESC) WHERE (leida = false);

CREATE INDEX idx_notificaciones_para_usuario ON public.notificaciones_admin USING btree (para_usuario) WHERE (para_usuario IS NOT NULL);

CREATE INDEX idx_notificaciones_tipo ON public.notificaciones_admin USING btree (tipo);

CREATE INDEX idx_notification_prefs_user ON public.user_notification_preferences USING btree (user_id);

CREATE INDEX idx_notifications_created ON public.notifications USING btree (created_at DESC);

CREATE INDEX idx_notifications_expires ON public.notifications USING btree (expires_at) WHERE (expires_at IS NOT NULL);

CREATE INDEX idx_notifications_type ON public.notifications USING btree (type);

CREATE INDEX idx_notifications_unread ON public.notifications USING btree (user_id) WHERE (read_at IS NULL);

CREATE INDEX idx_notifications_user ON public.notifications USING btree (user_id);

CREATE INDEX idx_outbox_pendientes ON public.event_outbox USING btree (procesado, proximo_intento) WHERE (procesado = false);

CREATE INDEX idx_outbox_saga ON public.event_outbox USING btree (saga_transaction_id);

CREATE INDEX idx_push_tokens_active ON public.user_push_tokens USING btree (user_id) WHERE (is_active = true);

CREATE INDEX idx_push_tokens_user ON public.user_push_tokens USING btree (user_id);

CREATE INDEX idx_recolecciones_evidencias_recoleccion ON public.recolecciones_evidencias USING btree (recoleccion_id);

CREATE INDEX idx_recolecciones_usuario_estado ON public.recolecciones USING btree (id_usuario, estado, created_at DESC);

CREATE INDEX idx_recolecciones_visit ON public.recolecciones USING btree (visit_id);

CREATE INDEX idx_saga_adjustments_compensation ON public.saga_adjustments USING btree (compensation_id);

CREATE INDEX idx_saga_adjustments_sku ON public.saga_adjustments USING btree (item_sku);

CREATE INDEX idx_saga_compensations_by ON public.saga_compensations USING btree (compensated_by);

CREATE INDEX idx_saga_compensations_created ON public.saga_compensations USING btree (created_at DESC);

CREATE INDEX idx_saga_compensations_saga_tx ON public.saga_compensations USING btree (saga_transaction_id);

CREATE INDEX idx_saga_compensations_type ON public.saga_compensations USING btree (compensation_type);

CREATE INDEX idx_saga_odv_links_odv ON public.saga_zoho_links USING btree (zoho_id);

CREATE INDEX idx_saga_odv_links_saga ON public.saga_zoho_links USING btree (id_saga_transaction);

CREATE INDEX idx_saga_transactions_estado ON public.saga_transactions USING btree (estado) WHERE (estado = 'BORRADOR'::public.estado_saga_transaction);

CREATE INDEX idx_saga_transactions_usuario ON public.saga_transactions USING btree (id_usuario);

CREATE INDEX idx_saga_transactions_visit_id ON public.saga_transactions USING btree (visit_id);

CREATE INDEX idx_saga_tx_cliente ON public.saga_transactions USING btree (id_cliente);

CREATE INDEX idx_saga_tx_created ON public.saga_transactions USING btree (created_at);

CREATE INDEX idx_saga_tx_tipo ON public.saga_transactions USING btree (tipo);

CREATE INDEX idx_saga_visit_tipo ON public.saga_transactions USING btree (visit_id, tipo, created_at DESC);

CREATE INDEX idx_saga_zoho_links_items_not_null ON public.saga_zoho_links USING btree (id_saga_transaction) WHERE (items IS NOT NULL);

CREATE INDEX idx_saga_zoho_links_sync_status ON public.saga_zoho_links USING btree (zoho_sync_status) WHERE (zoho_sync_status <> 'synced'::text);

CREATE INDEX idx_saga_zoho_links_zoho_id ON public.saga_zoho_links USING btree (zoho_id);

CREATE INDEX idx_usuarios_id_zoho ON public.usuarios USING btree (id_zoho) WHERE (id_zoho IS NOT NULL);

CREATE INDEX idx_ventas_odv_id_cliente ON public.ventas_odv USING btree (id_cliente);

CREATE INDEX idx_visit_tasks_estado ON public.visit_tasks USING btree (estado, due_at, created_at DESC);

CREATE INDEX idx_visit_tasks_task_id ON public.visit_tasks USING btree (task_id);

CREATE INDEX idx_visit_tasks_visit ON public.visit_tasks USING btree (visit_id);

CREATE INDEX idx_visita_informes_completada ON public.visita_informes USING btree (completada, fecha_completada DESC);

CREATE INDEX idx_visita_odvs_odv_id ON public.visita_odvs USING btree (odv_id);

CREATE INDEX idx_visita_odvs_visit_id ON public.visita_odvs USING btree (visit_id);

CREATE INDEX idx_visitas_ciclo ON public.visitas USING btree (id_ciclo);

CREATE INDEX idx_visitas_cliente_estado ON public.visitas USING btree (id_cliente, estado, created_at DESC);

CREATE INDEX idx_visitas_usuario_estado ON public.visitas USING btree (id_usuario, estado, created_at DESC);

CREATE UNIQUE INDEX inventario_botiquin_pkey ON public.inventario_botiquin USING btree (id_cliente, sku);

CREATE UNIQUE INDEX medicamento_padecimientos_pkey ON public.medicamento_padecimientos USING btree (sku, id_padecimiento);

CREATE UNIQUE INDEX medicamentos_codigo_barras_key ON public.medicamentos USING btree (codigo_barras);

CREATE UNIQUE INDEX medicamentos_pkey ON public.medicamentos USING btree (sku);

CREATE UNIQUE INDEX movimientos_inventario_pkey ON public.movimientos_inventario USING btree (id);

CREATE UNIQUE INDEX notificaciones_admin_pkey ON public.notificaciones_admin USING btree (id);

CREATE UNIQUE INDEX notifications_pkey ON public.notifications USING btree (id);

CREATE UNIQUE INDEX notifications_user_id_dedup_key_key ON public.notifications USING btree (user_id, dedup_key);

CREATE UNIQUE INDEX padecimientos_nombre_key ON public.padecimientos USING btree (nombre);

CREATE UNIQUE INDEX padecimientos_pkey ON public.padecimientos USING btree (id_padecimiento);

CREATE UNIQUE INDEX recolecciones_evidencias_pkey ON public.recolecciones_evidencias USING btree (evidencia_id);

CREATE UNIQUE INDEX recolecciones_firmas_pkey ON public.recolecciones_firmas USING btree (recoleccion_id);

CREATE UNIQUE INDEX recolecciones_items_pkey ON public.recolecciones_items USING btree (recoleccion_id, sku);

CREATE UNIQUE INDEX recolecciones_pkey ON public.recolecciones USING btree (recoleccion_id);

CREATE UNIQUE INDEX saga_adjustments_pkey ON public.saga_adjustments USING btree (id);

CREATE UNIQUE INDEX saga_compensations_pkey ON public.saga_compensations USING btree (id);

CREATE UNIQUE INDEX saga_odv_links_id_saga_transaction_odv_id_key ON public.saga_zoho_links USING btree (id_saga_transaction, zoho_id);

CREATE UNIQUE INDEX saga_odv_links_pkey ON public.saga_zoho_links USING btree (id);

CREATE UNIQUE INDEX saga_transactions_pkey ON public.saga_transactions USING btree (id);

CREATE UNIQUE INDEX saga_zoho_links_saga_zoho_unique ON public.saga_zoho_links USING btree (id_saga_transaction, zoho_id);

CREATE UNIQUE INDEX uk_visit_tasks_task_id ON public.visit_tasks USING btree (task_id);

CREATE UNIQUE INDEX uq_venta_odv_cliente_sku ON public.ventas_odv USING btree (odv_id, id_cliente, sku);

CREATE UNIQUE INDEX user_notification_preferences_pkey ON public.user_notification_preferences USING btree (id);

CREATE UNIQUE INDEX user_notification_preferences_user_id_key ON public.user_notification_preferences USING btree (user_id);

CREATE UNIQUE INDEX user_push_tokens_pkey ON public.user_push_tokens USING btree (id);

CREATE UNIQUE INDEX user_push_tokens_token_key ON public.user_push_tokens USING btree (token);

CREATE UNIQUE INDEX usuarios_auth_user_id_key ON public.usuarios USING btree (auth_user_id);

CREATE UNIQUE INDEX usuarios_email_key ON public.usuarios USING btree (email);

CREATE UNIQUE INDEX usuarios_id_zoho_unique ON public.usuarios USING btree (id_zoho);

CREATE UNIQUE INDEX usuarios_pkey ON public.usuarios USING btree (id_usuario);

CREATE UNIQUE INDEX ventas_odv_pkey ON public.ventas_odv USING btree (id_venta);

CREATE UNIQUE INDEX visit_tasks_pkey ON public.visit_tasks USING btree (visit_id, task_tipo);

CREATE UNIQUE INDEX visita_informes_informe_id_key ON public.visita_informes USING btree (informe_id);

CREATE UNIQUE INDEX visita_informes_pkey ON public.visita_informes USING btree (visit_id);

CREATE UNIQUE INDEX visita_odvs_pkey ON public.visita_odvs USING btree (id);

CREATE UNIQUE INDEX visita_odvs_visit_id_odv_id_key ON public.visita_odvs USING btree (visit_id, odv_id);

CREATE UNIQUE INDEX visitas_pkey ON public.visitas USING btree (visit_id);

CREATE UNIQUE INDEX zoho_health_status_pkey ON public.zoho_health_status USING btree (id);

CREATE UNIQUE INDEX zoho_tokens_pkey ON public.zoho_tokens USING btree (auth_user_id);

CREATE UNIQUE INDEX zonas_pkey ON public.zonas USING btree (id_zona);

alter table "archive"."ciclos_botiquin" add constraint "ciclos_botiquin_pkey" PRIMARY KEY using index "ciclos_botiquin_pkey";

alter table "archive"."encuestas_ciclo" add constraint "encuestas_ciclo_pkey" PRIMARY KEY using index "encuestas_ciclo_pkey";

alter table "public"."audit_log" add constraint "audit_log_pkey" PRIMARY KEY using index "audit_log_pkey";

alter table "public"."botiquin_clientes_sku_disponibles" add constraint "botiquin_clientes_sku_disponibles_pkey" PRIMARY KEY using index "botiquin_clientes_sku_disponibles_pkey";

alter table "public"."botiquin_odv" add constraint "botiquin_odv_pkey" PRIMARY KEY using index "botiquin_odv_pkey";

alter table "public"."cliente_estado_log" add constraint "cliente_estado_log_pkey" PRIMARY KEY using index "cliente_estado_log_pkey";

alter table "public"."clientes" add constraint "clientes_pkey" PRIMARY KEY using index "clientes_pkey";

alter table "public"."event_outbox" add constraint "event_outbox_pkey" PRIMARY KEY using index "event_outbox_pkey";

alter table "public"."inventario_botiquin" add constraint "inventario_botiquin_pkey" PRIMARY KEY using index "inventario_botiquin_pkey";

alter table "public"."medicamento_padecimientos" add constraint "medicamento_padecimientos_pkey" PRIMARY KEY using index "medicamento_padecimientos_pkey";

alter table "public"."medicamentos" add constraint "medicamentos_pkey" PRIMARY KEY using index "medicamentos_pkey";

alter table "public"."movimientos_inventario" add constraint "movimientos_inventario_pkey" PRIMARY KEY using index "movimientos_inventario_pkey";

alter table "public"."notificaciones_admin" add constraint "notificaciones_admin_pkey" PRIMARY KEY using index "notificaciones_admin_pkey";

alter table "public"."notifications" add constraint "notifications_pkey" PRIMARY KEY using index "notifications_pkey";

alter table "public"."padecimientos" add constraint "padecimientos_pkey" PRIMARY KEY using index "padecimientos_pkey";

alter table "public"."recolecciones" add constraint "recolecciones_pkey" PRIMARY KEY using index "recolecciones_pkey";

alter table "public"."recolecciones_evidencias" add constraint "recolecciones_evidencias_pkey" PRIMARY KEY using index "recolecciones_evidencias_pkey";

alter table "public"."recolecciones_firmas" add constraint "recolecciones_firmas_pkey" PRIMARY KEY using index "recolecciones_firmas_pkey";

alter table "public"."recolecciones_items" add constraint "recolecciones_items_pkey" PRIMARY KEY using index "recolecciones_items_pkey";

alter table "public"."saga_adjustments" add constraint "saga_adjustments_pkey" PRIMARY KEY using index "saga_adjustments_pkey";

alter table "public"."saga_compensations" add constraint "saga_compensations_pkey" PRIMARY KEY using index "saga_compensations_pkey";

alter table "public"."saga_transactions" add constraint "saga_transactions_pkey" PRIMARY KEY using index "saga_transactions_pkey";

alter table "public"."saga_zoho_links" add constraint "saga_odv_links_pkey" PRIMARY KEY using index "saga_odv_links_pkey";

alter table "public"."user_notification_preferences" add constraint "user_notification_preferences_pkey" PRIMARY KEY using index "user_notification_preferences_pkey";

alter table "public"."user_push_tokens" add constraint "user_push_tokens_pkey" PRIMARY KEY using index "user_push_tokens_pkey";

alter table "public"."usuarios" add constraint "usuarios_pkey" PRIMARY KEY using index "usuarios_pkey";

alter table "public"."ventas_odv" add constraint "ventas_odv_pkey" PRIMARY KEY using index "ventas_odv_pkey";

alter table "public"."visit_tasks" add constraint "visit_tasks_pkey" PRIMARY KEY using index "visit_tasks_pkey";

alter table "public"."visita_informes" add constraint "visita_informes_pkey" PRIMARY KEY using index "visita_informes_pkey";

alter table "public"."visita_odvs" add constraint "visita_odvs_pkey" PRIMARY KEY using index "visita_odvs_pkey";

alter table "public"."visitas" add constraint "visitas_pkey" PRIMARY KEY using index "visitas_pkey";

alter table "public"."zoho_health_status" add constraint "zoho_health_status_pkey" PRIMARY KEY using index "zoho_health_status_pkey";

alter table "public"."zoho_tokens" add constraint "zoho_tokens_pkey" PRIMARY KEY using index "zoho_tokens_pkey";

alter table "public"."zonas" add constraint "zonas_pkey" PRIMARY KEY using index "zonas_pkey";

alter table "archive"."ciclos_botiquin" add constraint "ciclos_botiquin_id_ciclo_anterior_fkey" FOREIGN KEY (id_ciclo_anterior) REFERENCES archive.ciclos_botiquin(id_ciclo) not valid;

alter table "archive"."ciclos_botiquin" validate constraint "ciclos_botiquin_id_ciclo_anterior_fkey";

alter table "archive"."ciclos_botiquin" add constraint "ciclos_botiquin_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "archive"."ciclos_botiquin" validate constraint "ciclos_botiquin_id_cliente_fkey";

alter table "archive"."ciclos_botiquin" add constraint "ciclos_botiquin_id_usuario_fkey" FOREIGN KEY (id_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "archive"."ciclos_botiquin" validate constraint "ciclos_botiquin_id_usuario_fkey";

alter table "archive"."encuestas_ciclo" add constraint "encuestas_ciclo_id_ciclo_fkey" FOREIGN KEY (id_ciclo) REFERENCES archive.ciclos_botiquin(id_ciclo) not valid;

alter table "archive"."encuestas_ciclo" validate constraint "encuestas_ciclo_id_ciclo_fkey";

alter table "public"."audit_log" add constraint "audit_log_accion_check" CHECK (((accion)::text = ANY (ARRAY[('INSERT'::character varying)::text, ('UPDATE'::character varying)::text, ('DELETE'::character varying)::text]))) not valid;

alter table "public"."audit_log" validate constraint "audit_log_accion_check";

alter table "public"."audit_log" add constraint "audit_log_usuario_id_fkey" FOREIGN KEY (usuario_id) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."audit_log" validate constraint "audit_log_usuario_id_fkey";

alter table "public"."botiquin_clientes_sku_disponibles" add constraint "botiquin_clientes_sku_disponibles_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."botiquin_clientes_sku_disponibles" validate constraint "botiquin_clientes_sku_disponibles_id_cliente_fkey";

alter table "public"."botiquin_clientes_sku_disponibles" add constraint "botiquin_clientes_sku_disponibles_sku_fkey" FOREIGN KEY (sku) REFERENCES public.medicamentos(sku) ON DELETE CASCADE not valid;

alter table "public"."botiquin_clientes_sku_disponibles" validate constraint "botiquin_clientes_sku_disponibles_sku_fkey";

alter table "public"."botiquin_odv" add constraint "fk_botiquin_odv_cliente" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."botiquin_odv" validate constraint "fk_botiquin_odv_cliente";

alter table "public"."cliente_estado_log" add constraint "cliente_estado_log_changed_by_fkey" FOREIGN KEY (changed_by) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."cliente_estado_log" validate constraint "cliente_estado_log_changed_by_fkey";

alter table "public"."cliente_estado_log" add constraint "cliente_estado_log_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) ON DELETE CASCADE not valid;

alter table "public"."cliente_estado_log" validate constraint "cliente_estado_log_id_cliente_fkey";

alter table "public"."clientes" add constraint "clientes_id_usuario_fkey" FOREIGN KEY (id_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."clientes" validate constraint "clientes_id_usuario_fkey";

alter table "public"."clientes" add constraint "clientes_id_zona_fkey" FOREIGN KEY (id_zona) REFERENCES public.zonas(id_zona) not valid;

alter table "public"."clientes" validate constraint "clientes_id_zona_fkey";

alter table "public"."event_outbox" add constraint "event_outbox_saga_transaction_id_fkey" FOREIGN KEY (saga_transaction_id) REFERENCES public.saga_transactions(id) not valid;

alter table "public"."event_outbox" validate constraint "event_outbox_saga_transaction_id_fkey";

alter table "public"."inventario_botiquin" add constraint "inventario_botiquin_cantidad_disponible_check" CHECK ((cantidad_disponible >= 0)) not valid;

alter table "public"."inventario_botiquin" validate constraint "inventario_botiquin_cantidad_disponible_check";

alter table "public"."inventario_botiquin" add constraint "inventario_botiquin_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."inventario_botiquin" validate constraint "inventario_botiquin_id_cliente_fkey";

alter table "public"."inventario_botiquin" add constraint "inventario_botiquin_sku_fkey" FOREIGN KEY (sku) REFERENCES public.medicamentos(sku) not valid;

alter table "public"."inventario_botiquin" validate constraint "inventario_botiquin_sku_fkey";

alter table "public"."medicamento_padecimientos" add constraint "medicamento_padecimientos_id_padecimiento_fkey" FOREIGN KEY (id_padecimiento) REFERENCES public.padecimientos(id_padecimiento) ON DELETE CASCADE not valid;

alter table "public"."medicamento_padecimientos" validate constraint "medicamento_padecimientos_id_padecimiento_fkey";

alter table "public"."medicamento_padecimientos" add constraint "medicamento_padecimientos_sku_fkey" FOREIGN KEY (sku) REFERENCES public.medicamentos(sku) ON DELETE CASCADE not valid;

alter table "public"."medicamento_padecimientos" validate constraint "medicamento_padecimientos_sku_fkey";

alter table "public"."medicamentos" add constraint "medicamentos_codigo_barras_key" UNIQUE using index "medicamentos_codigo_barras_key";

alter table "public"."medicamentos" add constraint "medicamentos_precio_check" CHECK ((precio >= (0)::numeric)) not valid;

alter table "public"."medicamentos" validate constraint "medicamentos_precio_check";

alter table "public"."movimientos_inventario" add constraint "movimientos_inventario_cantidad_check" CHECK ((cantidad >= 0)) not valid;

alter table "public"."movimientos_inventario" validate constraint "movimientos_inventario_cantidad_check";

alter table "public"."movimientos_inventario" add constraint "movimientos_inventario_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."movimientos_inventario" validate constraint "movimientos_inventario_id_cliente_fkey";

alter table "public"."movimientos_inventario" add constraint "movimientos_inventario_id_saga_transaction_fkey" FOREIGN KEY (id_saga_transaction) REFERENCES public.saga_transactions(id) not valid;

alter table "public"."movimientos_inventario" validate constraint "movimientos_inventario_id_saga_transaction_fkey";

alter table "public"."movimientos_inventario" add constraint "movimientos_inventario_sku_fkey" FOREIGN KEY (sku) REFERENCES public.medicamentos(sku) not valid;

alter table "public"."movimientos_inventario" validate constraint "movimientos_inventario_sku_fkey";

alter table "public"."notificaciones_admin" add constraint "notificaciones_admin_leida_por_fkey" FOREIGN KEY (leida_por) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."notificaciones_admin" validate constraint "notificaciones_admin_leida_por_fkey";

alter table "public"."notificaciones_admin" add constraint "notificaciones_admin_para_usuario_fkey" FOREIGN KEY (para_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."notificaciones_admin" validate constraint "notificaciones_admin_para_usuario_fkey";

alter table "public"."notifications" add constraint "notifications_type_check" CHECK ((type = ANY (ARRAY['TASK_COMPLETED'::text, 'TASK_ERROR'::text, 'VISIT_REMINDER'::text, 'ADMIN_ACTION'::text, 'SYSTEM'::text]))) not valid;

alter table "public"."notifications" validate constraint "notifications_type_check";

alter table "public"."notifications" add constraint "notifications_user_id_dedup_key_key" UNIQUE using index "notifications_user_id_dedup_key_key";

alter table "public"."notifications" add constraint "notifications_user_id_fkey" FOREIGN KEY (user_id) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."notifications" validate constraint "notifications_user_id_fkey";

alter table "public"."padecimientos" add constraint "padecimientos_nombre_key" UNIQUE using index "padecimientos_nombre_key";

alter table "public"."recolecciones" add constraint "recolecciones_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."recolecciones" validate constraint "recolecciones_id_cliente_fkey";

alter table "public"."recolecciones" add constraint "recolecciones_id_usuario_fkey" FOREIGN KEY (id_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."recolecciones" validate constraint "recolecciones_id_usuario_fkey";

alter table "public"."recolecciones" add constraint "recolecciones_visit_id_fkey" FOREIGN KEY (visit_id) REFERENCES public.visitas(visit_id) ON DELETE CASCADE not valid;

alter table "public"."recolecciones" validate constraint "recolecciones_visit_id_fkey";

alter table "public"."recolecciones_evidencias" add constraint "recolecciones_evidencias_recoleccion_id_fkey" FOREIGN KEY (recoleccion_id) REFERENCES public.recolecciones(recoleccion_id) ON DELETE CASCADE not valid;

alter table "public"."recolecciones_evidencias" validate constraint "recolecciones_evidencias_recoleccion_id_fkey";

alter table "public"."recolecciones_firmas" add constraint "recolecciones_firmas_recoleccion_id_fkey" FOREIGN KEY (recoleccion_id) REFERENCES public.recolecciones(recoleccion_id) ON DELETE CASCADE not valid;

alter table "public"."recolecciones_firmas" validate constraint "recolecciones_firmas_recoleccion_id_fkey";

alter table "public"."recolecciones_items" add constraint "recolecciones_items_cantidad_check" CHECK ((cantidad >= 0)) not valid;

alter table "public"."recolecciones_items" validate constraint "recolecciones_items_cantidad_check";

alter table "public"."recolecciones_items" add constraint "recolecciones_items_recoleccion_id_fkey" FOREIGN KEY (recoleccion_id) REFERENCES public.recolecciones(recoleccion_id) ON DELETE CASCADE not valid;

alter table "public"."recolecciones_items" validate constraint "recolecciones_items_recoleccion_id_fkey";

alter table "public"."recolecciones_items" add constraint "recolecciones_items_sku_fkey" FOREIGN KEY (sku) REFERENCES public.medicamentos(sku) not valid;

alter table "public"."recolecciones_items" validate constraint "recolecciones_items_sku_fkey";

alter table "public"."saga_adjustments" add constraint "saga_adjustments_compensation_id_fkey" FOREIGN KEY (compensation_id) REFERENCES public.saga_compensations(id) not valid;

alter table "public"."saga_adjustments" validate constraint "saga_adjustments_compensation_id_fkey";

alter table "public"."saga_adjustments" add constraint "saga_adjustments_saga_transaction_id_fkey" FOREIGN KEY (saga_transaction_id) REFERENCES public.saga_transactions(id) not valid;

alter table "public"."saga_adjustments" validate constraint "saga_adjustments_saga_transaction_id_fkey";

alter table "public"."saga_compensations" add constraint "saga_compensations_compensated_by_fkey" FOREIGN KEY (compensated_by) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."saga_compensations" validate constraint "saga_compensations_compensated_by_fkey";

alter table "public"."saga_compensations" add constraint "saga_compensations_compensation_type_check" CHECK ((compensation_type = ANY (ARRAY['ADJUSTMENT'::text, 'RETRY'::text, 'FORCE_COMPLETE'::text, 'REVERT'::text]))) not valid;

alter table "public"."saga_compensations" validate constraint "saga_compensations_compensation_type_check";

alter table "public"."saga_compensations" add constraint "saga_compensations_saga_transaction_id_fkey" FOREIGN KEY (saga_transaction_id) REFERENCES public.saga_transactions(id) not valid;

alter table "public"."saga_compensations" validate constraint "saga_compensations_saga_transaction_id_fkey";

alter table "public"."saga_compensations" add constraint "saga_compensations_zoho_sync_status_check" CHECK ((zoho_sync_status = ANY (ARRAY['PENDING'::text, 'SYNCED'::text, 'FAILED'::text, 'MANUAL'::text]))) not valid;

alter table "public"."saga_compensations" validate constraint "saga_compensations_zoho_sync_status_check";

alter table "public"."saga_transactions" add constraint "saga_transactions_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."saga_transactions" validate constraint "saga_transactions_id_cliente_fkey";

alter table "public"."saga_transactions" add constraint "saga_transactions_id_usuario_fkey" FOREIGN KEY (id_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."saga_transactions" validate constraint "saga_transactions_id_usuario_fkey";

alter table "public"."saga_transactions" add constraint "saga_transactions_visit_id_fkey" FOREIGN KEY (visit_id) REFERENCES public.visitas(visit_id) not valid;

alter table "public"."saga_transactions" validate constraint "saga_transactions_visit_id_fkey";

alter table "public"."saga_zoho_links" add constraint "saga_odv_links_id_saga_transaction_fkey" FOREIGN KEY (id_saga_transaction) REFERENCES public.saga_transactions(id) ON DELETE CASCADE not valid;

alter table "public"."saga_zoho_links" validate constraint "saga_odv_links_id_saga_transaction_fkey";

alter table "public"."saga_zoho_links" add constraint "saga_odv_links_id_saga_transaction_odv_id_key" UNIQUE using index "saga_odv_links_id_saga_transaction_odv_id_key";

alter table "public"."saga_zoho_links" add constraint "saga_odv_links_zoho_sync_status_check" CHECK ((zoho_sync_status = ANY (ARRAY['pending'::text, 'synced'::text, 'error'::text]))) not valid;

alter table "public"."saga_zoho_links" validate constraint "saga_odv_links_zoho_sync_status_check";

alter table "public"."saga_zoho_links" add constraint "saga_zoho_links_saga_zoho_unique" UNIQUE using index "saga_zoho_links_saga_zoho_unique";

alter table "public"."user_notification_preferences" add constraint "user_notification_preferences_user_id_fkey" FOREIGN KEY (user_id) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."user_notification_preferences" validate constraint "user_notification_preferences_user_id_fkey";

alter table "public"."user_notification_preferences" add constraint "user_notification_preferences_user_id_key" UNIQUE using index "user_notification_preferences_user_id_key";

alter table "public"."user_push_tokens" add constraint "user_push_tokens_platform_check" CHECK ((platform = ANY (ARRAY['ios'::text, 'android'::text, 'web'::text]))) not valid;

alter table "public"."user_push_tokens" validate constraint "user_push_tokens_platform_check";

alter table "public"."user_push_tokens" add constraint "user_push_tokens_token_key" UNIQUE using index "user_push_tokens_token_key";

alter table "public"."user_push_tokens" add constraint "user_push_tokens_user_id_fkey" FOREIGN KEY (user_id) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."user_push_tokens" validate constraint "user_push_tokens_user_id_fkey";

alter table "public"."usuarios" add constraint "usuarios_auth_user_fk" FOREIGN KEY (auth_user_id) REFERENCES auth.users(id) ON DELETE SET NULL not valid;

alter table "public"."usuarios" validate constraint "usuarios_auth_user_fk";

alter table "public"."usuarios" add constraint "usuarios_email_key" UNIQUE using index "usuarios_email_key";

alter table "public"."usuarios" add constraint "usuarios_id_zoho_unique" UNIQUE using index "usuarios_id_zoho_unique";

alter table "public"."ventas_odv" add constraint "uq_venta_odv_cliente_sku" UNIQUE using index "uq_venta_odv_cliente_sku";

alter table "public"."ventas_odv" add constraint "ventas_odv_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."ventas_odv" validate constraint "ventas_odv_id_cliente_fkey";

alter table "public"."visit_tasks" add constraint "uk_visit_tasks_task_id" UNIQUE using index "uk_visit_tasks_task_id";

alter table "public"."visit_tasks" add constraint "visit_tasks_visit_id_fkey" FOREIGN KEY (visit_id) REFERENCES public.visitas(visit_id) ON DELETE CASCADE not valid;

alter table "public"."visit_tasks" validate constraint "visit_tasks_visit_id_fkey";

alter table "public"."visita_informes" add constraint "visita_informes_informe_id_key" UNIQUE using index "visita_informes_informe_id_key";

alter table "public"."visita_informes" add constraint "visita_informes_visit_id_fkey" FOREIGN KEY (visit_id) REFERENCES public.visitas(visit_id) ON DELETE CASCADE not valid;

alter table "public"."visita_informes" validate constraint "visita_informes_visit_id_fkey";

alter table "public"."visita_odvs" add constraint "visita_odvs_visit_id_fkey" FOREIGN KEY (visit_id) REFERENCES public.visitas(visit_id) ON DELETE CASCADE not valid;

alter table "public"."visita_odvs" validate constraint "visita_odvs_visit_id_fkey";

alter table "public"."visita_odvs" add constraint "visita_odvs_visit_id_odv_id_key" UNIQUE using index "visita_odvs_visit_id_odv_id_key";

alter table "public"."visitas" add constraint "visitas_id_ciclo_fkey" FOREIGN KEY (id_ciclo) REFERENCES archive.ciclos_botiquin(id_ciclo) not valid;

alter table "public"."visitas" validate constraint "visitas_id_ciclo_fkey";

alter table "public"."visitas" add constraint "visitas_id_cliente_fkey" FOREIGN KEY (id_cliente) REFERENCES public.clientes(id_cliente) not valid;

alter table "public"."visitas" validate constraint "visitas_id_cliente_fkey";

alter table "public"."visitas" add constraint "visitas_id_usuario_fkey" FOREIGN KEY (id_usuario) REFERENCES public.usuarios(id_usuario) not valid;

alter table "public"."visitas" validate constraint "visitas_id_usuario_fkey";

alter table "public"."zoho_health_status" add constraint "zoho_health_status_id_check" CHECK ((id = 1)) not valid;

alter table "public"."zoho_health_status" validate constraint "zoho_health_status_id_check";

alter table "public"."zoho_tokens" add constraint "zoho_tokens_auth_user_id_fkey" FOREIGN KEY (auth_user_id) REFERENCES auth.users(id) ON DELETE CASCADE not valid;

alter table "public"."zoho_tokens" validate constraint "zoho_tokens_auth_user_id_fkey";

set check_function_bodies = off;

create or replace view "analytics"."v_cambios_estado_recientes" as  SELECT cel.id,
    cel.id_cliente,
    c.nombre_cliente,
    cel.estado_anterior,
    cel.estado_nuevo,
    cel.changed_by,
    u.nombre AS changed_by_nombre,
    cel.changed_at,
    cel.razon,
    cel.dias_en_estado_anterior,
    cel.metadata
   FROM ((public.cliente_estado_log cel
     JOIN public.clientes c ON (((c.id_cliente)::text = (cel.id_cliente)::text)))
     LEFT JOIN public.usuarios u ON (((u.id_usuario)::text = (cel.changed_by)::text)))
  ORDER BY cel.changed_at DESC;


create or replace view "analytics"."v_clientes_por_estado" as  SELECT estado,
    count(*) AS cantidad,
    round((((count(*))::numeric * 100.0) / NULLIF(sum(count(*)) OVER (), (0)::numeric)), 2) AS porcentaje
   FROM public.clientes
  GROUP BY estado
  ORDER BY (count(*)) DESC;


create or replace view "analytics"."v_metricas_desercion" as  SELECT date_trunc('month'::text, changed_at) AS mes,
    count(*) FILTER (WHERE (estado_nuevo = 'INACTIVO'::public.estado_cliente)) AS bajas,
    count(*) FILTER (WHERE ((estado_nuevo = 'ACTIVO'::public.estado_cliente) AND (estado_anterior = 'INACTIVO'::public.estado_cliente))) AS reactivaciones,
    count(*) FILTER (WHERE (estado_nuevo = 'EN_BAJA'::public.estado_cliente)) AS marcados_baja,
    count(*) FILTER (WHERE (estado_nuevo = 'SUSPENDIDO'::public.estado_cliente)) AS suspensiones,
    ( SELECT count(*) AS count
           FROM public.clientes
          WHERE (clientes.estado = 'ACTIVO'::public.estado_cliente)) AS activos_actuales,
    ( SELECT count(*) AS count
           FROM public.clientes
          WHERE (clientes.estado = 'INACTIVO'::public.estado_cliente)) AS inactivos_actuales
   FROM public.cliente_estado_log
  GROUP BY (date_trunc('month'::text, changed_at))
  ORDER BY (date_trunc('month'::text, changed_at)) DESC;


create or replace view "analytics"."v_tiempo_activo_clientes" as  WITH periodos AS (
         SELECT cel.id_cliente,
            cel.estado_nuevo,
            cel.changed_at,
            lead(cel.changed_at) OVER (PARTITION BY cel.id_cliente ORDER BY cel.changed_at) AS siguiente_cambio
           FROM public.cliente_estado_log cel
        )
 SELECT p.id_cliente,
    c.nombre_cliente,
    c.estado AS estado_actual,
    (sum(
        CASE
            WHEN (p.estado_nuevo = 'ACTIVO'::public.estado_cliente) THEN EXTRACT(day FROM (COALESCE(p.siguiente_cambio, now()) - p.changed_at))
            ELSE (0)::numeric
        END))::integer AS dias_activo_total,
    count(*) FILTER (WHERE (p.estado_nuevo = 'ACTIVO'::public.estado_cliente)) AS veces_activado,
    count(*) FILTER (WHERE (p.estado_nuevo = 'INACTIVO'::public.estado_cliente)) AS veces_dado_baja,
    min(p.changed_at) FILTER (WHERE (p.estado_nuevo = 'ACTIVO'::public.estado_cliente)) AS primera_activacion,
    max(p.changed_at) FILTER (WHERE (p.estado_nuevo = 'INACTIVO'::public.estado_cliente)) AS ultima_baja
   FROM (periodos p
     JOIN public.clientes c ON (((c.id_cliente)::text = (p.id_cliente)::text)))
  GROUP BY p.id_cliente, c.nombre_cliente, c.estado;


create or replace view "audit"."audit_log" as  SELECT id,
    tabla,
    registro_id,
    accion,
    usuario_id,
    "timestamp",
    valores_antes,
    valores_despues,
    ip_address,
    user_agent
   FROM public.audit_log;


create or replace view "audit"."cliente_estado_log" as  SELECT id,
    id_cliente,
    estado_anterior,
    estado_nuevo,
    changed_by,
    changed_at,
    razon,
    metadata,
    dias_en_estado_anterior
   FROM public.cliente_estado_log;


create or replace view "audit"."notificaciones_admin" as  SELECT id,
    tipo,
    titulo,
    mensaje,
    metadata,
    leida,
    created_at,
    para_usuario,
    leida_at,
    leida_por
   FROM public.notificaciones_admin;


CREATE OR REPLACE FUNCTION public.audit_saga_transactions()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  INSERT INTO audit_log (
    tabla, registro_id, accion, usuario_id,
    valores_antes, valores_despues, timestamp
  ) VALUES (
    TG_TABLE_NAME,
    COALESCE(NEW.id::text, OLD.id::text),
    TG_OP,
    COALESCE(NEW.id_usuario, OLD.id_usuario),
    CASE WHEN TG_OP = 'DELETE' THEN row_to_json(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END,
    NOW()
  );
  RETURN COALESCE(NEW, OLD);
END;
$function$
;

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
      tabla,
      registro_id,
      accion,
      usuario_id,
      valores_antes,
      valores_despues
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
      tabla,
      registro_id,
      accion,
      usuario_id,
      valores_antes,
      valores_despues
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
      tabla,
      registro_id,
      accion,
      usuario_id,
      valores_antes,
      valores_despues
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
$function$
;

CREATE OR REPLACE FUNCTION public.can_access_cliente(p_id_cliente text)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select public.is_admin()
    or exists (
      select 1
      from public.clientes c
      where c.id_cliente = p_id_cliente
        and c.id_usuario = public.current_user_id()
    );
$function$
;

CREATE OR REPLACE FUNCTION public.can_access_visita(p_visit_id uuid)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  SELECT public.is_admin()
    OR EXISTS (
      SELECT 1 FROM public.visitas v
      WHERE v.visit_id = p_visit_id
        AND v.id_usuario = public.current_user_id()
    )
    OR EXISTS (
      SELECT 1 FROM public.visitas v
      JOIN public.clientes c ON c.id_cliente = v.id_cliente
      WHERE v.visit_id = p_visit_id
        AND c.id_usuario = public.current_user_id()
    );
$function$
;

CREATE OR REPLACE FUNCTION public.consolidate_duplicate_items(items_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
DECLARE
  result jsonb := '[]'::jsonb;
  item_record record;
BEGIN
  -- Agrupar por SKU y tipo_movimiento, sumando cantidades
  FOR item_record IN
    SELECT 
      item->>'sku' as sku,
      item->>'tipo_movimiento' as tipo_movimiento,
      SUM((item->>'cantidad')::int) as cantidad_total
    FROM jsonb_array_elements(items_json) as item
    GROUP BY item->>'sku', item->>'tipo_movimiento'
  LOOP
    result := result || jsonb_build_object(
      'sku', item_record.sku,
      'cantidad', item_record.cantidad_total,
      'tipo_movimiento', item_record.tipo_movimiento
    );
  END LOOP;
  
  RETURN result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.create_notification(p_user_id character varying, p_type text, p_title text, p_body text, p_data jsonb DEFAULT '{}'::jsonb, p_dedup_key text DEFAULT NULL::text, p_expires_in_hours integer DEFAULT NULL::integer)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_notification_id UUID;
    v_expires_at TIMESTAMPTZ;
BEGIN
    -- Calculate expiration if provided
    IF p_expires_in_hours IS NOT NULL THEN
        v_expires_at := NOW() + (p_expires_in_hours || ' hours')::INTERVAL;
    END IF;

    -- Insert notification (with dedup if key provided)
    IF p_dedup_key IS NOT NULL THEN
        INSERT INTO notifications (
            user_id, type, title, body, data, dedup_key, expires_at
        ) VALUES (
            p_user_id, p_type, p_title, p_body, p_data, p_dedup_key, v_expires_at
        )
        ON CONFLICT (user_id, dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
        RETURNING id INTO v_notification_id;
    ELSE
        INSERT INTO notifications (
            user_id, type, title, body, data, expires_at
        ) VALUES (
            p_user_id, p_type, p_title, p_body, p_data, v_expires_at
        )
        RETURNING id INTO v_notification_id;
    END IF;

    -- Push notification is now handled via database webhook on INSERT
    -- No need for pgmq queue

    RETURN v_notification_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.current_user_id()
 RETURNS text
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET row_security TO 'off'
AS $function$
  select u.id_usuario
  from public.usuarios u
  where u.auth_user_id = auth.uid()
  limit 1;
$function$
;

CREATE OR REPLACE FUNCTION public.deduplicate_saga_items(items_array jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  result jsonb;
BEGIN
  -- Agrupa por SKU y tipo_movimiento, sumando cantidades
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', sku,
      'tipo_movimiento', tipo_movimiento,
      'cantidad', total_cantidad
    )
  )
  INTO result
  FROM (
    SELECT 
      item->>'sku' as sku,
      item->>'tipo_movimiento' as tipo_movimiento,
      SUM((item->>'cantidad')::int) as total_cantidad
    FROM jsonb_array_elements(items_array) as item
    GROUP BY item->>'sku', item->>'tipo_movimiento'
  ) aggregated;
  
  RETURN result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_cliente_estado_log_dias()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Buscar el último cambio de estado para este cliente
  SELECT EXTRACT(DAY FROM (now() - changed_at))::integer
  INTO NEW.dias_en_estado_anterior
  FROM public.cliente_estado_log
  WHERE id_cliente = NEW.id_cliente
  ORDER BY changed_at DESC
  LIMIT 1;

  -- Si es el primer registro, días = NULL (ya está por defecto)
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_crear_movimiento_creacion_lote()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
    INSERT INTO public.movimientos_botiquin (
        id_lote,
        id_ciclo,
        fecha_movimiento,
        tipo,
        cantidad
    )
    VALUES (
        NEW.id_lote,
        NEW.id_ciclo_ingreso,
        COALESCE(NEW.fecha_ingreso, CURRENT_TIMESTAMP),
        'CREACION'::public.tipo_movimiento_botiquin,
        NEW.cantidad_inicial
    );

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_crear_notificacion(p_tipo public.tipo_notificacion, p_titulo character varying, p_mensaje text DEFAULT NULL::text, p_metadata jsonb DEFAULT '{}'::jsonb, p_para_usuario character varying DEFAULT NULL::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id UUID;
BEGIN
  INSERT INTO public.notificaciones_admin (tipo, titulo, mensaje, metadata, para_usuario)
  VALUES (p_tipo, p_titulo, p_mensaje, p_metadata, p_para_usuario)
  RETURNING id INTO v_id;

  RETURN v_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_notificar_cambio_estado()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_nombre_cliente VARCHAR;
BEGIN
  -- Obtener nombre del cliente
  SELECT nombre_cliente INTO v_nombre_cliente
  FROM public.clientes
  WHERE id_cliente = NEW.id_cliente;

  -- Crear notificación según el nuevo estado
  IF NEW.estado_nuevo = 'EN_BAJA' THEN
    PERFORM fn_crear_notificacion(
      'CLIENTE_EN_BAJA',
      'Cliente marcado para baja: ' || COALESCE(v_nombre_cliente, NEW.id_cliente),
      NEW.razon,
      jsonb_build_object(
        'id_cliente', NEW.id_cliente, 
        'changed_by', NEW.changed_by,
        'estado_anterior', NEW.estado_anterior
      )
    );
  ELSIF NEW.estado_nuevo = 'INACTIVO' THEN
    PERFORM fn_crear_notificacion(
      'CLIENTE_INACTIVO',
      'Cliente dado de baja: ' || COALESCE(v_nombre_cliente, NEW.id_cliente),
      COALESCE(NEW.razon, 'Baja completada'),
      jsonb_build_object(
        'id_cliente', NEW.id_cliente,
        'automatico', NEW.metadata->>'automatico' = 'true'
      )
    );
  ELSIF NEW.estado_nuevo = 'ACTIVO' AND NEW.estado_anterior = 'INACTIVO' THEN
    PERFORM fn_crear_notificacion(
      'CLIENTE_REACTIVADO',
      'Cliente reactivado: ' || COALESCE(v_nombre_cliente, NEW.id_cliente),
      NEW.razon,
      jsonb_build_object(
        'id_cliente', NEW.id_cliente, 
        'changed_by', NEW.changed_by
      )
    );
  ELSIF NEW.estado_nuevo = 'SUSPENDIDO' THEN
    PERFORM fn_crear_notificacion(
      'CLIENTE_SUSPENDIDO',
      'Cliente suspendido: ' || COALESCE(v_nombre_cliente, NEW.id_cliente),
      NEW.razon,
      jsonb_build_object(
        'id_cliente', NEW.id_cliente, 
        'changed_by', NEW.changed_by
      )
    );
  END IF;

  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.fn_remove_sku_disponible_on_venta()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Solo actuar en movimientos tipo VENTA
  IF NEW.tipo = 'VENTA' THEN
    DELETE FROM public.botiquin_clientes_sku_disponibles
    WHERE id_cliente = NEW.id_cliente
      AND sku = NEW.sku;
    
    -- Log en audit_log si existe la infraestructura
    BEGIN
      INSERT INTO public.audit_log (
        tabla,
        registro_id,
        accion,
        usuario_id,
        valores_antes,
        valores_despues
      )
      VALUES (
        'botiquin_clientes_sku_disponibles',
        NEW.id_cliente || ':' || NEW.sku,
        'DELETE',
        NULL,  -- Sistema automático
        jsonb_build_object(
          'id_cliente', NEW.id_cliente,
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
$function$
;

CREATE OR REPLACE FUNCTION public.fn_sync_cliente_activo()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Sincronizar campo activo basándose en estado
  NEW.activo := (NEW.estado IN ('ACTIVO', 'EN_BAJA', 'SUSPENDIDO'));
  NEW.updated_at := now();
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_balance_metrics()
 RETURNS TABLE(concepto text, valor_creado numeric, valor_ventas numeric, valor_recoleccion numeric, valor_permanencia_entrada numeric, valor_permanencia_virtual numeric, valor_calculado_total numeric, diferencia numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH metricas_inventario AS (
    -- Current stock: only active clients (they still have inventory)
    SELECT
      SUM(inv.cantidad_disponible * med.precio) as total_stock_vivo
    FROM inventario_botiquin inv
    JOIN medicamentos med ON inv.sku = med.sku
    JOIN clientes c ON inv.id_cliente = c.id_cliente
    WHERE c.activo = TRUE
  ),
  metricas_movimientos AS (
    -- Historical movements: ALL clients (including inactive like Ericka)
    SELECT
      SUM(CASE WHEN mov.tipo = 'CREACION'
        THEN mov.cantidad * med.precio ELSE 0 END) as total_creado_historico,
      SUM(CASE WHEN mov.tipo = 'PERMANENCIA'
        THEN mov.cantidad * med.precio ELSE 0 END) as total_permanencia_entrada,
      SUM(CASE WHEN mov.tipo = 'VENTA'
        THEN mov.cantidad * med.precio ELSE 0 END) as total_ventas,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION'
        THEN mov.cantidad * med.precio ELSE 0 END) as total_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    -- NO filter on c.activo - include all historical data
  )
  SELECT
    'BALANCE_GLOBAL_SISTEMA'::TEXT as concepto,
    COALESCE(M.total_creado_historico, 0) as valor_creado,
    COALESCE(M.total_ventas, 0) as valor_ventas,
    COALESCE(M.total_recoleccion, 0) as valor_recoleccion,
    COALESCE(M.total_permanencia_entrada, 0) as valor_permanencia_entrada,
    COALESCE(I.total_stock_vivo, 0) as valor_permanencia_virtual,
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as valor_calculado_total,
    -- Formula: CREACION - (VENTA + RECOLECCION + STOCK)
    COALESCE(M.total_creado_historico, 0) -
    (COALESCE(M.total_ventas, 0) + COALESCE(M.total_recoleccion, 0) + COALESCE(I.total_stock_vivo, 0)) as diferencia
  FROM metricas_inventario I
  CROSS JOIN metricas_movimientos M;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_botiquin_data()
 RETURNS TABLE(sku character varying, id_movimiento bigint, tipo_movimiento text, cantidad integer, fecha_movimiento text, id_lote text, fecha_ingreso text, cantidad_inicial integer, cantidad_disponible integer, id_cliente character varying, nombre_cliente character varying, rango character varying, facturacion_promedio numeric, facturacion_total numeric, producto character varying, precio numeric, marca character varying, top boolean, padecimiento character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    med.sku,
    mov.id as id_movimiento,
    CAST(mov.tipo AS TEXT) AS tipo_movimiento,
    mov.cantidad,
    TO_CHAR(mov.fecha_movimiento, 'DD/MM/YYYY') AS fecha_movimiento,
    mov.id::TEXT as id_lote,
    TO_CHAR(mov.fecha_movimiento, 'DD/MM/YYYY') AS fecha_ingreso,
    COALESCE(inv.cantidad_disponible, 0)::INTEGER as cantidad_inicial,
    COALESCE(inv.cantidad_disponible, 0)::INTEGER as cantidad_disponible,
    mov.id_cliente,
    c.nombre_cliente,
    c.rango,
    c.facturacion_promedio,
    c.facturacion_total,
    med.producto,
    med.precio,
    med.marca,
    med.top,
    p.nombre as padecimiento
  FROM movimientos_inventario mov
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  JOIN medicamentos med ON mov.sku = med.sku
  LEFT JOIN inventario_botiquin inv
    ON mov.id_cliente = inv.id_cliente AND mov.sku = inv.sku
  LEFT JOIN medicamento_padecimientos mp ON mov.sku = mp.sku
  LEFT JOIN padecimientos p ON mp.id_padecimiento = p.id_padecimiento;
  -- NO FILTER: include all clients (active and inactive)
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_conversion_details()
 RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, sku character varying, producto character varying, fecha_botiquin date, fecha_primera_odv date, dias_conversion integer, num_ventas_odv bigint, total_piezas bigint, valor_generado numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH botiquin_first_sale AS (
    -- Primera VENTA por cliente+sku (TODOS los clientes, sin filtro activo)
    SELECT m.id_cliente, c.nombre_cliente, m.sku, med.producto,
           MIN(m.fecha_movimiento::date) as primera_venta
    FROM movimientos_inventario m
    JOIN clientes c ON m.id_cliente = c.id_cliente
    JOIN medicamentos med ON m.sku = med.sku
    WHERE m.tipo = 'VENTA'
    GROUP BY m.id_cliente, c.nombre_cliente, m.sku, med.producto
  )
  SELECT
    b.id_cliente::varchar, b.nombre_cliente::varchar, b.sku::varchar, b.producto::varchar,
    b.primera_venta as fecha_botiquin,
    MIN(v.fecha) as fecha_primera_odv,
    (MIN(v.fecha) - b.primera_venta)::int as dias_conversion,
    COUNT(v.id_venta) as num_ventas_odv,
    SUM(v.cantidad)::bigint as total_piezas,
    SUM(v.cantidad * v.precio) as valor_generado
  FROM botiquin_first_sale b
  JOIN ventas_odv v ON b.id_cliente = v.id_cliente AND b.sku = v.sku
  WHERE v.fecha >= b.primera_venta
  GROUP BY b.id_cliente, b.nombre_cliente, b.sku, b.producto, b.primera_venta
  ORDER BY SUM(v.cantidad * v.precio) DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_conversion_metrics()
 RETURNS TABLE(total_adopciones bigint, total_conversiones bigint, valor_generado numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH botiquin_first_sale AS (
    -- Primera VENTA por cliente+sku (TODOS los clientes, sin filtro activo)
    SELECT m.id_cliente, m.sku, MIN(m.fecha_movimiento::date) as primera_venta
    FROM movimientos_inventario m
    WHERE m.tipo = 'VENTA'
    GROUP BY m.id_cliente, m.sku
  ),
  conversiones AS (
    SELECT b.id_cliente, b.sku, SUM(v.cantidad * v.precio) as valor
    FROM botiquin_first_sale b
    JOIN ventas_odv v ON b.id_cliente = v.id_cliente AND b.sku = v.sku
    WHERE v.fecha >= b.primera_venta
    GROUP BY b.id_cliente, b.sku
  )
  SELECT
    (SELECT COUNT(*) FROM botiquin_first_sale)::bigint,
    (SELECT COUNT(*) FROM conversiones)::bigint,
    COALESCE((SELECT SUM(valor) FROM conversiones), 0)::numeric;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_actual_rango()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_fin date;
  v_fecha_inicio date;
  v_prev_fecha date;
BEGIN
  -- Obtener la fecha más reciente con movimientos
  SELECT MAX(fecha_movimiento::date) INTO v_fecha_fin
  FROM movimientos_inventario;
  
  -- Encontrar el inicio del corte (retroceder hasta encontrar gap > 3 días)
  v_fecha_inicio := v_fecha_fin;
  v_prev_fecha := v_fecha_fin;
  
  FOR v_prev_fecha IN 
    SELECT DISTINCT fecha_movimiento::date 
    FROM movimientos_inventario 
    WHERE fecha_movimiento::date <= v_fecha_fin
    ORDER BY fecha_movimiento::date DESC
  LOOP
    IF v_fecha_inicio - v_prev_fecha > 3 THEN
      EXIT;
    END IF;
    v_fecha_inicio := v_prev_fecha;
  END LOOP;
  
  RETURN QUERY SELECT v_fecha_inicio, v_fecha_fin, (v_fecha_fin - v_fecha_inicio + 1)::int;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_anterior_rango()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_current_inicio date;
  v_fecha_fin date;
  v_fecha_inicio date;
  v_prev_fecha date;
  v_num_visitas int;
BEGIN
  -- Get current cut start date
  SELECT r.fecha_inicio INTO v_current_inicio
  FROM get_corte_actual_rango() r;
  
  -- Find candidate end dates (before current cut with gap > 3 days)
  -- and check if they have enough visits to be a "real" corte
  FOR v_fecha_fin IN 
    SELECT DISTINCT fecha_movimiento::date 
    FROM movimientos_inventario
    WHERE fecha_movimiento::date < v_current_inicio - INTERVAL '3 days'
    ORDER BY fecha_movimiento::date DESC
  LOOP
    -- Find the start of this potential corte
    v_fecha_inicio := v_fecha_fin;
    
    FOR v_prev_fecha IN 
      SELECT DISTINCT fecha_movimiento::date 
      FROM movimientos_inventario 
      WHERE fecha_movimiento::date <= v_fecha_fin
      ORDER BY fecha_movimiento::date DESC
    LOOP
      IF v_fecha_inicio - v_prev_fecha > 3 THEN
        EXIT;
      END IF;
      v_fecha_inicio := v_prev_fecha;
    END LOOP;
    
    -- Count distinct visits (saga_transactions) in this range
    SELECT COUNT(DISTINCT id_saga_transaction) INTO v_num_visitas
    FROM movimientos_inventario
    WHERE fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND id_saga_transaction IS NOT NULL;
    
    -- If this corte has at least 3 visits, it's a "real" corte
    IF v_num_visitas >= 3 THEN
      RETURN QUERY SELECT v_fecha_inicio, v_fecha_fin, (v_fecha_fin - v_fecha_inicio + 1)::int;
      RETURN;
    END IF;
    
    -- Otherwise, continue searching further back
  END LOOP;
  
  -- No valid previous corte found
  RETURN;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_anterior_stats()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, id_cliente character varying, nombre_cliente character varying, valor_venta numeric, piezas_venta integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_corte_actual_inicio date;
  v_corte_anterior_fin date;
  v_corte_anterior_inicio date;
  v_prev_fecha date;
BEGIN
  -- Obtener inicio del corte actual
  SELECT r.fecha_inicio INTO v_corte_actual_inicio
  FROM get_corte_actual_rango() r;

  -- Buscar la fecha más reciente con VENTAS antes del corte actual
  SELECT MAX(fecha_movimiento::date) INTO v_corte_anterior_fin
  FROM movimientos_inventario
  WHERE fecha_movimiento::date < v_corte_actual_inicio
    AND tipo = 'VENTA';  -- Solo buscar períodos con VENTAS

  IF v_corte_anterior_fin IS NULL THEN
    -- No hay corte anterior con ventas
    RETURN;
  END IF;

  -- Encontrar el inicio del corte anterior (retroceder hasta gap > 3 días)
  -- Solo considerar fechas con VENTAS
  v_corte_anterior_inicio := v_corte_anterior_fin;

  FOR v_prev_fecha IN
    SELECT DISTINCT fecha_movimiento::date
    FROM movimientos_inventario
    WHERE fecha_movimiento::date <= v_corte_anterior_fin
      AND tipo = 'VENTA'  -- Solo fechas con VENTAS
    ORDER BY fecha_movimiento::date DESC
  LOOP
    IF v_corte_anterior_inicio - v_prev_fecha > 3 THEN
      EXIT;
    END IF;
    v_corte_anterior_inicio := v_prev_fecha;
  END LOOP;

  -- Retornar stats del corte anterior por médico
  RETURN QUERY
  SELECT
    v_corte_anterior_inicio as fecha_inicio,
    v_corte_anterior_fin as fecha_fin,
    c.id_cliente,
    c.nombre_cliente,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0) as valor_venta,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END), 0)::int as piezas_venta
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_corte_anterior_inicio AND v_corte_anterior_fin
  GROUP BY c.id_cliente, c.nombre_cliente;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_filtros_disponibles()
 RETURNS TABLE(marcas character varying[], medicos jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;
  
  RETURN QUERY
  SELECT 
    ARRAY_AGG(DISTINCT med.marca)::varchar[] as marcas,
    jsonb_agg(DISTINCT jsonb_build_object('id', c.id_cliente, 'nombre', c.nombre_cliente)) as medicos
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_skus_valor_por_visita(p_id_cliente character varying DEFAULT NULL::character varying, p_marca character varying DEFAULT NULL::character varying)
 RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, marca character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Obtener rango del corte actual
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;
  
  RETURN QUERY
  SELECT 
    c.id_cliente,
    c.nombre_cliente,
    mov.fecha_movimiento::date as fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0) as valor_venta,
    med.marca
  FROM movimientos_inventario mov
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON mov.id_cliente = c.id_cliente
  WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
    AND mov.tipo = 'VENTA'
    AND (p_id_cliente IS NULL OR c.id_cliente = p_id_cliente)
    AND (p_marca IS NULL OR med.marca = p_marca)
  GROUP BY c.id_cliente, c.nombre_cliente, mov.fecha_movimiento::date, med.marca
  ORDER BY valor_venta DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_stats_generales()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Obtener rango del corte actual
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;
  
  RETURN QUERY
  WITH medicos_visitados AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
  ),
  medicos_con_venta AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.tipo = 'VENTA'
  ),
  stats AS (
    SELECT
      COUNT(*)::int as total_mov,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int as pz_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int as pz_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int as pz_recoleccion,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END) as val_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END) as val_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END) as val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
  )
  SELECT 
    v_fecha_inicio,
    v_fecha_fin,
    (v_fecha_fin - v_fecha_inicio + 1)::int,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta)
  FROM stats s;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_stats_generales_con_comparacion()
 RETURNS TABLE(fecha_inicio date, fecha_fin date, dias_corte integer, total_medicos_visitados integer, total_movimientos integer, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, medicos_con_venta integer, medicos_sin_venta integer, valor_venta_anterior numeric, valor_creacion_anterior numeric, valor_recoleccion_anterior numeric, promedio_por_medico_anterior numeric, porcentaje_cambio_venta numeric, porcentaje_cambio_creacion numeric, porcentaje_cambio_recoleccion numeric, porcentaje_cambio_promedio numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
  v_ant_fecha_inicio date;
  v_ant_fecha_fin date;
  v_ant_val_venta numeric;
  v_ant_val_creacion numeric;
  v_ant_val_recoleccion numeric;
  v_ant_medicos_con_venta int;
BEGIN
  -- Get current cut range
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;
  
  -- Get previous cut range (must have 3+ visits to be "real")
  SELECT r.fecha_inicio, r.fecha_fin INTO v_ant_fecha_inicio, v_ant_fecha_fin
  FROM get_corte_anterior_rango() r;
  
  -- Get previous cut values - NO filter on activo (includes Ericka!)
  IF v_ant_fecha_inicio IS NOT NULL THEN
    SELECT 
      COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END), 0),
      COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END), 0)
    INTO v_ant_val_venta, v_ant_val_creacion, v_ant_val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    WHERE mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin;
    
    SELECT COUNT(DISTINCT mov.id_cliente) INTO v_ant_medicos_con_venta
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_ant_fecha_inicio AND v_ant_fecha_fin
      AND mov.tipo = 'VENTA';
  ELSE
    v_ant_val_venta := NULL;
    v_ant_val_creacion := NULL;
    v_ant_val_recoleccion := NULL;
    v_ant_medicos_con_venta := NULL;
  END IF;
  
  RETURN QUERY
  WITH 
  -- Current corte: only active clients
  medicos_visitados AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND c.activo = TRUE
  ),
  medicos_con_venta_actual AS (
    SELECT DISTINCT mov.id_cliente
    FROM movimientos_inventario mov
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.tipo = 'VENTA'
      AND c.activo = TRUE
  ),
  stats_actual AS (
    SELECT
      COUNT(*)::int as total_mov,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int as pz_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int as pz_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int as pz_recoleccion,
      SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END) as val_venta,
      SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END) as val_creacion,
      SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END) as val_recoleccion
    FROM movimientos_inventario mov
    JOIN medicamentos med ON mov.sku = med.sku
    JOIN clientes c ON mov.id_cliente = c.id_cliente
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND c.activo = TRUE
  )
  SELECT 
    v_fecha_inicio,
    v_fecha_fin,
    (v_fecha_fin - v_fecha_inicio + 1)::int,
    (SELECT COUNT(*)::int FROM medicos_visitados),
    s.total_mov,
    s.pz_venta,
    s.pz_creacion,
    s.pz_recoleccion,
    COALESCE(s.val_venta, 0),
    COALESCE(s.val_creacion, 0),
    COALESCE(s.val_recoleccion, 0),
    (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    (SELECT COUNT(*)::int FROM medicos_visitados) - (SELECT COUNT(*)::int FROM medicos_con_venta_actual),
    -- Previous corte values (includes inactive doctors)
    v_ant_val_venta,
    v_ant_val_creacion,
    v_ant_val_recoleccion,
    CASE WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0 
      THEN v_ant_val_venta / v_ant_medicos_con_venta 
      ELSE NULL 
    END,
    -- Percentage changes
    CASE WHEN v_ant_val_venta IS NOT NULL AND v_ant_val_venta > 0 
      THEN ROUND(((COALESCE(s.val_venta, 0) - v_ant_val_venta) / v_ant_val_venta * 100)::numeric, 1)
      ELSE NULL 
    END,
    CASE WHEN v_ant_val_creacion IS NOT NULL AND v_ant_val_creacion > 0 
      THEN ROUND(((COALESCE(s.val_creacion, 0) - v_ant_val_creacion) / v_ant_val_creacion * 100)::numeric, 1)
      ELSE NULL 
    END,
    CASE WHEN v_ant_val_recoleccion IS NOT NULL AND v_ant_val_recoleccion > 0 
      THEN ROUND(((COALESCE(s.val_recoleccion, 0) - v_ant_val_recoleccion) / v_ant_val_recoleccion * 100)::numeric, 1)
      ELSE NULL 
    END,
    CASE 
      WHEN v_ant_medicos_con_venta IS NOT NULL AND v_ant_medicos_con_venta > 0 
           AND (SELECT COUNT(*)::int FROM medicos_con_venta_actual) > 0 
           AND v_ant_val_venta > 0 THEN
        ROUND((
          (COALESCE(s.val_venta, 0) / (SELECT COUNT(*)::int FROM medicos_con_venta_actual)) - 
          (v_ant_val_venta / v_ant_medicos_con_venta)
        ) / (v_ant_val_venta / v_ant_medicos_con_venta) * 100, 1)
      ELSE NULL 
    END
  FROM stats_actual s;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_stats_por_medico()
 RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, skus_creados text, skus_recolectados text, tiene_venta boolean)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_fecha_inicio date;
  v_fecha_fin date;
BEGIN
  -- Obtener rango del corte actual
  SELECT r.fecha_inicio, r.fecha_fin INTO v_fecha_inicio, v_fecha_fin
  FROM get_corte_actual_rango() r;
  
  RETURN QUERY
  WITH visitas_en_corte AS (
    -- Get unique visits (saga_transactions) in the corte period
    SELECT DISTINCT
      mov.id_cliente,
      mov.id_saga_transaction,
      MIN(mov.fecha_movimiento::date) as fecha_saga
    FROM movimientos_inventario mov
    WHERE mov.fecha_movimiento::date BETWEEN v_fecha_inicio AND v_fecha_fin
      AND mov.id_saga_transaction IS NOT NULL
    GROUP BY mov.id_cliente, mov.id_saga_transaction
  )
  SELECT 
    c.id_cliente,
    c.nombre_cliente,
    MAX(v.fecha_saga) as fecha_visita,  -- Most recent visit date
    SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad ELSE 0 END)::int,
    SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad ELSE 0 END)::int,
    SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad ELSE 0 END)::int,
    COALESCE(SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'CREACION' THEN mov.cantidad * med.precio ELSE 0 END), 0),
    COALESCE(SUM(CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.cantidad * med.precio ELSE 0 END), 0),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'VENTA' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'CREACION' THEN mov.sku END, ', '),
    STRING_AGG(DISTINCT CASE WHEN mov.tipo = 'RECOLECCION' THEN mov.sku END, ', '),
    SUM(CASE WHEN mov.tipo = 'VENTA' THEN 1 ELSE 0 END) > 0
  FROM visitas_en_corte v
  JOIN movimientos_inventario mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON v.id_cliente = c.id_cliente
  -- Group by doctor only (aggregate all their visits)
  GROUP BY c.id_cliente, c.nombre_cliente
  ORDER BY SUM(CASE WHEN mov.tipo = 'VENTA' THEN mov.cantidad * med.precio ELSE 0 END) DESC,
           c.nombre_cliente;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_corte_stats_por_medico_con_comparacion()
 RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, piezas_venta integer, piezas_creacion integer, piezas_recoleccion integer, valor_venta numeric, valor_creacion numeric, valor_recoleccion numeric, skus_vendidos text, tiene_venta boolean, valor_venta_anterior numeric, porcentaje_cambio numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH corte_actual AS (
    SELECT * FROM get_corte_stats_por_medico()
  ),
  corte_anterior AS (
    SELECT * FROM get_corte_anterior_stats()
  )
  SELECT 
    ca.id_cliente,
    ca.nombre_cliente,
    ca.fecha_visita,
    ca.piezas_venta,
    ca.piezas_creacion,
    ca.piezas_recoleccion,
    ca.valor_venta,
    ca.valor_creacion,
    ca.valor_recoleccion,
    ca.skus_vendidos,
    ca.tiene_venta,
    COALESCE(cp.valor_venta, 0) as valor_venta_anterior,
    CASE 
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta > 0 THEN 100.00
      WHEN COALESCE(cp.valor_venta, 0) = 0 AND ca.valor_venta = 0 THEN 0.00
      ELSE ROUND(((ca.valor_venta - COALESCE(cp.valor_venta, 0)) / cp.valor_venta * 100), 1)
    END as porcentaje_cambio
  FROM corte_actual ca
  LEFT JOIN corte_anterior cp ON ca.id_cliente = cp.id_cliente
  ORDER BY ca.valor_venta DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_current_user_id()
 RETURNS character varying
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
    SELECT id_usuario FROM usuarios WHERE auth_user_id = auth.uid()
$function$
;

CREATE OR REPLACE FUNCTION public.get_direccion_movimiento(p_tipo migration.tipo_movimiento_botiquin)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'migration'
AS $function$
  SELECT CASE p_tipo
    WHEN 'CREACION' THEN 'ENTRADA'
    WHEN 'VENTA' THEN 'SALIDA'
    WHEN 'RECOLECCION' THEN 'SALIDA'
    WHEN 'PERMANENCIA' THEN NULL
  END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_historico_conversiones_evolucion(p_fecha_inicio date DEFAULT NULL::date, p_fecha_fin date DEFAULT NULL::date, p_agrupacion text DEFAULT 'day'::text)
 RETURNS TABLE(fecha_grupo date, fecha_label text, skus_unicos_total integer, skus_unicos_botiquin integer, skus_unicos_directo integer, valor_total numeric, valor_botiquin numeric, valor_directo numeric, num_transacciones integer, num_clientes integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH primera_venta_botiquin AS (
    -- Primera VENTA por cliente+sku (TODOS los clientes)
    SELECT
      m.id_cliente,
      m.sku,
      MIN(m.fecha_movimiento::DATE) as primera_venta
    FROM movimientos_inventario m
    WHERE m.tipo = 'VENTA'
    GROUP BY m.id_cliente, m.sku
  ),
  ventas_clasificadas AS (
    SELECT
      v.id_cliente,
      v.sku,
      v.fecha,
      v.cantidad,
      v.precio,
      (v.cantidad * COALESCE(v.precio, 0)) as valor_venta,
      CASE
        WHEN pv.id_cliente IS NOT NULL AND v.fecha >= pv.primera_venta THEN TRUE
        ELSE FALSE
      END as es_de_botiquin,
      CASE
        WHEN p_agrupacion = 'week' THEN date_trunc('week', v.fecha)::DATE
        ELSE v.fecha::DATE
      END as fecha_agrupada
    FROM ventas_odv v
    LEFT JOIN primera_venta_botiquin pv
      ON v.id_cliente = pv.id_cliente AND v.sku = pv.sku
    WHERE (p_fecha_inicio IS NULL OR v.fecha >= p_fecha_inicio)
      AND (p_fecha_fin IS NULL OR v.fecha <= p_fecha_fin)
  )
  SELECT
    vc.fecha_agrupada as fecha_grupo,
    CASE
      WHEN p_agrupacion = 'week' THEN 'Sem ' || to_char(vc.fecha_agrupada, 'DD/MM')
      ELSE to_char(vc.fecha_agrupada, 'DD Mon')
    END as fecha_label,
    COUNT(DISTINCT vc.sku)::INT as skus_unicos_total,
    -- Contar pares cliente-sku únicos, no solo SKUs
    COUNT(DISTINCT CASE WHEN vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as skus_unicos_botiquin,
    COUNT(DISTINCT CASE WHEN NOT vc.es_de_botiquin THEN vc.id_cliente || '-' || vc.sku END)::INT as skus_unicos_directo,
    COALESCE(SUM(vc.valor_venta), 0)::NUMERIC as valor_total,
    COALESCE(SUM(CASE WHEN vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_botiquin,
    COALESCE(SUM(CASE WHEN NOT vc.es_de_botiquin THEN vc.valor_venta ELSE 0 END), 0)::NUMERIC as valor_directo,
    COUNT(*)::INT as num_transacciones,
    COUNT(DISTINCT vc.id_cliente)::INT as num_clientes
  FROM ventas_clasificadas vc
  GROUP BY vc.fecha_agrupada
  ORDER BY vc.fecha_agrupada ASC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_historico_skus_valor_por_visita(p_fecha_inicio date DEFAULT NULL::date, p_fecha_fin date DEFAULT NULL::date, p_id_cliente character varying DEFAULT NULL::character varying)
 RETURNS TABLE(id_cliente character varying, nombre_cliente character varying, fecha_visita date, skus_unicos integer, valor_venta numeric, piezas_venta integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  WITH visitas AS (
    -- Get the date of each visit (saga_transaction)
    SELECT 
      mov.id_saga_transaction,
      mov.id_cliente,
      MIN(mov.fecha_movimiento::date) as fecha_visita
    FROM movimientos_inventario mov
    WHERE mov.id_saga_transaction IS NOT NULL
      AND mov.tipo = 'VENTA'
    GROUP BY mov.id_saga_transaction, mov.id_cliente
  )
  SELECT 
    c.id_cliente,
    c.nombre_cliente,
    v.fecha_visita,
    COUNT(DISTINCT mov.sku)::int as skus_unicos,
    COALESCE(SUM(mov.cantidad * med.precio), 0) as valor_venta,
    SUM(mov.cantidad)::int as piezas_venta
  FROM visitas v
  JOIN movimientos_inventario mov ON v.id_saga_transaction = mov.id_saga_transaction
  JOIN medicamentos med ON mov.sku = med.sku
  JOIN clientes c ON v.id_cliente = c.id_cliente
  WHERE mov.tipo = 'VENTA'
    AND (p_fecha_inicio IS NULL OR v.fecha_visita >= p_fecha_inicio)
    AND (p_fecha_fin IS NULL OR v.fecha_visita <= p_fecha_fin)
    AND (p_id_cliente IS NULL OR v.id_cliente = p_id_cliente)
  GROUP BY c.id_cliente, c.nombre_cliente, v.fecha_visita
  ORDER BY v.fecha_visita ASC, c.nombre_cliente;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_recoleccion_activa()
 RETURNS json
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  result json;
BEGIN
  WITH borrador_items AS (
    SELECT
      st.id_cliente,
      (item->>'sku')::text as sku,
      (item->>'cantidad')::int as cantidad
    FROM saga_transactions st,
    jsonb_array_elements(st.items) as item
    WHERE st.tipo = 'RECOLECCION'
      AND st.estado = 'BORRADOR'
  )
  SELECT json_build_object(
    'total_piezas', COALESCE(SUM(bi.cantidad), 0)::bigint,
    'valor_total', COALESCE(SUM(bi.cantidad * med.precio), 0),
    'num_clientes', COALESCE(COUNT(DISTINCT bi.id_cliente), 0)::bigint
  ) INTO result
  FROM borrador_items bi
  JOIN medicamentos med ON bi.sku = med.sku
  JOIN clientes c ON bi.id_cliente = c.id_cliente
  WHERE c.activo = TRUE;
  
  RETURN COALESCE(result, json_build_object('total_piezas', 0, 'valor_total', 0, 'num_clientes', 0));
END;
$function$
;

CREATE OR REPLACE FUNCTION public.get_recurring_data()
 RETURNS TABLE(id_cliente character varying, sku character varying, fecha date, cantidad integer, precio numeric)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
  RETURN QUERY
  SELECT 
    v.id_cliente, 
    v.sku, 
    v.fecha, 
    v.cantidad, 
    m.precio 
  FROM ventas_odv v
  JOIN medicamentos m ON v.sku = m.sku;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.is_admin()
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select exists (
    select 1
    from public.usuarios u
    where u.auth_user_id = auth.uid()
      and u.rol IN ('ADMINISTRADOR', 'OWNER')
      and u.activo = true
  );
$function$
;

CREATE OR REPLACE FUNCTION public.is_current_user_admin()
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
    SELECT EXISTS (
        SELECT 1 FROM usuarios
        WHERE auth_user_id = auth.uid() AND rol IN ('ADMINISTRADOR', 'OWNER')
    )
$function$
;

create materialized view "public"."mv_balance_metrics" as  WITH lotes_actuales AS (
         SELECT ib.id_cliente,
            ib.sku,
            ib.cantidad_disponible,
            m_1.precio
           FROM (public.inventario_botiquin ib
             JOIN public.medicamentos m_1 ON (((ib.sku)::text = (m_1.sku)::text)))
        )
 SELECT COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'VENTA'::public.tipo_movimiento_botiquin) THEN ((mi.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_ventas,
    COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'VENTA'::public.tipo_movimiento_botiquin) THEN mi.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS cantidad_ventas,
    COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'CREACION'::public.tipo_movimiento_botiquin) THEN ((mi.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_creado,
    COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'CREACION'::public.tipo_movimiento_botiquin) THEN mi.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS cantidad_creada,
    COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'RECOLECCION'::public.tipo_movimiento_botiquin) THEN ((mi.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_recoleccion,
    COALESCE(sum(
        CASE
            WHEN (mi.tipo = 'RECOLECCION'::public.tipo_movimiento_botiquin) THEN mi.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS cantidad_recoleccion,
    COALESCE(( SELECT sum(((lotes_actuales.cantidad_disponible)::numeric * lotes_actuales.precio)) AS sum
           FROM lotes_actuales), (0)::numeric) AS valor_permanencia_virtual,
    COALESCE(( SELECT sum(lotes_actuales.cantidad_disponible) AS sum
           FROM lotes_actuales), (0)::bigint) AS cantidad_permanencia_virtual,
    now() AS ultima_actualizacion
   FROM (public.movimientos_inventario mi
     JOIN public.medicamentos m ON (((mi.sku)::text = (m.sku)::text)));


create materialized view "public"."mv_brand_performance" as  WITH movimientos_con_tipo AS (
         SELECT (item.value ->> 'sku'::text) AS sku,
            ((item.value ->> 'cantidad'::text))::integer AS cantidad,
            (item.value ->> 'tipo_movimiento'::text) AS tipo_movimiento
           FROM public.saga_transactions st,
            LATERAL jsonb_array_elements(st.items) item(value)
          WHERE (st.items IS NOT NULL)
        )
 SELECT COALESCE(m.marca, 'OTROS'::character varying) AS marca,
    COALESCE(sum(mc.cantidad), (0)::bigint) AS piezas,
    COALESCE(sum(((mc.cantidad)::numeric * m.precio)), (0)::numeric) AS valor,
    now() AS ultima_actualizacion
   FROM (movimientos_con_tipo mc
     JOIN public.medicamentos m ON ((mc.sku = (m.sku)::text)))
  WHERE (mc.tipo_movimiento = 'VENTA'::text)
  GROUP BY COALESCE(m.marca, 'OTROS'::character varying);


create materialized view "public"."mv_cumulative_daily" as  WITH movimientos_valorados AS (
         SELECT date(mi.fecha_movimiento) AS fecha,
            mi.tipo,
            ((mi.cantidad)::numeric * m.precio) AS valor,
            mi.cantidad
           FROM (public.movimientos_inventario mi
             JOIN public.medicamentos m ON (((mi.sku)::text = (m.sku)::text)))
          WHERE (mi.fecha_movimiento IS NOT NULL)
        ), fechas AS (
         SELECT DISTINCT movimientos_valorados.fecha
           FROM movimientos_valorados
          ORDER BY movimientos_valorados.fecha
        ), acumulados AS (
         SELECT f.fecha,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'CREACION'::public.tipo_movimiento_botiquin) THEN m.valor
                    ELSE (0)::numeric
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS creacion_valor,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'CREACION'::public.tipo_movimiento_botiquin) THEN m.cantidad
                    ELSE 0
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS creacion_cantidad,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'VENTA'::public.tipo_movimiento_botiquin) THEN m.valor
                    ELSE (0)::numeric
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS venta_valor,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'VENTA'::public.tipo_movimiento_botiquin) THEN m.cantidad
                    ELSE 0
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS venta_cantidad,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'RECOLECCION'::public.tipo_movimiento_botiquin) THEN m.valor
                    ELSE (0)::numeric
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS recoleccion_valor,
            sum(sum(
                CASE
                    WHEN (m.tipo = 'RECOLECCION'::public.tipo_movimiento_botiquin) THEN m.cantidad
                    ELSE 0
                END)) OVER (ORDER BY f.fecha ROWS UNBOUNDED PRECEDING) AS recoleccion_cantidad
           FROM (fechas f
             LEFT JOIN movimientos_valorados m ON ((f.fecha = m.fecha)))
          GROUP BY f.fecha
        )
 SELECT fecha,
    creacion_valor,
    creacion_cantidad,
    venta_valor,
    venta_cantidad,
    recoleccion_valor,
    recoleccion_cantidad
   FROM acumulados;


create materialized view "public"."mv_doctor_stats" as  SELECT c.id_cliente,
    c.nombre_cliente AS nombre,
    c.rango,
    c.facturacion_promedio,
    COALESCE(sum(
        CASE
            WHEN (st.tipo = 'VENTA'::public.tipo_saga_transaction) THEN mi.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS piezas_vendidas,
    COALESCE(sum(
        CASE
            WHEN (st.tipo = 'VENTA'::public.tipo_saga_transaction) THEN ((mi.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_generado,
    count(DISTINCT
        CASE
            WHEN (st.tipo = 'VENTA'::public.tipo_saga_transaction) THEN mi.sku
            ELSE NULL::character varying
        END) AS skus_unicos,
    now() AS ultima_actualizacion
   FROM (((public.clientes c
     LEFT JOIN public.saga_transactions st ON (((c.id_cliente)::text = (st.id_cliente)::text)))
     LEFT JOIN public.movimientos_inventario mi ON ((st.id = mi.id_saga_transaction)))
     LEFT JOIN public.medicamentos m ON (((mi.sku)::text = (m.sku)::text)))
  GROUP BY c.id_cliente, c.nombre_cliente, c.rango, c.facturacion_promedio;


create materialized view "public"."mv_opportunity_matrix" as  WITH movimientos_con_tipo AS (
         SELECT st.id_cliente,
            (item.value ->> 'sku'::text) AS sku,
            ((item.value ->> 'cantidad'::text))::integer AS cantidad,
            (item.value ->> 'tipo_movimiento'::text) AS tipo_movimiento
           FROM public.saga_transactions st,
            LATERAL jsonb_array_elements(st.items) item(value)
          WHERE (st.items IS NOT NULL)
        )
 SELECT COALESCE(p.nombre, 'OTROS'::character varying) AS padecimiento,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'VENTA'::text) THEN mc.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS venta,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'VENTA'::text) THEN ((mc.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_venta,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'RECOLECCION'::text) THEN mc.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS recoleccion,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'RECOLECCION'::text) THEN ((mc.cantidad)::numeric * m.precio)
            ELSE NULL::numeric
        END), (0)::numeric) AS valor_recoleccion,
    0 AS conversiones,
    now() AS ultima_actualizacion
   FROM (((movimientos_con_tipo mc
     JOIN public.medicamentos m ON ((mc.sku = (m.sku)::text)))
     LEFT JOIN public.medicamento_padecimientos mpad ON (((m.sku)::text = (mpad.sku)::text)))
     LEFT JOIN public.padecimientos p ON ((mpad.id_padecimiento = p.id_padecimiento)))
  WHERE (mc.tipo_movimiento = ANY (ARRAY['VENTA'::text, 'RECOLECCION'::text]))
  GROUP BY COALESCE(p.nombre, 'OTROS'::character varying);


create materialized view "public"."mv_padecimiento_performance" as  WITH movimientos_con_tipo AS (
         SELECT (item.value ->> 'sku'::text) AS sku,
            ((item.value ->> 'cantidad'::text))::integer AS cantidad,
            (item.value ->> 'tipo_movimiento'::text) AS tipo_movimiento
           FROM public.saga_transactions st,
            LATERAL jsonb_array_elements(st.items) item(value)
          WHERE (st.items IS NOT NULL)
        )
 SELECT COALESCE(p.nombre, 'OTROS'::character varying) AS padecimiento,
    COALESCE(sum(mc.cantidad), (0)::bigint) AS piezas,
    COALESCE(sum(((mc.cantidad)::numeric * m.precio)), (0)::numeric) AS valor,
    now() AS ultima_actualizacion
   FROM (((movimientos_con_tipo mc
     JOIN public.medicamentos m ON ((mc.sku = (m.sku)::text)))
     LEFT JOIN public.medicamento_padecimientos mp ON (((m.sku)::text = (mp.sku)::text)))
     LEFT JOIN public.padecimientos p ON ((mp.id_padecimiento = p.id_padecimiento)))
  WHERE (mc.tipo_movimiento = 'VENTA'::text)
  GROUP BY COALESCE(p.nombre, 'OTROS'::character varying);


create materialized view "public"."mv_product_interest" as  WITH movimientos_con_tipo AS (
         SELECT (item.value ->> 'sku'::text) AS sku,
            ((item.value ->> 'cantidad'::text))::integer AS cantidad,
            (item.value ->> 'tipo_movimiento'::text) AS tipo_movimiento
           FROM public.saga_transactions st,
            LATERAL jsonb_array_elements(st.items) item(value)
          WHERE (st.items IS NOT NULL)
        )
 SELECT m.sku,
    "substring"((m.producto)::text, 1, 20) AS producto_short,
    m.producto AS producto_full,
    m.marca,
    m.top,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'VENTA'::text) THEN mc.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS venta,
    COALESCE(sum(
        CASE
            WHEN (mc.tipo_movimiento = 'RECOLECCION'::text) THEN mc.cantidad
            ELSE NULL::integer
        END), (0)::bigint) AS recoleccion,
    COALESCE(( SELECT sum(inventario_botiquin.cantidad_disponible) AS sum
           FROM public.inventario_botiquin
          WHERE ((inventario_botiquin.sku)::text = (m.sku)::text)), (0)::bigint) AS permanencia,
    now() AS ultima_actualizacion
   FROM (public.medicamentos m
     LEFT JOIN movimientos_con_tipo mc ON (((m.sku)::text = mc.sku)))
  WHERE ((mc.tipo_movimiento IS NULL) OR (mc.tipo_movimiento = ANY (ARRAY['VENTA'::text, 'RECOLECCION'::text])))
  GROUP BY m.sku, m.producto, m.marca, m.top;


CREATE OR REPLACE FUNCTION public.notify_admins(p_type text, p_title text, p_body text, p_data jsonb DEFAULT '{}'::jsonb)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_admin RECORD;
    v_count INTEGER := 0;
BEGIN
    FOR v_admin IN
        SELECT id_usuario FROM usuarios WHERE rol IN ('ADMINISTRADOR', 'OWNER') AND activo = true
    LOOP
        PERFORM create_notification(
            v_admin.id_usuario, p_type, p_title, p_body, p_data
        );
        v_count := v_count + 1;
    END LOOP;

    RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.notify_visit_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_admin RECORD;
  v_user_nombre TEXT;
  v_cliente_nombre TEXT;
  v_dedup_key TEXT;
BEGIN
  -- Solo actuar cuando estado cambia a COMPLETADO
  IF NEW.estado = 'COMPLETADO' AND (OLD.estado IS NULL OR OLD.estado != 'COMPLETADO') THEN

    -- Obtener nombre del asesor que completó la visita
    SELECT u.nombre INTO v_user_nombre
    FROM usuarios u
    WHERE u.id_usuario = NEW.id_usuario;

    -- Obtener nombre del cliente/médico
    SELECT c.nombre_cliente INTO v_cliente_nombre
    FROM clientes c
    WHERE c.id_cliente = NEW.id_cliente;

    -- Clave de deduplicación para evitar notificaciones duplicadas
    v_dedup_key := 'visit_completed_' || NEW.visit_id::text;

    -- Insertar notificación para cada ADMIN y OWNER activo
    FOR v_admin IN
      SELECT id_usuario FROM usuarios
      WHERE rol IN ('ADMINISTRADOR', 'OWNER') AND activo = true
    LOOP
      -- Solo insertar si no existe ya una notificación con esta dedup_key para este usuario
      INSERT INTO notifications (
        user_id, 
        type, 
        title, 
        body, 
        data, 
        dedup_key,
        created_at
      )
      SELECT
        v_admin.id_usuario,
        'TASK_COMPLETED',
        'Visita Completada',
        COALESCE(v_user_nombre, 'Un asesor') || ' terminó su visita con ' || COALESCE(v_cliente_nombre, 'un cliente'),
        jsonb_build_object(
          'visit_id', NEW.visit_id,
          'user_id', NEW.id_usuario,
          'user_name', v_user_nombre,
          'cliente_id', NEW.id_cliente,
          'cliente_name', v_cliente_nombre,
          'screen', 'visits',
          'visit_tipo', NEW.tipo
        ),
        v_dedup_key || '_' || v_admin.id_usuario,
        NOW()
      WHERE NOT EXISTS (
        SELECT 1 FROM notifications n 
        WHERE n.dedup_key = v_dedup_key || '_' || v_admin.id_usuario
      );
    END LOOP;
  END IF;

  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.process_zoho_retry_queue()
 RETURNS integer
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    v_message RECORD;
    v_processed INTEGER := 0;
    v_max_messages INTEGER := 100;
BEGIN
    -- Process up to 100 messages
    FOR v_message IN
        SELECT * FROM pgmq.read('zoho_retry_queue', 30, v_max_messages)
    LOOP
        BEGIN
            -- For now, just mark as processed
            -- TODO: Call Edge Function for Zoho sync
            
            -- Update compensation status
            UPDATE saga_compensations
            SET zoho_sync_status = 'SYNCED'
            WHERE id = (v_message.message->>'compensation_id')::UUID;

            -- Delete from queue
            PERFORM pgmq.delete('zoho_retry_queue', v_message.msg_id);
            v_processed := v_processed + 1;

        EXCEPTION WHEN OTHERS THEN
            -- Log error but continue
            RAISE WARNING 'Error procesando mensaje %: %', v_message.msg_id, SQLERRM;

            -- Mark as failed after 3 attempts
            IF v_message.read_ct >= 3 THEN
                UPDATE saga_compensations
                SET zoho_sync_status = 'FAILED'
                WHERE id = (v_message.message->>'compensation_id')::UUID;

                PERFORM pgmq.archive('zoho_retry_queue', v_message.msg_id);
            END IF;
        END;
    END LOOP;

    RETURN v_processed;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.publish_saga_event()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  IF NEW.estado IN ('PENDIENTE_CONFIRMACION', 'PROCESANDO_ZOHO') THEN
    INSERT INTO event_outbox (
      evento_tipo, saga_transaction_id, payload,
      procesado, intentos, proximo_intento
    ) VALUES (
      'SAGA_' || NEW.tipo::text,
      NEW.id,
      jsonb_build_object(
        'id', NEW.id, 'tipo', NEW.tipo, 'estado', NEW.estado,
        'id_cliente', NEW.id_cliente, 'items', NEW.items
      ),
      false, 0, NOW()
    );
  END IF;
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rebuild_movimientos_inventario()
 RETURNS TABLE(movimientos_creados bigint, inventario_final bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  saga_rec RECORD;
  item_rec RECORD;
  current_stock INTEGER;
  new_stock INTEGER;
  tipo_mov tipo_movimiento_botiquin;
  mov_count BIGINT := 0;
  inv_count BIGINT := 0;
BEGIN
  TRUNCATE TABLE movimientos_inventario RESTART IDENTITY CASCADE;
  TRUNCATE TABLE inventario_botiquin RESTART IDENTITY CASCADE;
  
  FOR saga_rec IN 
    SELECT id, id_cliente, created_at, items
    FROM saga_transactions
    WHERE items IS NOT NULL
    ORDER BY created_at, id
  LOOP
    FOR item_rec IN
      SELECT 
        item->>'sku' as sku,
        (item->>'cantidad')::int as cantidad,
        item->>'tipo_movimiento' as tipo_movimiento
      FROM jsonb_array_elements(saga_rec.items) as item
      WHERE item->>'tipo_movimiento' != 'PERMANENCIA'
    LOOP
      IF item_rec.tipo_movimiento = 'CREACION' THEN
        tipo_mov := 'CREACION';
      ELSIF item_rec.tipo_movimiento = 'VENTA' THEN
        tipo_mov := 'VENTA';
      ELSIF item_rec.tipo_movimiento = 'RECOLECCION' THEN
        tipo_mov := 'RECOLECCION';
      ELSE
        CONTINUE;
      END IF;
      
      SELECT COALESCE(cantidad_disponible, 0)
      INTO current_stock
      FROM inventario_botiquin
      WHERE id_cliente = saga_rec.id_cliente AND sku = item_rec.sku;
      
      IF current_stock IS NULL THEN
        current_stock := 0;
      END IF;
      
      IF tipo_mov = 'CREACION' THEN
        new_stock := current_stock + item_rec.cantidad;
      ELSE
        new_stock := current_stock - item_rec.cantidad;
      END IF;
      
      INSERT INTO movimientos_inventario (
        id_saga_transaction,
        id_cliente,
        sku,
        tipo,
        cantidad,
        cantidad_antes,
        cantidad_despues,
        fecha_movimiento
      ) VALUES (
        saga_rec.id,
        saga_rec.id_cliente,
        item_rec.sku,
        tipo_mov,
        item_rec.cantidad,
        current_stock,
        new_stock,
        saga_rec.created_at
      );
      
      mov_count := mov_count + 1;
      
      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
      VALUES (saga_rec.id_cliente, item_rec.sku, new_stock)
      ON CONFLICT (id_cliente, sku) 
      DO UPDATE SET cantidad_disponible = new_stock;
      
    END LOOP;
  END LOOP;
  
  SELECT COUNT(*) INTO inv_count FROM inventario_botiquin;
  
  RETURN QUERY SELECT mov_count, inv_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.refresh_all_materialized_views()
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_balance_metrics;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cumulative_daily;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_opportunity_matrix;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_doctor_stats;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_interest;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_brand_performance;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_padecimiento_performance;
  
  RAISE NOTICE 'Vistas materializadas actualizadas: %', NOW();
END;
$function$
;

CREATE OR REPLACE FUNCTION public.regenerar_movimientos_desde_saga(p_saga_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_tipo_movimiento tipo_movimiento_botiquin;
BEGIN
  SELECT * INTO v_saga FROM saga_transactions WHERE id = p_saga_id;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'SAGA % no encontrada', p_saga_id;
  END IF;
  
  FOR v_item IN
    SELECT 
      item->>'sku' as sku,
      (item->>'cantidad')::int as cantidad,
      item->>'tipo_movimiento' as tipo_movimiento
    FROM jsonb_array_elements(v_saga.items) as item
  LOOP
    SELECT COALESCE(cantidad_disponible, 0)
    INTO v_cantidad_antes
    FROM inventario_botiquin
    WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    
    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;
    
    CASE v_item.tipo_movimiento
      WHEN 'CREACION' THEN
        v_tipo_movimiento := 'CREACION';
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      WHEN 'VENTA' THEN
        v_tipo_movimiento := 'VENTA';
        v_cantidad_despues := v_cantidad_antes - v_item.cantidad;
      WHEN 'RECOLECCION' THEN
        v_tipo_movimiento := 'RECOLECCION';
        v_cantidad_despues := v_cantidad_antes - v_item.cantidad;
      WHEN 'PERMANENCIA' THEN
        CONTINUE;
      ELSE
        RAISE EXCEPTION 'Tipo de movimiento desconocido: %', v_item.tipo_movimiento;
    END CASE;
    
    INSERT INTO movimientos_inventario (
      id_saga_transaction,
      id_cliente,
      sku,
      tipo,
      cantidad,
      cantidad_antes,
      cantidad_despues,
      fecha_movimiento
    ) VALUES (
      p_saga_id,
      v_saga.id_cliente,
      v_item.sku,
      v_tipo_movimiento,
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      v_saga.created_at
    );
    
    INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible)
    VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues)
    ON CONFLICT (id_cliente, sku)
    DO UPDATE SET cantidad_disponible = v_cantidad_despues;
  END LOOP;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_compensate_task(p_saga_transaction_id uuid, p_admin_id character varying, p_reason text, p_new_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_compensation_id UUID;
    v_old_items JSONB;
    v_result JSONB;
BEGIN
    -- Verify admin permissions
    IF NOT EXISTS (
        SELECT 1 FROM usuarios
        WHERE id_usuario = p_admin_id AND rol IN ('ADMINISTRADOR', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden compensar tareas';
    END IF;

    -- Get saga transaction
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    v_old_items := v_saga.items;

    -- Create compensation record
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        p_saga_transaction_id,
        p_admin_id,
        p_reason,
        'ADJUSTMENT',
        jsonb_build_object('items', v_old_items, 'estado', v_saga.estado),
        jsonb_build_object('items', p_new_items),
        'PENDING'
    ) RETURNING id INTO v_compensation_id;

    -- Register individual adjustments
    INSERT INTO saga_adjustments (
        compensation_id,
        saga_transaction_id,
        item_sku,
        old_quantity,
        new_quantity,
        adjustment_reason
    )
    SELECT
        v_compensation_id,
        p_saga_transaction_id,
        COALESCE(old_item->>'sku', new_item->>'sku'),
        COALESCE((old_item->>'cantidad')::INTEGER, 0),
        COALESCE((new_item->>'cantidad')::INTEGER, 0),
        p_reason
    FROM
        jsonb_array_elements(v_old_items) WITH ORDINALITY AS old_items(old_item, ord)
    FULL OUTER JOIN
        jsonb_array_elements(p_new_items) WITH ORDINALITY AS new_items(new_item, ord2)
    ON old_item->>'sku' = new_item->>'sku'
    WHERE COALESCE((old_item->>'cantidad')::INTEGER, 0) != COALESCE((new_item->>'cantidad')::INTEGER, 0);

    -- Update saga_transactions with new items
    UPDATE saga_transactions
    SET
        items = p_new_items,
        updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    -- Queue for Zoho sync
    PERFORM pgmq.send(
        'zoho_retry_queue',
        jsonb_build_object(
            'compensation_id', v_compensation_id,
            'saga_transaction_id', p_saga_transaction_id,
            'action', 'SYNC_ADJUSTMENT',
            'items', p_new_items
        )
    );

    v_result := jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', 'Compensación registrada, sincronización en cola'
    );

    RETURN v_result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_force_task_status(p_visit_task_id uuid, p_admin_id character varying, p_new_status text, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_task RECORD;
    v_saga RECORD;
    v_compensation_id UUID;
BEGIN
    -- Only OWNER can force states
    IF NOT EXISTS (
        SELECT 1 FROM usuarios
        WHERE id_usuario = p_admin_id AND rol = 'OWNER'
    ) THEN
        RAISE EXCEPTION 'Solo OWNER puede forzar estados de tareas';
    END IF;

    -- Validate new status
    IF p_new_status NOT IN ('PENDIENTE', 'COMPLETADO', 'ERROR', 'OMITIDO', 'OMITIDA') THEN
        RAISE EXCEPTION 'Estado inválido: %', p_new_status;
    END IF;

    -- Get task by task_id
    SELECT * INTO v_task FROM visit_tasks WHERE task_id = p_visit_task_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tarea no encontrada';
    END IF;

    -- Try to find related saga_transaction
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE visit_id = v_task.visit_id
      AND tipo::text ILIKE '%' || v_task.task_tipo::text || '%'
    LIMIT 1;

    -- Record in compensations (for audit)
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        v_saga.id,
        p_admin_id,
        p_reason,
        'FORCE_COMPLETE',
        jsonb_build_object('task_status', v_task.estado::text),
        jsonb_build_object('task_status', p_new_status),
        'MANUAL'
    ) RETURNING id INTO v_compensation_id;

    -- Force status change
    UPDATE visit_tasks
    SET
        estado = p_new_status::visit_task_estado,
        completed_at = CASE WHEN p_new_status = 'COMPLETADO' THEN NOW() ELSE completed_at END,
        metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
            'forced_at', NOW(),
            'forced_by', p_admin_id,
            'forced_reason', p_reason
        )
    WHERE task_id = p_visit_task_id;

    RETURN jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', format('Estado forzado a %s', p_new_status)
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_get_all_visits(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_estado text DEFAULT NULL::text, p_search text DEFAULT NULL::text, p_fecha_desde date DEFAULT NULL::date, p_fecha_hasta date DEFAULT NULL::date)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_visits jsonb;
  v_total int;
BEGIN
  -- Verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  -- Get total count
  SELECT COUNT(*)
  INTO v_total
  FROM public.visitas v
  JOIN public.clientes c ON c.id_cliente = v.id_cliente
  WHERE (p_estado IS NULL OR v.estado::text = p_estado)
    AND (p_search IS NULL OR c.nombre_cliente ILIKE '%' || p_search || '%')
    AND (p_fecha_desde IS NULL OR v.created_at::date >= p_fecha_desde)
    AND (p_fecha_hasta IS NULL OR v.created_at::date <= p_fecha_hasta);

  -- Get visits with client info and resource counts
  SELECT jsonb_agg(row_data)
  INTO v_visits
  FROM (
    SELECT jsonb_build_object(
      'visit_id', v.visit_id,
      'id_cliente', v.id_cliente,
      'nombre_cliente', c.nombre_cliente,
      'id_usuario', v.id_usuario,
      'nombre_usuario', u.nombre,
      'tipo', v.tipo::text,
      'estado', v.estado::text,
      'saga_status', COALESCE(
        CASE WHEN v.estado = 'COMPLETADO' THEN 'COMPLETED'
             WHEN v.estado = 'CANCELADO' THEN 'COMPENSATED'
             ELSE 'RUNNING' END,
        'RUNNING'
      ),
      'etiqueta', v.etiqueta,
      'created_at', v.created_at,
      'started_at', v.started_at,
      'completed_at', v.completed_at,
      'metadata', v.metadata,
      'tasks_count', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id),
      'tasks_completed', (SELECT COUNT(*) FROM visit_tasks vt WHERE vt.visit_id = v.visit_id AND vt.estado = 'COMPLETADO'),
      'sagas_count', (SELECT COUNT(*) FROM saga_transactions st WHERE st.visit_id = v.visit_id)
    ) as row_data
    FROM public.visitas v
    JOIN public.clientes c ON c.id_cliente = v.id_cliente
    LEFT JOIN public.usuarios u ON u.id_usuario = v.id_usuario
    WHERE (p_estado IS NULL OR v.estado::text = p_estado)
      AND (p_search IS NULL OR c.nombre_cliente ILIKE '%' || p_search || '%')
      AND (p_fecha_desde IS NULL OR v.created_at::date >= p_fecha_desde)
      AND (p_fecha_hasta IS NULL OR v.created_at::date <= p_fecha_hasta)
    ORDER BY v.created_at DESC
    LIMIT p_limit
    OFFSET p_offset
  ) sub;

  RETURN jsonb_build_object(
    'visits', COALESCE(v_visits, '[]'::jsonb),
    'total', v_total,
    'limit', p_limit,
    'offset', p_offset
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_get_visit_detail(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_visit jsonb;
  v_tasks jsonb;
  v_odvs jsonb;
  v_movimientos jsonb;
  v_informe jsonb;
  v_recolecciones jsonb;
BEGIN
  -- Verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo administradores pueden acceder a esta función';
  END IF;

  -- Get visit with client info
  SELECT jsonb_build_object(
    'visit_id', v.visit_id,
    'id_cliente', v.id_cliente,
    'nombre_cliente', c.nombre_cliente,
    'id_usuario', v.id_usuario,
    'nombre_usuario', u.nombre,
    'tipo', v.tipo::text,
    'estado', v.estado::text,
    'saga_status', COALESCE(
      CASE WHEN v.estado = 'COMPLETADO' THEN 'COMPLETED'
           WHEN v.estado = 'CANCELADO' THEN 'COMPENSATED'
           ELSE 'RUNNING' END,
      'RUNNING'
    ),
    'etiqueta', v.etiqueta,
    'created_at', v.created_at,
    'started_at', v.started_at,
    'completed_at', v.completed_at,
    'metadata', v.metadata
  ), v.id_cliente
  INTO v_visit, v_id_cliente
  FROM public.visitas v
  JOIN public.clientes c ON c.id_cliente = v.id_cliente
  LEFT JOIN public.usuarios u ON u.id_usuario = v.id_usuario
  WHERE v.visit_id = p_visit_id;

  IF v_visit IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Get visit_tasks
  SELECT jsonb_agg(row_data)
  INTO v_tasks
  FROM (
    SELECT jsonb_build_object(
      'task_id', COALESCE(vt.task_id::text, vt.task_tipo::text || '-' || p_visit_id::text),
      'task_tipo', vt.task_tipo::text,
      'estado', vt.estado::text,
      'required', vt.required,
      'created_at', vt.created_at,
      'started_at', vt.started_at,
      'completed_at', vt.completed_at,
      'due_at', vt.due_at,
      'metadata', vt.metadata,
      'transaction_type', CASE vt.task_tipo::text
        WHEN 'LEVANTAMIENTO_INICIAL' THEN 'COMPENSABLE'
        WHEN 'CORTE' THEN 'COMPENSABLE'
        WHEN 'LEV_POST_CORTE' THEN 'COMPENSABLE'
        WHEN 'ODV_BOTIQUIN' THEN 'PIVOT'
        WHEN 'VENTA_ODV' THEN 'PIVOT'
        ELSE 'RETRYABLE'
      END,
      'step_order', CASE vt.task_tipo::text
        WHEN 'LEVANTAMIENTO_INICIAL' THEN 1
        WHEN 'CORTE' THEN 1
        WHEN 'VENTA_ODV' THEN 2
        WHEN 'RECOLECCION' THEN 3
        WHEN 'LEV_POST_CORTE' THEN 4
        WHEN 'ODV_BOTIQUIN' THEN 5
        WHEN 'INFORME_VISITA' THEN 6
        ELSE 99
      END,
      'compensation_status', 'NOT_NEEDED'
    ) as row_data
    FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
    ORDER BY CASE vt.task_tipo::text
      WHEN 'LEVANTAMIENTO_INICIAL' THEN 1
      WHEN 'CORTE' THEN 1
      WHEN 'VENTA_ODV' THEN 2
      WHEN 'RECOLECCION' THEN 3
      WHEN 'LEV_POST_CORTE' THEN 4
      WHEN 'ODV_BOTIQUIN' THEN 5
      WHEN 'INFORME_VISITA' THEN 6
      ELSE 99
    END
  ) sub;

  -- Get ODVs from saga_zoho_links
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'tipo', szl.tipo::text,
      'fecha_odv', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'saga_tipo', st.tipo::text,
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE((item->>'cantidad')::int, (item->>'cantidad_entrada')::int, 0)
          )
          FROM jsonb_array_elements(st.items) AS item
        ),
        0
      )::int,
      'items', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'sku', item->>'sku',
              'producto', COALESCE(m.producto, item->>'sku'),
              'cantidad', COALESCE((item->>'cantidad')::int, (item->>'cantidad_entrada')::int, 0)
            )
          )
          FROM jsonb_array_elements(st.items) AS item
          LEFT JOIN medicamentos m ON m.sku = item->>'sku'
          WHERE item->>'sku' IS NOT NULL
        ),
        '[]'::jsonb
      )
    ) as odv_data
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    ORDER BY szl.created_at
  ) sub;

  -- Get movimientos with detailed items
  -- by_tipo now shows SUM of cantidad per tipo (not count of movements)
  SELECT jsonb_build_object(
    'total', COALESCE(mov_stats.cnt, 0),
    'total_cantidad', COALESCE(mov_stats.suma_cantidad, 0),
    'unique_skus', COALESCE(mov_stats.skus_unicos, 0),
    'by_tipo', COALESCE(mov_tipos.tipos, '{}'::jsonb),
    'items', COALESCE(mov_items.items, '[]'::jsonb)
  )
  INTO v_movimientos
  FROM (
    SELECT 
      COUNT(*)::int as cnt,
      COALESCE(SUM(mi.cantidad), 0)::int as suma_cantidad,
      COUNT(DISTINCT mi.sku)::int as skus_unicos
    FROM public.movimientos_inventario mi
    WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
  ) mov_stats,
  (
    -- SUM cantidad by tipo (not COUNT)
    SELECT jsonb_object_agg(tipo::text, suma_cantidad) as tipos
    FROM (
      SELECT mi.tipo, COALESCE(SUM(mi.cantidad), 0)::int as suma_cantidad
      FROM public.movimientos_inventario mi
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
      GROUP BY mi.tipo
    ) sub
  ) mov_tipos,
  (
    SELECT jsonb_agg(row_data) as items
    FROM (
      SELECT jsonb_build_object(
        'sku', mi.sku,
        'tipo', mi.tipo::text,
        'cantidad', mi.cantidad,
        'cantidad_antes', mi.cantidad_antes,
        'cantidad_despues', mi.cantidad_despues,
        'created_at', mi.fecha_movimiento
      ) as row_data
      FROM public.movimientos_inventario mi
      WHERE mi.id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id)
      ORDER BY mi.fecha_movimiento
      LIMIT 100
    ) sub
  ) mov_items;

  -- Get informe de visita
  SELECT jsonb_build_object(
    'informe_id', vi.informe_id,
    'completada', vi.completada,
    'cumplimiento_score', vi.cumplimiento_score,
    'etiqueta', vi.etiqueta,
    'respuestas', vi.respuestas,
    'fecha_completada', vi.fecha_completada,
    'created_at', vi.created_at
  )
  INTO v_informe
  FROM public.visita_informes vi
  WHERE vi.visit_id = p_visit_id;

  -- Get recolecciones
  SELECT jsonb_agg(row_data)
  INTO v_recolecciones
  FROM (
    SELECT jsonb_build_object(
      'recoleccion_id', r.recoleccion_id,
      'estado', r.estado,
      'latitud', r.latitud,
      'longitud', r.longitud,
      'cedis_observaciones', r.cedis_observaciones,
      'cedis_responsable_nombre', r.cedis_responsable_nombre,
      'entregada_at', r.entregada_at,
      'created_at', r.created_at,
      'metadata', r.metadata
    ) as row_data
    FROM public.recolecciones r
    WHERE r.visit_id = p_visit_id
    ORDER BY r.created_at
  ) sub;

  RETURN jsonb_build_object(
    'visit', v_visit,
    'tasks', COALESCE(v_tasks, '[]'::jsonb),
    'odvs', COALESCE(v_odvs, '[]'::jsonb),
    'movimientos', COALESCE(v_movimientos, '{"total": 0, "total_cantidad": 0, "unique_skus": 0, "by_tipo": {}, "items": []}'::jsonb),
    'informe', v_informe,
    'recolecciones', COALESCE(v_recolecciones, '[]'::jsonb)
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_retry_pivot(p_saga_transaction_id uuid, p_admin_id character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_saga RECORD;
    v_health RECORD;
    v_compensation_id UUID;
BEGIN
    -- Verify permissions
    IF NOT EXISTS (
        SELECT 1 FROM usuarios
        WHERE id_usuario = p_admin_id AND rol IN ('ADMINISTRADOR', 'OWNER')
    ) THEN
        RAISE EXCEPTION 'Solo ADMIN o OWNER pueden reintentar PIVOT';
    END IF;

    -- Get saga
    SELECT * INTO v_saga
    FROM saga_transactions
    WHERE id = p_saga_transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Saga transaction no encontrada';
    END IF;

    IF v_saga.estado NOT IN ('FALLIDA', 'ERROR') THEN
        RAISE EXCEPTION 'Solo se pueden reintentar transacciones en estado FALLIDA o ERROR';
    END IF;

    -- Check Zoho health
    SELECT * INTO v_health FROM zoho_health_status WHERE id = 1;

    IF v_health IS NOT NULL AND NOT v_health.is_healthy THEN
        RAISE EXCEPTION 'Zoho no está disponible. Último error: %', v_health.last_error;
    END IF;

    -- Create compensation record for RETRY
    INSERT INTO saga_compensations (
        saga_transaction_id,
        compensated_by,
        reason,
        compensation_type,
        old_state,
        new_state,
        zoho_sync_status
    ) VALUES (
        p_saga_transaction_id,
        p_admin_id,
        'Reintento manual de PIVOT',
        'RETRY',
        jsonb_build_object('estado', v_saga.estado::text),
        jsonb_build_object('estado', 'PENDIENTE_SYNC'),
        'PENDING'
    ) RETURNING id INTO v_compensation_id;

    -- Update state to PENDIENTE_SYNC for retry
    UPDATE saga_transactions
    SET
        estado = 'PENDIENTE_SYNC',
        updated_at = NOW()
    WHERE id = p_saga_transaction_id;

    -- Queue for immediate processing
    PERFORM pgmq.send(
        'zoho_retry_queue',
        jsonb_build_object(
            'compensation_id', v_compensation_id,
            'saga_transaction_id', p_saga_transaction_id,
            'action', 'RETRY_PIVOT',
            'items', v_saga.items
        )
    );

    RETURN jsonb_build_object(
        'success', true,
        'compensation_id', v_compensation_id,
        'message', 'Reintento de PIVOT encolado'
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_admin_rollback_visit(p_visit_id uuid, p_razon text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_user_id text;
  v_user_rol text;
  v_id_cliente text;
  v_id_ciclo integer;
  v_saga_ids uuid[];
  v_recoleccion_ids uuid[];
  v_deleted_event_outbox int := 0;
  v_deleted_movimientos int := 0;
  v_deleted_saga int := 0;
  v_deleted_tasks int := 0;
  v_deleted_odvs int := 0;
  v_deleted_recolecciones int := 0;
  v_deleted_rec_items int := 0;
  v_deleted_rec_firmas int := 0;
  v_deleted_rec_evidencias int := 0;
  v_deleted_informes int := 0;
  v_visit_data jsonb;
  -- Variables para restauración de inventario
  v_current_visit_had_lev_post_corte boolean := false;
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_restore_source text := NULL;
  v_count_inventario_restored int := 0;
  v_inventory_reverted boolean := false;
BEGIN
  -- Get current user and verify admin role
  SELECT u.id_usuario, u.rol::text
  INTO v_user_id, v_user_rol
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid();

  IF v_user_id IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado';
  END IF;

  IF v_user_rol != 'ADMINISTRADOR' THEN
    RAISE EXCEPTION 'Solo administradores pueden ejecutar rollback de visitas';
  END IF;

  -- Get visit info and snapshot before deletion
  SELECT
    v.id_cliente,
    v.id_ciclo,
    jsonb_build_object(
      'visit_id', v.visit_id,
      'id_cliente', v.id_cliente,
      'id_usuario', v.id_usuario,
      'id_ciclo', v.id_ciclo,
      'tipo', v.tipo::text,
      'estado', v.estado::text,
      'created_at', v.created_at,
      'etiqueta', v.etiqueta,
      'metadata', v.metadata
    )
  INTO v_id_cliente, v_id_ciclo, v_visit_data
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- ============ RESTAURAR INVENTARIO ANTES DE BORRAR ============
  -- Verificar si esta visita tiene un LEV_POST_CORTE confirmado
  SELECT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.tipo = 'LEV_POST_CORTE'
      AND st.estado = 'CONFIRMADO'
  ) INTO v_current_visit_had_lev_post_corte;

  IF v_current_visit_had_lev_post_corte THEN
    -- Buscar la última visita COMPLETADA del mismo cliente (excluyendo la actual)
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.estado = 'COMPLETADO'
      AND v.tipo IN ('VISITA_CORTE', 'VISITA_LEVANTAMIENTO_INICIAL')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      -- Intentar LEV_POST_CORTE primero
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.tipo = 'LEV_POST_CORTE'
        AND st.estado = 'CONFIRMADO'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        -- Fallback a LEVANTAMIENTO_INICIAL
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.tipo = 'LEVANTAMIENTO_INICIAL'
          AND st.estado = 'CONFIRMADO'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- Restaurar inventario si encontramos items
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      -- Limpiar inventario actual del cliente
      DELETE FROM public.inventario_botiquin WHERE id_cliente = v_id_cliente;

      -- Restaurar desde la última visita COMPLETADA
      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'cantidad')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      -- También restaurar botiquin_clientes_sku_disponibles
      INSERT INTO public.botiquin_clientes_sku_disponibles (id_cliente, sku, fecha_ingreso)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0
      ON CONFLICT (id_cliente, sku) DO NOTHING;

      v_inventory_reverted := true;
    ELSE
      v_restore_source := 'Sin visita completada anterior - inventario no modificado';
    END IF;
  END IF;
  -- ============ FIN RESTAURACIÓN DE INVENTARIO ============

  -- Get saga transaction IDs for this visit
  SELECT ARRAY_AGG(st.id) INTO v_saga_ids
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id;

  -- Get recoleccion IDs for this visit
  SELECT ARRAY_AGG(r.recoleccion_id) INTO v_recoleccion_ids
  FROM public.recolecciones r
  WHERE r.visit_id = p_visit_id;

  -- DELETE IN ORDER (child tables first)

  -- 1. Delete event_outbox (references saga_transactions)
  IF v_saga_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.event_outbox
      WHERE saga_transaction_id = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_event_outbox FROM deleted;

    -- 2. Delete movimientos_inventario (references saga_transactions)
    WITH deleted AS (
      DELETE FROM public.movimientos_inventario
      WHERE id_saga_transaction = ANY(v_saga_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_movimientos FROM deleted;

    -- 3. Delete saga_transactions
    WITH deleted AS (
      DELETE FROM public.saga_transactions
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_saga FROM deleted;
  END IF;

  -- 4. Delete visit_tasks
  WITH deleted AS (
    DELETE FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_tasks FROM deleted;

  -- 5. Delete visita_odvs
  WITH deleted AS (
    DELETE FROM public.visita_odvs
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_odvs FROM deleted;

  -- 6. Delete recolecciones and related tables
  IF v_recoleccion_ids IS NOT NULL THEN
    WITH deleted AS (
      DELETE FROM public.recolecciones_evidencias
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_evidencias FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_firmas
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_firmas FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones_items
      WHERE recoleccion_id = ANY(v_recoleccion_ids)
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_rec_items FROM deleted;

    WITH deleted AS (
      DELETE FROM public.recolecciones
      WHERE visit_id = p_visit_id
      RETURNING 1
    )
    SELECT COUNT(*) INTO v_deleted_recolecciones FROM deleted;
  END IF;

  -- 7. Delete visita_informes
  WITH deleted AS (
    DELETE FROM public.visita_informes
    WHERE visit_id = p_visit_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_deleted_informes FROM deleted;

  -- 8. Update visit status to CANCELADO
  UPDATE public.visitas
  SET
    estado = 'CANCELADO',
    updated_at = NOW(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'rollback_at', NOW(),
      'rollback_by', v_user_id,
      'rollback_razon', p_razon,
      'rollback_deleted', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'visita_odvs', v_deleted_odvs,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored
    )
  WHERE visit_id = p_visit_id;

  -- LOG TO AUDIT_LOG
  INSERT INTO public.audit_log (
    tabla,
    registro_id,
    accion,
    usuario_id,
    valores_antes,
    valores_despues
  )
  VALUES (
    'visitas',
    p_visit_id::text,
    'DELETE',
    v_user_id,
    v_visit_data || jsonb_build_object(
      'accion_tipo', 'ADMIN_ROLLBACK',
      'razon', p_razon
    ),
    jsonb_build_object(
      'deleted_counts', jsonb_build_object(
        'event_outbox', v_deleted_event_outbox,
        'movimientos_inventario', v_deleted_movimientos,
        'saga_transactions', v_deleted_saga,
        'visit_tasks', v_deleted_tasks,
        'visita_odvs', v_deleted_odvs,
        'recolecciones', v_deleted_recolecciones,
        'recolecciones_items', v_deleted_rec_items,
        'recolecciones_firmas', v_deleted_rec_firmas,
        'recolecciones_evidencias', v_deleted_rec_evidencias,
        'visita_informes', v_deleted_informes
      ),
      'inventory_reverted', v_inventory_reverted,
      'inventory_restore_source', v_restore_source,
      'inventory_items_restored', v_count_inventario_restored,
      'executed_at', NOW(),
      'executed_by', v_user_id
    )
  );

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'id_cliente', v_id_cliente,
    'id_ciclo', v_id_ciclo,
    'executed_by', v_user_id,
    'razon', p_razon,
    'deleted', jsonb_build_object(
      'event_outbox', v_deleted_event_outbox,
      'movimientos_inventario', v_deleted_movimientos,
      'saga_transactions', v_deleted_saga,
      'visit_tasks', v_deleted_tasks,
      'visita_odvs', v_deleted_odvs,
      'recolecciones', v_deleted_recolecciones,
      'recolecciones_items', v_deleted_rec_items,
      'recolecciones_firmas', v_deleted_rec_firmas,
      'recolecciones_evidencias', v_deleted_rec_evidencias,
      'visita_informes', v_deleted_informes
    ),
    'inventory_reverted', v_inventory_reverted,
    'inventory_restore_source', v_restore_source,
    'inventory_items_restored', v_count_inventario_restored,
    'last_completed_visit_id', v_last_completed_visit_id,
    'message', 'Rollback completado exitosamente'
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cambiar_estado_cliente(p_id_cliente character varying, p_nuevo_estado public.estado_cliente, p_user_id character varying, p_razon text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_estado_actual public.estado_cliente;
  v_user_rol VARCHAR;
  v_tiene_visita_activa BOOLEAN;
  v_nombre_cliente VARCHAR;
BEGIN
  -- 1. Validar permisos (solo ADMINISTRADOR u OWNER)
  SELECT rol INTO v_user_rol FROM public.usuarios WHERE id_usuario = p_user_id;
  IF v_user_rol IS NULL THEN
    RAISE EXCEPTION 'Usuario no encontrado: %', p_user_id;
  END IF;
  
  IF v_user_rol NOT IN ('ADMINISTRADOR', 'OWNER') THEN
    RAISE EXCEPTION 'Solo ADMIN u OWNER pueden cambiar estado de cliente. Rol actual: %', v_user_rol;
  END IF;

  -- 2. Obtener estado actual del cliente
  SELECT estado, nombre_cliente INTO v_estado_actual, v_nombre_cliente 
  FROM public.clientes WHERE id_cliente = p_id_cliente;
  
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Cliente no encontrado: %', p_id_cliente;
  END IF;

  -- 3. Validar que no sea el mismo estado
  IF v_estado_actual = p_nuevo_estado THEN
    RAISE EXCEPTION 'El cliente ya está en estado %', p_nuevo_estado;
  END IF;

  -- 4. Validar transiciones permitidas (máquina de estados)
  -- ACTIVO -> EN_BAJA, SUSPENDIDO
  -- EN_BAJA -> ACTIVO, INACTIVO
  -- INACTIVO -> ACTIVO
  -- SUSPENDIDO -> ACTIVO, EN_BAJA
  IF NOT (
    (v_estado_actual = 'ACTIVO' AND p_nuevo_estado IN ('EN_BAJA', 'SUSPENDIDO')) OR
    (v_estado_actual = 'EN_BAJA' AND p_nuevo_estado IN ('ACTIVO', 'INACTIVO')) OR
    (v_estado_actual = 'INACTIVO' AND p_nuevo_estado = 'ACTIVO') OR
    (v_estado_actual = 'SUSPENDIDO' AND p_nuevo_estado IN ('ACTIVO', 'EN_BAJA'))
  ) THEN
    RAISE EXCEPTION 'Transición no permitida: % -> %. Transiciones válidas desde %: %', 
      v_estado_actual, 
      p_nuevo_estado,
      v_estado_actual,
      CASE v_estado_actual
        WHEN 'ACTIVO' THEN 'EN_BAJA, SUSPENDIDO'
        WHEN 'EN_BAJA' THEN 'ACTIVO, INACTIVO'
        WHEN 'INACTIVO' THEN 'ACTIVO'
        WHEN 'SUSPENDIDO' THEN 'ACTIVO, EN_BAJA'
      END;
  END IF;

  -- 5. Verificar si tiene visita activa
  SELECT EXISTS(
    SELECT 1 FROM public.visitas
    WHERE id_cliente = p_id_cliente
    AND estado NOT IN ('COMPLETADO', 'CANCELADO')
  ) INTO v_tiene_visita_activa;

  -- 6. Actualizar estado del cliente (el trigger sincroniza activo)
  UPDATE public.clientes
  SET estado = p_nuevo_estado,
      updated_at = now()
  WHERE id_cliente = p_id_cliente;

  -- 7. Registrar en auditoría
  INSERT INTO public.cliente_estado_log (
    id_cliente, 
    estado_anterior, 
    estado_nuevo, 
    changed_by, 
    razon,
    metadata
  )
  VALUES (
    p_id_cliente, 
    v_estado_actual, 
    p_nuevo_estado, 
    p_user_id, 
    p_razon,
    jsonb_build_object(
      'tiene_visita_activa', v_tiene_visita_activa,
      'nombre_cliente', v_nombre_cliente
    )
  );

  -- 8. Retornar resultado
  RETURN jsonb_build_object(
    'success', true,
    'id_cliente', p_id_cliente,
    'nombre_cliente', v_nombre_cliente,
    'estado_anterior', v_estado_actual,
    'estado_nuevo', p_nuevo_estado,
    'tiene_visita_activa', v_tiene_visita_activa,
    'mensaje', CASE p_nuevo_estado
      WHEN 'EN_BAJA' THEN 'Cliente marcado para baja. La visita actual (si existe) será la última.'
      WHEN 'INACTIVO' THEN 'Cliente dado de baja. Ya no recibirá visitas.'
      WHEN 'SUSPENDIDO' THEN 'Cliente suspendido temporalmente.'
      WHEN 'ACTIVO' THEN 'Cliente reactivado exitosamente.'
    END
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_can_access_task(p_visit_id uuid, p_task_tipo text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_can_access boolean := true;
  v_reason text := NULL;
  v_prerequisite_estado text;
BEGIN
  -- Validaciones específicas por tipo de tarea
  CASE p_task_tipo
    WHEN 'LEV_POST_CORTE' THEN
      -- Requiere que CORTE esté completado
      SELECT vt.estado::text INTO v_prerequisite_estado
      FROM public.visit_tasks vt
      WHERE vt.visit_id = p_visit_id AND vt.task_tipo = 'CORTE';
      
      IF v_prerequisite_estado IS NULL OR v_prerequisite_estado != 'COMPLETADO' THEN
        v_can_access := false;
        v_reason := 'Debe completar el CORTE primero';
      END IF;

      -- Requiere que VENTA_ODV esté completada/omitida
      IF v_can_access THEN
        SELECT vt.estado::text INTO v_prerequisite_estado
        FROM public.visit_tasks vt
        WHERE vt.visit_id = p_visit_id AND vt.task_tipo = 'VENTA_ODV';
        
        IF v_prerequisite_estado IS NOT NULL 
           AND v_prerequisite_estado NOT IN ('COMPLETADO', 'OMITIDA', 'OMITIDO') THEN
          v_can_access := false;
          v_reason := 'Debe confirmar la ODV de Venta primero';
        END IF;
      END IF;

    WHEN 'ODV_BOTIQUIN' THEN
      -- Requiere LEV_POST_CORTE o LEVANTAMIENTO_INICIAL completado
      SELECT vt.estado::text INTO v_prerequisite_estado
      FROM public.visit_tasks vt
      WHERE vt.visit_id = p_visit_id 
      AND vt.task_tipo IN ('LEV_POST_CORTE', 'LEVANTAMIENTO_INICIAL')
      AND vt.estado IN ('COMPLETADO', 'OMITIDO', 'OMITIDA')
      LIMIT 1;
      
      IF v_prerequisite_estado IS NULL THEN
        v_can_access := false;
        v_reason := 'Debe completar el levantamiento primero';
      END IF;

    WHEN 'INFORME_VISITA' THEN
      -- Puede acceder siempre (es la última tarea)
      v_can_access := true;

    ELSE
      -- Otras tareas: verificar step_order
      v_can_access := true;
  END CASE;

  RETURN jsonb_build_object(
    'can_access', v_can_access,
    'reason', v_reason,
    'task_tipo', p_task_tipo
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cancel_task(p_visit_id uuid, p_task_tipo character varying, p_reason text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_task_exists boolean;
BEGIN
  -- Verificar que la tarea existe y no está completada/cancelada
  SELECT EXISTS(
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_tipo = p_task_tipo::tipo_visit_task
    AND estado NOT IN ('COMPLETADO', 'CANCELADO')
  ) INTO v_task_exists;

  IF NOT v_task_exists THEN
    RETURN false;
  END IF;

  -- Cancelar la tarea
  UPDATE public.visit_tasks
  SET
    estado = 'CANCELADO',
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('cancel_reason', p_reason)
  WHERE visit_id = p_visit_id
  AND task_tipo = p_task_tipo::tipo_visit_task;

  RETURN true;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cancel_visit(p_visit_id uuid, p_reason text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visita_exists boolean;
BEGIN
  -- Verificar que la visita existe y no está completada/cancelada
  SELECT EXISTS(
    SELECT 1 FROM public.visitas
    WHERE visit_id = p_visit_id
    AND estado NOT IN ('COMPLETADO', 'CANCELADO')
  ) INTO v_visita_exists;

  IF NOT v_visita_exists THEN
    RETURN false;
  END IF;

  -- Cancelar todas las tareas pendientes
  UPDATE public.visit_tasks
  SET
    estado = 'CANCELADO',
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('cancel_reason', p_reason)
  WHERE visit_id = p_visit_id
  AND estado NOT IN ('COMPLETADO', 'CANCELADO');

  -- Cancelar la visita
  UPDATE public.visitas
  SET
    estado = 'CANCELADO',
    updated_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('cancel_reason', p_reason)
  WHERE visit_id = p_visit_id;

  -- Cancelar sagas asociadas que estén en BORRADOR
  UPDATE public.saga_transactions
  SET
    estado = 'CANCELADA'::estado_saga_transaction,
    updated_at = now()
  WHERE visit_id = p_visit_id
  AND estado = 'BORRADOR'::estado_saga_transaction;

  RETURN true;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_cliente_tuvo_botiquin(p_id_cliente text)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Un cliente tuvo botiquín si tiene visitas históricas
  RETURN EXISTS (
    SELECT 1 FROM public.visitas WHERE id_cliente = p_id_cliente
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_compensate_saga(p_saga_id uuid, p_reason text DEFAULT 'Cancelado por usuario'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_task_tipo text;
BEGIN
  -- 1. Obtener y validar saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  IF v_saga.estado = 'CONFIRMADO' THEN
    RAISE EXCEPTION 'No se puede compensar una saga ya CONFIRMADA (PIVOT ejecutado): %', p_saga_id;
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    -- Ya cancelada, retornar éxito
    RETURN jsonb_build_object(
      'success', true,
      'already_cancelled', true,
      'saga_id', p_saga_id
    );
  END IF;

  -- 2. Determinar task_tipo según saga.tipo
  CASE v_saga.tipo::text
    WHEN 'LEVANTAMIENTO_INICIAL' THEN
      v_task_tipo := 'LEVANTAMIENTO_INICIAL';
    WHEN 'LEV_POST_CORTE' THEN
      v_task_tipo := 'LEV_POST_CORTE';
    WHEN 'VENTA' THEN
      v_task_tipo := 'CORTE';
    WHEN 'RECOLECCION' THEN
      v_task_tipo := 'CORTE';
    ELSE
      v_task_tipo := NULL;
  END CASE;

  -- 3. Cambiar estado de saga a CANCELADA
  UPDATE public.saga_transactions
  SET 
    estado = 'CANCELADA'::estado_saga_transaction,
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'cancel_reason', p_reason,
      'cancelled_at', now()
    ),
    updated_at = now()
  WHERE id = p_saga_id;

  -- 4. Marcar tarea asociada como CANCELADO (si aplica)
  IF v_task_tipo IS NOT NULL THEN
    UPDATE public.visit_tasks
    SET 
      estado = 'CANCELADO',
      metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'cancel_reason', p_reason,
        'cancelled_saga_id', p_saga_id
      ),
      last_activity_at = now()
    WHERE visit_id = v_saga.visit_id
    AND task_tipo = v_task_tipo::visit_task_tipo
    AND estado NOT IN ('COMPLETADO', 'OMITIDA');
  END IF;

  -- 5. Si es RECOLECCION, también eliminar de tabla recolecciones
  IF v_saga.tipo::text = 'RECOLECCION' THEN
    -- Eliminar items primero (FK)
    DELETE FROM public.recolecciones_items
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.recolecciones
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar evidencias
    DELETE FROM public.recolecciones_evidencias
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.recolecciones
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar firmas
    DELETE FROM public.recolecciones_firmas
    WHERE recoleccion_id IN (
      SELECT recoleccion_id FROM public.recolecciones
      WHERE visit_id = v_saga.visit_id
    );
    
    -- Eliminar recolección
    DELETE FROM public.recolecciones
    WHERE visit_id = v_saga.visit_id;
  END IF;

  -- ❌ NO necesita revertir movimientos_inventario
  -- ❌ NO necesita revertir inventario_botiquin
  -- Porque nunca se crearon (SSoT: solo se crean en PIVOT)

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'tipo', v_saga.tipo,
    'reason', p_reason
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_complete_recoleccion(p_visit_id uuid, p_responsable text, p_observaciones text, p_firma_path text, p_evidencias_paths text[])
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_recoleccion_id uuid;
  v_saga_id uuid;
  v_result jsonb;
BEGIN
  -- Obtener recolección de la visita
  SELECT r.recoleccion_id INTO v_recoleccion_id
  FROM public.recolecciones r
  WHERE r.visit_id = p_visit_id
  LIMIT 1;

  IF v_recoleccion_id IS NULL THEN
    RAISE EXCEPTION 'No existe recolección para visit_id';
  END IF;

  -- Validar responsable
  IF p_responsable IS NULL OR length(trim(p_responsable)) = 0 THEN
    RAISE EXCEPTION 'Responsable CEDIS requerido';
  END IF;

  -- Validar firma
  IF p_firma_path IS NULL OR length(trim(p_firma_path)) = 0 THEN
    RAISE EXCEPTION 'Firma requerida';
  END IF;

  -- Insertar o actualizar firma (upsert)
  INSERT INTO public.recolecciones_firmas (recoleccion_id, storage_path)
  VALUES (v_recoleccion_id, p_firma_path)
  ON CONFLICT (recoleccion_id) DO UPDATE
  SET storage_path = EXCLUDED.storage_path, signed_at = now();

  -- Insertar evidencias fotográficas
  IF p_evidencias_paths IS NOT NULL THEN
    INSERT INTO public.recolecciones_evidencias (recoleccion_id, storage_path)
    SELECT v_recoleccion_id, unnest(p_evidencias_paths)
    ON CONFLICT DO NOTHING;
  END IF;

  -- Actualizar recolección como ENTREGADA
  UPDATE public.recolecciones
  SET
    estado = 'ENTREGADA',
    entregada_at = now(),
    cedis_responsable_nombre = p_responsable,
    cedis_observaciones = p_observaciones,
    updated_at = now()
  WHERE recoleccion_id = v_recoleccion_id;

  -- Buscar saga de RECOLECCION
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND tipo::text = 'RECOLECCION'
  ORDER BY created_at DESC LIMIT 1;

  -- Si existe saga, confirmarla para crear movimientos
  IF v_saga_id IS NOT NULL THEN
    SELECT rpc_confirm_saga_pivot(v_saga_id, NULL, NULL) INTO v_result;
  END IF;

  -- Completar tarea de recolección
  UPDATE public.visit_tasks
  SET estado = 'COMPLETADO', completed_at = now(), last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = 'RECOLECCION';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_confirm_odv(p_visit_id uuid, p_saga_tipo text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga_id uuid;
  v_task_tipo visit_task_tipo;
  v_result jsonb;
BEGIN
  -- Mapear saga_tipo a task_tipo
  v_task_tipo := CASE p_saga_tipo
    WHEN 'VENTA' THEN 'VENTA_ODV'::visit_task_tipo
    WHEN 'LEVANTAMIENTO_INICIAL' THEN 'ODV_BOTIQUIN'::visit_task_tipo
    WHEN 'LEV_POST_CORTE' THEN 'ODV_BOTIQUIN'::visit_task_tipo
    ELSE p_saga_tipo::visit_task_tipo
  END;

  -- Buscar saga
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND tipo::text = p_saga_tipo
  ORDER BY created_at DESC LIMIT 1;

  IF v_saga_id IS NULL THEN
    RAISE EXCEPTION 'Saga % no encontrada para la visita', p_saga_tipo;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  -- Si la saga ya está CONFIRMADO, la función retornará sin hacer nada
  SELECT rpc_confirm_saga_pivot(v_saga_id, NULL, NULL) INTO v_result;

  -- La tarea ya se actualiza dentro de rpc_confirm_saga_pivot
  -- pero por si acaso, asegurar que quede COMPLETADO
  UPDATE visit_tasks
  SET
    estado = 'COMPLETADO'::visit_task_estado,
    completed_at = COALESCE(completed_at, now()),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'confirmed_at', now(),
      'saga_id', v_saga_id,
      'saga_tipo', p_saga_tipo
    )
  WHERE visit_id = p_visit_id 
  AND task_tipo = v_task_tipo
  AND estado != 'COMPLETADO'::visit_task_estado;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_confirm_saga_pivot(p_saga_id uuid, p_zoho_id text DEFAULT NULL::text, p_zoho_items jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga record;
  v_zoho_link_id integer;
  v_task_tipo text;
  v_zoho_link_tipo tipo_zoho_link;
  v_item record;
  v_cantidad_antes integer;
  v_cantidad_despues integer;
  v_tipo_movimiento tipo_movimiento_botiquin;  -- Ahora usa tipo semántico
BEGIN
  -- 1. Obtener y validar saga
  SELECT * INTO v_saga
  FROM public.saga_transactions
  WHERE id = p_saga_id;

  IF v_saga IS NULL THEN
    RAISE EXCEPTION 'Saga no encontrada: %', p_saga_id;
  END IF;

  IF v_saga.estado = 'CONFIRMADO' THEN
    -- Ya confirmada, retornar info existente
    SELECT id INTO v_zoho_link_id
    FROM public.saga_zoho_links
    WHERE id_saga_transaction = p_saga_id
    LIMIT 1;
    
    RETURN jsonb_build_object(
      'success', true,
      'already_confirmed', true,
      'saga_id', p_saga_id,
      'zoho_link_id', v_zoho_link_id
    );
  END IF;

  IF v_saga.estado = 'CANCELADA' THEN
    RAISE EXCEPTION 'Saga ya fue cancelada: %', p_saga_id;
  END IF;

  -- 2. Determinar tipo de zoho_link, task_tipo y tipo_movimiento según saga.tipo
  CASE v_saga.tipo::text
    WHEN 'LEVANTAMIENTO_INICIAL' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';  -- Semántico: crear inventario
    WHEN 'LEV_POST_CORTE' THEN
      v_zoho_link_tipo := 'BOTIQUIN';
      v_task_tipo := 'ODV_BOTIQUIN';
      v_tipo_movimiento := 'CREACION';  -- Semántico: reponer inventario
    WHEN 'VENTA' THEN
      v_zoho_link_tipo := 'VENTA';
      v_task_tipo := 'VENTA_ODV';
      v_tipo_movimiento := 'VENTA';  -- Semántico: venta
    WHEN 'RECOLECCION' THEN
      v_zoho_link_tipo := 'DEVOLUCION';
      v_task_tipo := 'RECOLECCION';
      v_tipo_movimiento := 'RECOLECCION';  -- Semántico: recolección
    ELSE
      RAISE EXCEPTION 'Tipo de saga no soportado: %', v_saga.tipo;
  END CASE;

  -- 3. Cambiar estado de saga a CONFIRMADO
  UPDATE public.saga_transactions
  SET 
    estado = 'CONFIRMADO'::estado_saga_transaction,
    updated_at = now()
  WHERE id = p_saga_id;

  -- 4. Crear saga_zoho_links
  IF p_zoho_id IS NOT NULL THEN
    INSERT INTO public.saga_zoho_links (
      id_saga_transaction,
      zoho_id,
      tipo,
      items,
      zoho_sync_status,
      created_at,
      updated_at
    )
    VALUES (
      p_saga_id,
      p_zoho_id,
      v_zoho_link_tipo,
      COALESCE(p_zoho_items, v_saga.items),
      'pending',
      now(),
      now()
    )
    RETURNING id INTO v_zoho_link_id;
  END IF;

  -- 5. Generar movimientos_inventario desde saga.items
  FOR v_item IN
    SELECT 
      (item->>'sku')::varchar as sku,
      (item->>'cantidad')::int as cantidad
    FROM jsonb_array_elements(v_saga.items) as item
  LOOP
    -- Obtener cantidad_antes
    SELECT COALESCE(cantidad_disponible, 0)
    INTO v_cantidad_antes
    FROM public.inventario_botiquin
    WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    
    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;

    -- Calcular cantidad_despues según tipo de movimiento semántico
    IF v_tipo_movimiento = 'CREACION' THEN
      v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
    ELSE
      -- VENTA y RECOLECCION son salidas
      v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
    END IF;

    -- Insertar movimiento con tipo semántico
    INSERT INTO public.movimientos_inventario (
      id_saga_transaction,
      id_cliente,
      sku,
      tipo,
      cantidad,
      cantidad_antes,
      cantidad_despues,
      fecha_movimiento
    )
    VALUES (
      p_saga_id,
      v_saga.id_cliente,
      v_item.sku,
      v_tipo_movimiento,  -- Ahora es CREACION, VENTA o RECOLECCION
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      now()
    );

    -- Actualizar inventario_botiquin
    IF v_cantidad_despues > 0 THEN
      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      VALUES (v_saga.id_cliente, v_item.sku, v_cantidad_despues, now())
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET 
        cantidad_disponible = v_cantidad_despues,
        ultima_actualizacion = now();
    ELSE
      -- Si cantidad es 0, eliminar del inventario
      DELETE FROM public.inventario_botiquin 
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    END IF;

    -- Si es VENTA, eliminar de botiquin_clientes_sku_disponibles
    IF v_saga.tipo::text = 'VENTA' THEN
      DELETE FROM public.botiquin_clientes_sku_disponibles
      WHERE id_cliente = v_saga.id_cliente AND sku = v_item.sku;
    END IF;
  END LOOP;

  -- 6. Actualizar visit_tasks.reference_id → saga_zoho_links.id
  IF v_zoho_link_id IS NOT NULL THEN
    UPDATE public.visit_tasks
    SET 
      estado = 'COMPLETADO',
      completed_at = COALESCE(completed_at, now()),
      reference_table = 'saga_zoho_links',
      reference_id = v_zoho_link_id::text,
      last_activity_at = now()
    WHERE visit_id = v_saga.visit_id
    AND task_tipo = v_task_tipo::visit_task_tipo;
  ELSE
    -- Sin zoho_id, solo marcar como completado
    UPDATE public.visit_tasks
    SET 
      estado = 'COMPLETADO',
      completed_at = COALESCE(completed_at, now()),
      last_activity_at = now()
    WHERE visit_id = v_saga.visit_id
    AND task_tipo = v_task_tipo::visit_task_tipo;
  END IF;

  RETURN jsonb_build_object(
    'success', true,
    'saga_id', p_saga_id,
    'zoho_link_id', v_zoho_link_id,
    'tipo', v_saga.tipo,
    'items_count', jsonb_array_length(v_saga.items)
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_consolidate_visitas()
 RETURNS TABLE(visitas_consolidated integer, visitas_deleted integer, sagas_moved integer, tasks_moved integer, informes_created integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_consolidated integer := 0;
  v_visitas_deleted integer := 0;
  v_sagas_moved integer := 0;
  v_tasks_moved integer := 0;
  v_informes_created integer := 0;
  v_row_count integer := 0;
  v_dup record;
  v_primary_visit_id uuid;
  v_duplicate_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Consolidar visitas duplicadas
  -- =====================================================
  -- Encontrar grupos de visitas duplicadas (mismo cliente, usuario, fecha)

  FOR v_dup IN
    SELECT
      v.id_cliente,
      v.id_usuario,
      DATE(v.created_at) as fecha,
      array_agg(v.visit_id ORDER BY v.created_at ASC) as visit_ids,
      COUNT(*) as num_visitas
    FROM public.visitas v
    GROUP BY v.id_cliente, v.id_usuario, DATE(v.created_at)
    HAVING COUNT(*) > 1
  LOOP
    -- La primera visita (más antigua) será la principal
    v_primary_visit_id := v_dup.visit_ids[1];

    -- Procesar cada visita duplicada (desde la segunda en adelante)
    FOR i IN 2..array_length(v_dup.visit_ids, 1) LOOP
      v_duplicate_visit_id := v_dup.visit_ids[i];

      -- Mover saga_transactions al visit_id principal
      UPDATE public.saga_transactions
      SET visit_id = v_primary_visit_id, updated_at = now()
      WHERE visit_id = v_duplicate_visit_id;

      GET DIAGNOSTICS v_row_count = ROW_COUNT;
      v_sagas_moved := v_sagas_moved + v_row_count;

      -- Mover recolecciones al visit_id principal
      UPDATE public.recolecciones
      SET visit_id = v_primary_visit_id, updated_at = now()
      WHERE visit_id = v_duplicate_visit_id;

      -- Eliminar tareas duplicadas (las de la visita duplicada)
      DELETE FROM public.visit_tasks
      WHERE visit_id = v_duplicate_visit_id;

      GET DIAGNOSTICS v_row_count = ROW_COUNT;
      v_tasks_moved := v_tasks_moved + v_row_count;

      -- Eliminar la visita duplicada
      DELETE FROM public.visitas
      WHERE visit_id = v_duplicate_visit_id;

      v_visitas_deleted := v_visitas_deleted + 1;
    END LOOP;

    -- Asegurar que la visita principal sea VISITA_CORTE si tiene tareas de corte
    UPDATE public.visitas
    SET
      tipo = 'VISITA_CORTE'::public.visit_tipo,
      updated_at = now()
    WHERE visit_id = v_primary_visit_id
      AND EXISTS (
        SELECT 1 FROM public.saga_transactions st
        WHERE st.visit_id = v_primary_visit_id
          AND st.tipo::text IN ('CORTE', 'CORTE_RENOVACION', 'VENTA_ODV', 'VENTA', 'RECOLECCION', 'LEV_POST_CORTE')
      );

    v_visitas_consolidated := v_visitas_consolidated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Recrear tareas completas para visitas consolidadas
  -- =====================================================
  -- Para cada VISITA_CORTE, asegurar que tenga todas las tareas

  FOR v_dup IN
    SELECT v.visit_id, v.created_at
    FROM public.visitas v
    WHERE v.tipo = 'VISITA_CORTE'
      AND v.metadata->>'migrated_from_legacy' = 'true'
  LOOP
    -- Verificar y crear tarea CORTE si no existe
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'CORTE', 'COMPLETADO', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_tipo = 'CORTE');

    -- Verificar y crear tarea VENTA_ODV si no existe
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'VENTA_ODV', 'COMPLETADO', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_tipo = 'VENTA_ODV');

    -- Verificar y crear tarea RECOLECCION si no existe
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'RECOLECCION', 'COMPLETADO', false, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_tipo = 'RECOLECCION');

    -- Verificar y crear tarea LEV_POST_CORTE si no existe
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'LEV_POST_CORTE', 'COMPLETADO', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_tipo = 'LEV_POST_CORTE');

    -- Verificar y crear tarea INFORME_VISITA si no existe
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata)
    SELECT v_dup.visit_id, 'INFORME_VISITA', 'COMPLETADO', true, v_dup.created_at, v_dup.created_at, v_dup.created_at, '{}'::jsonb
    WHERE NOT EXISTS (SELECT 1 FROM public.visit_tasks vt WHERE vt.visit_id = v_dup.visit_id AND vt.task_tipo = 'INFORME_VISITA');
  END LOOP;

  -- =====================================================
  -- PASO 3: Vincular referencias de saga a tareas
  -- =====================================================

  -- Vincular CORTE/CORTE_RENOVACION a tarea CORTE
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('CORTE', 'CORTE_RENOVACION')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_tipo = 'CORTE'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('CORTE', 'CORTE_RENOVACION')
    );

  -- Vincular VENTA_ODV/VENTA a tarea VENTA_ODV
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('VENTA_ODV', 'VENTA')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_tipo = 'VENTA_ODV'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('VENTA_ODV', 'VENTA')
    );

  -- Vincular LEV_POST_CORTE/LEVANTAMIENTO a tarea LEV_POST_CORTE
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('LEV_POST_CORTE', 'LEVANTAMIENTO_INICIAL')
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
      ORDER BY
        CASE st.tipo::text WHEN 'LEV_POST_CORTE' THEN 1 ELSE 2 END,
        st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_tipo = 'LEV_POST_CORTE'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text IN ('LEV_POST_CORTE', 'LEVANTAMIENTO_INICIAL')
    );

  -- Vincular RECOLECCION saga a tarea RECOLECCION
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'saga_transactions',
    reference_id = (
      SELECT st.id::text
      FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text = 'RECOLECCION'
      ORDER BY st.created_at DESC
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_tipo = 'RECOLECCION'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.saga_transactions st
      WHERE st.visit_id = vt.visit_id
        AND st.tipo::text = 'RECOLECCION'
    );

  -- =====================================================
  -- PASO 4: Migrar encuestas_ciclo a visita_informes
  -- =====================================================

  INSERT INTO public.visita_informes (visit_id, respuestas, etiqueta, cumplimiento_score, created_at)
  SELECT
    v.visit_id,
    ec.respuestas,
    'MIGRADO'::varchar,
    0,
    COALESCE(ec.fecha_completada, ec.created_at)
  FROM public.encuestas_ciclo ec
  JOIN public.visitas v ON v.id_ciclo = ec.id_ciclo
  WHERE ec.completada = true
    AND NOT EXISTS (
      SELECT 1 FROM public.visita_informes vi WHERE vi.visit_id = v.visit_id
    )
  ON CONFLICT (visit_id) DO NOTHING;

  GET DIAGNOSTICS v_informes_created = ROW_COUNT;

  -- Vincular informes a tareas INFORME_VISITA
  UPDATE public.visit_tasks vt
  SET
    reference_table = 'visita_informes',
    reference_id = (
      SELECT vi.informe_id::text
      FROM public.visita_informes vi
      WHERE vi.visit_id = vt.visit_id
      LIMIT 1
    ),
    last_activity_at = now()
  WHERE vt.task_tipo = 'INFORME_VISITA'
    AND vt.reference_id IS NULL
    AND EXISTS (
      SELECT 1 FROM public.visita_informes vi WHERE vi.visit_id = vt.visit_id
    );

  RETURN QUERY SELECT v_visitas_consolidated, v_visitas_deleted, v_sagas_moved, v_tasks_moved, v_informes_created;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_count_notificaciones_no_leidas()
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
  v_count INTEGER;
BEGIN
  SELECT u.id_usuario INTO v_user_id
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  SELECT COUNT(*)::integer INTO v_count
  FROM public.notificaciones_admin n
  WHERE n.leida = false
  AND (n.para_usuario IS NULL OR n.para_usuario = v_user_id);

  RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_id_ciclo integer, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
BEGIN
  -- Obtener el usuario actual
  SELECT u.id_usuario INTO v_id_usuario
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en tabla usuarios';
  END IF;

  -- Validar que no exista una visita activa para el cliente
  -- Solo bloquear si hay visitas en estados PENDIENTE, EN_CURSO, o PROGRAMADO
  -- COMPLETADO y CANCELADO no deben bloquear la creación de nuevas visitas
  IF EXISTS (
    SELECT 1
    FROM public.visitas v
    WHERE v.id_cliente = p_id_cliente
      AND v.estado IN ('PENDIENTE', 'EN_CURSO', 'PROGRAMADO')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Crear la visita
  -- NOTA: due_at usa now() para que la fecha mostrada sea la fecha de creación
  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at
  )
  VALUES (
    p_id_cliente, v_id_usuario, p_id_ciclo, p_tipo,
    'PENDIENTE', now(), now(), now()
  )
  RETURNING visit_id INTO v_visit_id;

  -- Crear registro de informe vacío
  INSERT INTO public.visita_informes (visit_id, respuestas, completada)
  VALUES (v_visit_id, '{}'::jsonb, false);

  -- Crear tareas según el tipo de visita
  -- Incluye transaction_type y step_order requeridos por el patrón Saga
  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'LEVANTAMIENTO_INICIAL', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    -- VISITA_CORTE: 6 tareas con transaction_types correctos
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'VENTA_ODV', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'RECOLECCION', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'LEV_POST_CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_create_visit(p_id_cliente character varying, p_tipo character varying DEFAULT 'VISITA_CORTE'::character varying)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_visit_id uuid;
BEGIN
  -- Obtener el usuario actual
  SELECT u.id_usuario INTO v_id_usuario
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  IF v_id_usuario IS NULL THEN
    RAISE EXCEPTION 'Usuario no mapeado en tabla usuarios';
  END IF;

  -- Validar que no exista una visita activa para el cliente
  IF EXISTS (
    SELECT 1
    FROM public.visitas v
    WHERE v.id_cliente = p_id_cliente
      AND v.estado IN ('PENDIENTE', 'EN_CURSO', 'PROGRAMADO')
  ) THEN
    RAISE EXCEPTION 'Ya existe una visita activa para este cliente';
  END IF;

  -- Crear la visita (sin p_id_ciclo, será NULL)
  INSERT INTO public.visitas (
    id_cliente, id_usuario, id_ciclo, tipo,
    estado, created_at, due_at, last_activity_at
  )
  VALUES (
    p_id_cliente, v_id_usuario, NULL, p_tipo::visit_tipo,
    'PENDIENTE', now(), now(), now()
  )
  RETURNING visit_id INTO v_visit_id;

  -- Crear registro de informe vacío
  INSERT INTO public.visita_informes (visit_id, respuestas, completada)
  VALUES (v_visit_id, '{}'::jsonb, false);

  -- Crear tareas según el tipo de visita
  IF p_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'LEVANTAMIENTO_INICIAL', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 3);
  ELSE
    -- VISITA_CORTE: 6 tareas con ODV_BOTIQUIN
    INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, due_at, transaction_type, step_order)
    VALUES
      (v_visit_id, 'CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 1),
      (v_visit_id, 'VENTA_ODV', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 2),
      (v_visit_id, 'RECOLECCION', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 3),
      (v_visit_id, 'LEV_POST_CORTE', 'PENDIENTE', now() + interval '7 days', 'COMPENSABLE', 4),
      (v_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE', now() + interval '7 days', 'PIVOT', 5),
      (v_visit_id, 'INFORME_VISITA', 'PENDIENTE', now() + interval '7 days', 'RETRYABLE', 6);
  END IF;

  RETURN v_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_cliente_estado_historial(p_id_cliente character varying)
 RETURNS TABLE(id uuid, estado_anterior public.estado_cliente, estado_nuevo public.estado_cliente, changed_by character varying, changed_by_nombre character varying, changed_at timestamp with time zone, razon text, dias_en_estado_anterior integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT 
    cel.id,
    cel.estado_anterior,
    cel.estado_nuevo,
    cel.changed_by,
    u.nombre as changed_by_nombre,
    cel.changed_at,
    cel.razon,
    cel.dias_en_estado_anterior
  FROM public.cliente_estado_log cel
  LEFT JOIN public.usuarios u ON u.id_usuario = cel.changed_by
  WHERE cel.id_cliente = p_id_cliente
  ORDER BY cel.changed_at DESC;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_corte_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
BEGIN
  -- Verificar que la visita existe
  IF NOT EXISTS (SELECT 1 FROM public.visitas WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar acceso a la visita
  IF NOT public.can_access_visita(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- 1. Primero buscar en visit_tasks metadata del CORTE (nuevo formato)
  SELECT vt.metadata->'items' INTO v_items
  FROM public.visit_tasks vt
  WHERE vt.visit_id = p_visit_id
    AND vt.task_tipo = 'CORTE'
    AND vt.estado = 'COMPLETADO'
    AND vt.metadata->'items' IS NOT NULL
    AND jsonb_array_length(vt.metadata->'items') > 0;

  IF v_items IS NOT NULL THEN
    RETURN v_items;
  END IF;

  -- 2. Buscar en saga_transactions tipo CORTE (formato legacy)
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'CORTE'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_items IS NOT NULL THEN
    RETURN v_items;
  END IF;

  -- 3. Combinar items de sagas VENTA y RECOLECCION
  -- NOTA: Ahora parsea correctamente: VENTA usa 'cantidad', RECOLECCION usa 'cantidad_salida'
  SELECT jsonb_agg(combined_item)
  INTO v_items
  FROM (
    -- Items de VENTA: cantidad representa lo vendido
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'cantidad_actual', 0,
      'vendido', COALESCE((item->>'cantidad')::int, (item->>'vendido')::int, 0),
      'recolectado', 0,
      'permanencia', false
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'VENTA'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0

    UNION ALL

    -- Items de RECOLECCION: cantidad_salida representa lo recolectado
    SELECT jsonb_build_object(
      'sku', item->>'sku',
      'cantidad_actual', 0,
      'vendido', 0,
      'recolectado', COALESCE(
        (item->>'cantidad_salida')::int, 
        (item->>'recolectado')::int, 
        (item->>'cantidad')::int, 
        0
      ),
      'permanencia', COALESCE((item->>'cantidad_permanencia')::int, 0) > 0
    ) as combined_item
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'RECOLECCION'
      AND st.items IS NOT NULL
      AND jsonb_array_length(st.items) > 0
  ) items
  WHERE (combined_item->>'vendido')::int > 0 
     OR (combined_item->>'recolectado')::int > 0;

  IF v_items IS NOT NULL AND jsonb_array_length(v_items) > 0 THEN
    RETURN v_items;
  END IF;

  -- 4. Buscar en movimientos_inventario (usando tipo semántico)
  SELECT jsonb_agg(item_data)
  INTO v_items
  FROM (
    SELECT jsonb_build_object(
      'sku', mi.sku,
      'cantidad_actual', 0,
      'vendido', COALESCE(SUM(CASE WHEN mi.tipo::text = 'VENTA' THEN mi.cantidad ELSE 0 END), 0),
      'recolectado', COALESCE(SUM(CASE WHEN mi.tipo::text = 'RECOLECCION' THEN mi.cantidad ELSE 0 END), 0),
      'permanencia', CASE WHEN SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN 1 ELSE 0 END) > 0 THEN true ELSE false END
    ) as item_data
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text IN ('VENTA', 'RECOLECCION', 'PERMANENCIA')
    GROUP BY mi.sku
    HAVING SUM(CASE WHEN mi.tipo::text IN ('VENTA', 'RECOLECCION') THEN mi.cantidad ELSE 0 END) > 0
       OR SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN 1 ELSE 0 END) > 0
  ) items;

  -- Retornar items o array vacío
  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_corte_permanencia_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
BEGIN
  -- Leer items de permanencia del CORTE
  -- Calcula la cantidad restante: cantidad_actual - vendido - recolectado
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'producto', item->>'sku'),
        'cantidad', GREATEST(
          0,
          (COALESCE(item->>'cantidad_actual', item->>'cantidad', '0'))::int
          - (COALESCE(item->>'vendido', '0'))::int
          - (COALESCE(item->>'recolectado', '0'))::int
        )
      )
    ) FILTER (WHERE 
      GREATEST(
        0,
        (COALESCE(item->>'cantidad_actual', item->>'cantidad', '0'))::int
        - (COALESCE(item->>'vendido', '0'))::int
        - (COALESCE(item->>'recolectado', '0'))::int
      ) > 0
    ),
    '[]'::jsonb
  )
  INTO v_items
  FROM saga_transactions st
  CROSS JOIN LATERAL jsonb_array_elements(st.items) AS item
  LEFT JOIN medicamentos m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.tipo = 'CORTE'
    AND (item->>'permanencia')::boolean = true;

  RETURN v_items;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_lev_post_corte_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb;
  v_permanencia_skus text[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM public.visitas WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_visita(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Obtener SKUs con permanencia del CORTE para esta visita
  SELECT ARRAY_AGG(DISTINCT item->>'sku')
  INTO v_permanencia_skus
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'CORTE'
    AND st.items IS NOT NULL
    AND (item->>'permanencia')::boolean = true;

  -- También buscar en movimientos_inventario con PERMANENCIA (usando tipo semántico)
  IF v_permanencia_skus IS NULL THEN
    SELECT ARRAY_AGG(DISTINCT mi.sku)
    INTO v_permanencia_skus
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text = 'PERMANENCIA';
  END IF;

  -- Primero buscar en LEV_POST_CORTE (nuevo formato)
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEV_POST_CORTE'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_items IS NOT NULL THEN
    -- LEV_POST_CORTE: {sku, cantidad, es_permanencia}
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'cantidad', COALESCE((item->>'cantidad')::int, 0),
        'es_permanencia', COALESCE(
          (item->>'es_permanencia')::boolean,
          (item->>'sku') = ANY(v_permanencia_skus)
        )
      )
    )
    INTO v_items
    FROM jsonb_array_elements(v_items) AS item
    WHERE COALESCE((item->>'cantidad')::int, 0) > 0;

    RETURN COALESCE(v_items, '[]'::jsonb);
  END IF;

  -- Buscar en movimientos_inventario con PERMANENCIA (usando tipo semántico)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', items.sku,
      'cantidad', items.cantidad,
      'es_permanencia', true
    )
  )
  INTO v_items
  FROM (
    SELECT mi.sku, COUNT(*)::int as cantidad
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text = 'PERMANENCIA'
    GROUP BY mi.sku
  ) items;

  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_levantamiento_items(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente varchar;
  v_items jsonb;
BEGIN
  SELECT v.id_cliente INTO v_id_cliente
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  IF NOT public.can_access_cliente(v_id_cliente) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Buscar saga de LEVANTAMIENTO_INICIAL
  SELECT st.items INTO v_items
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
  ORDER BY st.created_at DESC
  LIMIT 1;

  -- Transformar items: cantidad_entrada es la cantidad en el botiquín
  IF v_items IS NOT NULL THEN
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'cantidad', COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
      )
    )
    INTO v_items
    FROM jsonb_array_elements(v_items) AS item
    WHERE COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0) > 0;
  END IF;

  RETURN COALESCE(v_items, '[]'::jsonb);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_next_visit_type(p_id_cliente character varying)
 RETURNS character varying
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_has_completed_visit boolean;
BEGIN
  -- Check if client has any completed VISITA_LEVANTAMIENTO_INICIAL or any VISITA_CORTE
  SELECT EXISTS (
    SELECT 1
    FROM public.visitas v
    WHERE v.id_cliente = p_id_cliente
      AND v.estado = 'COMPLETADO'
      AND (v.tipo = 'VISITA_LEVANTAMIENTO_INICIAL' OR v.tipo = 'VISITA_CORTE')
  ) INTO v_has_completed_visit;

  IF v_has_completed_visit THEN
    RETURN 'VISITA_CORTE';
  ELSE
    RETURN 'VISITA_LEVANTAMIENTO_INICIAL';
  END IF;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_notificaciones_no_leidas()
 RETURNS TABLE(id uuid, tipo public.tipo_notificacion, titulo character varying, mensaje text, metadata jsonb, created_at timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
BEGIN
  -- Obtener usuario actual
  SELECT u.id_usuario INTO v_user_id
  FROM public.usuarios u
  WHERE u.auth_user_id = auth.uid()
  LIMIT 1;

  RETURN QUERY
  SELECT 
    n.id,
    n.tipo,
    n.titulo,
    n.mensaje,
    n.metadata,
    n.created_at
  FROM public.notificaciones_admin n
  WHERE n.leida = false
  AND (n.para_usuario IS NULL OR n.para_usuario = v_user_id)
  ORDER BY n.created_at DESC
  LIMIT 50;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_rangos_cliente()
 RETURNS TABLE(rango character varying)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT DISTINCT c.rango
  FROM clientes c
  WHERE c.rango IS NOT NULL AND c.rango != ''
  ORDER BY c.rango;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_user_notifications(p_user_id character varying, p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_unread_only boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_notifications JSONB;
    v_unread_count INTEGER;
BEGIN
    -- Count unread
    SELECT COUNT(*) INTO v_unread_count
    FROM notifications
    WHERE user_id = p_user_id
      AND read_at IS NULL
      AND (expires_at IS NULL OR expires_at > NOW());

    -- Get notifications
    SELECT COALESCE(jsonb_agg(n ORDER BY n.created_at DESC), '[]')
    INTO v_notifications
    FROM (
        SELECT
            id, type, title, body, data,
            read_at, created_at,
            read_at IS NULL as is_unread
        FROM notifications
        WHERE user_id = p_user_id
          AND (expires_at IS NULL OR expires_at > NOW())
          AND (NOT p_unread_only OR read_at IS NULL)
        ORDER BY created_at DESC
        LIMIT p_limit OFFSET p_offset
    ) n;

    RETURN jsonb_build_object(
        'notifications', v_notifications,
        'unread_count', v_unread_count
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_visit_odvs(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_result jsonb;
BEGIN
  -- Obtener todas las ODVs vinculadas a la visita a través de saga_zoho_links
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_result
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'odv_numero', szl.zoho_id,
      'tipo', szl.tipo::text,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'fecha', szl.created_at,
      'saga_id', st.id,
      'saga_tipo', st.tipo::text,
      'items', COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'sku', item->>'sku',
              'producto', COALESCE(m.producto, item->>'sku'),
              'cantidad', COALESCE(
                (item->>'cantidad')::int,
                (item->>'cantidad_entrada')::int,
                0
              )
            )
          )
          FROM jsonb_array_elements(st.items) AS item
          LEFT JOIN medicamentos m ON m.sku = item->>'sku'
          WHERE item->>'sku' IS NOT NULL
        ),
        '[]'::jsonb
      ),
      'total_piezas', COALESCE(
        (
          SELECT SUM(
            COALESCE(
              (item->>'cantidad')::int,
              (item->>'cantidad_entrada')::int,
              0
            )
          )
          FROM jsonb_array_elements(st.items) AS item
        ),
        0
      )::int
    ) as odv_data
    FROM saga_zoho_links szl
    JOIN saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    ORDER BY szl.created_at
  ) sub;

  RETURN v_result;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_get_visit_saga_summary(p_visit_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente text;
  v_visit_tipo text;
  v_corte_items jsonb;
  v_levantamiento_items jsonb;
  v_lev_post_corte_items jsonb;
  v_odvs_venta jsonb;
  v_odvs_botiquin jsonb;
  v_movimientos_resumen jsonb;
  v_recoleccion_items jsonb;
  v_has_movimientos boolean;
  v_mov_total_count int;
  v_mov_total_cantidad int;
  v_mov_unique_skus int;
  v_mov_by_tipo jsonb;
  v_total_vendido int;
  v_total_recolectado int;
  v_total_levantamiento int;
  v_total_lev_post_corte int;
  v_total_recoleccion int;
BEGIN
  -- Verificar que la visita existe y obtener info básica
  SELECT v.id_cliente, v.tipo::text
  INTO v_id_cliente, v_visit_tipo
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar acceso
  IF NOT public.can_access_visita(p_visit_id) THEN
    RAISE EXCEPTION 'Acceso denegado';
  END IF;

  -- Verificar si hay movimientos_inventario (fuente de verdad)
  SELECT EXISTS(
    SELECT 1 FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
  ) INTO v_has_movimientos;

  -- 1. CORTE: FIRST try to read from visit_tasks.metadata (most reliable source)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'vendido', COALESCE((item->>'vendido')::int, 0),
      'recolectado', COALESCE((item->>'recolectado')::int, 0),
      'permanencia', 0
    )
  )
  INTO v_corte_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_tipo = 'CORTE'
    AND vt.estado = 'COMPLETADO'
    AND vt.metadata->'items' IS NOT NULL
    AND jsonb_array_length(vt.metadata->'items') > 0
    AND (COALESCE((item->>'vendido')::int, 0) > 0 OR COALESCE((item->>'recolectado')::int, 0) > 0);

  IF v_corte_items IS NOT NULL THEN
    -- Calculate totals from the corte items
    SELECT 
      COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0)::int,
      COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.visit_tasks vt,
         jsonb_array_elements(vt.metadata->'items') AS item
    WHERE vt.visit_id = p_visit_id
      AND vt.task_tipo = 'CORTE'
      AND vt.estado = 'COMPLETADO';
  ELSIF v_has_movimientos THEN
    -- FALLBACK: Use movimientos_inventario if visit_tasks.metadata is empty
    SELECT jsonb_agg(item_data)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'producto', producto,
        'vendido', vendido,
        'recolectado', recolectado,
        'permanencia', permanencia
      ) as item_data
      FROM (
        SELECT 
          mi.sku,
          COALESCE(m.producto, mi.sku) as producto,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'VENTA' THEN mi.cantidad ELSE 0 END), 0)::int as vendido,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'RECOLECCION' THEN mi.cantidad ELSE 0 END), 0)::int as recolectado,
          COALESCE(SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN mi.cantidad ELSE 0 END), 0)::int as permanencia
        FROM public.movimientos_inventario mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medicamentos m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.tipo::text IN ('VENTA', 'RECOLECCION', 'PERMANENCIA')
        GROUP BY mi.sku, m.producto
        -- FIX: Use mi.cantidad for PERMANENCIA in HAVING clause
        HAVING SUM(CASE WHEN mi.tipo::text IN ('VENTA', 'RECOLECCION') THEN mi.cantidad ELSE 0 END) > 0
           OR SUM(CASE WHEN mi.tipo::text = 'PERMANENCIA' THEN mi.cantidad ELSE 0 END) > 0
      ) grouped
    ) items;
    
    SELECT 
      COALESCE(SUM(CASE WHEN mi.tipo::text = 'VENTA' THEN mi.cantidad ELSE 0 END), 0)::int,
      COALESCE(SUM(CASE WHEN mi.tipo::text = 'RECOLECCION' THEN mi.cantidad ELSE 0 END), 0)::int
    INTO v_total_vendido, v_total_recolectado
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND mi.tipo::text IN ('VENTA', 'RECOLECCION');
  ELSE
    -- FALLBACK: Use saga_transactions.items
    SELECT jsonb_agg(combined_item)
    INTO v_corte_items
    FROM (
      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'vendido', COALESCE((item->>'cantidad')::int, (item->>'vendido')::int, 0),
        'recolectado', 0,
        'permanencia', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.tipo::text = 'VENTA'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0

      UNION ALL

      SELECT jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'vendido', 0,
        'recolectado', COALESCE(
          (item->>'cantidad_salida')::int, 
          (item->>'cantidad')::int, 
          0
        ),
        'permanencia', 0
      ) as combined_item
      FROM public.saga_transactions st,
           jsonb_array_elements(st.items) AS item
      LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
      WHERE st.visit_id = p_visit_id
        AND st.tipo::text = 'RECOLECCION'
        AND st.items IS NOT NULL
        AND jsonb_array_length(st.items) > 0
    ) items
    WHERE (combined_item->>'vendido')::int > 0 
       OR (combined_item->>'recolectado')::int > 0;
    
    SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
    INTO v_total_vendido
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'VENTA'
      AND st.items IS NOT NULL;
    
    SELECT COALESCE(SUM(COALESCE((item->>'cantidad_salida')::int, (item->>'cantidad')::int, 0)), 0)::int
    INTO v_total_recolectado
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'RECOLECCION'
      AND st.items IS NOT NULL;
  END IF;

  -- 2. LEVANTAMIENTO_INICIAL
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
    )
  )
  INTO v_levantamiento_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)), 0)::int
  INTO v_total_levantamiento
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
    AND st.items IS NOT NULL;

  -- 3. LEV_POST_CORTE
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'cantidad')::int, 0),
      'es_permanencia', COALESCE((item->>'es_permanencia')::boolean, false)
    )
  )
  INTO v_lev_post_corte_items
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEV_POST_CORTE'
    AND st.items IS NOT NULL
    AND jsonb_array_length(st.items) > 0
    AND COALESCE((item->>'cantidad')::int, 0) > 0;

  SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
  INTO v_total_lev_post_corte
  FROM public.saga_transactions st,
       jsonb_array_elements(st.items) AS item
  WHERE st.visit_id = p_visit_id
    AND st.tipo::text = 'LEV_POST_CORTE'
    AND st.items IS NOT NULL;

  -- 4. ODVs de VENTA (usando saga_zoho_links.tipo = 'VENTA')
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_venta
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'fecha', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'tipo', szl.tipo::text,
      'total_piezas', (
        SELECT COALESCE(SUM(COALESCE((item->>'cantidad')::int, 0)), 0)::int
        FROM jsonb_array_elements(st.items) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'producto', COALESCE(m.producto, item->>'sku'),
            'cantidad_vendida', COALESCE((item->>'cantidad')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(st.items) AS item
        LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.tipo::text = 'VENTA'
    ORDER BY szl.created_at
  ) sub;

  -- 5. ODVs de BOTIQUIN (usando saga_zoho_links.tipo = 'BOTIQUIN')
  SELECT COALESCE(jsonb_agg(odv_data), '[]'::jsonb)
  INTO v_odvs_botiquin
  FROM (
    SELECT jsonb_build_object(
      'odv_id', szl.zoho_id,
      'fecha', szl.created_at,
      'estado', COALESCE(szl.zoho_sync_status, 'pending'),
      'tipo', szl.tipo::text,
      'total_piezas', (
        SELECT COALESCE(SUM(COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)), 0)::int
        FROM jsonb_array_elements(st.items) AS item
      ),
      'items', (
        SELECT COALESCE(jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'producto', COALESCE(m.producto, item->>'sku'),
            'cantidad', COALESCE((item->>'cantidad_entrada')::int, (item->>'cantidad')::int, 0)
          )
        ), '[]'::jsonb)
        FROM jsonb_array_elements(st.items) AS item
        LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
        WHERE item->>'sku' IS NOT NULL
      )
    ) as odv_data
    FROM public.saga_zoho_links szl
    JOIN public.saga_transactions st ON st.id = szl.id_saga_transaction
    WHERE st.visit_id = p_visit_id
      AND szl.tipo::text = 'BOTIQUIN'
    ORDER BY szl.created_at
  ) sub;

  -- 6. Resumen de movimientos
  SELECT 
    COALESCE(COUNT(*)::int, 0),
    COALESCE(SUM(mi.cantidad)::int, 0),
    COALESCE(COUNT(DISTINCT mi.sku)::int, 0)
  INTO v_mov_total_count, v_mov_total_cantidad, v_mov_unique_skus
  FROM public.movimientos_inventario mi
  JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
  WHERE st.visit_id = p_visit_id;

  SELECT COALESCE(jsonb_object_agg(tipo_text, suma), '{}'::jsonb)
  INTO v_mov_by_tipo
  FROM (
    SELECT mi.tipo::text as tipo_text, SUM(mi.cantidad)::int as suma
    FROM public.movimientos_inventario mi
    JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
    WHERE st.visit_id = p_visit_id
    GROUP BY mi.tipo
  ) sub;

  v_movimientos_resumen := jsonb_build_object(
    'total_movimientos', v_mov_total_count,
    'total_cantidad', v_mov_total_cantidad,
    'unique_skus', v_mov_unique_skus,
    'by_tipo', v_mov_by_tipo
  );

  -- 7. Items de recolección (from CORTE task metadata or movimientos)
  -- First try visit_tasks.metadata from CORTE (filtered to recolectado > 0)
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'producto', COALESCE(m.producto, item->>'sku'),
      'cantidad', COALESCE((item->>'recolectado')::int, 0)
    )
  )
  INTO v_recoleccion_items
  FROM public.visit_tasks vt,
       jsonb_array_elements(vt.metadata->'items') AS item
  LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
  WHERE vt.visit_id = p_visit_id
    AND vt.task_tipo = 'CORTE'
    AND vt.estado = 'COMPLETADO'
    AND vt.metadata->'items' IS NOT NULL
    AND COALESCE((item->>'recolectado')::int, 0) > 0;

  IF v_recoleccion_items IS NOT NULL THEN
    v_total_recoleccion := v_total_recolectado;
  ELSIF v_has_movimientos THEN
    SELECT jsonb_agg(item_data)
    INTO v_recoleccion_items
    FROM (
      SELECT jsonb_build_object(
        'sku', sku,
        'producto', producto,
        'cantidad', cantidad
      ) as item_data
      FROM (
        SELECT 
          mi.sku,
          COALESCE(m.producto, mi.sku) as producto,
          SUM(mi.cantidad)::int as cantidad
        FROM public.movimientos_inventario mi
        JOIN public.saga_transactions st ON st.id = mi.id_saga_transaction
        LEFT JOIN public.medicamentos m ON m.sku = mi.sku
        WHERE st.visit_id = p_visit_id
          AND mi.tipo::text = 'RECOLECCION'
        GROUP BY mi.sku, m.producto
        HAVING SUM(mi.cantidad) > 0
      ) grouped
    ) items;
    
    v_total_recoleccion := v_total_recolectado;
  ELSE
    SELECT jsonb_agg(
      jsonb_build_object(
        'sku', item->>'sku',
        'producto', COALESCE(m.producto, item->>'sku'),
        'cantidad', COALESCE(
          (item->>'cantidad_salida')::int, 
          (item->>'cantidad')::int, 
          0
        )
      )
    )
    INTO v_recoleccion_items
    FROM public.saga_transactions st,
         jsonb_array_elements(st.items) AS item
    LEFT JOIN public.medicamentos m ON m.sku = item->>'sku'
    WHERE st.visit_id = p_visit_id
      AND st.tipo::text = 'RECOLECCION'
      AND st.items IS NOT NULL
      AND COALESCE((item->>'cantidad_salida')::int, (item->>'cantidad')::int, 0) > 0;
    
    v_total_recoleccion := v_total_recolectado;
  END IF;

  RETURN jsonb_build_object(
    'visit_id', p_visit_id,
    'visit_tipo', v_visit_tipo,
    'id_cliente', v_id_cliente,
    'corte', jsonb_build_object(
      'items', COALESCE(v_corte_items, '[]'::jsonb),
      'total_vendido', COALESCE(v_total_vendido, 0),
      'total_recolectado', COALESCE(v_total_recolectado, 0)
    ),
    'levantamiento', jsonb_build_object(
      'items', COALESCE(v_levantamiento_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_levantamiento, 0)
    ),
    'lev_post_corte', jsonb_build_object(
      'items', COALESCE(v_lev_post_corte_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_lev_post_corte, 0)
    ),
    'recoleccion', jsonb_build_object(
      'items', COALESCE(v_recoleccion_items, '[]'::jsonb),
      'total_piezas', COALESCE(v_total_recoleccion, 0)
    ),
    'odvs', jsonb_build_object(
      'venta', COALESCE(v_odvs_venta, '[]'::jsonb),
      'botiquin', COALESCE(v_odvs_botiquin, '[]'::jsonb),
      'all', COALESCE(v_odvs_venta, '[]'::jsonb) || COALESCE(v_odvs_botiquin, '[]'::jsonb)
    ),
    'movimientos', v_movimientos_resumen
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_marcar_notificacion_leida(p_notificacion_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_user_id VARCHAR;
BEGIN
  -- Obtener usuario actual
  SELECT id_usuario INTO v_user_id
  FROM public.usuarios
  WHERE auth_user_id = auth.uid()
  LIMIT 1;

  UPDATE public.notificaciones_admin
  SET 
    leida = true,
    leida_at = now(),
    leida_por = v_user_id
  WHERE id = p_notificacion_id
  AND leida = false;

  RETURN FOUND;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_mark_all_notifications_read(p_user_id character varying)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE notifications
    SET read_at = NOW()
    WHERE user_id = p_user_id
      AND read_at IS NULL;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_mark_notification_read(p_notification_id uuid, p_user_id character varying)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
    UPDATE notifications
    SET read_at = NOW()
    WHERE id = p_notification_id
      AND user_id = p_user_id
      AND read_at IS NULL;

    RETURN FOUND;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_dev_legacy()
 RETURNS TABLE(visitas_created integer, tasks_created integer, sagas_updated integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_updated integer := 0;
  v_row_count integer := 0;
  v_saga record;
  v_visit_id uuid;
  v_existing_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Migrar CORTE_RENOVACION → VISITA_CORTE
  -- =====================================================
  -- CORTE_RENOVACION representa un ciclo de corte completo

  FOR v_saga IN
    SELECT DISTINCT ON (st.id_cliente, st.id_ciclo)
      st.id as saga_id,
      st.id_cliente,
      st.id_usuario,
      st.id_ciclo,
      st.estado,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo = 'CORTE_RENOVACION'
      AND st.visit_id IS NULL
    ORDER BY st.id_cliente, st.id_ciclo, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de corte
      INSERT INTO public.visitas (
        visit_id, id_cliente, id_usuario, id_ciclo, tipo, estado,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.id_cliente,
        v_saga.id_usuario,
        v_saga.id_ciclo,
        'VISITA_CORTE'::public.visit_tipo,
        'COMPLETADO'::public.visit_estado,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'legacy_tipo', 'CORTE_RENOVACION', 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear todas las tareas para visita de corte
      -- CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'CORTE'::public.visit_task_tipo,
        'COMPLETADO'::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- VENTA_ODV
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'VENTA_ODV'::public.visit_task_tipo,
        'COMPLETADO'::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- RECOLECCION
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'RECOLECCION'::public.visit_task_tipo,
        'COMPLETADO'::public.visit_task_estado,
        false,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- LEV_POST_CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'LEV_POST_CORTE'::public.visit_task_tipo,
        'COMPLETADO'::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
      )
      VALUES (
        v_visit_id,
        'INFORME_VISITA'::public.visit_task_tipo,
        'COMPLETADO'::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga con visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Migrar VENTA → Enlazar a VISITA_CORTE existente
  -- =====================================================

  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.id_cliente,
      st.id_ciclo,
      st.estado,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo = 'VENTA'
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga con visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar tarea VENTA_ODV con referencia
      UPDATE public.visit_tasks
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga.saga_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id
        AND task_tipo = 'VENTA_ODV'
        AND reference_id IS NULL;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 3: Migrar RECOLECCION (saga) → Enlazar a VISITA_CORTE
  -- =====================================================

  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.id_cliente,
      st.id_ciclo,
      st.estado,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo = 'RECOLECCION'
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga con visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar tarea RECOLECCION con referencia
      UPDATE public.visit_tasks
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga.saga_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id
        AND task_tipo = 'RECOLECCION'
        AND reference_id IS NULL;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 4: Enlazar sagas restantes a visitas existentes
  -- =====================================================

  -- Actualizar cualquier saga sin visit_id que tenga un id_ciclo coincidente
  UPDATE public.saga_transactions st
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visitas v
      WHERE v.id_cliente = st.id_cliente
        AND v.id_ciclo = st.id_ciclo
      LIMIT 1
    ),
    updated_at = now()
  WHERE st.visit_id IS NULL
    AND st.id_ciclo IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visitas v
      WHERE v.id_cliente = st.id_cliente AND v.id_ciclo = st.id_ciclo
    );

  GET DIAGNOSTICS v_row_count = ROW_COUNT;
  v_sagas_updated := v_sagas_updated + v_row_count;

  -- =====================================================
  -- PASO 5: Enlazar recolecciones (tabla) sin visit_id
  -- =====================================================

  UPDATE public.recolecciones r
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visitas v
      WHERE v.id_cliente = r.id_cliente
        AND v.id_ciclo = r.id_ciclo
        AND v.tipo = 'VISITA_CORTE'
      LIMIT 1
    ),
    updated_at = now()
  WHERE r.visit_id IS NULL
    AND r.id_ciclo IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visitas v
      WHERE v.id_cliente = r.id_cliente
        AND v.id_ciclo = r.id_ciclo
        AND v.tipo = 'VISITA_CORTE'
    );

  RETURN QUERY SELECT v_visitas_created, v_tasks_created, v_sagas_updated;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_full_history()
 RETURNS TABLE(ciclos_processed integer, visitas_created integer, tasks_created integer, sagas_linked integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_ciclos_processed integer := 0;
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_linked integer := 0;
  v_row_count integer := 0;
  v_ciclo record;
  v_visit_id uuid;
  v_visit_tipo public.visit_tipo;
  v_visit_estado public.visit_estado;
  v_existing_visit_id uuid;
  v_saga_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Crear visitas desde migration.ciclos_botiquin
  -- =====================================================

  FOR v_ciclo IN
    SELECT
      cb.id_ciclo,
      cb.id_cliente,
      cb.id_usuario,
      cb.tipo,
      cb.fecha_creacion,
      cb.id_ciclo_anterior
    FROM migration.ciclos_botiquin cb
    ORDER BY cb.fecha_creacion ASC
  LOOP
    v_ciclos_processed := v_ciclos_processed + 1;

    -- Determinar tipo de visita
    IF v_ciclo.tipo::text = 'LEVANTAMIENTO' THEN
      v_visit_tipo := 'VISITA_LEVANTAMIENTO_INICIAL'::public.visit_tipo;
    ELSE
      v_visit_tipo := 'VISITA_CORTE'::public.visit_tipo;
    END IF;

    -- Verificar si ya existe una visita para este ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_ciclo = v_ciclo.id_ciclo
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- La visita se considera COMPLETADA porque viene del historial
      v_visit_estado := 'COMPLETADO'::public.visit_estado;

      -- Crear nueva visita
      INSERT INTO public.visitas (
        visit_id, id_cliente, id_usuario, id_ciclo, tipo, estado,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_ciclo.id_cliente,
        v_ciclo.id_usuario,
        v_ciclo.id_ciclo,
        v_visit_tipo,
        v_visit_estado,
        v_ciclo.fecha_creacion,
        v_ciclo.fecha_creacion,
        v_ciclo.fecha_creacion,
        v_ciclo.fecha_creacion,
        jsonb_build_object(
          'migrated_from_legacy', true,
          'migration_date', now(),
          'source_ciclo_id', v_ciclo.id_ciclo,
          'id_ciclo_anterior', v_ciclo.id_ciclo_anterior
        )
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas según el tipo de visita
      IF v_visit_tipo = 'VISITA_LEVANTAMIENTO_INICIAL' THEN
        -- LEVANTAMIENTO_INICIAL
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'LEVANTAMIENTO_INICIAL'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- ODV_BOTIQUIN
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'ODV_BOTIQUIN'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- INFORME_VISITA
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'INFORME_VISITA'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

      ELSE -- VISITA_CORTE
        -- CORTE
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'CORTE'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- VENTA_ODV
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'VENTA_ODV'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- RECOLECCION
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'RECOLECCION'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- LEV_POST_CORTE
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'LEV_POST_CORTE'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;

        -- INFORME_VISITA
        INSERT INTO public.visit_tasks (
          visit_id, task_tipo, estado, required, created_at, started_at, completed_at, metadata
        ) VALUES (
          v_visit_id,
          'INFORME_VISITA'::public.visit_task_tipo,
          'COMPLETADO'::public.visit_task_estado,
          true,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          v_ciclo.fecha_creacion,
          '{}'::jsonb
        );
        v_tasks_created := v_tasks_created + 1;
      END IF;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- =====================================================
    -- PASO 2: Enlazar saga_transactions al visit_id
    -- =====================================================

    -- Buscar saga_transactions que coincidan con este ciclo
    UPDATE public.saga_transactions st
    SET
      visit_id = v_visit_id,
      updated_at = now()
    WHERE st.id_ciclo = v_ciclo.id_ciclo
      AND st.visit_id IS NULL;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_sagas_linked := v_sagas_linked + v_row_count;

    -- También intentar enlazar por id_cliente y fecha aproximada si no tiene id_ciclo
    UPDATE public.saga_transactions st
    SET
      visit_id = v_visit_id,
      id_ciclo = v_ciclo.id_ciclo,
      updated_at = now()
    WHERE st.id_cliente = v_ciclo.id_cliente
      AND st.visit_id IS NULL
      AND st.id_ciclo IS NULL
      AND st.created_at >= v_ciclo.fecha_creacion - interval '1 day'
      AND st.created_at <= v_ciclo.fecha_creacion + interval '1 day'
      AND (
        (st.tipo::text = 'LEVANTAMIENTO_INICIAL' AND v_ciclo.tipo::text = 'LEVANTAMIENTO')
        OR (st.tipo::text IN ('CORTE', 'LEV_POST_CORTE', 'VENTA_ODV') AND v_ciclo.tipo::text = 'CORTE')
      );

    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    v_sagas_linked := v_sagas_linked + v_row_count;

    -- Actualizar las tareas con referencia a saga_transactions si existe
    FOR v_saga_id IN
      SELECT st.id FROM public.saga_transactions st
      WHERE st.visit_id = v_visit_id
    LOOP
      -- Actualizar tarea correspondiente según tipo de saga
      UPDATE public.visit_tasks vt
      SET
        reference_table = 'saga_transactions',
        reference_id = v_saga_id::text,
        last_activity_at = now()
      WHERE vt.visit_id = v_visit_id
        AND vt.reference_id IS NULL
        AND (
          (vt.task_tipo::text = 'LEVANTAMIENTO_INICIAL' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.tipo::text = 'LEVANTAMIENTO_INICIAL'
          ))
          OR (vt.task_tipo::text = 'CORTE' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.tipo::text = 'CORTE'
          ))
          OR (vt.task_tipo::text = 'LEV_POST_CORTE' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.tipo::text = 'LEV_POST_CORTE'
          ))
          OR (vt.task_tipo::text = 'VENTA_ODV' AND EXISTS (
            SELECT 1 FROM public.saga_transactions st WHERE st.id = v_saga_id AND st.tipo::text = 'VENTA_ODV'
          ))
        );
    END LOOP;

  END LOOP;

  -- =====================================================
  -- PASO 3: Enlazar saga_transactions restantes sin visit_id
  -- =====================================================

  -- Para sagas que aún no tienen visit_id, buscar visita por cliente y fecha
  UPDATE public.saga_transactions st
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visitas v
      WHERE v.id_cliente = st.id_cliente
        AND v.id_ciclo = st.id_ciclo
      LIMIT 1
    ),
    updated_at = now()
  WHERE st.visit_id IS NULL
    AND st.id_ciclo IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visitas v
      WHERE v.id_cliente = st.id_cliente AND v.id_ciclo = st.id_ciclo
    );

  GET DIAGNOSTICS v_row_count = ROW_COUNT;
  v_sagas_linked := v_sagas_linked + v_row_count;

  -- =====================================================
  -- PASO 4: Enlazar recolecciones sin visit_id
  -- =====================================================

  UPDATE public.recolecciones r
  SET
    visit_id = (
      SELECT v.visit_id
      FROM public.visitas v
      WHERE v.id_cliente = r.id_cliente
        AND v.id_ciclo = r.id_ciclo
        AND v.tipo = 'VISITA_CORTE'
      LIMIT 1
    ),
    updated_at = now()
  WHERE r.visit_id IS NULL
    AND r.id_ciclo IS NOT NULL
    AND EXISTS (
      SELECT 1 FROM public.visitas v
      WHERE v.id_cliente = r.id_cliente
        AND v.id_ciclo = r.id_ciclo
        AND v.tipo = 'VISITA_CORTE'
    );

  RETURN QUERY SELECT v_ciclos_processed, v_visitas_created, v_tasks_created, v_sagas_linked;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_migrate_legacy_sagas()
 RETURNS TABLE(visitas_created integer, tasks_created integer, sagas_updated integer, recolecciones_updated integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visitas_created integer := 0;
  v_tasks_created integer := 0;
  v_sagas_updated integer := 0;
  v_recolecciones_updated integer := 0;
  v_saga record;
  v_visit_id uuid;
  v_visit_tipo public.visit_tipo;
  v_existing_visit_id uuid;
BEGIN
  -- =====================================================
  -- PASO 1: Migrar LEVANTAMIENTO_INICIAL sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT DISTINCT ON (st.id_cliente, st.id_ciclo)
      st.id as saga_id,
      st.id_cliente,
      st.id_usuario,
      st.id_ciclo,
      st.estado,
      st.items,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo = 'LEVANTAMIENTO_INICIAL'
      AND st.visit_id IS NULL
    ORDER BY st.id_cliente, st.id_ciclo, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_LEVANTAMIENTO_INICIAL'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de levantamiento inicial
      INSERT INTO public.visitas (
        visit_id, id_cliente, id_usuario, id_ciclo, tipo, estado,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.id_cliente,
        v_saga.id_usuario,
        v_saga.id_ciclo,
        'VISITA_LEVANTAMIENTO_INICIAL'::public.visit_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'EN_CURSO' END)::public.visit_estado,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE NULL END,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas para levantamiento inicial
      -- LEVANTAMIENTO_INICIAL
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'LEVANTAMIENTO_INICIAL'::public.visit_task_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'EN_CURSO' END)::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE NULL END,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- ODV_BOTIQUIN
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'ODV_BOTIQUIN'::public.visit_task_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'PENDIENTE' END)::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'INFORME_VISITA'::public.visit_task_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'PENDIENTE' END)::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga_transactions con el visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 2: Migrar CORTE sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT DISTINCT ON (st.id_cliente, st.id_ciclo)
      st.id as saga_id,
      st.id_cliente,
      st.id_usuario,
      st.id_ciclo,
      st.estado,
      st.items,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo = 'CORTE'
      AND st.visit_id IS NULL
    ORDER BY st.id_cliente, st.id_ciclo, st.created_at ASC
  LOOP
    -- Verificar si ya existe una visita de corte para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    LIMIT 1;

    IF v_existing_visit_id IS NULL THEN
      -- Crear nueva visita de corte
      INSERT INTO public.visitas (
        visit_id, id_cliente, id_usuario, id_ciclo, tipo, estado,
        created_at, started_at, completed_at, last_activity_at, metadata
      )
      VALUES (
        gen_random_uuid(),
        v_saga.id_cliente,
        v_saga.id_usuario,
        v_saga.id_ciclo,
        'VISITA_CORTE'::public.visit_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'EN_CURSO' END)::public.visit_estado,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE NULL END,
        v_saga.created_at,
        jsonb_build_object('migrated_from_legacy', true, 'migration_date', now())
      )
      RETURNING visit_id INTO v_visit_id;

      v_visitas_created := v_visitas_created + 1;

      -- Crear tareas para visita de corte
      -- CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, started_at, completed_at,
        reference_table, reference_id, metadata
      )
      VALUES (
        v_visit_id,
        'CORTE'::public.visit_task_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'EN_CURSO' END)::public.visit_task_estado,
        true,
        v_saga.created_at,
        v_saga.created_at,
        CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE NULL END,
        'saga_transactions',
        v_saga.saga_id::text,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- VENTA_ODV
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'VENTA_ODV'::public.visit_task_tipo,
        (CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE 'PENDIENTE' END)::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- RECOLECCION
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'RECOLECCION'::public.visit_task_tipo,
        'PENDIENTE'::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- LEV_POST_CORTE
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'LEV_POST_CORTE'::public.visit_task_tipo,
        'PENDIENTE'::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

      -- INFORME_VISITA
      INSERT INTO public.visit_tasks (
        visit_id, task_tipo, estado, required, created_at, metadata
      )
      VALUES (
        v_visit_id,
        'INFORME_VISITA'::public.visit_task_tipo,
        'PENDIENTE'::public.visit_task_estado,
        true,
        v_saga.created_at,
        '{}'::jsonb
      );
      v_tasks_created := v_tasks_created + 1;

    ELSE
      v_visit_id := v_existing_visit_id;
    END IF;

    -- Actualizar saga_transactions con el visit_id
    UPDATE public.saga_transactions
    SET visit_id = v_visit_id, updated_at = now()
    WHERE id = v_saga.saga_id;

    v_sagas_updated := v_sagas_updated + 1;
  END LOOP;

  -- =====================================================
  -- PASO 3: Migrar LEV_POST_CORTE y VENTA_ODV sin visit_id
  -- (Estos deben asociarse a una VISITA_CORTE existente)
  -- =====================================================
  FOR v_saga IN
    SELECT
      st.id as saga_id,
      st.id_cliente,
      st.id_usuario,
      st.id_ciclo,
      st.tipo,
      st.estado,
      st.created_at
    FROM public.saga_transactions st
    WHERE st.tipo IN ('LEV_POST_CORTE', 'VENTA_ODV')
      AND st.visit_id IS NULL
    ORDER BY st.created_at ASC
  LOOP
    -- Buscar visita de corte existente para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar saga_transactions con el visit_id
      UPDATE public.saga_transactions
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE id = v_saga.saga_id;

      v_sagas_updated := v_sagas_updated + 1;

      -- Actualizar la tarea correspondiente si existe
      IF v_saga.tipo::text = 'LEV_POST_CORTE' THEN
        UPDATE public.visit_tasks
        SET
          estado = CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE estado END,
          completed_at = CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE completed_at END,
          reference_table = 'saga_transactions',
          reference_id = v_saga.saga_id::text,
          last_activity_at = now()
        WHERE visit_id = v_existing_visit_id AND task_tipo = 'LEV_POST_CORTE';
      ELSIF v_saga.tipo::text = 'VENTA_ODV' THEN
        UPDATE public.visit_tasks
        SET
          estado = CASE WHEN v_saga.estado = 'CONFIRMADO' THEN 'COMPLETADO' ELSE estado END,
          completed_at = CASE WHEN v_saga.estado = 'CONFIRMADO' THEN v_saga.created_at ELSE completed_at END,
          reference_table = 'saga_transactions',
          reference_id = v_saga.saga_id::text,
          last_activity_at = now()
        WHERE visit_id = v_existing_visit_id AND task_tipo = 'VENTA_ODV';
      END IF;
    END IF;
  END LOOP;

  -- =====================================================
  -- PASO 4: Migrar recolecciones sin visit_id
  -- =====================================================
  FOR v_saga IN
    SELECT
      r.recoleccion_id,
      r.id_cliente,
      r.id_usuario,
      r.id_ciclo,
      r.estado,
      r.created_at
    FROM public.recolecciones r
    WHERE r.visit_id IS NULL
    ORDER BY r.created_at ASC
  LOOP
    -- Buscar visita de corte existente para este cliente/ciclo
    SELECT v.visit_id INTO v_existing_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_saga.id_cliente
      AND (v.id_ciclo = v_saga.id_ciclo OR (v.id_ciclo IS NULL AND v_saga.id_ciclo IS NULL))
      AND v.tipo = 'VISITA_CORTE'
    ORDER BY v.created_at DESC
    LIMIT 1;

    IF v_existing_visit_id IS NOT NULL THEN
      -- Actualizar recolección con el visit_id
      UPDATE public.recolecciones
      SET visit_id = v_existing_visit_id, updated_at = now()
      WHERE recoleccion_id = v_saga.recoleccion_id;

      v_recolecciones_updated := v_recolecciones_updated + 1;

      -- Actualizar la tarea de recolección
      UPDATE public.visit_tasks
      SET
        estado = CASE WHEN v_saga.estado = 'ENTREGADA' THEN 'COMPLETADO' ELSE estado END,
        completed_at = CASE WHEN v_saga.estado = 'ENTREGADA' THEN v_saga.created_at ELSE completed_at END,
        reference_table = 'recolecciones',
        reference_id = v_saga.recoleccion_id::text,
        last_activity_at = now()
      WHERE visit_id = v_existing_visit_id AND task_tipo = 'RECOLECCION';
    END IF;
  END LOOP;

  RETURN QUERY SELECT v_visitas_created, v_tasks_created, v_sagas_updated, v_recolecciones_updated;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_owner_delete_visit(p_visit_id uuid, p_user_id text, p_reason text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
DECLARE
  v_user_rol text;
  v_visit_estado text;
  v_id_cliente text;
  v_deleted_counts jsonb;
  v_count_visitas int := 0;
  v_count_tasks int := 0;
  v_count_sagas int := 0;
  v_count_task_odvs int := 0;
  v_count_visita_odvs int := 0;
  v_count_movimientos int := 0;
  v_count_recolecciones int := 0;
  v_count_recolecciones_items int := 0;
  v_count_recolecciones_firmas int := 0;
  v_count_recolecciones_evidencias int := 0;
  v_count_informes int := 0;
  v_count_compensation_log int := 0;
  v_count_inventario_restored int := 0;
  v_has_task_id boolean := false;
  v_has_saga_comp_log boolean := false;
  v_has_task_odvs boolean := false;
  -- Variables para restaurar inventario
  v_last_completed_visit_id uuid;
  v_lev_post_corte_items jsonb;
  v_current_visit_had_lev_post_corte boolean := false;
  v_restore_source text := NULL;
BEGIN
  -- Verificar rol OWNER
  SELECT u.rol::text
  INTO v_user_rol
  FROM public.usuarios u
  WHERE u.id_usuario = p_user_id;

  IF v_user_rol IS NULL OR v_user_rol != 'OWNER' THEN
    RAISE EXCEPTION 'Solo usuarios OWNER pueden eliminar visitas permanentemente';
  END IF;

  -- Obtener info de la visita
  SELECT v.estado::text, v.id_cliente
  INTO v_visit_estado, v_id_cliente
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_visit_estado IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada: %', p_visit_id;
  END IF;

  -- Solo permitir eliminar visitas CANCELADAS
  IF v_visit_estado != 'CANCELADO' THEN
    RAISE EXCEPTION 'Solo se pueden eliminar visitas canceladas. Estado actual: %', v_visit_estado;
  END IF;

  -- Detectar qué esquema tenemos disponible
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'movimientos_inventario'
    AND column_name = 'task_id'
  ) INTO v_has_task_id;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name = 'saga_compensation_log'
  ) INTO v_has_saga_comp_log;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name = 'task_odvs'
  ) INTO v_has_task_odvs;

  -- ============ VERIFICAR SI ESTA VISITA MODIFICÓ EL INVENTARIO ============
  -- Verificar PRIMERO en visit_tasks (normal)
  -- Si no encuentra, verificar en saga_transactions (por si rollback ya borró tasks)
  SELECT EXISTS (
    SELECT 1 FROM public.visit_tasks vt
    WHERE vt.visit_id = p_visit_id
      AND vt.task_tipo = 'LEV_POST_CORTE'
      AND vt.estado IN ('COMPLETADO', 'OMITIDO', 'OMITIDA')
  ) OR EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
      AND st.tipo = 'LEV_POST_CORTE'
      AND st.estado = 'CONFIRMADO'
  ) INTO v_current_visit_had_lev_post_corte;

  -- ============ BUSCAR ÚLTIMA VISITA COMPLETADA DEL CLIENTE ============
  IF v_current_visit_had_lev_post_corte THEN
    SELECT v.visit_id
    INTO v_last_completed_visit_id
    FROM public.visitas v
    WHERE v.id_cliente = v_id_cliente
      AND v.visit_id != p_visit_id
      AND v.estado = 'COMPLETADO'
      AND v.tipo IN ('VISITA_CORTE', 'VISITA_LEVANTAMIENTO_INICIAL')
    ORDER BY v.completed_at DESC NULLS LAST, v.created_at DESC
    LIMIT 1;

    IF v_last_completed_visit_id IS NOT NULL THEN
      SELECT st.items
      INTO v_lev_post_corte_items
      FROM public.saga_transactions st
      WHERE st.visit_id = v_last_completed_visit_id
        AND st.tipo = 'LEV_POST_CORTE'
        AND st.estado = 'CONFIRMADO'
      ORDER BY st.created_at DESC
      LIMIT 1;

      IF v_lev_post_corte_items IS NOT NULL THEN
        v_restore_source := 'LEV_POST_CORTE de visita ' || v_last_completed_visit_id::text;
      ELSE
        SELECT st.items
        INTO v_lev_post_corte_items
        FROM public.saga_transactions st
        WHERE st.visit_id = v_last_completed_visit_id
          AND st.tipo = 'LEVANTAMIENTO_INICIAL'
          AND st.estado = 'CONFIRMADO'
        ORDER BY st.created_at DESC
        LIMIT 1;

        IF v_lev_post_corte_items IS NOT NULL THEN
          v_restore_source := 'LEVANTAMIENTO_INICIAL de visita ' || v_last_completed_visit_id::text;
        END IF;
      END IF;
    END IF;

    -- ============ RESTAURAR INVENTARIO ============
    IF v_lev_post_corte_items IS NOT NULL AND jsonb_array_length(v_lev_post_corte_items) > 0 THEN
      DELETE FROM public.inventario_botiquin WHERE id_cliente = v_id_cliente;

      INSERT INTO public.inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      SELECT
        v_id_cliente,
        (item->>'sku')::text,
        (item->>'cantidad')::integer,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0;

      GET DIAGNOSTICS v_count_inventario_restored = ROW_COUNT;

      INSERT INTO public.botiquin_clientes_sku_disponibles (id_cliente, sku, fecha_ingreso)
      SELECT DISTINCT
        v_id_cliente,
        (item->>'sku')::text,
        NOW()
      FROM jsonb_array_elements(v_lev_post_corte_items) AS item
      WHERE (item->>'cantidad')::integer > 0
      ON CONFLICT (id_cliente, sku) DO NOTHING;
    ELSE
      v_restore_source := 'Sin visita completada anterior - inventario no modificado';
    END IF;
  END IF;

  -- Eliminar en orden para respetar foreign keys

  IF v_has_saga_comp_log THEN
    EXECUTE 'DELETE FROM public.saga_compensation_log WHERE visit_id = $1' USING p_visit_id;
    GET DIAGNOSTICS v_count_compensation_log = ROW_COUNT;
  END IF;

  IF v_has_task_id THEN
    DELETE FROM public.movimientos_inventario
    WHERE task_id IN (SELECT task_id FROM public.visit_tasks WHERE visit_id = p_visit_id);
    GET DIAGNOSTICS v_count_movimientos = ROW_COUNT;
  ELSE
    DELETE FROM public.movimientos_inventario
    WHERE id_saga_transaction IN (SELECT id FROM public.saga_transactions WHERE visit_id = p_visit_id);
    GET DIAGNOSTICS v_count_movimientos = ROW_COUNT;
  END IF;

  IF v_has_task_odvs THEN
    EXECUTE 'DELETE FROM public.task_odvs WHERE task_id IN (SELECT task_id FROM public.visit_tasks WHERE visit_id = $1)' USING p_visit_id;
    GET DIAGNOSTICS v_count_task_odvs = ROW_COUNT;
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'visita_odvs') THEN
    EXECUTE 'DELETE FROM public.visita_odvs WHERE visit_id = $1' USING p_visit_id;
    GET DIAGNOSTICS v_count_visita_odvs = ROW_COUNT;
  END IF;

  DELETE FROM public.recolecciones_evidencias
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.recolecciones WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_evidencias = ROW_COUNT;

  DELETE FROM public.recolecciones_firmas
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.recolecciones WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_firmas = ROW_COUNT;

  DELETE FROM public.recolecciones_items
  WHERE recoleccion_id IN (SELECT recoleccion_id FROM public.recolecciones WHERE visit_id = p_visit_id);
  GET DIAGNOSTICS v_count_recolecciones_items = ROW_COUNT;

  DELETE FROM public.recolecciones
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_recolecciones = ROW_COUNT;

  DELETE FROM public.visita_informes
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_informes = ROW_COUNT;

  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'saga_transactions') THEN
    DELETE FROM public.saga_transactions
    WHERE visit_id = p_visit_id;
    GET DIAGNOSTICS v_count_sagas = ROW_COUNT;
  END IF;

  DELETE FROM public.visit_tasks
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_tasks = ROW_COUNT;

  DELETE FROM public.visitas
  WHERE visit_id = p_visit_id;
  GET DIAGNOSTICS v_count_visitas = ROW_COUNT;

  v_deleted_counts := jsonb_build_object(
    'visitas', v_count_visitas,
    'visit_tasks', v_count_tasks,
    'saga_transactions', v_count_sagas,
    'task_odvs', v_count_task_odvs,
    'visita_odvs', v_count_visita_odvs,
    'movimientos_inventario', v_count_movimientos,
    'recolecciones', v_count_recolecciones,
    'recolecciones_items', v_count_recolecciones_items,
    'recolecciones_firmas', v_count_recolecciones_firmas,
    'recolecciones_evidencias', v_count_recolecciones_evidencias,
    'visita_informes', v_count_informes,
    'saga_compensation_log', v_count_compensation_log,
    'inventario_restored', v_count_inventario_restored
  );

  RETURN jsonb_build_object(
    'success', true,
    'visit_id', p_visit_id,
    'deleted_counts', v_deleted_counts,
    'reason', p_reason,
    'inventory_reverted', v_current_visit_had_lev_post_corte AND v_lev_post_corte_items IS NOT NULL,
    'restore_source', v_restore_source,
    'last_completed_visit_id', v_last_completed_visit_id
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_set_manual_botiquin_odv_id(p_visit_id uuid, p_zoho_odv_id text, p_task_tipo text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_cliente varchar;
  v_id_usuario varchar;
  v_saga_id uuid;
  v_result jsonb;
BEGIN
  -- Validar task_tipo
  IF p_task_tipo NOT IN ('LEVANTAMIENTO_INICIAL', 'LEV_POST_CORTE') THEN
    RAISE EXCEPTION 'task_tipo debe ser LEVANTAMIENTO_INICIAL o LEV_POST_CORTE';
  END IF;

  -- Obtener datos de la visita
  SELECT v.id_cliente, v.id_usuario
  INTO v_id_cliente, v_id_usuario
  FROM visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Buscar saga existente del tipo especificado
  SELECT id INTO v_saga_id
  FROM saga_transactions
  WHERE visit_id = p_visit_id AND tipo::text = p_task_tipo
  ORDER BY created_at DESC LIMIT 1;

  IF v_saga_id IS NULL THEN
    -- Crear saga nueva (sin items, se asume que vienen de levantamiento)
    INSERT INTO saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      p_task_tipo::tipo_saga_transaction,
      'BORRADOR'::estado_saga_transaction,
      v_id_cliente,
      v_id_usuario,
      '[]'::jsonb,
      jsonb_build_object('manual_odv', true, 'zoho_odv_id', p_zoho_odv_id),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  SELECT rpc_confirm_saga_pivot(v_saga_id, p_zoho_odv_id, NULL) INTO v_result;

  -- Actualizar tarea ODV_BOTIQUIN
  UPDATE visit_tasks
  SET
    estado = 'COMPLETADO'::visit_task_estado,
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'saga_id', v_saga_id,
      'zoho_odv_id', p_zoho_odv_id,
      'manual_odv', true,
      'saga_tipo', p_task_tipo
    )
  WHERE visit_id = p_visit_id 
  AND task_tipo = 'ODV_BOTIQUIN'::visit_task_tipo;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_set_manual_odv_id(p_visit_id uuid, p_zoho_odv_id text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_saga_id uuid;
  v_id_cliente varchar;
  v_id_usuario varchar;
  v_items jsonb;
  v_odv_id text;
  v_result jsonb;
BEGIN
  IF p_zoho_odv_id IS NULL THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Normalizar el formato del ODV ID
  IF p_zoho_odv_id ~ '^[0-9]{1,5}$' THEN
    v_odv_id := 'DCOdV-' || p_zoho_odv_id;
  ELSE
    v_odv_id := p_zoho_odv_id;
  END IF;

  IF v_odv_id !~ '^DCOdV-[0-9]{1,5}$' THEN
    RAISE EXCEPTION 'Formato inválido para zoho_odv_id';
  END IF;

  -- Buscar saga existente de tipo VENTA
  SELECT st.id INTO v_saga_id
  FROM public.saga_transactions st
  WHERE st.visit_id = p_visit_id AND st.tipo::text = 'VENTA'
  ORDER BY st.created_at DESC
  LIMIT 1;

  IF v_saga_id IS NULL THEN
    -- Obtener datos de la visita
    SELECT v.id_cliente, v.id_usuario
    INTO v_id_cliente, v_id_usuario
    FROM public.visitas v
    WHERE v.visit_id = p_visit_id;

    IF v_id_cliente IS NULL THEN
      RAISE EXCEPTION 'Visita no encontrada';
    END IF;

    -- Obtener items del corte (saga VENTA que se creó en submit_corte)
    -- o de la metadata de la tarea CORTE
    SELECT COALESCE(
      (SELECT st.items FROM saga_transactions st 
       WHERE st.visit_id = p_visit_id AND st.tipo::text = 'VENTA' 
       ORDER BY created_at DESC LIMIT 1),
      '[]'::jsonb
    ) INTO v_items;

    -- Si no hay items, intentar extraer del corte
    IF v_items = '[]'::jsonb THEN
      SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'sku', item->>'sku',
            'cantidad', (item->>'vendido')::int
          )
        ),
        '[]'::jsonb
      )
      INTO v_items
      FROM (
        SELECT item
        FROM saga_transactions st,
        LATERAL jsonb_array_elements(st.items) AS item
        WHERE st.visit_id = p_visit_id 
        AND st.tipo::text = 'VENTA'
        AND COALESCE((item->>'vendido')::int, (item->>'cantidad')::int, 0) > 0
        ORDER BY st.created_at DESC
        LIMIT 100
      ) sub;
    END IF;

    -- Crear nueva saga de tipo VENTA
    INSERT INTO public.saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'VENTA'::tipo_saga_transaction,
      'BORRADOR'::estado_saga_transaction,  -- Empezar en BORRADOR
      v_id_cliente,
      v_id_usuario,
      v_items,
      jsonb_build_object(
        'zoho_required', true,
        'zoho_manual', true,
        'zoho_odv_id', v_odv_id
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- LLAMAR A rpc_confirm_saga_pivot para crear movimientos e inventario
  SELECT rpc_confirm_saga_pivot(v_saga_id, v_odv_id, NULL) INTO v_result;

  -- Actualizar tarea VENTA_ODV
  UPDATE public.visit_tasks
  SET 
    estado = 'COMPLETADO', 
    completed_at = now(), 
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'saga_id', v_saga_id,
      'zoho_odv_id', v_odv_id,
      'manual_odv', true
    )
  WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_skip_recoleccion(p_visit_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Verificar visita existe
  IF NOT EXISTS (SELECT 1 FROM visitas WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar tarea existe y está pendiente
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_tipo = 'RECOLECCION'
    AND estado NOT IN ('COMPLETADO', 'OMITIDA', 'OMITIDO')
  ) THEN
    RAISE EXCEPTION 'Tarea RECOLECCION no encontrada o ya completada';
  END IF;

  -- Marcar como OMITIDA
  UPDATE visit_tasks
  SET
    estado = 'OMITIDA',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'skipped', true,
      'skipped_at', now()
    )
  WHERE visit_id = p_visit_id AND task_tipo = 'RECOLECCION';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_skip_venta_odv(p_visit_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Verificar visita existe
  IF NOT EXISTS (SELECT 1 FROM visitas WHERE visit_id = p_visit_id) THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar tarea existe y está pendiente
  IF NOT EXISTS (
    SELECT 1 FROM visit_tasks
    WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV'
    AND estado NOT IN ('COMPLETADO', 'OMITIDA', 'OMITIDO')
  ) THEN
    RAISE EXCEPTION 'Tarea VENTA_ODV no encontrada o ya completada';
  END IF;

  -- Marcar como OMITIDA
  UPDATE visit_tasks
  SET
    estado = 'OMITIDA',
    completed_at = now(),
    last_activity_at = now(),
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
      'skipped', true,
      'skipped_at', now()
    )
  WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV';
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_start_task(p_visit_id uuid, p_task public.visit_task_tipo)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Actualizar la tarea a EN_CURSO
  UPDATE public.visit_tasks
  SET
    estado = 'EN_CURSO',
    started_at = COALESCE(started_at, now()),
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = p_task;

  -- Actualizar la visita a EN_CURSO también
  UPDATE public.visitas
  SET
    estado = 'EN_CURSO',
    started_at = COALESCE(started_at, now()),
    last_activity_at = now()
  WHERE visit_id = p_visit_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_corte(p_visit_id uuid, p_items jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_saga_venta_id uuid;
  v_saga_recoleccion_id uuid;
  v_recoleccion_id uuid;
  v_total_vendido integer := 0;
  v_total_recolectado integer := 0;
  v_items_venta jsonb;
  v_items_recoleccion jsonb;
BEGIN
  -- Calcular totales
  SELECT
    COALESCE(SUM(COALESCE((item->>'vendido')::int, 0)), 0),
    COALESCE(SUM(COALESCE((item->>'recolectado')::int, 0)), 0)
  INTO v_total_vendido, v_total_recolectado
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item;

  -- Filtrar items para VENTA
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'cantidad', (item->>'vendido')::int
    )
  ) INTO v_items_venta
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'vendido')::int, 0) > 0;

  -- Filtrar items para RECOLECCION
  SELECT jsonb_agg(
    jsonb_build_object(
      'sku', item->>'sku',
      'cantidad', (item->>'recolectado')::int
    )
  ) INTO v_items_recoleccion
  FROM jsonb_array_elements(COALESCE(p_items, '[]'::jsonb)) AS item
  WHERE COALESCE((item->>'recolectado')::int, 0) > 0;

  -- Obtener datos de la visita (sin id_ciclo)
  SELECT v.id_usuario, v.id_cliente
  INTO v_id_usuario, v_id_cliente
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- CREAR SAGA VENTA
  IF v_total_vendido > 0 THEN
    INSERT INTO public.saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'VENTA'::tipo_saga_transaction,
      'BORRADOR'::estado_saga_transaction,
      v_id_cliente,
      v_id_usuario,
      COALESCE(v_items_venta, '[]'::jsonb),
      jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'NORMAL',
        'zoho_required', true
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_venta_id;

    UPDATE public.visit_tasks
    SET
      reference_table = NULL,
      reference_id = NULL,
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV';
  ELSE
    UPDATE public.visit_tasks
    SET
      estado = 'OMITIDA',
      completed_at = now(),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_tipo = 'VENTA_ODV';
  END IF;

  -- CREAR SAGA RECOLECCION
  IF v_total_recolectado > 0 THEN
    INSERT INTO public.saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'RECOLECCION'::tipo_saga_transaction,
      'BORRADOR'::estado_saga_transaction,
      v_id_cliente,
      v_id_usuario,
      COALESCE(v_items_recoleccion, '[]'::jsonb),
      jsonb_build_object('visit_id', p_visit_id),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_recoleccion_id;

    -- INSERT sin id_ciclo (columna no existe en recolecciones)
    INSERT INTO public.recolecciones (
      visit_id, id_cliente, id_usuario, estado
    )
    SELECT p_visit_id, v_id_cliente, v_id_usuario, 'PENDIENTE'
    WHERE NOT EXISTS (
      SELECT 1 FROM public.recolecciones r WHERE r.visit_id = p_visit_id
    )
    RETURNING recoleccion_id INTO v_recoleccion_id;

    IF v_recoleccion_id IS NULL THEN
      SELECT recoleccion_id INTO v_recoleccion_id
      FROM public.recolecciones WHERE visit_id = p_visit_id LIMIT 1;
    END IF;

    -- Insertar items en recolecciones_items
    INSERT INTO public.recolecciones_items (recoleccion_id, sku, cantidad)
    SELECT
      v_recoleccion_id,
      (item->>'sku')::varchar,
      (item->>'cantidad')::int
    FROM jsonb_array_elements(COALESCE(v_items_recoleccion, '[]'::jsonb)) AS item
    ON CONFLICT (recoleccion_id, sku) DO UPDATE
    SET cantidad = EXCLUDED.cantidad;

    UPDATE public.visit_tasks
    SET
      reference_table = NULL,
      reference_id = NULL,
      metadata = jsonb_build_object('recoleccion_id', v_recoleccion_id),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_tipo = 'RECOLECCION';
  ELSE
    UPDATE public.visit_tasks
    SET
      estado = 'OMITIDA',
      completed_at = now(),
      last_activity_at = now()
    WHERE visit_id = p_visit_id AND task_tipo = 'RECOLECCION';
  END IF;

  -- MARCAR CORTE COMPLETADO
  UPDATE public.visit_tasks
  SET
    estado = 'COMPLETADO',
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    metadata = jsonb_build_object(
      'items', p_items,
      'saga_venta_id', v_saga_venta_id,
      'saga_recoleccion_id', v_saga_recoleccion_id,
      'total_vendido', v_total_vendido,
      'total_recolectado', v_total_recolectado
    ),
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = 'CORTE';

  RETURN jsonb_build_object(
    'success', true,
    'saga_venta_id', v_saga_venta_id,
    'saga_recoleccion_id', v_saga_recoleccion_id,
    'total_vendido', v_total_vendido,
    'total_recolectado', v_total_recolectado
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_informe_visita(p_visit_id uuid, p_respuestas jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_id_ciclo integer;
  v_tipo_visita visit_tipo;
  v_informe_id uuid;
  v_next_visit_id uuid;
  v_fecha_proxima date;
  v_etiqueta varchar;
  v_cumplimiento_score integer := 0;
  v_total_preguntas integer := 0;
BEGIN
  -- Obtener datos de la visita actual
  SELECT v.id_usuario, v.id_cliente, v.id_ciclo, v.tipo
  INTO v_id_usuario, v_id_cliente, v_id_ciclo, v_tipo_visita
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Validar que ODV_BOTIQUIN esté completada antes del informe
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_tipo = 'ODV_BOTIQUIN'
    AND estado = 'COMPLETADO'::visit_task_estado
  ) THEN
    RAISE EXCEPTION 'Debe completar la confirmación ODV Botiquín antes de enviar el informe';
  END IF;

  -- Extraer fecha próxima visita
  v_fecha_proxima := (p_respuestas->>'fecha_proxima_visita')::date;

  -- Calcular score de cumplimiento
  SELECT
    COALESCE(SUM(CASE WHEN value::text = 'true' THEN 1 ELSE 0 END), 0),
    COUNT(*)
  INTO v_cumplimiento_score, v_total_preguntas
  FROM jsonb_each(p_respuestas)
  WHERE key NOT IN ('fecha_proxima_visita', 'imagen_visita', 'imagen_visita_local')
  AND jsonb_typeof(value) = 'boolean';

  -- Determinar etiqueta
  IF v_total_preguntas > 0 THEN
    IF v_cumplimiento_score = v_total_preguntas THEN
      v_etiqueta := 'EXCELENTE';
    ELSIF v_cumplimiento_score >= (v_total_preguntas * 0.8) THEN
      v_etiqueta := 'BUENO';
    ELSIF v_cumplimiento_score >= (v_total_preguntas * 0.6) THEN
      v_etiqueta := 'REGULAR';
    ELSE
      v_etiqueta := 'REQUIERE_ATENCION';
    END IF;
  ELSE
    v_etiqueta := 'SIN_EVALUAR';
  END IF;

  -- Crear o actualizar informe
  INSERT INTO public.visita_informes (
    visit_id, respuestas, etiqueta, cumplimiento_score, completada, fecha_completada, created_at
  )
  VALUES (
    p_visit_id,
    p_respuestas,
    v_etiqueta,
    v_cumplimiento_score,
    true,
    now(),
    now()
  )
  ON CONFLICT (visit_id) DO UPDATE SET
    respuestas = EXCLUDED.respuestas,
    etiqueta = EXCLUDED.etiqueta,
    cumplimiento_score = EXCLUDED.cumplimiento_score,
    completada = true,
    fecha_completada = COALESCE(visita_informes.fecha_completada, now()),
    updated_at = now()
  RETURNING informe_id INTO v_informe_id;

  -- Marcar tarea INFORME_VISITA como completada
  UPDATE public.visit_tasks
  SET
    estado = 'COMPLETADO'::visit_task_estado,
    completed_at = now(),
    reference_table = 'visita_informes',
    reference_id = v_informe_id::text,
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = 'INFORME_VISITA';

  -- Actualizar etiqueta en la visita actual
  UPDATE public.visitas
  SET
    etiqueta = v_etiqueta,
    updated_at = now()
  WHERE visit_id = p_visit_id;

  -- Crear próxima visita si se especificó fecha
  IF v_fecha_proxima IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1 FROM public.visitas
      WHERE id_cliente = v_id_cliente
      AND DATE(due_at) = v_fecha_proxima
      AND estado != 'CANCELADO'
    ) THEN
      INSERT INTO public.visitas (
        id_cliente, id_usuario, id_ciclo, tipo, estado, due_at, created_at
      )
      VALUES (
        v_id_cliente,
        v_id_usuario,
        v_id_ciclo,
        'VISITA_CORTE'::visit_tipo,
        'PROGRAMADO'::visit_estado,
        v_fecha_proxima,
        now()
      )
      RETURNING visit_id INTO v_next_visit_id;

      -- Crear tareas para la próxima visita CON transaction_type y step_order
      INSERT INTO public.visit_tasks (visit_id, task_tipo, estado, required, due_at, transaction_type, step_order, created_at)
      VALUES
        (v_next_visit_id, 'CORTE', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'COMPENSABLE', 1, now()),
        (v_next_visit_id, 'VENTA_ODV', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'PIVOT', 2, now()),
        (v_next_visit_id, 'RECOLECCION', 'PENDIENTE'::visit_task_estado, false, v_fecha_proxima, 'RETRYABLE', 3, now()),
        (v_next_visit_id, 'LEV_POST_CORTE', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'COMPENSABLE', 4, now()),
        (v_next_visit_id, 'ODV_BOTIQUIN', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'PIVOT', 5, now()),
        (v_next_visit_id, 'INFORME_VISITA', 'PENDIENTE'::visit_task_estado, true, v_fecha_proxima, 'RETRYABLE', 6, now());
    END IF;
  END IF;

  RETURN v_informe_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_lev_post_corte(p_visit_id uuid, p_items jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_saga_id uuid;
  v_estado_cliente public.estado_cliente;
  v_items_count integer;
  v_venta_odv_estado text;
BEGIN
  -- Obtener datos de la visita
  SELECT v.id_usuario, v.id_cliente
  INTO v_id_usuario, v_id_cliente
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Obtener estado del cliente
  SELECT c.estado INTO v_estado_cliente
  FROM public.clientes c
  WHERE c.id_cliente = v_id_cliente;

  -- Contar items en el array
  v_items_count := COALESCE(jsonb_array_length(p_items), 0);

  -- =========================================================================
  -- NUEVA VALIDACIÓN: Verificar que VENTA_ODV esté completada u omitida
  -- Esto asegura que los SKUs vendidos ya fueron eliminados de disponibles
  -- =========================================================================
  SELECT vt.estado::text INTO v_venta_odv_estado
  FROM public.visit_tasks vt
  WHERE vt.visit_id = p_visit_id 
  AND vt.task_tipo = 'VENTA_ODV'::visit_task_tipo;

  -- Si existe tarea VENTA_ODV y no está completada/omitida, bloquear
  IF v_venta_odv_estado IS NOT NULL 
     AND v_venta_odv_estado NOT IN ('COMPLETADO', 'OMITIDA', 'OMITIDO') THEN
    RAISE EXCEPTION 'Debe confirmar la ODV de Venta antes de realizar el levantamiento post-corte. Estado actual de VENTA_ODV: %', v_venta_odv_estado;
  END IF;
  -- =========================================================================

  -- Verificar que la tarea CORTE está completada
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_tipo = 'CORTE'::visit_task_tipo
    AND estado = 'COMPLETADO'::visit_task_estado
  ) THEN
    RAISE EXCEPTION 'Debe completar el CORTE antes del levantamiento post-corte';
  END IF;

  -- Validar items vacíos según estado del cliente
  IF v_items_count = 0 THEN
    IF v_estado_cliente != 'EN_BAJA' THEN
      RAISE EXCEPTION 'Inventario vacío solo permitido para clientes EN_BAJA. Estado actual: %. Para dar de baja al cliente, primero cambie su estado a EN_BAJA usando el panel de administración.', v_estado_cliente;
    END IF;
    
    -- Cliente EN_BAJA con items = 0: marcar como INACTIVO automáticamente
    UPDATE public.clientes
    SET estado = 'INACTIVO', updated_at = now()
    WHERE id_cliente = v_id_cliente;

    -- Registrar en auditoría
    INSERT INTO public.cliente_estado_log (
      id_cliente, 
      estado_anterior, 
      estado_nuevo, 
      changed_by, 
      razon,
      metadata
    )
    VALUES (
      v_id_cliente, 
      'EN_BAJA', 
      'INACTIVO', 
      v_id_usuario, 
      'Baja automática por LEV_POST_CORTE vacío',
      jsonb_build_object(
        'visit_id', p_visit_id,
        'automatico', true
      )
    );
  END IF;

  -- Buscar saga existente
  SELECT id INTO v_saga_id
  FROM public.saga_transactions
  WHERE visit_id = p_visit_id
  AND tipo = 'LEV_POST_CORTE'::tipo_saga_transaction
  LIMIT 1;

  -- Actualizar saga_transaction existente o crear nueva
  IF v_saga_id IS NOT NULL THEN
    UPDATE public.saga_transactions
    SET
      items = p_items,
      estado = CASE 
        WHEN v_items_count = 0 THEN 'OMITIDA'::estado_saga_transaction
        ELSE 'BORRADOR'::estado_saga_transaction
      END,
      updated_at = now(),
      metadata = jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'BOTIQUIN',
        'zoho_required', v_items_count > 0,
        'cliente_en_baja', v_estado_cliente = 'EN_BAJA',
        'items_count', v_items_count
      )
    WHERE id = v_saga_id;
  ELSE
    INSERT INTO public.saga_transactions (
      tipo, estado, id_cliente, id_usuario,
      items, metadata, visit_id, created_at, updated_at
    )
    VALUES (
      'LEV_POST_CORTE'::tipo_saga_transaction,
      CASE 
        WHEN v_items_count = 0 THEN 'OMITIDA'::estado_saga_transaction
        ELSE 'BORRADOR'::estado_saga_transaction
      END,
      v_id_cliente,
      v_id_usuario,
      p_items,
      jsonb_build_object(
        'visit_id', p_visit_id,
        'zoho_account_mode', 'BOTIQUIN',
        'zoho_required', v_items_count > 0,
        'cliente_en_baja', v_estado_cliente = 'EN_BAJA',
        'items_count', v_items_count
      ),
      p_visit_id,
      now(), now()
    )
    RETURNING id INTO v_saga_id;
  END IF;

  -- Marcar tarea LEV_POST_CORTE como completada (u omitida si 0 items)
  UPDATE public.visit_tasks
  SET
    estado = CASE 
      WHEN v_items_count = 0 THEN 'OMITIDO'::visit_task_estado
      ELSE 'COMPLETADO'::visit_task_estado
    END,
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    last_activity_at = now(),
    metadata = jsonb_build_object(
      'items_count', v_items_count,
      'cliente_estado', v_estado_cliente
    )
  WHERE visit_id = p_visit_id AND task_tipo = 'LEV_POST_CORTE'::visit_task_tipo;

  RETURN v_saga_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_submit_levantamiento_inicial(p_visit_id uuid, p_items jsonb)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_id_usuario varchar;
  v_id_cliente varchar;
  v_id_ciclo integer;
  v_saga_id uuid;
BEGIN
  -- Obtener datos de la visita
  SELECT v.id_usuario, v.id_cliente, v.id_ciclo
  INTO v_id_usuario, v_id_cliente, v_id_ciclo
  FROM public.visitas v
  WHERE v.visit_id = p_visit_id;

  IF v_id_cliente IS NULL THEN
    RAISE EXCEPTION 'Visita no encontrada';
  END IF;

  -- Verificar que la tarea existe y no está completada
  IF NOT EXISTS (
    SELECT 1 FROM public.visit_tasks
    WHERE visit_id = p_visit_id
    AND task_tipo = 'LEVANTAMIENTO_INICIAL'
    AND estado NOT IN ('COMPLETADO', 'OMITIDA', 'CANCELADO')
  ) THEN
    -- Si ya está completada, devolver el saga_id existente
    SELECT st.id INTO v_saga_id
    FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
    AND st.tipo = 'LEVANTAMIENTO_INICIAL'::tipo_saga_transaction
    LIMIT 1;

    RETURN v_saga_id;
  END IF;

  -- Crear saga transaction con estado BORRADOR (idempotente)
  INSERT INTO public.saga_transactions (
    tipo, estado, id_cliente, id_usuario,
    items, metadata, visit_id, created_at, updated_at
  )
  SELECT
    'LEVANTAMIENTO_INICIAL'::tipo_saga_transaction,
    'BORRADOR'::estado_saga_transaction,
    v_id_cliente,
    v_id_usuario,
    p_items,
    jsonb_build_object(
      'visit_id', p_visit_id,
      'zoho_account_mode', 'BOTIQUIN',
      'zoho_required', true
    ),
    p_visit_id,
    now(), now()
  WHERE NOT EXISTS (
    SELECT 1 FROM public.saga_transactions st
    WHERE st.visit_id = p_visit_id
    AND st.tipo = 'LEVANTAMIENTO_INICIAL'::tipo_saga_transaction
  )
  RETURNING id INTO v_saga_id;

  -- Si ya existía, obtener el ID y actualizar items
  IF v_saga_id IS NULL THEN
    UPDATE public.saga_transactions
    SET items = p_items, updated_at = now()
    WHERE visit_id = p_visit_id
    AND tipo = 'LEVANTAMIENTO_INICIAL'::tipo_saga_transaction
    RETURNING id INTO v_saga_id;
  END IF;

  -- Marcar tarea LEVANTAMIENTO_INICIAL como completada
  -- reference_id = NULL según nuevo patrón (COMPENSABLE)
  UPDATE public.visit_tasks
  SET
    estado = 'COMPLETADO',
    completed_at = now(),
    reference_table = NULL,
    reference_id = NULL,
    last_activity_at = now()
  WHERE visit_id = p_visit_id AND task_tipo = 'LEVANTAMIENTO_INICIAL';

  -- ❌ NO crear inventario_botiquin aquí
  -- ❌ NO crear movimientos_inventario aquí
  -- Eso se hace en rpc_confirm_saga_pivot cuando se confirma ODV_BOTIQUIN

  RETURN v_saga_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_sync_botiquin_skus_disponibles(p_id_cliente character varying DEFAULT NULL::character varying)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_cliente record;
  v_inserted integer := 0;
  v_deleted_inactive integer := 0;
  v_deleted_sold integer := 0;
  v_total_clients integer := 0;
  v_results jsonb := '[]'::jsonb;
BEGIN
  -- 1. Eliminar registros de clientes inactivos o que ya no existen
  DELETE FROM public.botiquin_clientes_sku_disponibles
  WHERE id_cliente IN (
    SELECT c.id_cliente FROM public.clientes c WHERE c.activo = false
  )
  OR id_cliente NOT IN (
    SELECT c.id_cliente FROM public.clientes c
  );

  GET DIAGNOSTICS v_deleted_inactive = ROW_COUNT;

  -- 2. NUEVO: Eliminar SKUs que ya fueron vendidos según movimientos_inventario
  -- Esto asegura que si un SKU fue vendido (movimiento tipo VENTA), 
  -- no esté disponible para lev_post_corte
  DELETE FROM public.botiquin_clientes_sku_disponibles bcs
  WHERE EXISTS (
    SELECT 1 FROM public.movimientos_inventario mi
    WHERE mi.id_cliente = bcs.id_cliente
      AND mi.sku = bcs.sku
      AND mi.tipo = 'VENTA'
  );

  GET DIAGNOSTICS v_deleted_sold = ROW_COUNT;

  -- 3. También eliminar SKUs que fueron vendidos según ventas_odv (legacy)
  DELETE FROM public.botiquin_clientes_sku_disponibles bcs
  WHERE EXISTS (
    SELECT 1 FROM public.ventas_odv vo
    WHERE vo.id_cliente = bcs.id_cliente
      AND vo.sku = bcs.sku
  );

  -- 4. Sincronizar SKUs para clientes activos
  FOR v_cliente IN
    SELECT c.id_cliente
    FROM public.clientes c
    WHERE c.activo = true
      AND (p_id_cliente IS NULL OR c.id_cliente = p_id_cliente)
  LOOP
    v_total_clients := v_total_clients + 1;

    WITH inserted AS (
      INSERT INTO public.botiquin_clientes_sku_disponibles (id_cliente, sku, fecha_ingreso)
      SELECT
        v_cliente.id_cliente,
        m.sku,
        now()
      FROM public.medicamentos m
      WHERE 
        -- No fue vendido en ventas_odv (legacy)
        NOT EXISTS (
          SELECT 1 FROM public.ventas_odv vo
          WHERE vo.id_cliente = v_cliente.id_cliente
            AND vo.sku = m.sku
        )
        -- NUEVO: No fue vendido según movimientos_inventario
        AND NOT EXISTS (
          SELECT 1 FROM public.movimientos_inventario mi
          WHERE mi.id_cliente = v_cliente.id_cliente
            AND mi.sku = m.sku
            AND mi.tipo = 'VENTA'
        )
        -- No existe ya en la tabla
        AND NOT EXISTS (
          SELECT 1 FROM public.botiquin_clientes_sku_disponibles bcs
          WHERE bcs.id_cliente = v_cliente.id_cliente
            AND bcs.sku = m.sku
        )
      RETURNING sku
    )
    SELECT COUNT(*) INTO v_inserted FROM inserted;

    v_results := v_results || jsonb_build_object(
      'id_cliente', v_cliente.id_cliente,
      'skus_added', v_inserted
    );
  END LOOP;

  RETURN jsonb_build_object(
    'success', true,
    'deleted_inactive_records', v_deleted_inactive,
    'deleted_sold_records', v_deleted_sold,
    'total_clients_synced', v_total_clients,
    'details', v_results
  );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_try_complete_visit(p_visit_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_visita_exists boolean;
  v_all_required_completed boolean;
  v_pending_required integer;
BEGIN
  -- Verificar que la visita existe y no está completada/cancelada
  SELECT EXISTS(
    SELECT 1 FROM public.visitas
    WHERE visit_id = p_visit_id
    AND estado NOT IN ('COMPLETADO', 'CANCELADO')
  ) INTO v_visita_exists;

  IF NOT v_visita_exists THEN
    -- Si ya está completada, devolver true
    IF EXISTS (
      SELECT 1 FROM public.visitas
      WHERE visit_id = p_visit_id
      AND estado = 'COMPLETADO'
    ) THEN
      RETURN true;
    END IF;
    RETURN false;
  END IF;

  -- Contar tareas requeridas NO finalizadas
  -- COMPLETADO y OMITIDA son estados finales válidos que permiten completar la visita
  SELECT COUNT(*)
  INTO v_pending_required
  FROM public.visit_tasks
  WHERE visit_id = p_visit_id
  AND required = true
  AND estado NOT IN ('COMPLETADO', 'OMITIDA');

  v_all_required_completed := (v_pending_required = 0);

  IF v_all_required_completed THEN
    -- Marcar visita como completada
    UPDATE public.visitas
    SET
      estado = 'COMPLETADO',
      completed_at = now(),
      updated_at = now()
    WHERE visit_id = p_visit_id;

    RETURN true;
  END IF;

  RETURN false;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_consolidation()
 RETURNS TABLE(total_visitas integer, visitas_duplicadas integer, sagas_sin_visit integer, tareas_sin_referencia integer, informes_creados integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM public.visitas),
    (
      SELECT COUNT(*)::integer FROM (
        SELECT id_cliente, id_usuario, DATE(created_at)
        FROM public.visitas
        GROUP BY id_cliente, id_usuario, DATE(created_at)
        HAVING COUNT(*) > 1
      ) dups
    ),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.visit_tasks WHERE reference_id IS NULL AND estado = 'COMPLETADO'),
    (SELECT COUNT(*)::integer FROM public.visita_informes);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_dev_migration()
 RETURNS TABLE(total_visitas integer, visitas_migradas integer, total_sagas integer, sagas_sin_visit integer, total_recolecciones integer, recolecciones_sin_visit integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM public.visitas),
    (SELECT COUNT(*)::integer FROM public.visitas WHERE metadata->>'migrated_from_legacy' = 'true'),
    (SELECT COUNT(*)::integer FROM public.saga_transactions),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.recolecciones),
    (SELECT COUNT(*)::integer FROM public.recolecciones WHERE visit_id IS NULL);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.rpc_verify_migration_consistency()
 RETURNS TABLE(total_ciclos_migration integer, total_visitas integer, visitas_migradas integer, sagas_sin_visit integer, recolecciones_sin_visit integer, ciclos_sin_visita integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN QUERY
  SELECT
    (SELECT COUNT(*)::integer FROM migration.ciclos_botiquin),
    (SELECT COUNT(*)::integer FROM public.visitas),
    (SELECT COUNT(*)::integer FROM public.visitas WHERE metadata->>'migrated_from_legacy' = 'true'),
    (SELECT COUNT(*)::integer FROM public.saga_transactions WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM public.recolecciones WHERE visit_id IS NULL),
    (SELECT COUNT(*)::integer FROM migration.ciclos_botiquin cb
     WHERE NOT EXISTS (SELECT 1 FROM public.visitas v WHERE v.id_ciclo = cb.id_ciclo));
END;
$function$
;

CREATE OR REPLACE FUNCTION public.saga_outbox_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  -- Solo para estados que requieren procesamiento externo
  IF NEW.estado IN ('PENDIENTE_CONFIRMACION', 'PROCESANDO_ZOHO') THEN
    
    -- Insertar evento en outbox según el tipo de saga
    INSERT INTO event_outbox (
      evento_tipo,
      saga_transaction_id,
      payload,
      procesado,
      proximo_intento
    ) VALUES (
      CASE 
        WHEN NEW.tipo = 'VENTA' THEN 'CREAR_ODV_VENTA'::tipo_evento_outbox
        WHEN NEW.tipo IN ('LEVANTAMIENTO_INICIAL', 'CORTE_RENOVACION') 
          THEN 'CREAR_ODV_CONSIGNACION'::tipo_evento_outbox
        WHEN NEW.tipo = 'RECOLECCION' THEN 'CREAR_DEVOLUCION'::tipo_evento_outbox
        ELSE 'SINCRONIZAR_ZOHO'::tipo_evento_outbox
      END,
      NEW.id,
      jsonb_build_object(
        'tipo', NEW.tipo,
        'id_cliente', NEW.id_cliente,
        'id_usuario', NEW.id_usuario,
        'items', NEW.items,
        'metadata', NEW.metadata
      ),
      FALSE,
      NOW()
    )
    ON CONFLICT DO NOTHING; -- Evitar duplicados
    
  END IF;
  
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.set_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
begin
  new.updated_at = now();
  return new;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_generate_movements_from_saga()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_item record;
  v_cantidad_antes int;
  v_cantidad_despues int;
  v_tipo_movimiento tipo_movimiento_botiquin;
BEGIN
  -- Only process if estado is CONFIRMADO
  IF NEW.estado != 'CONFIRMADO' THEN
    RETURN NEW;
  END IF;
  
  -- Skip if this saga already has movements (avoid duplicates)
  IF EXISTS (SELECT 1 FROM movimientos_inventario WHERE id_saga_transaction = NEW.id) THEN
    RETURN NEW;
  END IF;
  
  -- Skip if no items
  IF NEW.items IS NULL OR jsonb_array_length(NEW.items) = 0 THEN
    RETURN NEW;
  END IF;

  -- Process each item in the saga
  FOR v_item IN
    SELECT 
      item->>'sku' as sku,
      (item->>'cantidad')::int as cantidad,
      item->>'tipo_movimiento' as tipo_movimiento
    FROM jsonb_array_elements(NEW.items) as item
  LOOP
    -- Skip PERMANENCIA movements
    IF v_item.tipo_movimiento = 'PERMANENCIA' THEN
      CONTINUE;
    END IF;
    
    -- Get cantidad_antes
    SELECT COALESCE(cantidad_despues, 0)
    INTO v_cantidad_antes
    FROM movimientos_inventario
    WHERE id_cliente = NEW.id_cliente 
      AND sku = v_item.sku
    ORDER BY fecha_movimiento DESC, id DESC
    LIMIT 1;
    
    IF v_cantidad_antes IS NULL THEN
      v_cantidad_antes := 0;
    END IF;
    
    -- Determine movement type
    CASE v_item.tipo_movimiento
      WHEN 'CREACION' THEN
        v_tipo_movimiento := 'CREACION';
        v_cantidad_despues := v_cantidad_antes + v_item.cantidad;
      WHEN 'VENTA' THEN
        v_tipo_movimiento := 'VENTA';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      WHEN 'RECOLECCION' THEN
        v_tipo_movimiento := 'RECOLECCION';
        v_cantidad_despues := GREATEST(0, v_cantidad_antes - v_item.cantidad);
      ELSE
        CONTINUE;
    END CASE;
    
    -- Insert movement
    INSERT INTO movimientos_inventario (
      id_saga_transaction,
      id_cliente,
      sku,
      tipo,
      cantidad,
      cantidad_antes,
      cantidad_despues,
      fecha_movimiento
    ) VALUES (
      NEW.id,
      NEW.id_cliente,
      v_item.sku,
      v_tipo_movimiento,
      v_item.cantidad,
      v_cantidad_antes,
      v_cantidad_despues,
      COALESCE(NEW.created_at, now())
    );
    
    -- Update inventario_botiquin
    IF v_cantidad_despues > 0 THEN
      INSERT INTO inventario_botiquin (id_cliente, sku, cantidad_disponible, ultima_actualizacion)
      VALUES (NEW.id_cliente, v_item.sku, v_cantidad_despues, now())
      ON CONFLICT (id_cliente, sku)
      DO UPDATE SET 
        cantidad_disponible = v_cantidad_despues,
        ultima_actualizacion = now();
    ELSE
      DELETE FROM inventario_botiquin 
      WHERE id_cliente = NEW.id_cliente AND sku = v_item.sku;
    END IF;
  END LOOP;
  
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_notify_task_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    v_visit RECORD;
    v_task_name TEXT;
BEGIN
    -- Only when status changes to COMPLETADO
    IF NEW.estado = 'COMPLETADO' AND OLD.estado != 'COMPLETADO' THEN
        -- Get visit info
        SELECT v.*, c.nombre_cliente as cliente_nombre
        INTO v_visit
        FROM visitas v
        JOIN clientes c ON c.id_cliente = v.id_cliente
        WHERE v.visit_id = NEW.visit_id;

        -- Task name mapping
        v_task_name := CASE NEW.task_tipo::text
            WHEN 'LEVANTAMIENTO_INICIAL' THEN 'Levantamiento'
            WHEN 'CORTE' THEN 'Corte'
            WHEN 'RECOLECCION' THEN 'Recolección'
            WHEN 'LEV_POST_CORTE' THEN 'Lev. Post Corte'
            WHEN 'VENTA_ODV' THEN 'Venta ODV'
            WHEN 'ODV_BOTIQUIN' THEN 'ODV Botiquín'
            WHEN 'INFORME_VISITA' THEN 'Informe'
            ELSE NEW.task_tipo::text
        END;

        -- Notify the representative
        PERFORM create_notification(
            v_visit.id_usuario,
            'TASK_COMPLETED',
            format('%s completado', v_task_name),
            format('Cliente: %s', v_visit.cliente_nombre),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'task_id', NEW.task_id,
                'task_type', NEW.task_tipo::text
            ),
            format('task_%s_%s', NEW.task_id, 'completed')
        );
    END IF;

    -- Notify ERROR to admins
    IF NEW.estado = 'ERROR' AND OLD.estado != 'ERROR' THEN
        PERFORM notify_admins(
            'TASK_ERROR',
            format('Error en %s', NEW.task_tipo::text),
            format('Visita: %s, Tarea: %s', NEW.visit_id, NEW.task_tipo::text),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'task_id', NEW.task_id,
                'task_type', NEW.task_tipo::text,
                'error', NEW.metadata->>'error_message'
            )
        );
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_notify_visit_completed()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
    v_cliente TEXT;
BEGIN
    IF NEW.estado = 'COMPLETADO' AND OLD.estado != 'COMPLETADO' THEN
        SELECT nombre_cliente INTO v_cliente FROM clientes WHERE id_cliente = NEW.id_cliente;

        -- Notify the representative
        PERFORM create_notification(
            NEW.id_usuario,
            'TASK_COMPLETED',
            'Visita completada',
            format('Cliente: %s', v_cliente),
            jsonb_build_object(
                'visit_id', NEW.visit_id,
                'cliente', v_cliente
            ),
            format('visit_%s_completed', NEW.visit_id)
        );

        -- Notify admins
        PERFORM notify_admins(
            'ADMIN_ACTION',
            'Visita completada',
            format('Representante finalizó visita a %s', v_cliente),
            jsonb_build_object('visit_id', NEW.visit_id)
        );
    END IF;

    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.trigger_refresh_stats()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  PERFORM refresh_all_materialized_views();
  RETURN NULL;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.update_updated_at_column()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.upsert_push_token(p_user_id character varying, p_token text, p_platform text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_current_user_id VARCHAR;
BEGIN
  -- Obtener el id_usuario actual basado en auth.uid()
  SELECT id_usuario INTO v_current_user_id
  FROM usuarios
  WHERE auth_user_id = auth.uid();
  
  -- Verificar que el usuario solo puede guardar tokens para sí mismo
  IF v_current_user_id IS NULL OR v_current_user_id != p_user_id THEN
    RAISE EXCEPTION 'Unauthorized: Cannot save push token for another user';
  END IF;
  
  -- Desactivar tokens anteriores de este usuario en esta plataforma
  UPDATE user_push_tokens
  SET is_active = false, updated_at = NOW()
  WHERE user_id = p_user_id 
    AND platform = p_platform
    AND token != p_token;
  
  -- Upsert el nuevo token
  INSERT INTO user_push_tokens (user_id, token, platform, is_active, created_at, updated_at)
  VALUES (p_user_id, p_token, p_platform, true, NOW(), NOW())
  ON CONFLICT (token) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    is_active = true,
    updated_at = NOW();
    
END;
$function$
;

create or replace view "public"."v_clientes_con_inventario" as  SELECT c.id_cliente,
    c.nombre_cliente,
    c.id_zona,
    c.id_usuario,
    c.activo,
    c.estado,
    c.facturacion_promedio,
    c.facturacion_total,
    c.meses_con_venta,
    c.rango,
    c.id_cliente_zoho_botiquin,
    c.id_cliente_zoho_normal,
    COALESCE(inv.total_inventario, (0)::bigint) AS total_inventario,
    (COALESCE(inv.total_inventario, (0)::bigint) > 0) AS tiene_botiquin_activo
   FROM (public.clientes c
     LEFT JOIN ( SELECT inventario_botiquin.id_cliente,
            sum(inventario_botiquin.cantidad_disponible) AS total_inventario
           FROM public.inventario_botiquin
          GROUP BY inventario_botiquin.id_cliente) inv ON (((c.id_cliente)::text = (inv.id_cliente)::text)))
  ORDER BY c.nombre_cliente;


create or replace view "public"."v_visit_tasks_operativo" as  SELECT (task_id)::text AS task_id,
    visit_id,
    task_tipo,
    estado,
    required,
    created_at,
    started_at,
    completed_at,
    due_at,
    last_activity_at,
    reference_table,
    reference_id,
    metadata,
    (transaction_type)::text AS transaction_type,
    step_order,
    'NOT_NEEDED'::text AS compensation_status,
    '{}'::jsonb AS input_payload,
    '{}'::jsonb AS output_result,
    NULL::jsonb AS compensation_payload,
    (gen_random_uuid())::text AS idempotency_key,
    0 AS retry_count,
    3 AS max_retries,
    NULL::text AS last_error,
    NULL::timestamp with time zone AS compensation_executed_at,
        CASE
            WHEN (estado = 'COMPLETADO'::public.visit_task_estado) THEN 'COMPLETADO'::public.visit_task_estado
            WHEN (estado = 'OMITIDO'::public.visit_task_estado) THEN estado
            WHEN (estado = 'OMITIDA'::public.visit_task_estado) THEN estado
            WHEN (estado = 'ERROR'::public.visit_task_estado) THEN estado
            WHEN ((due_at IS NOT NULL) AND (due_at < now()) AND (estado <> ALL (ARRAY['COMPLETADO'::public.visit_task_estado, 'OMITIDO'::public.visit_task_estado, 'OMITIDA'::public.visit_task_estado]))) THEN 'RETRASADO'::public.visit_task_estado
            ELSE estado
        END AS estado_operativo
   FROM public.visit_tasks vt;


create or replace view "public"."v_visitas_operativo" as  SELECT v.visit_id,
    v.id_cliente,
    v.id_usuario,
    u.nombre AS nombre_usuario,
    v.tipo,
    v.estado,
    v.id_ciclo,
    v.created_at,
    v.due_at,
    v.completed_at,
    c.nombre_cliente,
    c.id_zona,
    c.rango,
    ( SELECT count(*) AS count
           FROM public.visit_tasks vt
          WHERE (vt.visit_id = v.visit_id)) AS total_tareas,
    ( SELECT count(*) AS count
           FROM public.visit_tasks vt
          WHERE ((vt.visit_id = v.visit_id) AND (vt.estado = 'COMPLETADO'::public.visit_task_estado))) AS tareas_completadas,
        CASE
            WHEN (v.estado = 'COMPLETADO'::public.visit_estado) THEN 'COMPLETADO'::text
            WHEN ((v.due_at < now()) AND (v.estado <> 'COMPLETADO'::public.visit_estado)) THEN 'RETRASADO'::text
            WHEN (EXISTS ( SELECT 1
               FROM public.visit_tasks vt
              WHERE ((vt.visit_id = v.visit_id) AND (vt.estado = ANY (ARRAY['EN_CURSO'::public.visit_task_estado, 'PENDIENTE_SYNC'::public.visit_task_estado, 'COMPLETADO'::public.visit_task_estado]))))) THEN 'EN_CURSO'::text
            ELSE 'PENDIENTE'::text
        END AS estado_operativo
   FROM ((public.visitas v
     JOIN public.clientes c ON (((v.id_cliente)::text = (c.id_cliente)::text)))
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)));


CREATE OR REPLACE FUNCTION public.validate_unique_skus_in_items()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public'
AS $function$
DECLARE
  sku_duplicados jsonb;
BEGIN
  -- Buscar SKUs que aparecen más de una vez en items
  SELECT jsonb_object_agg(sku, count)
  INTO sku_duplicados
  FROM (
    SELECT 
      item->>'sku' as sku,
      item->>'tipo_movimiento' as tipo,
      COUNT(*) as count
    FROM jsonb_array_elements(NEW.items) as item
    GROUP BY item->>'sku', item->>'tipo_movimiento'
    HAVING COUNT(*) > 1
  ) duplicates;

  -- Si hay duplicados, rechazar la transacción
  IF sku_duplicados IS NOT NULL THEN
    RAISE EXCEPTION 'ERROR: SKUs duplicados detectados en items: %. Cada SKU solo puede aparecer una vez por tipo de movimiento en una SAGA. Una SAGA representa una operación completa (1 SAGA = 1 ODV en Zoho).', 
      sku_duplicados::text
    USING HINT = 'Verifique que no esté agregando el mismo SKU múltiples veces al array items.';
  END IF;

  RETURN NEW;
END;
$function$
;

CREATE UNIQUE INDEX idx_mv_brand_performance_marca ON public.mv_brand_performance USING btree (marca);

CREATE UNIQUE INDEX idx_mv_doctor_stats_cliente ON public.mv_doctor_stats USING btree (id_cliente);

CREATE UNIQUE INDEX idx_mv_opportunity_matrix_padec ON public.mv_opportunity_matrix USING btree (padecimiento);

CREATE UNIQUE INDEX idx_mv_padec_performance_nombre ON public.mv_padecimiento_performance USING btree (padecimiento);

CREATE UNIQUE INDEX idx_mv_product_interest_sku ON public.mv_product_interest USING btree (sku);

grant delete on table "archive"."ciclos_botiquin" to "anon";

grant insert on table "archive"."ciclos_botiquin" to "anon";

grant references on table "archive"."ciclos_botiquin" to "anon";

grant select on table "archive"."ciclos_botiquin" to "anon";

grant trigger on table "archive"."ciclos_botiquin" to "anon";

grant truncate on table "archive"."ciclos_botiquin" to "anon";

grant update on table "archive"."ciclos_botiquin" to "anon";

grant delete on table "archive"."ciclos_botiquin" to "authenticated";

grant insert on table "archive"."ciclos_botiquin" to "authenticated";

grant references on table "archive"."ciclos_botiquin" to "authenticated";

grant select on table "archive"."ciclos_botiquin" to "authenticated";

grant trigger on table "archive"."ciclos_botiquin" to "authenticated";

grant truncate on table "archive"."ciclos_botiquin" to "authenticated";

grant update on table "archive"."ciclos_botiquin" to "authenticated";

grant delete on table "archive"."ciclos_botiquin" to "service_role";

grant insert on table "archive"."ciclos_botiquin" to "service_role";

grant references on table "archive"."ciclos_botiquin" to "service_role";

grant select on table "archive"."ciclos_botiquin" to "service_role";

grant trigger on table "archive"."ciclos_botiquin" to "service_role";

grant truncate on table "archive"."ciclos_botiquin" to "service_role";

grant update on table "archive"."ciclos_botiquin" to "service_role";

grant delete on table "archive"."encuestas_ciclo" to "anon";

grant insert on table "archive"."encuestas_ciclo" to "anon";

grant references on table "archive"."encuestas_ciclo" to "anon";

grant select on table "archive"."encuestas_ciclo" to "anon";

grant trigger on table "archive"."encuestas_ciclo" to "anon";

grant truncate on table "archive"."encuestas_ciclo" to "anon";

grant update on table "archive"."encuestas_ciclo" to "anon";

grant delete on table "archive"."encuestas_ciclo" to "authenticated";

grant insert on table "archive"."encuestas_ciclo" to "authenticated";

grant references on table "archive"."encuestas_ciclo" to "authenticated";

grant select on table "archive"."encuestas_ciclo" to "authenticated";

grant trigger on table "archive"."encuestas_ciclo" to "authenticated";

grant truncate on table "archive"."encuestas_ciclo" to "authenticated";

grant update on table "archive"."encuestas_ciclo" to "authenticated";

grant delete on table "archive"."encuestas_ciclo" to "service_role";

grant insert on table "archive"."encuestas_ciclo" to "service_role";

grant references on table "archive"."encuestas_ciclo" to "service_role";

grant select on table "archive"."encuestas_ciclo" to "service_role";

grant trigger on table "archive"."encuestas_ciclo" to "service_role";

grant truncate on table "archive"."encuestas_ciclo" to "service_role";

grant update on table "archive"."encuestas_ciclo" to "service_role";

grant delete on table "public"."audit_log" to "anon";

grant insert on table "public"."audit_log" to "anon";

grant references on table "public"."audit_log" to "anon";

grant select on table "public"."audit_log" to "anon";

grant trigger on table "public"."audit_log" to "anon";

grant truncate on table "public"."audit_log" to "anon";

grant update on table "public"."audit_log" to "anon";

grant delete on table "public"."audit_log" to "authenticated";

grant insert on table "public"."audit_log" to "authenticated";

grant references on table "public"."audit_log" to "authenticated";

grant select on table "public"."audit_log" to "authenticated";

grant trigger on table "public"."audit_log" to "authenticated";

grant truncate on table "public"."audit_log" to "authenticated";

grant update on table "public"."audit_log" to "authenticated";

grant delete on table "public"."audit_log" to "service_role";

grant insert on table "public"."audit_log" to "service_role";

grant references on table "public"."audit_log" to "service_role";

grant select on table "public"."audit_log" to "service_role";

grant trigger on table "public"."audit_log" to "service_role";

grant truncate on table "public"."audit_log" to "service_role";

grant update on table "public"."audit_log" to "service_role";

grant delete on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant insert on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant references on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant select on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant trigger on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant truncate on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant update on table "public"."botiquin_clientes_sku_disponibles" to "anon";

grant delete on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant insert on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant references on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant select on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant trigger on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant truncate on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant update on table "public"."botiquin_clientes_sku_disponibles" to "authenticated";

grant delete on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant insert on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant references on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant select on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant trigger on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant truncate on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant update on table "public"."botiquin_clientes_sku_disponibles" to "service_role";

grant select on table "public"."botiquin_odv" to "authenticated";

grant delete on table "public"."botiquin_odv" to "service_role";

grant insert on table "public"."botiquin_odv" to "service_role";

grant references on table "public"."botiquin_odv" to "service_role";

grant select on table "public"."botiquin_odv" to "service_role";

grant trigger on table "public"."botiquin_odv" to "service_role";

grant truncate on table "public"."botiquin_odv" to "service_role";

grant update on table "public"."botiquin_odv" to "service_role";

grant delete on table "public"."clientes" to "anon";

grant insert on table "public"."clientes" to "anon";

grant references on table "public"."clientes" to "anon";

grant select on table "public"."clientes" to "anon";

grant trigger on table "public"."clientes" to "anon";

grant truncate on table "public"."clientes" to "anon";

grant update on table "public"."clientes" to "anon";

grant delete on table "public"."clientes" to "authenticated";

grant insert on table "public"."clientes" to "authenticated";

grant references on table "public"."clientes" to "authenticated";

grant select on table "public"."clientes" to "authenticated";

grant trigger on table "public"."clientes" to "authenticated";

grant truncate on table "public"."clientes" to "authenticated";

grant update on table "public"."clientes" to "authenticated";

grant delete on table "public"."clientes" to "service_role";

grant insert on table "public"."clientes" to "service_role";

grant references on table "public"."clientes" to "service_role";

grant select on table "public"."clientes" to "service_role";

grant trigger on table "public"."clientes" to "service_role";

grant truncate on table "public"."clientes" to "service_role";

grant update on table "public"."clientes" to "service_role";

grant delete on table "public"."event_outbox" to "anon";

grant insert on table "public"."event_outbox" to "anon";

grant references on table "public"."event_outbox" to "anon";

grant select on table "public"."event_outbox" to "anon";

grant trigger on table "public"."event_outbox" to "anon";

grant truncate on table "public"."event_outbox" to "anon";

grant update on table "public"."event_outbox" to "anon";

grant delete on table "public"."event_outbox" to "authenticated";

grant insert on table "public"."event_outbox" to "authenticated";

grant references on table "public"."event_outbox" to "authenticated";

grant select on table "public"."event_outbox" to "authenticated";

grant trigger on table "public"."event_outbox" to "authenticated";

grant truncate on table "public"."event_outbox" to "authenticated";

grant update on table "public"."event_outbox" to "authenticated";

grant delete on table "public"."event_outbox" to "service_role";

grant insert on table "public"."event_outbox" to "service_role";

grant references on table "public"."event_outbox" to "service_role";

grant select on table "public"."event_outbox" to "service_role";

grant trigger on table "public"."event_outbox" to "service_role";

grant truncate on table "public"."event_outbox" to "service_role";

grant update on table "public"."event_outbox" to "service_role";

grant delete on table "public"."inventario_botiquin" to "anon";

grant insert on table "public"."inventario_botiquin" to "anon";

grant references on table "public"."inventario_botiquin" to "anon";

grant select on table "public"."inventario_botiquin" to "anon";

grant trigger on table "public"."inventario_botiquin" to "anon";

grant truncate on table "public"."inventario_botiquin" to "anon";

grant update on table "public"."inventario_botiquin" to "anon";

grant delete on table "public"."inventario_botiquin" to "authenticated";

grant insert on table "public"."inventario_botiquin" to "authenticated";

grant references on table "public"."inventario_botiquin" to "authenticated";

grant select on table "public"."inventario_botiquin" to "authenticated";

grant trigger on table "public"."inventario_botiquin" to "authenticated";

grant truncate on table "public"."inventario_botiquin" to "authenticated";

grant update on table "public"."inventario_botiquin" to "authenticated";

grant delete on table "public"."inventario_botiquin" to "service_role";

grant insert on table "public"."inventario_botiquin" to "service_role";

grant references on table "public"."inventario_botiquin" to "service_role";

grant select on table "public"."inventario_botiquin" to "service_role";

grant trigger on table "public"."inventario_botiquin" to "service_role";

grant truncate on table "public"."inventario_botiquin" to "service_role";

grant update on table "public"."inventario_botiquin" to "service_role";

grant delete on table "public"."medicamento_padecimientos" to "anon";

grant insert on table "public"."medicamento_padecimientos" to "anon";

grant references on table "public"."medicamento_padecimientos" to "anon";

grant select on table "public"."medicamento_padecimientos" to "anon";

grant trigger on table "public"."medicamento_padecimientos" to "anon";

grant truncate on table "public"."medicamento_padecimientos" to "anon";

grant update on table "public"."medicamento_padecimientos" to "anon";

grant delete on table "public"."medicamento_padecimientos" to "authenticated";

grant insert on table "public"."medicamento_padecimientos" to "authenticated";

grant references on table "public"."medicamento_padecimientos" to "authenticated";

grant select on table "public"."medicamento_padecimientos" to "authenticated";

grant trigger on table "public"."medicamento_padecimientos" to "authenticated";

grant truncate on table "public"."medicamento_padecimientos" to "authenticated";

grant update on table "public"."medicamento_padecimientos" to "authenticated";

grant delete on table "public"."medicamento_padecimientos" to "service_role";

grant insert on table "public"."medicamento_padecimientos" to "service_role";

grant references on table "public"."medicamento_padecimientos" to "service_role";

grant select on table "public"."medicamento_padecimientos" to "service_role";

grant trigger on table "public"."medicamento_padecimientos" to "service_role";

grant truncate on table "public"."medicamento_padecimientos" to "service_role";

grant update on table "public"."medicamento_padecimientos" to "service_role";

grant delete on table "public"."medicamentos" to "anon";

grant insert on table "public"."medicamentos" to "anon";

grant references on table "public"."medicamentos" to "anon";

grant select on table "public"."medicamentos" to "anon";

grant trigger on table "public"."medicamentos" to "anon";

grant truncate on table "public"."medicamentos" to "anon";

grant update on table "public"."medicamentos" to "anon";

grant delete on table "public"."medicamentos" to "authenticated";

grant insert on table "public"."medicamentos" to "authenticated";

grant references on table "public"."medicamentos" to "authenticated";

grant select on table "public"."medicamentos" to "authenticated";

grant trigger on table "public"."medicamentos" to "authenticated";

grant truncate on table "public"."medicamentos" to "authenticated";

grant update on table "public"."medicamentos" to "authenticated";

grant delete on table "public"."medicamentos" to "service_role";

grant insert on table "public"."medicamentos" to "service_role";

grant references on table "public"."medicamentos" to "service_role";

grant select on table "public"."medicamentos" to "service_role";

grant trigger on table "public"."medicamentos" to "service_role";

grant truncate on table "public"."medicamentos" to "service_role";

grant update on table "public"."medicamentos" to "service_role";

grant delete on table "public"."movimientos_inventario" to "anon";

grant insert on table "public"."movimientos_inventario" to "anon";

grant references on table "public"."movimientos_inventario" to "anon";

grant select on table "public"."movimientos_inventario" to "anon";

grant trigger on table "public"."movimientos_inventario" to "anon";

grant truncate on table "public"."movimientos_inventario" to "anon";

grant update on table "public"."movimientos_inventario" to "anon";

grant delete on table "public"."movimientos_inventario" to "authenticated";

grant insert on table "public"."movimientos_inventario" to "authenticated";

grant references on table "public"."movimientos_inventario" to "authenticated";

grant select on table "public"."movimientos_inventario" to "authenticated";

grant trigger on table "public"."movimientos_inventario" to "authenticated";

grant truncate on table "public"."movimientos_inventario" to "authenticated";

grant update on table "public"."movimientos_inventario" to "authenticated";

grant delete on table "public"."movimientos_inventario" to "service_role";

grant insert on table "public"."movimientos_inventario" to "service_role";

grant references on table "public"."movimientos_inventario" to "service_role";

grant select on table "public"."movimientos_inventario" to "service_role";

grant trigger on table "public"."movimientos_inventario" to "service_role";

grant truncate on table "public"."movimientos_inventario" to "service_role";

grant update on table "public"."movimientos_inventario" to "service_role";

grant delete on table "public"."notifications" to "service_role";

grant insert on table "public"."notifications" to "service_role";

grant references on table "public"."notifications" to "service_role";

grant select on table "public"."notifications" to "service_role";

grant trigger on table "public"."notifications" to "service_role";

grant truncate on table "public"."notifications" to "service_role";

grant update on table "public"."notifications" to "service_role";

grant delete on table "public"."padecimientos" to "anon";

grant insert on table "public"."padecimientos" to "anon";

grant references on table "public"."padecimientos" to "anon";

grant select on table "public"."padecimientos" to "anon";

grant trigger on table "public"."padecimientos" to "anon";

grant truncate on table "public"."padecimientos" to "anon";

grant update on table "public"."padecimientos" to "anon";

grant delete on table "public"."padecimientos" to "authenticated";

grant insert on table "public"."padecimientos" to "authenticated";

grant references on table "public"."padecimientos" to "authenticated";

grant select on table "public"."padecimientos" to "authenticated";

grant trigger on table "public"."padecimientos" to "authenticated";

grant truncate on table "public"."padecimientos" to "authenticated";

grant update on table "public"."padecimientos" to "authenticated";

grant delete on table "public"."padecimientos" to "service_role";

grant insert on table "public"."padecimientos" to "service_role";

grant references on table "public"."padecimientos" to "service_role";

grant select on table "public"."padecimientos" to "service_role";

grant trigger on table "public"."padecimientos" to "service_role";

grant truncate on table "public"."padecimientos" to "service_role";

grant update on table "public"."padecimientos" to "service_role";

grant delete on table "public"."recolecciones" to "anon";

grant insert on table "public"."recolecciones" to "anon";

grant references on table "public"."recolecciones" to "anon";

grant select on table "public"."recolecciones" to "anon";

grant trigger on table "public"."recolecciones" to "anon";

grant truncate on table "public"."recolecciones" to "anon";

grant update on table "public"."recolecciones" to "anon";

grant delete on table "public"."recolecciones" to "authenticated";

grant insert on table "public"."recolecciones" to "authenticated";

grant references on table "public"."recolecciones" to "authenticated";

grant select on table "public"."recolecciones" to "authenticated";

grant trigger on table "public"."recolecciones" to "authenticated";

grant truncate on table "public"."recolecciones" to "authenticated";

grant update on table "public"."recolecciones" to "authenticated";

grant delete on table "public"."recolecciones" to "service_role";

grant insert on table "public"."recolecciones" to "service_role";

grant references on table "public"."recolecciones" to "service_role";

grant select on table "public"."recolecciones" to "service_role";

grant trigger on table "public"."recolecciones" to "service_role";

grant truncate on table "public"."recolecciones" to "service_role";

grant update on table "public"."recolecciones" to "service_role";

grant delete on table "public"."recolecciones_evidencias" to "anon";

grant insert on table "public"."recolecciones_evidencias" to "anon";

grant references on table "public"."recolecciones_evidencias" to "anon";

grant select on table "public"."recolecciones_evidencias" to "anon";

grant trigger on table "public"."recolecciones_evidencias" to "anon";

grant truncate on table "public"."recolecciones_evidencias" to "anon";

grant update on table "public"."recolecciones_evidencias" to "anon";

grant delete on table "public"."recolecciones_evidencias" to "authenticated";

grant insert on table "public"."recolecciones_evidencias" to "authenticated";

grant references on table "public"."recolecciones_evidencias" to "authenticated";

grant select on table "public"."recolecciones_evidencias" to "authenticated";

grant trigger on table "public"."recolecciones_evidencias" to "authenticated";

grant truncate on table "public"."recolecciones_evidencias" to "authenticated";

grant update on table "public"."recolecciones_evidencias" to "authenticated";

grant delete on table "public"."recolecciones_evidencias" to "service_role";

grant insert on table "public"."recolecciones_evidencias" to "service_role";

grant references on table "public"."recolecciones_evidencias" to "service_role";

grant select on table "public"."recolecciones_evidencias" to "service_role";

grant trigger on table "public"."recolecciones_evidencias" to "service_role";

grant truncate on table "public"."recolecciones_evidencias" to "service_role";

grant update on table "public"."recolecciones_evidencias" to "service_role";

grant delete on table "public"."recolecciones_firmas" to "anon";

grant insert on table "public"."recolecciones_firmas" to "anon";

grant references on table "public"."recolecciones_firmas" to "anon";

grant select on table "public"."recolecciones_firmas" to "anon";

grant trigger on table "public"."recolecciones_firmas" to "anon";

grant truncate on table "public"."recolecciones_firmas" to "anon";

grant update on table "public"."recolecciones_firmas" to "anon";

grant delete on table "public"."recolecciones_firmas" to "authenticated";

grant insert on table "public"."recolecciones_firmas" to "authenticated";

grant references on table "public"."recolecciones_firmas" to "authenticated";

grant select on table "public"."recolecciones_firmas" to "authenticated";

grant trigger on table "public"."recolecciones_firmas" to "authenticated";

grant truncate on table "public"."recolecciones_firmas" to "authenticated";

grant update on table "public"."recolecciones_firmas" to "authenticated";

grant delete on table "public"."recolecciones_firmas" to "service_role";

grant insert on table "public"."recolecciones_firmas" to "service_role";

grant references on table "public"."recolecciones_firmas" to "service_role";

grant select on table "public"."recolecciones_firmas" to "service_role";

grant trigger on table "public"."recolecciones_firmas" to "service_role";

grant truncate on table "public"."recolecciones_firmas" to "service_role";

grant update on table "public"."recolecciones_firmas" to "service_role";

grant delete on table "public"."recolecciones_items" to "anon";

grant insert on table "public"."recolecciones_items" to "anon";

grant references on table "public"."recolecciones_items" to "anon";

grant select on table "public"."recolecciones_items" to "anon";

grant trigger on table "public"."recolecciones_items" to "anon";

grant truncate on table "public"."recolecciones_items" to "anon";

grant update on table "public"."recolecciones_items" to "anon";

grant delete on table "public"."recolecciones_items" to "authenticated";

grant insert on table "public"."recolecciones_items" to "authenticated";

grant references on table "public"."recolecciones_items" to "authenticated";

grant select on table "public"."recolecciones_items" to "authenticated";

grant trigger on table "public"."recolecciones_items" to "authenticated";

grant truncate on table "public"."recolecciones_items" to "authenticated";

grant update on table "public"."recolecciones_items" to "authenticated";

grant delete on table "public"."recolecciones_items" to "service_role";

grant insert on table "public"."recolecciones_items" to "service_role";

grant references on table "public"."recolecciones_items" to "service_role";

grant select on table "public"."recolecciones_items" to "service_role";

grant trigger on table "public"."recolecciones_items" to "service_role";

grant truncate on table "public"."recolecciones_items" to "service_role";

grant update on table "public"."recolecciones_items" to "service_role";

grant select on table "public"."saga_adjustments" to "authenticated";

grant select on table "public"."saga_compensations" to "authenticated";

grant delete on table "public"."saga_transactions" to "anon";

grant insert on table "public"."saga_transactions" to "anon";

grant references on table "public"."saga_transactions" to "anon";

grant select on table "public"."saga_transactions" to "anon";

grant trigger on table "public"."saga_transactions" to "anon";

grant truncate on table "public"."saga_transactions" to "anon";

grant update on table "public"."saga_transactions" to "anon";

grant delete on table "public"."saga_transactions" to "authenticated";

grant insert on table "public"."saga_transactions" to "authenticated";

grant references on table "public"."saga_transactions" to "authenticated";

grant select on table "public"."saga_transactions" to "authenticated";

grant trigger on table "public"."saga_transactions" to "authenticated";

grant truncate on table "public"."saga_transactions" to "authenticated";

grant update on table "public"."saga_transactions" to "authenticated";

grant delete on table "public"."saga_transactions" to "service_role";

grant insert on table "public"."saga_transactions" to "service_role";

grant references on table "public"."saga_transactions" to "service_role";

grant select on table "public"."saga_transactions" to "service_role";

grant trigger on table "public"."saga_transactions" to "service_role";

grant truncate on table "public"."saga_transactions" to "service_role";

grant update on table "public"."saga_transactions" to "service_role";

grant insert on table "public"."saga_zoho_links" to "authenticated";

grant select on table "public"."saga_zoho_links" to "authenticated";

grant update on table "public"."saga_zoho_links" to "authenticated";

grant select on table "public"."user_push_tokens" to "anon";

grant delete on table "public"."user_push_tokens" to "authenticated";

grant insert on table "public"."user_push_tokens" to "authenticated";

grant select on table "public"."user_push_tokens" to "authenticated";

grant update on table "public"."user_push_tokens" to "authenticated";

grant delete on table "public"."user_push_tokens" to "service_role";

grant insert on table "public"."user_push_tokens" to "service_role";

grant references on table "public"."user_push_tokens" to "service_role";

grant select on table "public"."user_push_tokens" to "service_role";

grant trigger on table "public"."user_push_tokens" to "service_role";

grant truncate on table "public"."user_push_tokens" to "service_role";

grant update on table "public"."user_push_tokens" to "service_role";

grant delete on table "public"."usuarios" to "anon";

grant insert on table "public"."usuarios" to "anon";

grant references on table "public"."usuarios" to "anon";

grant select on table "public"."usuarios" to "anon";

grant trigger on table "public"."usuarios" to "anon";

grant truncate on table "public"."usuarios" to "anon";

grant update on table "public"."usuarios" to "anon";

grant delete on table "public"."usuarios" to "authenticated";

grant insert on table "public"."usuarios" to "authenticated";

grant references on table "public"."usuarios" to "authenticated";

grant select on table "public"."usuarios" to "authenticated";

grant trigger on table "public"."usuarios" to "authenticated";

grant truncate on table "public"."usuarios" to "authenticated";

grant update on table "public"."usuarios" to "authenticated";

grant delete on table "public"."usuarios" to "service_role";

grant insert on table "public"."usuarios" to "service_role";

grant references on table "public"."usuarios" to "service_role";

grant select on table "public"."usuarios" to "service_role";

grant trigger on table "public"."usuarios" to "service_role";

grant truncate on table "public"."usuarios" to "service_role";

grant update on table "public"."usuarios" to "service_role";

grant delete on table "public"."ventas_odv" to "anon";

grant insert on table "public"."ventas_odv" to "anon";

grant references on table "public"."ventas_odv" to "anon";

grant select on table "public"."ventas_odv" to "anon";

grant trigger on table "public"."ventas_odv" to "anon";

grant truncate on table "public"."ventas_odv" to "anon";

grant update on table "public"."ventas_odv" to "anon";

grant delete on table "public"."ventas_odv" to "authenticated";

grant insert on table "public"."ventas_odv" to "authenticated";

grant references on table "public"."ventas_odv" to "authenticated";

grant select on table "public"."ventas_odv" to "authenticated";

grant trigger on table "public"."ventas_odv" to "authenticated";

grant truncate on table "public"."ventas_odv" to "authenticated";

grant update on table "public"."ventas_odv" to "authenticated";

grant delete on table "public"."ventas_odv" to "service_role";

grant insert on table "public"."ventas_odv" to "service_role";

grant references on table "public"."ventas_odv" to "service_role";

grant select on table "public"."ventas_odv" to "service_role";

grant trigger on table "public"."ventas_odv" to "service_role";

grant truncate on table "public"."ventas_odv" to "service_role";

grant update on table "public"."ventas_odv" to "service_role";

grant delete on table "public"."visit_tasks" to "anon";

grant insert on table "public"."visit_tasks" to "anon";

grant references on table "public"."visit_tasks" to "anon";

grant select on table "public"."visit_tasks" to "anon";

grant trigger on table "public"."visit_tasks" to "anon";

grant truncate on table "public"."visit_tasks" to "anon";

grant update on table "public"."visit_tasks" to "anon";

grant delete on table "public"."visit_tasks" to "authenticated";

grant insert on table "public"."visit_tasks" to "authenticated";

grant references on table "public"."visit_tasks" to "authenticated";

grant select on table "public"."visit_tasks" to "authenticated";

grant trigger on table "public"."visit_tasks" to "authenticated";

grant truncate on table "public"."visit_tasks" to "authenticated";

grant update on table "public"."visit_tasks" to "authenticated";

grant delete on table "public"."visit_tasks" to "service_role";

grant insert on table "public"."visit_tasks" to "service_role";

grant references on table "public"."visit_tasks" to "service_role";

grant select on table "public"."visit_tasks" to "service_role";

grant trigger on table "public"."visit_tasks" to "service_role";

grant truncate on table "public"."visit_tasks" to "service_role";

grant update on table "public"."visit_tasks" to "service_role";

grant delete on table "public"."visita_informes" to "anon";

grant insert on table "public"."visita_informes" to "anon";

grant references on table "public"."visita_informes" to "anon";

grant select on table "public"."visita_informes" to "anon";

grant trigger on table "public"."visita_informes" to "anon";

grant truncate on table "public"."visita_informes" to "anon";

grant update on table "public"."visita_informes" to "anon";

grant delete on table "public"."visita_informes" to "authenticated";

grant insert on table "public"."visita_informes" to "authenticated";

grant references on table "public"."visita_informes" to "authenticated";

grant select on table "public"."visita_informes" to "authenticated";

grant trigger on table "public"."visita_informes" to "authenticated";

grant truncate on table "public"."visita_informes" to "authenticated";

grant update on table "public"."visita_informes" to "authenticated";

grant delete on table "public"."visita_informes" to "service_role";

grant insert on table "public"."visita_informes" to "service_role";

grant references on table "public"."visita_informes" to "service_role";

grant select on table "public"."visita_informes" to "service_role";

grant trigger on table "public"."visita_informes" to "service_role";

grant truncate on table "public"."visita_informes" to "service_role";

grant update on table "public"."visita_informes" to "service_role";

grant delete on table "public"."visita_odvs" to "authenticated";

grant insert on table "public"."visita_odvs" to "authenticated";

grant references on table "public"."visita_odvs" to "authenticated";

grant select on table "public"."visita_odvs" to "authenticated";

grant trigger on table "public"."visita_odvs" to "authenticated";

grant truncate on table "public"."visita_odvs" to "authenticated";

grant update on table "public"."visita_odvs" to "authenticated";

grant delete on table "public"."visita_odvs" to "service_role";

grant insert on table "public"."visita_odvs" to "service_role";

grant references on table "public"."visita_odvs" to "service_role";

grant select on table "public"."visita_odvs" to "service_role";

grant trigger on table "public"."visita_odvs" to "service_role";

grant truncate on table "public"."visita_odvs" to "service_role";

grant update on table "public"."visita_odvs" to "service_role";

grant delete on table "public"."visitas" to "anon";

grant insert on table "public"."visitas" to "anon";

grant references on table "public"."visitas" to "anon";

grant select on table "public"."visitas" to "anon";

grant trigger on table "public"."visitas" to "anon";

grant truncate on table "public"."visitas" to "anon";

grant update on table "public"."visitas" to "anon";

grant delete on table "public"."visitas" to "authenticated";

grant insert on table "public"."visitas" to "authenticated";

grant references on table "public"."visitas" to "authenticated";

grant select on table "public"."visitas" to "authenticated";

grant trigger on table "public"."visitas" to "authenticated";

grant truncate on table "public"."visitas" to "authenticated";

grant update on table "public"."visitas" to "authenticated";

grant delete on table "public"."visitas" to "service_role";

grant insert on table "public"."visitas" to "service_role";

grant references on table "public"."visitas" to "service_role";

grant select on table "public"."visitas" to "service_role";

grant trigger on table "public"."visitas" to "service_role";

grant truncate on table "public"."visitas" to "service_role";

grant update on table "public"."visitas" to "service_role";

grant select on table "public"."zoho_health_status" to "authenticated";

grant delete on table "public"."zoho_tokens" to "anon";

grant insert on table "public"."zoho_tokens" to "anon";

grant references on table "public"."zoho_tokens" to "anon";

grant select on table "public"."zoho_tokens" to "anon";

grant trigger on table "public"."zoho_tokens" to "anon";

grant truncate on table "public"."zoho_tokens" to "anon";

grant update on table "public"."zoho_tokens" to "anon";

grant delete on table "public"."zoho_tokens" to "authenticated";

grant insert on table "public"."zoho_tokens" to "authenticated";

grant references on table "public"."zoho_tokens" to "authenticated";

grant select on table "public"."zoho_tokens" to "authenticated";

grant trigger on table "public"."zoho_tokens" to "authenticated";

grant truncate on table "public"."zoho_tokens" to "authenticated";

grant update on table "public"."zoho_tokens" to "authenticated";

grant delete on table "public"."zoho_tokens" to "service_role";

grant insert on table "public"."zoho_tokens" to "service_role";

grant references on table "public"."zoho_tokens" to "service_role";

grant select on table "public"."zoho_tokens" to "service_role";

grant trigger on table "public"."zoho_tokens" to "service_role";

grant truncate on table "public"."zoho_tokens" to "service_role";

grant update on table "public"."zoho_tokens" to "service_role";

grant delete on table "public"."zonas" to "anon";

grant insert on table "public"."zonas" to "anon";

grant references on table "public"."zonas" to "anon";

grant select on table "public"."zonas" to "anon";

grant trigger on table "public"."zonas" to "anon";

grant truncate on table "public"."zonas" to "anon";

grant update on table "public"."zonas" to "anon";

grant delete on table "public"."zonas" to "authenticated";

grant insert on table "public"."zonas" to "authenticated";

grant references on table "public"."zonas" to "authenticated";

grant select on table "public"."zonas" to "authenticated";

grant trigger on table "public"."zonas" to "authenticated";

grant truncate on table "public"."zonas" to "authenticated";

grant update on table "public"."zonas" to "authenticated";

grant delete on table "public"."zonas" to "service_role";

grant insert on table "public"."zonas" to "service_role";

grant references on table "public"."zonas" to "service_role";

grant select on table "public"."zonas" to "service_role";

grant trigger on table "public"."zonas" to "service_role";

grant truncate on table "public"."zonas" to "service_role";

grant update on table "public"."zonas" to "service_role";


  create policy "ciclos_delete"
  on "archive"."ciclos_botiquin"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "ciclos_insert"
  on "archive"."ciclos_botiquin"
  as permissive
  for insert
  to public
with check ((public.can_access_cliente((id_cliente)::text) AND ((id_usuario)::text = public.current_user_id())));



  create policy "ciclos_select"
  on "archive"."ciclos_botiquin"
  as permissive
  for select
  to public
using (public.can_access_cliente((id_cliente)::text));



  create policy "ciclos_update"
  on "archive"."ciclos_botiquin"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "service_role_all"
  on "archive"."encuestas_ciclo"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "service_role_all"
  on "public"."audit_log"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "botiquin_sku_modify_delete"
  on "public"."botiquin_clientes_sku_disponibles"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "botiquin_sku_modify_insert"
  on "public"."botiquin_clientes_sku_disponibles"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "botiquin_sku_modify_update"
  on "public"."botiquin_clientes_sku_disponibles"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "botiquin_sku_select"
  on "public"."botiquin_clientes_sku_disponibles"
  as permissive
  for select
  to public
using (public.can_access_cliente((id_cliente)::text));



  create policy "Service role full access"
  on "public"."botiquin_odv"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "Users can view their botiquin_odv"
  on "public"."botiquin_odv"
  as permissive
  for select
  to authenticated
using ((EXISTS ( SELECT 1
   FROM public.clientes c
  WHERE (((c.id_cliente)::text = (botiquin_odv.id_cliente)::text) AND ((c.id_usuario)::text = public.current_user_id())))));



  create policy "Admins pueden insertar logs"
  on "public"."cliente_estado_log"
  as permissive
  for insert
  to authenticated
with check ((EXISTS ( SELECT 1
   FROM public.usuarios
  WHERE ((usuarios.auth_user_id = auth.uid()) AND (usuarios.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "Admins pueden ver logs de estado"
  on "public"."cliente_estado_log"
  as permissive
  for select
  to authenticated
using ((EXISTS ( SELECT 1
   FROM public.usuarios
  WHERE (((usuarios.id_usuario)::text = (auth.jwt() ->> 'sub'::text)) AND (usuarios.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "clientes_modify_delete"
  on "public"."clientes"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "clientes_modify_insert"
  on "public"."clientes"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "clientes_modify_update"
  on "public"."clientes"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "clientes_select"
  on "public"."clientes"
  as permissive
  for select
  to public
using (public.can_access_cliente((id_cliente)::text));



  create policy "service_role_all"
  on "public"."event_outbox"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "inventario_select"
  on "public"."inventario_botiquin"
  as permissive
  for select
  to authenticated
using (public.can_access_cliente((id_cliente)::text));



  create policy "service_role_all"
  on "public"."inventario_botiquin"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "medicamento_padecimientos_modify_delete"
  on "public"."medicamento_padecimientos"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "medicamento_padecimientos_modify_insert"
  on "public"."medicamento_padecimientos"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "medicamento_padecimientos_modify_update"
  on "public"."medicamento_padecimientos"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "medicamento_padecimientos_select"
  on "public"."medicamento_padecimientos"
  as permissive
  for select
  to public
using ((( SELECT ( SELECT auth.role() AS role) AS role) = 'authenticated'::text));



  create policy "medicamentos_modify_delete"
  on "public"."medicamentos"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "medicamentos_modify_insert"
  on "public"."medicamentos"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "medicamentos_modify_update"
  on "public"."medicamentos"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "medicamentos_select"
  on "public"."medicamentos"
  as permissive
  for select
  to public
using ((( SELECT ( SELECT auth.role() AS role) AS role) = 'authenticated'::text));



  create policy "service_role_all"
  on "public"."movimientos_inventario"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "Admins pueden actualizar notificaciones"
  on "public"."notificaciones_admin"
  as permissive
  for update
  to authenticated
using ((EXISTS ( SELECT 1
   FROM public.usuarios u
  WHERE ((u.auth_user_id = auth.uid()) AND (u.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "Admins pueden insertar notificaciones"
  on "public"."notificaciones_admin"
  as permissive
  for insert
  to authenticated
with check ((EXISTS ( SELECT 1
   FROM public.usuarios
  WHERE ((usuarios.auth_user_id = auth.uid()) AND (usuarios.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "Admins ven notificaciones"
  on "public"."notificaciones_admin"
  as permissive
  for select
  to authenticated
using ((EXISTS ( SELECT 1
   FROM public.usuarios u
  WHERE ((u.auth_user_id = auth.uid()) AND (u.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario])) AND ((notificaciones_admin.para_usuario IS NULL) OR ((notificaciones_admin.para_usuario)::text = (u.id_usuario)::text))))));



  create policy "notifications_select"
  on "public"."notifications"
  as permissive
  for select
  to public
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "notifications_update"
  on "public"."notifications"
  as permissive
  for update
  to public
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "padecimientos_modify_delete"
  on "public"."padecimientos"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "padecimientos_modify_insert"
  on "public"."padecimientos"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "padecimientos_modify_update"
  on "public"."padecimientos"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "padecimientos_select"
  on "public"."padecimientos"
  as permissive
  for select
  to public
using ((( SELECT ( SELECT auth.role() AS role) AS role) = 'authenticated'::text));



  create policy "admin_recolecciones_select"
  on "public"."recolecciones"
  as permissive
  for select
  to public
using (public.is_admin());



  create policy "usuarios_recolecciones_insert"
  on "public"."recolecciones"
  as permissive
  for insert
  to public
with check (((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_select"
  on "public"."recolecciones"
  as permissive
  for select
  to public
using (((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_update"
  on "public"."recolecciones"
  as permissive
  for update
  to public
using (((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid()))));



  create policy "admin_recolecciones_evidencias_select"
  on "public"."recolecciones_evidencias"
  as permissive
  for select
  to public
using (public.is_admin());



  create policy "usuarios_recolecciones_evidencias_insert"
  on "public"."recolecciones_evidencias"
  as permissive
  for insert
  to public
with check ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_evidencias_select"
  on "public"."recolecciones_evidencias"
  as permissive
  for select
  to public
using ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "admin_recolecciones_firmas_select"
  on "public"."recolecciones_firmas"
  as permissive
  for select
  to public
using (public.is_admin());



  create policy "usuarios_recolecciones_firmas_insert"
  on "public"."recolecciones_firmas"
  as permissive
  for insert
  to public
with check ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_firmas_select"
  on "public"."recolecciones_firmas"
  as permissive
  for select
  to public
using ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_firmas_update"
  on "public"."recolecciones_firmas"
  as permissive
  for update
  to public
using ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "admin_recolecciones_items_select"
  on "public"."recolecciones_items"
  as permissive
  for select
  to public
using (public.is_admin());



  create policy "usuarios_recolecciones_items_insert"
  on "public"."recolecciones_items"
  as permissive
  for insert
  to public
with check ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_recolecciones_items_select"
  on "public"."recolecciones_items"
  as permissive
  for select
  to public
using ((recoleccion_id IN ( SELECT r.recoleccion_id
   FROM (public.recolecciones r
     JOIN public.usuarios u ON (((r.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "saga_adjustments_select_authenticated"
  on "public"."saga_adjustments"
  as permissive
  for select
  to authenticated
using (true);



  create policy "saga_compensations_select_authenticated"
  on "public"."saga_compensations"
  as permissive
  for select
  to authenticated
using (true);



  create policy "service_role_all"
  on "public"."saga_transactions"
  as permissive
  for all
  to service_role
using (true)
with check (true);



  create policy "Users can insert saga_zoho_links"
  on "public"."saga_zoho_links"
  as permissive
  for insert
  to authenticated
with check ((EXISTS ( SELECT 1
   FROM (public.saga_transactions st
     JOIN public.visitas v ON ((v.visit_id = st.visit_id)))
  WHERE ((st.id = saga_zoho_links.id_saga_transaction) AND public.can_access_visita(v.visit_id)))));



  create policy "Users can update saga_zoho_links"
  on "public"."saga_zoho_links"
  as permissive
  for update
  to authenticated
using ((EXISTS ( SELECT 1
   FROM (public.saga_transactions st
     JOIN public.visitas v ON ((v.visit_id = st.visit_id)))
  WHERE ((st.id = saga_zoho_links.id_saga_transaction) AND public.can_access_visita(v.visit_id)))));



  create policy "Users can view saga_zoho_links"
  on "public"."saga_zoho_links"
  as permissive
  for select
  to authenticated
using ((EXISTS ( SELECT 1
   FROM (public.saga_transactions st
     JOIN public.visitas v ON ((v.visit_id = st.visit_id)))
  WHERE ((st.id = saga_zoho_links.id_saga_transaction) AND public.can_access_visita(v.visit_id)))));



  create policy "preferences_insert"
  on "public"."user_notification_preferences"
  as permissive
  for insert
  to public
with check (((user_id)::text = (public.get_current_user_id())::text));



  create policy "preferences_select"
  on "public"."user_notification_preferences"
  as permissive
  for select
  to public
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "preferences_update"
  on "public"."user_notification_preferences"
  as permissive
  for update
  to public
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "push_tokens_delete"
  on "public"."user_push_tokens"
  as permissive
  for delete
  to authenticated
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "push_tokens_insert"
  on "public"."user_push_tokens"
  as permissive
  for insert
  to authenticated
with check (((user_id)::text = (public.get_current_user_id())::text));



  create policy "push_tokens_select"
  on "public"."user_push_tokens"
  as permissive
  for select
  to authenticated
using (((user_id)::text = (public.get_current_user_id())::text));



  create policy "push_tokens_update"
  on "public"."user_push_tokens"
  as permissive
  for update
  to authenticated
using (((user_id)::text = (public.get_current_user_id())::text))
with check (((user_id)::text = (public.get_current_user_id())::text));



  create policy "own_profile_read"
  on "public"."usuarios"
  as permissive
  for select
  to authenticated
using ((( SELECT auth.uid() AS uid) = auth_user_id));



  create policy "own_profile_update"
  on "public"."usuarios"
  as permissive
  for update
  to authenticated
using ((( SELECT auth.uid() AS uid) = auth_user_id))
with check ((( SELECT auth.uid() AS uid) = auth_user_id));



  create policy "usuarios_modify_delete"
  on "public"."usuarios"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "usuarios_modify_insert"
  on "public"."usuarios"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "usuarios_modify_update"
  on "public"."usuarios"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "usuarios_select"
  on "public"."usuarios"
  as permissive
  for select
  to public
using (((( SELECT ( SELECT auth.uid() AS uid) AS uid) = auth_user_id) OR public.is_admin()));



  create policy "ventas_odv_delete"
  on "public"."ventas_odv"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "ventas_odv_insert"
  on "public"."ventas_odv"
  as permissive
  for insert
  to public
with check (public.can_access_cliente((id_cliente)::text));



  create policy "ventas_odv_select"
  on "public"."ventas_odv"
  as permissive
  for select
  to public
using (public.can_access_cliente((id_cliente)::text));



  create policy "ventas_odv_update"
  on "public"."ventas_odv"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "usuarios_visit_tasks_insert"
  on "public"."visit_tasks"
  as permissive
  for insert
  to public
with check ((visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_visit_tasks_select"
  on "public"."visit_tasks"
  as permissive
  for select
  to public
using ((public.is_admin() OR (visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid())))));



  create policy "usuarios_visit_tasks_update"
  on "public"."visit_tasks"
  as permissive
  for update
  to public
using ((public.is_admin() OR (visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid())))));



  create policy "admin_visita_informes_insert"
  on "public"."visita_informes"
  as permissive
  for insert
  to public
with check ((EXISTS ( SELECT 1
   FROM public.usuarios u
  WHERE ((u.auth_user_id = auth.uid()) AND (u.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "admin_visita_informes_select"
  on "public"."visita_informes"
  as permissive
  for select
  to public
using ((EXISTS ( SELECT 1
   FROM public.usuarios u
  WHERE ((u.auth_user_id = auth.uid()) AND (u.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "admin_visita_informes_update"
  on "public"."visita_informes"
  as permissive
  for update
  to public
using ((EXISTS ( SELECT 1
   FROM public.usuarios u
  WHERE ((u.auth_user_id = auth.uid()) AND (u.rol = ANY (ARRAY['ADMINISTRADOR'::public.rol_usuario, 'OWNER'::public.rol_usuario]))))));



  create policy "usuarios_visita_informes_insert"
  on "public"."visita_informes"
  as permissive
  for insert
  to public
with check ((visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_visita_informes_select"
  on "public"."visita_informes"
  as permissive
  for select
  to public
using ((visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "usuarios_visita_informes_update"
  on "public"."visita_informes"
  as permissive
  for update
  to public
using ((visit_id IN ( SELECT v.visit_id
   FROM (public.visitas v
     JOIN public.usuarios u ON (((v.id_usuario)::text = (u.id_usuario)::text)))
  WHERE (u.auth_user_id = auth.uid()))));



  create policy "Users can view their visita_odvs"
  on "public"."visita_odvs"
  as permissive
  for select
  to authenticated
using ((EXISTS ( SELECT 1
   FROM public.visitas v
  WHERE ((v.visit_id = visita_odvs.visit_id) AND public.can_access_visita(v.visit_id)))));



  create policy "usuarios_visitas_insert"
  on "public"."visitas"
  as permissive
  for insert
  to public
with check (((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid()))));



  create policy "usuarios_visitas_select"
  on "public"."visitas"
  as permissive
  for select
  to public
using ((public.is_admin() OR ((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid())))));



  create policy "usuarios_visitas_update"
  on "public"."visitas"
  as permissive
  for update
  to public
using ((public.is_admin() OR ((id_usuario)::text IN ( SELECT usuarios.id_usuario
   FROM public.usuarios
  WHERE (usuarios.auth_user_id = auth.uid())))));



  create policy "zoho_health_status_select_authenticated"
  on "public"."zoho_health_status"
  as permissive
  for select
  to authenticated
using (true);



  create policy "zoho_tokens_owner"
  on "public"."zoho_tokens"
  as permissive
  for all
  to public
using ((( SELECT ( SELECT auth.uid() AS uid) AS uid) = auth_user_id))
with check ((( SELECT ( SELECT auth.uid() AS uid) AS uid) = auth_user_id));



  create policy "zonas_modify_delete"
  on "public"."zonas"
  as permissive
  for delete
  to public
using (public.is_admin());



  create policy "zonas_modify_insert"
  on "public"."zonas"
  as permissive
  for insert
  to public
with check (public.is_admin());



  create policy "zonas_modify_update"
  on "public"."zonas"
  as permissive
  for update
  to public
using (public.is_admin())
with check (public.is_admin());



  create policy "zonas_select"
  on "public"."zonas"
  as permissive
  for select
  to public
using ((( SELECT ( SELECT auth.role() AS role) AS role) = 'authenticated'::text));


CREATE TRIGGER update_encuestas_ciclo_updated_at BEFORE UPDATE ON archive.encuestas_ciclo FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

CREATE TRIGGER trg_cliente_estado_log_dias BEFORE INSERT ON public.cliente_estado_log FOR EACH ROW EXECUTE FUNCTION public.fn_cliente_estado_log_dias();

CREATE TRIGGER trg_notificar_cambio_estado AFTER INSERT ON public.cliente_estado_log FOR EACH ROW EXECUTE FUNCTION public.fn_notificar_cambio_estado();

CREATE TRIGGER trg_sync_cliente_activo BEFORE INSERT OR UPDATE OF estado ON public.clientes FOR EACH ROW EXECUTE FUNCTION public.fn_sync_cliente_activo();

CREATE TRIGGER audit_movimientos_inventario AFTER INSERT OR DELETE OR UPDATE ON public.movimientos_inventario FOR EACH ROW EXECUTE FUNCTION public.audit_trigger_func();

CREATE TRIGGER trg_remove_sku_disponible_on_venta AFTER INSERT ON public.movimientos_inventario FOR EACH ROW EXECUTE FUNCTION public.fn_remove_sku_disponible_on_venta();

CREATE TRIGGER "push-notification-webhook │" AFTER INSERT ON public.notifications FOR EACH ROW EXECUTE FUNCTION supabase_functions.http_request('https://ysxmbijpskjpwuvuklag.supabase.co/functions/v1/push-notification', 'POST', '{}', '{}', '5000');

CREATE TRIGGER audit_saga_transactions AFTER INSERT OR DELETE OR UPDATE ON public.saga_transactions FOR EACH ROW EXECUTE FUNCTION public.audit_trigger_func();

CREATE TRIGGER saga_transactions_audit AFTER INSERT OR DELETE OR UPDATE ON public.saga_transactions FOR EACH ROW EXECUTE FUNCTION public.audit_saga_transactions();

CREATE TRIGGER update_saga_transactions_updated_at BEFORE UPDATE ON public.saga_transactions FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

CREATE TRIGGER validate_saga_items_unique BEFORE INSERT OR UPDATE ON public.saga_transactions FOR EACH ROW WHEN ((new.items IS NOT NULL)) EXECUTE FUNCTION public.validate_unique_skus_in_items();

CREATE TRIGGER trg_notify_task_status AFTER UPDATE ON public.visit_tasks FOR EACH ROW EXECUTE FUNCTION public.trigger_notify_task_completed();

CREATE TRIGGER trg_notify_visit_completed AFTER UPDATE ON public.visitas FOR EACH ROW EXECUTE FUNCTION public.notify_visit_completed();

CREATE TRIGGER zoho_tokens_set_updated_at BEFORE UPDATE ON public.zoho_tokens FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


  create policy "medicaments_technical_sheet_delete_admin"
  on "storage"."objects"
  as permissive
  for delete
  to authenticated
using (((bucket_id = 'medicaments-technical-sheet'::text) AND public.is_admin()));



  create policy "medicaments_technical_sheet_insert_admin"
  on "storage"."objects"
  as permissive
  for insert
  to authenticated
with check (((bucket_id = 'medicaments-technical-sheet'::text) AND public.is_admin()));



  create policy "medicaments_technical_sheet_select_authenticated"
  on "storage"."objects"
  as permissive
  for select
  to authenticated
using ((bucket_id = 'medicaments-technical-sheet'::text));



  create policy "medicaments_technical_sheet_update_admin"
  on "storage"."objects"
  as permissive
  for update
  to authenticated
using (((bucket_id = 'medicaments-technical-sheet'::text) AND public.is_admin()))
with check (((bucket_id = 'medicaments-technical-sheet'::text) AND public.is_admin()));



  create policy "survey_evidence_delete"
  on "storage"."objects"
  as permissive
  for delete
  to authenticated
using ((bucket_id = 'survey-evidence'::text));



  create policy "survey_evidence_insert"
  on "storage"."objects"
  as permissive
  for insert
  to authenticated
with check ((bucket_id = 'survey-evidence'::text));



  create policy "survey_evidence_select"
  on "storage"."objects"
  as permissive
  for select
  to authenticated
using ((bucket_id = 'survey-evidence'::text));



  create policy "survey_evidence_update"
  on "storage"."objects"
  as permissive
  for update
  to authenticated
using ((bucket_id = 'survey-evidence'::text))
with check ((bucket_id = 'survey-evidence'::text));



