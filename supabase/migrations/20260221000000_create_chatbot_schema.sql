-- ============================================================================
-- Migration: Create chatbot schema for SyntIA assistant
-- Creates: schema, tables, RLS policies, and helper functions
-- ============================================================================

BEGIN;

-- ============================================================================
-- 1. Create schema
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS chatbot;

-- ============================================================================
-- 2. Tables
-- ============================================================================

-- Configuration key-value store (system prompt lives here)
CREATE TABLE chatbot.config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Conversation sessions
CREATE TABLE chatbot.conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_usuario VARCHAR(150) NOT NULL REFERENCES public.usuarios(id_usuario),
    summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_conversations_usuario ON chatbot.conversations(id_usuario);
CREATE INDEX idx_conversations_created ON chatbot.conversations(created_at DESC);

-- Individual messages
CREATE TABLE chatbot.messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES chatbot.conversations(id) ON DELETE CASCADE,
    role VARCHAR(10) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    context_cliente_id VARCHAR(150) REFERENCES public.clientes(id_cliente),
    tokens_input INTEGER,
    tokens_output INTEGER,
    latency_ms INTEGER,
    rating SMALLINT CHECK (rating >= 1 AND rating <= 5),
    rated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_messages_conversation ON chatbot.messages(conversation_id, created_at);
CREATE INDEX idx_messages_rating ON chatbot.messages(rating) WHERE rating IS NOT NULL;

-- Daily usage tracking per user
CREATE TABLE chatbot.usage_limits (
    id_usuario VARCHAR(150) NOT NULL REFERENCES public.usuarios(id_usuario),
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    queries_used INTEGER NOT NULL DEFAULT 0,
    queries_limit INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id_usuario, fecha)
);

-- ============================================================================
-- 3. RLS Policies
-- ============================================================================

ALTER TABLE chatbot.config ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE chatbot.usage_limits ENABLE ROW LEVEL SECURITY;

-- Config: read-only for authenticated, write for service_role only
CREATE POLICY config_read ON chatbot.config
    FOR SELECT TO authenticated
    USING (true);

-- Conversations: users see own, admins see all
CREATE POLICY conversations_own ON chatbot.conversations
    FOR ALL TO authenticated
    USING (
        id_usuario = (SELECT u.id_usuario FROM public.usuarios u WHERE u.auth_user_id = auth.uid())
        OR EXISTS (
            SELECT 1 FROM public.usuarios u
            WHERE u.auth_user_id = auth.uid()
            AND u.rol IN ('OWNER', 'ADMINISTRADOR')
        )
    );

-- Messages: users see messages in their conversations, admins see all
CREATE POLICY messages_read ON chatbot.messages
    FOR SELECT TO authenticated
    USING (
        conversation_id IN (
            SELECT c.id FROM chatbot.conversations c
            WHERE c.id_usuario = (SELECT u.id_usuario FROM public.usuarios u WHERE u.auth_user_id = auth.uid())
        )
        OR EXISTS (
            SELECT 1 FROM public.usuarios u
            WHERE u.auth_user_id = auth.uid()
            AND u.rol IN ('OWNER', 'ADMINISTRADOR')
        )
    );

-- Messages: users can update rating only on assistant messages in their conversations
CREATE POLICY messages_rate ON chatbot.messages
    FOR UPDATE TO authenticated
    USING (
        role = 'assistant'
        AND conversation_id IN (
            SELECT c.id FROM chatbot.conversations c
            WHERE c.id_usuario = (SELECT u.id_usuario FROM public.usuarios u WHERE u.auth_user_id = auth.uid())
        )
    )
    WITH CHECK (
        role = 'assistant'
        AND conversation_id IN (
            SELECT c.id FROM chatbot.conversations c
            WHERE c.id_usuario = (SELECT u.id_usuario FROM public.usuarios u WHERE u.auth_user_id = auth.uid())
        )
    );

-- Usage limits: users see own, admins see all
CREATE POLICY usage_own ON chatbot.usage_limits
    FOR SELECT TO authenticated
    USING (
        id_usuario = (SELECT u.id_usuario FROM public.usuarios u WHERE u.auth_user_id = auth.uid())
        OR EXISTS (
            SELECT 1 FROM public.usuarios u
            WHERE u.auth_user_id = auth.uid()
            AND u.rol IN ('OWNER', 'ADMINISTRADOR')
        )
    );

-- ============================================================================
-- 4. Functions (all in chatbot schema, no public wrappers)
-- ============================================================================

-- Atomic check-and-increment usage (immune to race conditions)
CREATE OR REPLACE FUNCTION chatbot.check_and_increment_usage(
    p_id_usuario VARCHAR(150),
    p_rol TEXT
)
RETURNS TABLE (
    allowed BOOLEAN,
    queries_used INTEGER,
    queries_limit INTEGER,
    remaining INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'chatbot'
AS $function$
DECLARE
    v_limit INTEGER;
    v_used INTEGER;
    v_total INTEGER;
BEGIN
    -- Determine limit by role
    IF p_rol IN ('OWNER', 'ADMINISTRADOR') THEN
        v_limit := 20;
    ELSE
        v_limit := 8;
    END IF;

    -- Upsert to ensure record exists
    INSERT INTO chatbot.usage_limits (id_usuario, fecha, queries_used, queries_limit)
    VALUES (p_id_usuario, CURRENT_DATE, 0, v_limit)
    ON CONFLICT (id_usuario, fecha) DO NOTHING;

    -- Atomic increment with row-level lock
    UPDATE chatbot.usage_limits ul
    SET queries_used = ul.queries_used + 1, updated_at = now()
    WHERE ul.id_usuario = p_id_usuario
      AND ul.fecha = CURRENT_DATE
      AND ul.queries_used < ul.queries_limit
    RETURNING ul.queries_used, ul.queries_limit
    INTO v_used, v_total;

    IF v_used IS NOT NULL THEN
        -- Increment succeeded
        RETURN QUERY SELECT
            true AS allowed,
            v_used AS queries_used,
            v_total AS queries_limit,
            (v_total - v_used) AS remaining;
    ELSE
        -- Limit exceeded, read current values
        SELECT ul.queries_used, ul.queries_limit
        INTO v_used, v_total
        FROM chatbot.usage_limits ul
        WHERE ul.id_usuario = p_id_usuario
          AND ul.fecha = CURRENT_DATE;

        RETURN QUERY SELECT
            false AS allowed,
            COALESCE(v_used, 0) AS queries_used,
            COALESCE(v_total, v_limit) AS queries_limit,
            0 AS remaining;
    END IF;
END;
$function$;

-- Read-only: get remaining queries without consuming
CREATE OR REPLACE FUNCTION chatbot.get_remaining_queries(
    p_id_usuario VARCHAR(150),
    p_rol TEXT
)
RETURNS TABLE (
    queries_used INTEGER,
    queries_limit INTEGER,
    remaining INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'chatbot'
AS $function$
DECLARE
    v_limit INTEGER;
    v_used INTEGER;
BEGIN
    IF p_rol IN ('OWNER', 'ADMINISTRADOR') THEN
        v_limit := 20;
    ELSE
        v_limit := 8;
    END IF;

    SELECT ul.queries_used INTO v_used
    FROM chatbot.usage_limits ul
    WHERE ul.id_usuario = p_id_usuario
      AND ul.fecha = CURRENT_DATE;

    IF v_used IS NULL THEN
        v_used := 0;
    END IF;

    RETURN QUERY SELECT
        v_used AS queries_used,
        v_limit AS queries_limit,
        GREATEST(v_limit - v_used, 0) AS remaining;
END;
$function$;

-- Rollback usage on Gemini failure (only callable by service_role)
CREATE OR REPLACE FUNCTION chatbot.rollback_usage(
    p_id_usuario VARCHAR(150)
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'chatbot'
AS $function$
BEGIN
    UPDATE chatbot.usage_limits
    SET queries_used = GREATEST(queries_used - 1, 0), updated_at = now()
    WHERE id_usuario = p_id_usuario
      AND fecha = CURRENT_DATE;
END;
$function$;

-- ============================================================================
-- 5. Seed data: system prompt
-- ============================================================================

INSERT INTO chatbot.config (key, value) VALUES ('system_prompt', E'Eres SyntIA, asistente de informacion para el equipo de DermiTrack.\n\nACCESO A DATOS — SOLO puedes consultar y responder sobre:\n- inventario_botiquin: estado actual del botiquin, SOLO de los medicos que aparecen en tu contexto (ya filtrados por asignacion)\n- movimientos_inventario: historial de creaciones, ventas, recolecciones y permanencias de SKUs\n- ventas_odv: ventas recurrentes facturadas via ordenes de venta de Zoho\n- botiquin_odv: ordenes de consignacion inicial\n- medicamentos: catalogo de productos (SKU, marca, descripcion, precio, contenido)\n- padecimientos: condiciones medicas y su relacion con productos\n- fichas_tecnicas: composicion, indicaciones y presentacion de productos que tengan ficha disponible\n- estadisticas generales: quien ha vendido mas, como van las ventas de un medicamento, tendencias\n\nRESTRICCION DE INVENTARIO:\n- Los datos de inventario que recibes en el contexto YA estan filtrados. Solo contienen medicos autorizados para este usuario.\n- Si el usuario pregunta por un medico cuyo inventario NO aparece en tu contexto, responde EXACTAMENTE: \"No cuentas con acceso al inventario de ese medico. Por favor contacta al administrador.\"\n- NUNCA intentes deducir o inventar datos de inventario que no estan en el contexto.\n\nPROHIBIDO:\n- Datos personales de usuarios, medicos o pacientes\n- Informacion financiera fuera de ventas de productos\n- Cualquier tabla o dato no listado arriba\nSi te piden algo fuera de tu alcance, responde EXACTAMENTE: \"No cuento con acceso a esa informacion. Por favor contacta al administrador.\"\n\nPATRON DE RECOMENDACION — \"Que le puedo ofrecer al medico X?\":\nCuando el usuario pregunte que ofrecer a un medico, presenta DOS opciones claras:\n\n1. VENTA RECURRENTE: Recomienda productos que el medico YA ha comprado desde el botiquin (movimientos tipo VENTA en movimientos_inventario). Estos son productos probados que el medico ya conoce y adopto. Prioriza los de mayor volumen/frecuencia.\n\n2. RELLENAR BOTIQUIN: Recomienda productos que han alcanzado clasificacion M2 (conversion: botiquin->ODV, el medico los probo y luego los compro por fuera) o M3 (exposicion->ODV, el medico los vio en botiquin y luego compro). Estos son productos con traccion demostrada. NO recomiendes productos M1 (solo en botiquin, sin conversion) para rellenar — prioriza los que funcionan.\n\nSiempre guia al usuario hacia decisiones basadas en datos de lo que ha funcionado. Nunca recomiendes productos sin evidencia de traccion.\n\nFORMATO DE RESPUESTA:\n1. Responde SIEMPRE en espanol\n2. Tono ejecutivo, amable y directo. Sin rodeos, sin explicar tu proceso de analisis\n3. Entrega SOLO lo que se pidio. No agregues informacion extra, contexto adicional ni sugerencias no solicitadas\n4. Rigor estadistico: cita cifras exactas del contexto. Si un dato no esta disponible, di que no lo tienes. NUNCA inventes numeros\n5. NO des pitch de ventas ni lenguaje comercial persuasivo\n6. NO muestres tu proceso de razonamiento. Entrega directamente la respuesta\n7. Maximo 3 parrafos. Usa listas cuando sea mas claro\n8. Si no tienes datos suficientes, responde: \"No cuento con informacion suficiente para responder eso.\"');

COMMIT;
