import { createClient } from "npm:@supabase/supabase-js@2.45.4";

// ============================================================================
// Environment Variables
// ============================================================================

export const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
export const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
export const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
export const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

// Cloud Run ADK agent (preferred — set CLOUD_RUN_URL to enable)
export const CLOUD_RUN_URL = Deno.env.get("CLOUD_RUN_URL") ?? "";

// Agent Engine feature flag (legacy fallback)
export const USE_AGENT_ENGINE = Deno.env.get("USE_AGENT_ENGINE") === "true";
export const AGENT_ENGINE_RESOURCE_NAME = Deno.env.get("AGENT_ENGINE_RESOURCE_NAME") ?? "";

// ============================================================================
// Model & Tuning Constants
// ============================================================================

export const VERTEX_MODEL = "gemini-2.5-flash";
export const EMBEDDING_MODEL = "text-embedding-005";
export const EMBEDDING_DIMENSION = 768;
export const SYSTEM_PROMPT_CACHE_TTL = 10 * 60 * 1000;
export const TOKEN_CACHE_TTL = 55 * 60 * 1000;
export const COMPACTION_THRESHOLD = 8;
export const MAX_HISTORY_MESSAGES = 8;
export const MAX_TOOL_ITERATIONS = 5;
export const MAX_TOOL_RESULT_LENGTH = 8000;
export const RATE_LIMIT_MESSAGE =
  "Los creditos diarios se han agotado. Si requieres mas, contactate con el administrador.";

// ============================================================================
// CORS Headers
// ============================================================================

export const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

// ============================================================================
// Supabase Clients
// ============================================================================

export const admin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

export const chatbot = admin.schema("chatbot");
