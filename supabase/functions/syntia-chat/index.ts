import { createClient } from "npm:@supabase/supabase-js@2.45.4";

// ============================================================================
// Types
// ============================================================================

interface UserInfo {
  id_usuario: string;
  rol: string;
  auth_user_id: string;
}

interface UsageResult {
  allowed: boolean;
  queries_used: number;
  queries_limit: number;
  remaining: number;
}

interface GeminiResponse {
  candidates?: Array<{
    content?: { parts?: Array<{ text?: string }> };
  }>;
  usageMetadata?: {
    promptTokenCount?: number;
    candidatesTokenCount?: number;
  };
}

// ============================================================================
// Constants
// ============================================================================

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
const GCP_REGION = Deno.env.get("GCP_REGION") || "us-central1";
const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

const VERTEX_MODEL = "gemini-3-flash-preview";
const CATALOG_CACHE_TTL = 10 * 60 * 1000; // 10 minutes
const SYSTEM_PROMPT_CACHE_TTL = 10 * 60 * 1000; // 10 minutes
const TOKEN_CACHE_TTL = 55 * 60 * 1000; // 55 minutes
const COMPACTION_THRESHOLD = 8;
const MAX_HISTORY_MESSAGES = 8;
const RATE_LIMIT_MESSAGE = "Los creditos diarios se han agotado. Si requieres mas, contactate con el administrador.";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

const admin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// Schema-scoped client for chatbot tables/functions
const chatbot = admin.schema("chatbot");

// ============================================================================
// Caches
// ============================================================================

let catalogCache: { data: string; expiry: number } | null = null;
let systemPromptCache: { data: string; expiry: number } | null = null;
let accessTokenCache: { token: string; expiry: number } | null = null;

// ============================================================================
// Vertex AI Auth (Service Account JWT → OAuth2 Token)
// ============================================================================

function base64UrlEncode(data: Uint8Array): string {
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function createSignedJwt(
  serviceAccount: { client_email: string; private_key: string }
): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  const payload = {
    iss: serviceAccount.client_email,
    scope: "https://www.googleapis.com/auth/cloud-platform",
    aud: "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600,
  };

  const encoder = new TextEncoder();
  const headerB64 = base64UrlEncode(encoder.encode(JSON.stringify(header)));
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)));
  const signingInput = `${headerB64}.${payloadB64}`;

  // Import the private key
  const pemContents = serviceAccount.private_key
    .replace(/-----BEGIN PRIVATE KEY-----/g, "")
    .replace(/-----END PRIVATE KEY-----/g, "")
    .replace(/\s/g, "");
  const binaryDer = Uint8Array.from(atob(pemContents), (c) => c.charCodeAt(0));

  const cryptoKey = await crypto.subtle.importKey(
    "pkcs8",
    binaryDer,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signature = await crypto.subtle.sign(
    "RSASSA-PKCS1-v1_5",
    cryptoKey,
    encoder.encode(signingInput)
  );

  const signatureB64 = base64UrlEncode(new Uint8Array(signature));
  return `${signingInput}.${signatureB64}`;
}

async function getAccessToken(): Promise<string> {
  if (accessTokenCache && Date.now() < accessTokenCache.expiry) {
    return accessTokenCache.token;
  }

  const keyJson = JSON.parse(atob(GCP_SERVICE_ACCOUNT_KEY));
  const jwt = await createSignedJwt(keyJson);

  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`OAuth2 token exchange failed: ${err}`);
  }

  const data = await res.json();
  accessTokenCache = {
    token: data.access_token,
    expiry: Date.now() + TOKEN_CACHE_TTL,
  };
  return data.access_token;
}

// ============================================================================
// Gemini API
// ============================================================================

async function callGemini(
  systemPrompt: string,
  contents: Array<{ role: string; parts: Array<{ text: string }> }>
): Promise<{ text: string; tokensInput: number; tokensOutput: number }> {
  const token = await getAccessToken();
  // Gemini 3 models require the global endpoint
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:generateContent`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: systemPrompt }] },
      contents,
      generationConfig: {
        maxOutputTokens: 1024,
        temperature: 0.3,
      },
    }),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Gemini API error (${res.status}): ${err}`);
  }

  const data: GeminiResponse = await res.json();
  const text = data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
  const tokensInput = data.usageMetadata?.promptTokenCount ?? 0;
  const tokensOutput = data.usageMetadata?.candidatesTokenCount ?? 0;

  return { text, tokensInput, tokensOutput };
}

// ============================================================================
// Data Helpers
// ============================================================================

async function getSystemPrompt(): Promise<string> {
  if (systemPromptCache && Date.now() < systemPromptCache.expiry) {
    return systemPromptCache.data;
  }

  const { data, error } = await chatbot
    .from("config")
    .select("value")
    .eq("key", "system_prompt")
    .single();

  if (error || !data) {
    throw new Error("System prompt not found in chatbot.config");
  }

  systemPromptCache = { data: data.value, expiry: Date.now() + SYSTEM_PROMPT_CACHE_TTL };
  return data.value;
}

async function getCatalog(): Promise<string> {
  if (catalogCache && Date.now() < catalogCache.expiry) {
    return catalogCache.data;
  }

  const [medsRes, padRes, mapRes] = await Promise.all([
    admin.from("medicamentos").select("id_medicamento, sku, marca, descripcion, contenido, precio, ficha_tecnica_url"),
    admin.from("padecimientos").select("id_padecimiento, nombre"),
    admin.from("medicamento_padecimientos").select("id_medicamento, id_padecimiento"),
  ]);

  const meds = medsRes.data ?? [];
  const pads = padRes.data ?? [];
  const maps = mapRes.data ?? [];

  const padMap = new Map(pads.map((p: { id_padecimiento: number; nombre: string }) => [p.id_padecimiento, p.nombre]));

  const medLines = meds.map((m: Record<string, unknown>) => {
    const relPads = maps
      .filter((mp: { id_medicamento: number }) => mp.id_medicamento === m.id_medicamento)
      .map((mp: { id_padecimiento: number }) => padMap.get(mp.id_padecimiento))
      .filter(Boolean);
    const padStr = relPads.length > 0 ? ` | Padecimientos: ${relPads.join(", ")}` : "";
    const fichaStr = m.ficha_tecnica_url ? " | Tiene ficha tecnica" : "";
    return `- ${m.sku}: ${m.descripcion} (${m.marca}) | $${m.precio} | ${m.contenido}${padStr}${fichaStr}`;
  });

  const padLines = pads.map((p: { nombre: string }) => `- ${p.nombre}`);

  const catalog = `CATALOGO DE MEDICAMENTOS (${meds.length}):\n${medLines.join("\n")}\n\nPADECIMIENTOS (${pads.length}):\n${padLines.join("\n")}`;

  catalogCache = { data: catalog, expiry: Date.now() + CATALOG_CACHE_TTL };
  return catalog;
}

async function getClientContext(
  clienteId: string,
  userId: string,
  userRol: string
): Promise<string | null> {
  // Verify access: ASESOR can only see assigned clients
  if (userRol !== "OWNER" && userRol !== "ADMINISTRADOR") {
    const { data: cliente } = await admin
      .from("clientes")
      .select("id_cliente, id_usuario")
      .eq("id_cliente", clienteId)
      .single();

    if (!cliente || cliente.id_usuario !== userId) {
      return null; // No access
    }
  }

  // Fetch client data in parallel
  const [clienteRes, invRes, movRes, odvRes, botOdvRes, clasRes] = await Promise.all([
    admin.from("clientes").select("id_cliente, nombre, especialidad").eq("id_cliente", clienteId).single(),
    admin.from("inventario_botiquin").select("id_medicamento, cantidad, medicamentos(sku, descripcion, marca)").eq("id_cliente", clienteId),
    admin.from("movimientos_inventario").select("tipo, cantidad, precio_unitario, created_at, medicamentos(sku, descripcion)").eq("id_cliente", clienteId).order("created_at", { ascending: false }).limit(20),
    admin.from("ventas_odv").select("sku, descripcion_producto, cantidad, precio_unitario, fecha_odv").eq("id_cliente", clienteId).order("fecha_odv", { ascending: false }).limit(20),
    admin.from("botiquin_odv").select("sku, descripcion_producto, cantidad, fecha_odv").eq("id_cliente", clienteId).order("fecha_odv", { ascending: false }).limit(10),
    admin.rpc("clasificacion_base").then((r) => {
      // Filter by client
      return (r.data ?? []).filter((row: { id_cliente: string }) => row.id_cliente === clienteId);
    }),
  ]);

  const cliente = clienteRes.data;
  if (!cliente) return null;

  const parts: string[] = [];
  parts.push(`MEDICO: ${cliente.nombre} (${cliente.especialidad ?? "Sin especialidad"})`);

  // Inventario
  const inv = invRes.data ?? [];
  if (inv.length > 0) {
    parts.push(`\nINVENTARIO BOTIQUIN ACTUAL (${inv.length} SKUs):`);
    for (const item of inv) {
      const med = item.medicamentos as { sku: string; descripcion: string; marca: string } | null;
      parts.push(`- ${med?.sku ?? "?"}: ${med?.descripcion ?? "?"} (${med?.marca ?? "?"}) x${item.cantidad}`);
    }
  }

  // Movimientos
  const mov = movRes.data ?? [];
  if (mov.length > 0) {
    parts.push(`\nULTIMOS MOVIMIENTOS (${mov.length}):`);
    for (const m of mov) {
      const med = m.medicamentos as { sku: string; descripcion: string } | null;
      parts.push(`- ${m.tipo}: ${med?.sku ?? "?"} x${m.cantidad} @ $${m.precio_unitario} (${m.created_at?.substring(0, 10)})`);
    }
  }

  // Ventas ODV
  const odv = odvRes.data ?? [];
  if (odv.length > 0) {
    parts.push(`\nVENTAS ODV RECIENTES (${odv.length}):`);
    for (const v of odv) {
      parts.push(`- ${v.sku}: ${v.descripcion_producto} x${v.cantidad} @ $${v.precio_unitario} (${v.fecha_odv})`);
    }
  }

  // Botiquin ODV
  const bot = botOdvRes.data ?? [];
  if (bot.length > 0) {
    parts.push(`\nORDENES BOTIQUIN ODV (${bot.length}):`);
    for (const b of bot) {
      parts.push(`- ${b.sku}: ${b.descripcion_producto} x${b.cantidad} (${b.fecha_odv})`);
    }
  }

  // Clasificacion
  const clas = clasRes ?? [];
  if (clas.length > 0) {
    parts.push(`\nCLASIFICACION M1/M2/M3:`);
    for (const c of clas) {
      parts.push(`- ${(c as Record<string, unknown>).sku}: ${(c as Record<string, unknown>).clasificacion}`);
    }
  }

  return parts.join("\n");
}

async function getConversationHistory(
  conversationId: string
): Promise<{ summary: string | null; messages: Array<{ role: string; content: string }> }> {
  const { data: conv } = await chatbot
    .from("conversations")
    .select("summary")
    .eq("id", conversationId)
    .single();

  const { data: msgs } = await chatbot
    .from("messages")
    .select("role, content")
    .eq("conversation_id", conversationId)
    .order("created_at", { ascending: true });

  return {
    summary: conv?.summary ?? null,
    messages: msgs ?? [],
  };
}

async function getPreviousSummaries(userId: string, excludeConvId?: string): Promise<string> {
  let query = chatbot
    .from("conversations")
    .select("summary")
    .eq("id_usuario", userId)
    .not("summary", "is", null)
    .order("created_at", { ascending: false })
    .limit(3);

  if (excludeConvId) {
    query = query.neq("id", excludeConvId);
  }

  const { data } = await query;
  if (!data || data.length === 0) return "";

  return `RESUMEN DE CONVERSACIONES ANTERIORES:\n${data.map((c: { summary: string }) => `- ${c.summary}`).join("\n")}`;
}

async function compactConversation(conversationId: string, messages: Array<{ role: string; content: string }>): Promise<void> {
  try {
    const systemPrompt = "Resume esta conversacion en maximo 200 palabras conservando los datos clave, nombres de productos y cifras mencionadas. Responde SOLO con el resumen, sin preambulos.";
    const contents = messages.map((m) => ({
      role: m.role === "assistant" ? "model" : "user",
      parts: [{ text: m.content }],
    }));

    const { text } = await callGemini(systemPrompt, contents);

    await chatbot
      .from("conversations")
      .update({ summary: text, updated_at: new Date().toISOString() })
      .eq("id", conversationId);
  } catch (e) {
    console.error("Compaction failed (non-critical):", e);
  }
}

// ============================================================================
// User Resolution
// ============================================================================

async function resolveUser(authUserId: string): Promise<UserInfo | null> {
  const { data, error } = await admin
    .from("usuarios")
    .select("id_usuario, rol, auth_user_id")
    .eq("auth_user_id", authUserId)
    .single();

  if (error || !data) return null;
  return { id_usuario: data.id_usuario, rol: data.rol, auth_user_id: data.auth_user_id };
}

// ============================================================================
// JWT Verification
// ============================================================================

async function verifyJwt(req: Request): Promise<string | null> {
  const authHeader = req.headers.get("authorization");
  if (!authHeader?.startsWith("Bearer ")) return null;

  const token = authHeader.replace("Bearer ", "");
  const tempClient = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
    global: { headers: { Authorization: `Bearer ${token}` } },
  });

  const { data, error } = await tempClient.auth.getUser(token);
  if (error || !data?.user) return null;
  return data.user.id;
}

// ============================================================================
// Request Handlers
// ============================================================================

async function handleUsage(user: UserInfo): Promise<Response> {
  const { data, error } = await chatbot.rpc("get_remaining_queries", {
    p_id_usuario: user.id_usuario,
    p_rol: user.rol,
  });

  if (error) {
    return jsonResponse({ error: "Error al consultar uso", details: error.message }, 500);
  }

  const row = Array.isArray(data) ? data[0] : data;
  return jsonResponse({
    queries_used: row?.queries_used ?? 0,
    queries_limit: row?.queries_limit ?? 0,
    remaining: row?.remaining ?? 0,
  });
}

async function handleRate(user: UserInfo, body: { message_id: string; rating: number }): Promise<Response> {
  if (!body.message_id || !body.rating || body.rating < 1 || body.rating > 5) {
    return jsonResponse({ error: "message_id y rating (1-5) son obligatorios" }, 400);
  }

  // Verify the message belongs to user's conversation
  const { data: msg } = await chatbot
    .from("messages")
    .select("id, conversation_id, role")
    .eq("id", body.message_id)
    .single();

  if (!msg || msg.role !== "assistant") {
    return jsonResponse({ error: "Mensaje no encontrado o no es de asistente" }, 404);
  }

  const { data: conv } = await chatbot
    .from("conversations")
    .select("id_usuario")
    .eq("id", msg.conversation_id)
    .single();

  if (!conv || conv.id_usuario !== user.id_usuario) {
    return jsonResponse({ error: "No tienes acceso a este mensaje" }, 403);
  }

  const { error } = await chatbot
    .from("messages")
    .update({ rating: body.rating, rated_at: new Date().toISOString() })
    .eq("id", body.message_id);

  if (error) {
    return jsonResponse({ error: "Error al calificar", details: error.message }, 500);
  }

  return jsonResponse({ success: true, rating: body.rating });
}

async function handleHistory(user: UserInfo, body: { conversation_id: string }): Promise<Response> {
  if (!body.conversation_id) {
    return jsonResponse({ error: "conversation_id es obligatorio" }, 400);
  }

  // Verify ownership
  const { data: conv } = await chatbot
    .from("conversations")
    .select("id_usuario")
    .eq("id", body.conversation_id)
    .single();

  const isAdmin = user.rol === "OWNER" || user.rol === "ADMINISTRADOR";
  if (!conv || (conv.id_usuario !== user.id_usuario && !isAdmin)) {
    return jsonResponse({ error: "Conversacion no encontrada" }, 404);
  }

  const { data: msgs, error } = await chatbot
    .from("messages")
    .select("id, role, content, rating, rated_at, created_at")
    .eq("conversation_id", body.conversation_id)
    .order("created_at", { ascending: true });

  if (error) {
    return jsonResponse({ error: "Error al obtener historial", details: error.message }, 500);
  }

  return jsonResponse({ messages: msgs ?? [] });
}

async function handleSendMessage(
  user: UserInfo,
  body: { message: string; conversation_id?: string; context_cliente_id?: string }
): Promise<Response> {
  if (!body.message?.trim()) {
    return jsonResponse({ error: "El mensaje no puede estar vacio" }, 400);
  }

  const startTime = Date.now();

  // 1. Check rate limit (atomic)
  const { data: usageData, error: usageError } = await chatbot.rpc("check_and_increment_usage", {
    p_id_usuario: user.id_usuario,
    p_rol: user.rol,
  });

  if (usageError) {
    return jsonResponse({ error: "Error al verificar limite", details: usageError.message }, 500);
  }

  const usage = Array.isArray(usageData) ? usageData[0] : usageData;
  if (!usage?.allowed) {
    return jsonResponse({
      error: "Rate limit exceeded",
      message: RATE_LIMIT_MESSAGE,
      remaining: 0,
      queries_limit: usage?.queries_limit ?? 0,
    }, 429);
  }

  try {
    // 2. Get or create conversation
    let conversationId = body.conversation_id;
    if (conversationId) {
      // Verify ownership
      const { data: conv } = await chatbot
        .from("conversations")
        .select("id_usuario")
        .eq("id", conversationId)
        .single();

      if (!conv || conv.id_usuario !== user.id_usuario) {
        conversationId = undefined;
      }
    }

    if (!conversationId) {
      const { data: newConv, error: convError } = await chatbot
        .from("conversations")
        .insert({ id_usuario: user.id_usuario })
        .select("id")
        .single();

      if (convError || !newConv) {
        throw new Error("Failed to create conversation");
      }
      conversationId = newConv.id;
    }

    // 3. Build context
    const [systemPrompt, catalog, prevSummaries] = await Promise.all([
      getSystemPrompt(),
      getCatalog(),
      getPreviousSummaries(user.id_usuario, conversationId),
    ]);

    let clientContext = "";
    const clienteId = body.context_cliente_id || null;
    if (clienteId) {
      const ctx = await getClientContext(clienteId, user.id_usuario, user.rol);
      if (ctx) {
        clientContext = `\n\n${ctx}`;
      }
    }

    // 4. Build conversation history for Gemini
    const history = await getConversationHistory(conversationId);
    let historyContents: Array<{ role: string; parts: Array<{ text: string }> }> = [];

    if (history.summary && history.messages.length > 4) {
      // Use summary + last 4 messages
      historyContents.push({ role: "user", parts: [{ text: `[Resumen previo: ${history.summary}]` }] });
      historyContents.push({ role: "model", parts: [{ text: "Entendido, tengo el contexto." }] });
      const recent = history.messages.slice(-4);
      for (const m of recent) {
        historyContents.push({
          role: m.role === "assistant" ? "model" : "user",
          parts: [{ text: m.content }],
        });
      }
    } else {
      // Use full history (up to last 8)
      const recent = history.messages.slice(-MAX_HISTORY_MESSAGES);
      for (const m of recent) {
        historyContents.push({
          role: m.role === "assistant" ? "model" : "user",
          parts: [{ text: m.content }],
        });
      }
    }

    // Add current message
    historyContents.push({ role: "user", parts: [{ text: body.message }] });

    // Ensure conversation starts with user role (Gemini requirement)
    if (historyContents.length > 0 && historyContents[0].role === "model") {
      historyContents = historyContents.slice(1);
    }

    // Ensure alternating roles (merge consecutive same-role messages)
    const mergedContents: typeof historyContents = [];
    for (const msg of historyContents) {
      if (mergedContents.length > 0 && mergedContents[mergedContents.length - 1].role === msg.role) {
        mergedContents[mergedContents.length - 1].parts[0].text += "\n" + msg.parts[0].text;
      } else {
        mergedContents.push({ ...msg });
      }
    }

    // Build full system prompt with context
    const fullSystemPrompt = [
      systemPrompt,
      `\n\n${catalog}`,
      clientContext,
      prevSummaries ? `\n\n${prevSummaries}` : "",
    ].join("");

    // 5. Call Gemini
    const geminiResult = await callGemini(fullSystemPrompt, mergedContents);
    const latencyMs = Date.now() - startTime;

    // 6. Store messages
    const messagesToInsert = [
      {
        conversation_id: conversationId,
        role: "user",
        content: body.message,
        context_cliente_id: clienteId,
      },
      {
        conversation_id: conversationId,
        role: "assistant",
        content: geminiResult.text,
        context_cliente_id: clienteId,
        tokens_input: geminiResult.tokensInput,
        tokens_output: geminiResult.tokensOutput,
        latency_ms: latencyMs,
      },
    ];

    const { data: insertedMsgs, error: insertError } = await chatbot
      .from("messages")
      .insert(messagesToInsert)
      .select("id, role");

    if (insertError) {
      console.error("Failed to store messages:", insertError);
    }

    const assistantMsgId = insertedMsgs?.find((m: { role: string }) => m.role === "assistant")?.id;

    // 7. Update conversation timestamp
    await chatbot
      .from("conversations")
      .update({ updated_at: new Date().toISOString() })
      .eq("id", conversationId);

    // 8. Trigger compaction if needed (async, does not count toward rate limit)
    const totalMessages = (history.messages.length ?? 0) + 2;
    if (totalMessages >= COMPACTION_THRESHOLD && !history.summary) {
      const allMsgs = [...history.messages, { role: "user", content: body.message }, { role: "assistant", content: geminiResult.text }];
      // Fire and forget
      compactConversation(conversationId, allMsgs).catch(() => {});
    }

    return jsonResponse({
      message: geminiResult.text,
      conversation_id: conversationId,
      message_id: assistantMsgId ?? null,
      remaining_queries: usage.remaining - 1,
      queries_limit: usage.queries_limit,
    });
  } catch (error) {
    // Rollback usage on Gemini failure
    try {
      await chatbot.rpc("rollback_usage", {
        p_id_usuario: user.id_usuario,
      });
    } catch (rollbackErr) {
      console.error("Rollback failed:", rollbackErr);
    }

    const message = error instanceof Error ? error.message : "Error desconocido";
    console.error("Send message error:", message);
    return jsonResponse({ error: "Error al procesar mensaje", details: message }, 500);
  }
}

// ============================================================================
// Helpers
// ============================================================================

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

// ============================================================================
// Main Handler
// ============================================================================

Deno.serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  try {
    // Verify JWT
    const authUserId = await verifyJwt(req);
    if (!authUserId) {
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    // Resolve user
    const user = await resolveUser(authUserId);
    if (!user) {
      return jsonResponse({ error: "Usuario no encontrado en el sistema" }, 403);
    }

    // Route by method
    if (req.method === "GET") {
      const url = new URL(req.url);
      const action = url.searchParams.get("action");
      if (action === "usage") {
        return await handleUsage(user);
      }
      return jsonResponse({ error: "Accion GET no reconocida" }, 400);
    }

    if (req.method === "POST") {
      const body = await req.json();

      // Route by action
      if (body.action === "rate") {
        return await handleRate(user, body);
      }
      if (body.action === "history") {
        return await handleHistory(user, body);
      }

      // Default: send message
      return await handleSendMessage(user, body);
    }

    return jsonResponse({ error: "Metodo no permitido" }, 405);
  } catch (e) {
    console.error("Unhandled error:", e);
    return jsonResponse({ error: "Error interno", details: String(e) }, 500);
  }
});
