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

interface GeminiSSEChunk {
  candidates?: Array<{
    content?: { parts?: Array<{ text?: string }> };
    finishReason?: string;
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
const SERVICE_ROLE_KEY = Deno.env.get("SERVICE_ROLE_KEY")!;
const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

const VERTEX_MODEL = "gemini-3-flash-preview";
const EMBEDDING_MODEL = "text-embedding-005";
const EMBEDDING_DIMENSION = 768;
const SYSTEM_PROMPT_CACHE_TTL = 10 * 60 * 1000;
const TOKEN_CACHE_TTL = 55 * 60 * 1000;
const COMPACTION_THRESHOLD = 8;
const MAX_HISTORY_MESSAGES = 8;
const RATE_LIMIT_MESSAGE =
  "Los creditos diarios se han agotado. Si requieres mas, contactate con el administrador.";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

const admin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const chatbot = admin.schema("chatbot");

// ============================================================================
// Caches
// ============================================================================

let systemPromptCache: { data: string; expiry: number } | null = null;
let accessTokenCache: { token: string; expiry: number } | null = null;

// ============================================================================
// Vertex AI Auth (Service Account JWT → OAuth2 Token)
// ============================================================================

function base64UrlEncode(data: Uint8Array): string {
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function createSignedJwt(serviceAccount: {
  client_email: string;
  private_key: string;
}): Promise<string> {
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

  return `${signingInput}.${base64UrlEncode(new Uint8Array(signature))}`;
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
    throw new Error(`OAuth2 token exchange failed: ${await res.text()}`);
  }

  const data = await res.json();
  accessTokenCache = {
    token: data.access_token,
    expiry: Date.now() + TOKEN_CACHE_TTL,
  };
  return data.access_token;
}

// ============================================================================
// Embedding Generation
// ============================================================================

async function generateEmbedding(
  text: string,
  taskType: "RETRIEVAL_QUERY" | "RETRIEVAL_DOCUMENT" = "RETRIEVAL_QUERY"
): Promise<number[]> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/us-central1/publishers/google/models/${EMBEDDING_MODEL}:predict`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      instances: [{ content: text, task_type: taskType }],
      parameters: { outputDimensionality: EMBEDDING_DIMENSION },
    }),
  });

  if (!res.ok) {
    throw new Error(`Embedding API error (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  return data.predictions[0].embeddings.values;
}

// ============================================================================
// Gemini API — Non-streaming (for compaction)
// ============================================================================

async function callGemini(
  systemPrompt: string,
  contents: Array<{ role: string; parts: Array<{ text: string }> }>
): Promise<{ text: string; tokensInput: number; tokensOutput: number }> {
  const token = await getAccessToken();
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
      generationConfig: { maxOutputTokens: 1024, temperature: 0.3 },
    }),
  });

  if (!res.ok) {
    throw new Error(`Gemini API error (${res.status}): ${await res.text()}`);
  }

  const data = await res.json();
  const text = data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
  const tokensInput = data.usageMetadata?.promptTokenCount ?? 0;
  const tokensOutput = data.usageMetadata?.candidatesTokenCount ?? 0;

  return { text, tokensInput, tokensOutput };
}

// ============================================================================
// Data Helpers
// ============================================================================

function getUserFilter(user: UserInfo): { isAdmin: boolean; userId: string } {
  const isAdmin = user.rol === "OWNER" || user.rol === "ADMINISTRADOR";
  return { isAdmin, userId: user.id_usuario };
}

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

  systemPromptCache = {
    data: data.value,
    expiry: Date.now() + SYSTEM_PROMPT_CACHE_TTL,
  };
  return data.value;
}

// ============================================================================
// RAG: Semantic Search Context
// ============================================================================

async function getRelevantMedicamentos(
  queryEmbedding: number[]
): Promise<string> {
  const { data, error } = await chatbot.rpc("match_medicamentos", {
    query_embedding: JSON.stringify(queryEmbedding),
    match_threshold: 0.60,
    match_count: 10,
  });

  if (error || !data || data.length === 0) return "";

  const lines = data.map(
    (m: {
      sku: string;
      descripcion: string;
      marca: string;
      precio: number;
      contenido: string;
      padecimientos: string;
    }) =>
      `- ${m.sku}: ${m.descripcion} (${m.marca}) $${m.precio} | ${m.contenido ?? ""} | ${m.padecimientos || "Sin padecimiento"}`
  );

  return `MEDICAMENTOS RELEVANTES (${data.length}):\n${lines.join("\n")}`;
}

async function getRelevantFichas(
  queryEmbedding: number[]
): Promise<string> {
  const { data, error } = await chatbot.rpc("match_fichas", {
    query_embedding: JSON.stringify(queryEmbedding),
    match_threshold: 0.65,
    match_count: 3,
  });

  if (error || !data || data.length === 0) return "";

  const sections = data.map(
    (f: { sku: string; content: string }) => `[${f.sku}]: ${f.content}`
  );

  return `INFORMACION DE FICHAS TECNICAS:\n${sections.join("\n\n")}`;
}

async function getFallbackCatalog(): Promise<string> {
  const { data: meds } = await admin
    .from("medicamentos")
    .select("sku, marca, precio")
    .order("sku");

  const { data: pads } = await admin
    .from("padecimientos")
    .select("nombre");

  const medLines = (meds ?? []).map(
    (m: { sku: string; marca: string; precio: number }) =>
      `${m.sku} (${m.marca}) $${m.precio}`
  );

  return `CATALOGO RESUMIDO (${medLines.length} productos):\n${medLines.join(", ")}\n\nPADECIMIENTOS: ${(pads ?? []).map((p: { nombre: string }) => p.nombre).join(", ")}`;
}

// ============================================================================
// RAG: Role-filtered Client Context
// ============================================================================

async function getClientContext(
  clienteId: string,
  user: UserInfo
): Promise<string | null> {
  const { isAdmin, userId } = getUserFilter(user);

  // Verify access
  if (!isAdmin) {
    const { data: cliente } = await admin
      .from("clientes")
      .select("id_cliente, id_usuario")
      .eq("id_cliente", clienteId)
      .single();

    if (!cliente || cliente.id_usuario !== userId) {
      return null;
    }
  }

  // Fetch client data in parallel
  const [clienteRes, invRes, movRes, odvRes, botOdvRes, clasRes, skuDispRes] =
    await Promise.all([
      admin
        .from("clientes")
        .select("id_cliente, nombre_cliente")
        .eq("id_cliente", clienteId)
        .single(),
      admin
        .from("inventario_botiquin")
        .select("cantidad, medicamentos(sku, descripcion, marca)")
        .eq("id_cliente", clienteId),
      admin
        .from("movimientos_inventario")
        .select(
          "tipo, cantidad, precio_unitario, created_at, medicamentos(sku, descripcion)"
        )
        .eq("id_cliente", clienteId)
        .order("created_at", { ascending: false })
        .limit(20),
      admin
        .from("ventas_odv")
        .select("sku, descripcion_producto, cantidad, precio_unitario, fecha_odv")
        .eq("id_cliente", clienteId)
        .order("fecha_odv", { ascending: false })
        .limit(20),
      admin
        .from("botiquin_odv")
        .select("sku, descripcion_producto, cantidad, fecha_odv")
        .eq("id_cliente", clienteId)
        .order("fecha_odv", { ascending: false })
        .limit(10),
      chatbot.rpc("clasificacion_por_cliente", {
        p_id_cliente: clienteId,
      }),
      admin
        .from("botiquin_clientes_sku_disponibles")
        .select("sku, fecha_ingreso")
        .eq("id_cliente", clienteId),
    ]);

  const cliente = clienteRes.data;
  if (!cliente) return null;

  const parts: string[] = [];
  parts.push(`MEDICO: ${cliente.nombre_cliente} (ID: ${cliente.id_cliente})`);

  // Inventario
  const inv = invRes.data ?? [];
  if (inv.length > 0) {
    parts.push(`\nINVENTARIO BOTIQUIN ACTUAL (${inv.length} SKUs):`);
    for (const item of inv) {
      const med = item.medicamentos as {
        sku: string;
        descripcion: string;
        marca: string;
      } | null;
      parts.push(
        `- ${med?.sku ?? "?"}: ${med?.descripcion ?? "?"} (${med?.marca ?? "?"}) x${item.cantidad}`
      );
    }
  }

  // SKUs disponibles
  const skuDisp = skuDispRes.data ?? [];
  if (skuDisp.length > 0) {
    parts.push(
      `\nSKUs DISPONIBLES PARA BOTIQUIN: ${skuDisp.map((s: { sku: string }) => s.sku).join(", ")}`
    );
  }

  // Movimientos
  const mov = movRes.data ?? [];
  if (mov.length > 0) {
    parts.push(`\nULTIMOS MOVIMIENTOS (${mov.length}):`);
    for (const m of mov) {
      const med = m.medicamentos as {
        sku: string;
        descripcion: string;
      } | null;
      parts.push(
        `- ${m.tipo}: ${med?.sku ?? "?"} x${m.cantidad} @ $${m.precio_unitario} (${m.created_at?.substring(0, 10)})`
      );
    }
  }

  // Ventas ODV
  const odv = odvRes.data ?? [];
  if (odv.length > 0) {
    parts.push(`\nVENTAS ODV RECIENTES (${odv.length}):`);
    for (const v of odv) {
      parts.push(
        `- ${v.sku}: ${v.descripcion_producto} x${v.cantidad} @ $${v.precio_unitario} (${v.fecha_odv})`
      );
    }
  }

  // Botiquin ODV
  const bot = botOdvRes.data ?? [];
  if (bot.length > 0) {
    parts.push(`\nORDENES BOTIQUIN ODV (${bot.length}):`);
    for (const b of bot) {
      parts.push(
        `- ${b.sku}: ${b.descripcion_producto} x${b.cantidad} (${b.fecha_odv})`
      );
    }
  }

  // Clasificación M1/M2/M3
  const clas = clasRes.data ?? [];
  if (clas.length > 0) {
    parts.push(`\nCLASIFICACION M1/M2/M3:`);
    for (const c of clas as Array<{ sku: string; clasificacion: string }>) {
      parts.push(`- ${c.sku}: ${c.clasificacion}`);
    }
  }

  return parts.join("\n");
}

// ============================================================================
// RAG: Recolecciones Context
// ============================================================================

async function getRecoleccionesContext(
  user: UserInfo,
  clienteId?: string
): Promise<string> {
  const { data, error } = await chatbot.rpc("get_recolecciones_usuario", {
    p_id_usuario: user.id_usuario,
    p_id_cliente: clienteId ?? null,
    p_limit: 20,
  });

  if (error || !data || data.length === 0) return "";

  const lines = data.map(
    (r: {
      nombre_cliente: string;
      estado: string;
      created_at: string;
      cedis_observaciones: string;
      items: Array<{ sku: string; cantidad: number; producto: string }>;
    }) => {
      const date = r.created_at?.substring(0, 10) ?? "?";
      const itemStr =
        r.items
          ?.map(
            (i: { sku: string; cantidad: number }) =>
              `${i.sku} x${i.cantidad}`
          )
          .join(", ") ?? "Sin items";
      const obs = r.cedis_observaciones
        ? ` | Obs: ${r.cedis_observaciones}`
        : "";
      return `- ${date} | ${r.nombre_cliente} | ${r.estado} | ${itemStr}${obs}`;
    }
  );

  return `RECOLECCIONES (${data.length}):\n${lines.join("\n")}`;
}

// ============================================================================
// RAG: Auditoría Context
// ============================================================================

async function getAuditoriaContext(
  clienteId: string,
  user: UserInfo
): Promise<string | null> {
  const { isAdmin, userId } = getUserFilter(user);

  if (!isAdmin) {
    const { data: cliente } = await admin
      .from("clientes")
      .select("id_usuario")
      .eq("id_cliente", clienteId)
      .single();

    if (!cliente || cliente.id_usuario !== userId) return null;
  }

  const { data, error } = await admin.rpc("get_auditoria_cliente", {
    p_cliente: clienteId,
  });

  if (error || !data) return null;

  const result = data as Record<string, unknown>;
  const parts: string[] = [`AUDITORIA DE CLIENTE ${clienteId}:`];

  if (result.resumen) {
    parts.push(`Resumen: ${JSON.stringify(result.resumen)}`);
  }
  if (result.ciclo_vida_skus) {
    const ciclo = result.ciclo_vida_skus as Array<Record<string, unknown>>;
    parts.push(
      `Ciclo de vida SKUs (${ciclo.length}): ${JSON.stringify(ciclo.slice(0, 10))}`
    );
  }
  if (result.anomalias) {
    const anomalias = result.anomalias as Array<Record<string, unknown>>;
    if (anomalias.length > 0) {
      parts.push(
        `Anomalias (${anomalias.length}): ${JSON.stringify(anomalias)}`
      );
    }
  }

  return parts.join("\n");
}

// ============================================================================
// Fuzzy Matching
// ============================================================================

async function resolveClienteByName(
  name: string,
  user: UserInfo
): Promise<string | null> {
  const { isAdmin, userId } = getUserFilter(user);

  const { data } = await chatbot.rpc("fuzzy_search_clientes", {
    p_search: name,
    p_id_usuario: isAdmin ? null : userId,
    p_limit: 1,
  });

  return data?.[0]?.id_cliente ?? null;
}

// ============================================================================
// Intent Detection
// ============================================================================

function detectIntent(message: string): {
  needsRecolecciones: boolean;
  needsAuditoria: boolean;
  mentionedNames: string[];
} {
  const lower = message.toLowerCase();
  const needsRecolecciones =
    /recolec|recog|devol|entreg.*cedis/.test(lower);
  const needsAuditoria =
    /auditor|historial completo|trazab|grafo|ciclo de vida/.test(lower);

  // Extract potential doctor/client names
  const namePatterns = [
    /(?:dr\.?\s+|doctor(?:a)?\s+|medico\s+|dra\.?\s+)([\w\sáéíóúñÁÉÍÓÚÑ]+?)(?:\s*[?,.]|$)/gi,
  ];
  const mentionedNames: string[] = [];
  for (const pat of namePatterns) {
    let match;
    while ((match = pat.exec(message)) !== null) {
      const name = match[1].trim();
      if (name.length > 2) mentionedNames.push(name);
    }
  }

  return { needsRecolecciones, needsAuditoria, mentionedNames };
}

// ============================================================================
// Build Full RAG Context
// ============================================================================

async function buildRAGContext(
  userMessage: string,
  user: UserInfo,
  clienteId?: string
): Promise<string> {
  const parts: string[] = [];

  // User identity
  parts.push(
    `USUARIO ACTUAL: id_usuario=${user.id_usuario}, rol=${user.rol}`
  );

  const intent = detectIntent(userMessage);

  // Resolve mentioned doctor name to client ID
  let resolvedClienteId = clienteId;
  if (!resolvedClienteId && intent.mentionedNames.length > 0) {
    resolvedClienteId =
      (await resolveClienteByName(intent.mentionedNames[0], user)) ??
      undefined;
  }

  // Parallel context fetching
  const contextPromises: Promise<string | null>[] = [];

  // 1. Semantic search for medicamentos + fichas
  let queryEmbedding: number[] | null = null;
  try {
    queryEmbedding = await generateEmbedding(userMessage, "RETRIEVAL_QUERY");
  } catch (e) {
    console.warn("Embedding generation failed, using fallback:", e);
  }

  if (queryEmbedding) {
    contextPromises.push(getRelevantMedicamentos(queryEmbedding));
    contextPromises.push(getRelevantFichas(queryEmbedding));
  } else {
    contextPromises.push(getFallbackCatalog());
    contextPromises.push(Promise.resolve(""));
  }

  // 2. Client context
  if (resolvedClienteId) {
    contextPromises.push(getClientContext(resolvedClienteId, user));
  } else {
    contextPromises.push(Promise.resolve(null));
  }

  // 3. Recolecciones
  if (intent.needsRecolecciones) {
    contextPromises.push(
      getRecoleccionesContext(user, resolvedClienteId)
    );
  } else {
    contextPromises.push(Promise.resolve(""));
  }

  // 4. Auditoría
  if (intent.needsAuditoria && resolvedClienteId) {
    contextPromises.push(getAuditoriaContext(resolvedClienteId, user));
  } else {
    contextPromises.push(Promise.resolve(null));
  }

  const [medsCtx, fichasCtx, clientCtx, recoCtx, auditCtx] =
    await Promise.all(contextPromises);

  if (medsCtx) parts.push(medsCtx);
  if (fichasCtx) parts.push(fichasCtx);
  if (clientCtx) parts.push(clientCtx);
  if (recoCtx) parts.push(recoCtx);
  if (auditCtx) parts.push(auditCtx);

  return parts.join("\n\n");
}

// ============================================================================
// Conversation Management
// ============================================================================

async function getConversationHistory(conversationId: string): Promise<{
  summary: string | null;
  messages: Array<{ role: string; content: string }>;
}> {
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

async function getPreviousSummaries(
  userId: string,
  excludeConvId?: string
): Promise<string> {
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

async function compactConversation(
  conversationId: string,
  messages: Array<{ role: string; content: string }>
): Promise<void> {
  try {
    const prompt =
      "Resume esta conversacion en maximo 200 palabras conservando datos clave, nombres de productos y cifras. Responde SOLO con el resumen.";
    const contents = messages.map((m) => ({
      role: m.role === "assistant" ? "model" : "user",
      parts: [{ text: m.content }],
    }));

    const { text } = await callGemini(prompt, contents);

    await chatbot
      .from("conversations")
      .update({ summary: text, updated_at: new Date().toISOString() })
      .eq("id", conversationId);
  } catch (e) {
    console.error("Compaction failed (non-critical):", e);
  }
}

function buildGeminiHistory(
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  currentMessage: string
): Array<{ role: string; parts: Array<{ text: string }> }> {
  let historyContents: Array<{
    role: string;
    parts: Array<{ text: string }>;
  }> = [];

  if (history.summary && history.messages.length > 4) {
    historyContents.push({
      role: "user",
      parts: [{ text: `[Resumen previo: ${history.summary}]` }],
    });
    historyContents.push({
      role: "model",
      parts: [{ text: "Entendido, tengo el contexto." }],
    });
    const recent = history.messages.slice(-4);
    for (const m of recent) {
      historyContents.push({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.content }],
      });
    }
  } else {
    const recent = history.messages.slice(-MAX_HISTORY_MESSAGES);
    for (const m of recent) {
      historyContents.push({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.content }],
      });
    }
  }

  historyContents.push({ role: "user", parts: [{ text: currentMessage }] });

  // Ensure starts with user role
  if (historyContents.length > 0 && historyContents[0].role === "model") {
    historyContents = historyContents.slice(1);
  }

  // Merge consecutive same-role messages
  const merged: typeof historyContents = [];
  for (const msg of historyContents) {
    if (
      merged.length > 0 &&
      merged[merged.length - 1].role === msg.role
    ) {
      merged[merged.length - 1].parts[0].text +=
        "\n" + msg.parts[0].text;
    } else {
      merged.push({ ...msg });
    }
  }

  return merged;
}

// ============================================================================
// User Resolution & JWT Verification
// ============================================================================

async function resolveUser(authUserId: string): Promise<UserInfo | null> {
  const { data, error } = await admin
    .from("usuarios")
    .select("id_usuario, rol, auth_user_id")
    .eq("auth_user_id", authUserId)
    .single();

  if (error || !data) return null;
  return {
    id_usuario: data.id_usuario,
    rol: data.rol,
    auth_user_id: data.auth_user_id,
  };
}

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
// Request Handlers — Usage, Rate, History
// ============================================================================

async function handleUsage(user: UserInfo): Promise<Response> {
  const { data, error } = await chatbot.rpc("get_remaining_queries", {
    p_id_usuario: user.id_usuario,
    p_rol: user.rol,
  });

  if (error) {
    return jsonResponse(
      { error: "Error al consultar uso", details: error.message },
      500
    );
  }

  const row = Array.isArray(data) ? data[0] : data;
  return jsonResponse({
    queries_used: row?.queries_used ?? 0,
    queries_limit: row?.queries_limit ?? 0,
    remaining: row?.remaining ?? 0,
  });
}

async function handleRate(
  user: UserInfo,
  body: { message_id: string; rating: number }
): Promise<Response> {
  if (
    !body.message_id ||
    !body.rating ||
    body.rating < 1 ||
    body.rating > 5
  ) {
    return jsonResponse(
      { error: "message_id y rating (1-5) son obligatorios" },
      400
    );
  }

  const { data: msg } = await chatbot
    .from("messages")
    .select("id, conversation_id, role")
    .eq("id", body.message_id)
    .single();

  if (!msg || msg.role !== "assistant") {
    return jsonResponse(
      { error: "Mensaje no encontrado o no es de asistente" },
      404
    );
  }

  const { data: conv } = await chatbot
    .from("conversations")
    .select("id_usuario")
    .eq("id", msg.conversation_id)
    .single();

  if (!conv || conv.id_usuario !== user.id_usuario) {
    return jsonResponse(
      { error: "No tienes acceso a este mensaje" },
      403
    );
  }

  const { error } = await chatbot
    .from("messages")
    .update({ rating: body.rating, rated_at: new Date().toISOString() })
    .eq("id", body.message_id);

  if (error) {
    return jsonResponse(
      { error: "Error al calificar", details: error.message },
      500
    );
  }

  return jsonResponse({ success: true, rating: body.rating });
}

async function handleHistory(
  user: UserInfo,
  body: { conversation_id: string }
): Promise<Response> {
  if (!body.conversation_id) {
    return jsonResponse(
      { error: "conversation_id es obligatorio" },
      400
    );
  }

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
    return jsonResponse(
      { error: "Error al obtener historial", details: error.message },
      500
    );
  }

  return jsonResponse({ messages: msgs ?? [] });
}

// ============================================================================
// Send Message Handler
// ============================================================================

async function handleSendMessage(
  req: Request,
  user: UserInfo,
  body: {
    message: string;
    conversation_id?: string;
    context_cliente_id?: string;
    stream?: boolean;
  }
): Promise<Response> {
  if (!body.message?.trim()) {
    return jsonResponse({ error: "El mensaje no puede estar vacio" }, 400);
  }

  const startTime = Date.now();
  const useStreaming = body.stream !== false;

  // 1. Rate limit + OAuth pre-warm in parallel
  const [usageResult, _token] = await Promise.all([
    chatbot.rpc("check_and_increment_usage", {
      p_id_usuario: user.id_usuario,
      p_rol: user.rol,
    }),
    getAccessToken().catch(() => null),
  ]);

  if (usageResult.error) {
    return jsonResponse(
      {
        error: "Error al verificar limite",
        details: usageResult.error.message,
      },
      500
    );
  }

  const usage: UsageResult = Array.isArray(usageResult.data)
    ? usageResult.data[0]
    : usageResult.data;
  if (!usage?.allowed) {
    return jsonResponse(
      {
        error: "Rate limit exceeded",
        message: RATE_LIMIT_MESSAGE,
        remaining: 0,
        queries_limit: usage?.queries_limit ?? 0,
      },
      429
    );
  }

  try {
    // 2. Get or create conversation
    let conversationId = body.conversation_id;
    if (conversationId) {
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

    // 3. Build context in parallel
    const [systemPrompt, ragContext, prevSummaries, history] =
      await Promise.all([
        getSystemPrompt(),
        buildRAGContext(
          body.message,
          user,
          body.context_cliente_id || undefined
        ),
        getPreviousSummaries(user.id_usuario, conversationId),
        getConversationHistory(conversationId),
      ]);

    // 4. Build Gemini contents
    const mergedContents = buildGeminiHistory(history, body.message);

    const fullSystemPrompt = [
      systemPrompt,
      "\n\n--- CONTEXTO DE DATOS ---\n",
      ragContext,
      prevSummaries ? `\n\n${prevSummaries}` : "",
    ].join("");

    // 5. Stream or non-stream
    if (useStreaming) {
      return handleStreamingResponse(
        req,
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_cliente_id || null,
        usage,
        history,
        startTime
      );
    } else {
      return handleNonStreamingResponse(
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_cliente_id || null,
        usage,
        history,
        startTime
      );
    }
  } catch (error) {
    try {
      await chatbot.rpc("rollback_usage", {
        p_id_usuario: user.id_usuario,
      });
    } catch (rollbackErr) {
      console.error("Rollback failed:", rollbackErr);
    }

    const message =
      error instanceof Error ? error.message : "Error desconocido";
    console.error("Send message error:", message);
    return jsonResponse(
      { error: "Error al procesar mensaje", details: message },
      500
    );
  }
}

// ============================================================================
// Streaming Response
// ============================================================================

async function handleStreamingResponse(
  req: Request,
  systemPrompt: string,
  contents: Array<{ role: string; parts: Array<{ text: string }> }>,
  conversationId: string,
  userMessage: string,
  clienteId: string | null,
  usage: UsageResult,
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  startTime: number
): Promise<Response> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:streamGenerateContent?alt=sse`;

  const geminiRes = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: systemPrompt }] },
      contents,
      generationConfig: { maxOutputTokens: 1024, temperature: 0.3 },
    }),
  });

  if (!geminiRes.ok) {
    const err = await geminiRes.text();
    throw new Error(`Gemini streaming error (${geminiRes.status}): ${err}`);
  }

  let fullText = "";
  let tokensInput = 0;
  let tokensOutput = 0;

  const encoder = new TextEncoder();
  const reader = geminiRes.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  const stream = new ReadableStream({
    async start(controller) {
      try {
        while (true) {
          if (req.signal.aborted) {
            console.warn("Client disconnected during stream");
            break;
          }

          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });

          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";

          for (const line of lines) {
            if (!line.startsWith("data: ")) continue;
            const jsonStr = line.slice(6).trim();
            if (!jsonStr || jsonStr === "[DONE]") continue;

            try {
              const chunk: GeminiSSEChunk = JSON.parse(jsonStr);
              const text =
                chunk.candidates?.[0]?.content?.parts?.[0]?.text ?? "";

              if (text) {
                fullText += text;
                controller.enqueue(
                  encoder.encode(
                    `data: ${JSON.stringify({ t: text, d: false })}\n\n`
                  )
                );
              }

              if (chunk.usageMetadata) {
                tokensInput =
                  chunk.usageMetadata.promptTokenCount ?? tokensInput;
                tokensOutput =
                  chunk.usageMetadata.candidatesTokenCount ?? tokensOutput;
              }
            } catch {
              // Skip unparseable chunks
            }
          }
        }

        // Store messages
        const latencyMs = Date.now() - startTime;
        const insertResult = await chatbot
          .from("messages")
          .insert([
            {
              conversation_id: conversationId,
              role: "user",
              content: userMessage,
              context_cliente_id: clienteId,
            },
            {
              conversation_id: conversationId,
              role: "assistant",
              content: fullText,
              context_cliente_id: clienteId,
              tokens_input: tokensInput,
              tokens_output: tokensOutput,
              latency_ms: latencyMs,
            },
          ])
          .select("id, role");

        const assistantMsgId =
          insertResult.data?.find(
            (m: { role: string }) => m.role === "assistant"
          )?.id ?? null;

        await chatbot
          .from("conversations")
          .update({ updated_at: new Date().toISOString() })
          .eq("id", conversationId);

        // Final SSE with metadata
        controller.enqueue(
          encoder.encode(
            `data: ${JSON.stringify({
              d: true,
              cid: conversationId,
              mid: assistantMsgId,
              r: Math.max((usage.remaining ?? 1) - 1, 0),
              l: usage.queries_limit,
            })}\n\n`
          )
        );

        controller.close();

        // Compaction (fire and forget)
        const totalMessages = (history.messages.length ?? 0) + 2;
        if (totalMessages >= COMPACTION_THRESHOLD && !history.summary) {
          const allMsgs = [
            ...history.messages,
            { role: "user", content: userMessage },
            { role: "assistant", content: fullText },
          ];
          compactConversation(conversationId, allMsgs).catch(() => {});
        }
      } catch (e) {
        const errMsg =
          e instanceof Error ? e.message : "Error al procesar";
        console.error("Streaming error:", errMsg);

        try {
          controller.enqueue(
            encoder.encode(
              `data: ${JSON.stringify({ d: true, e: errMsg })}\n\n`
            )
          );
        } catch {
          // Controller already closed
        }

        controller.close();
      }
    },
    cancel() {
      reader.cancel();
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      ...CORS_HEADERS,
    },
  });
}

// ============================================================================
// Non-Streaming Response (backward compat)
// ============================================================================

async function handleNonStreamingResponse(
  systemPrompt: string,
  contents: Array<{ role: string; parts: Array<{ text: string }> }>,
  conversationId: string,
  userMessage: string,
  clienteId: string | null,
  usage: UsageResult,
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  startTime: number
): Promise<Response> {
  const geminiResult = await callGemini(systemPrompt, contents);
  const latencyMs = Date.now() - startTime;

  const { data: insertedMsgs } = await chatbot
    .from("messages")
    .insert([
      {
        conversation_id: conversationId,
        role: "user",
        content: userMessage,
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
    ])
    .select("id, role");

  const assistantMsgId =
    insertedMsgs?.find((m: { role: string }) => m.role === "assistant")?.id ??
    null;

  await chatbot
    .from("conversations")
    .update({ updated_at: new Date().toISOString() })
    .eq("id", conversationId);

  const totalMessages = (history.messages.length ?? 0) + 2;
  if (totalMessages >= COMPACTION_THRESHOLD && !history.summary) {
    const allMsgs = [
      ...history.messages,
      { role: "user", content: userMessage },
      { role: "assistant", content: geminiResult.text },
    ];
    compactConversation(conversationId, allMsgs).catch(() => {});
  }

  return jsonResponse({
    message: geminiResult.text,
    conversation_id: conversationId,
    message_id: assistantMsgId,
    remaining_queries: Math.max((usage.remaining ?? 1) - 1, 0),
    queries_limit: usage.queries_limit,
  });
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
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  try {
    const authUserId = await verifyJwt(req);
    if (!authUserId) {
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    const user = await resolveUser(authUserId);
    if (!user) {
      return jsonResponse(
        { error: "Usuario no encontrado en el sistema" },
        403
      );
    }

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

      if (body.action === "rate") {
        return await handleRate(user, body);
      }
      if (body.action === "history") {
        return await handleHistory(user, body);
      }

      return await handleSendMessage(req, user, body);
    }

    return jsonResponse({ error: "Metodo no permitido" }, 405);
  } catch (e) {
    console.error("Unhandled error:", e);
    return jsonResponse(
      { error: "Error interno", details: String(e) },
      500
    );
  }
});
