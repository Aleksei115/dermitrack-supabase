import { createClient } from "npm:@supabase/supabase-js@2.45.4";

// ============================================================================
// Types
// ============================================================================

interface UserInfo {
  user_id: string;
  role: string;
  auth_user_id: string;
}

interface UsageResult {
  allowed: boolean;
  queries_used: number;
  queries_limit: number;
  remaining: number;
}

interface GeminiPart {
  text?: string;
  thought?: boolean;
  thoughtSignature?: string;
  functionCall?: { name: string; args: Record<string, unknown> };
  functionResponse?: { name: string; response: Record<string, unknown> };
}

interface GeminiContent {
  role: string;
  parts: GeminiPart[];
}

interface GeminiSSEChunk {
  candidates?: Array<{
    content?: { role?: string; parts?: GeminiPart[] };
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
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

const VERTEX_MODEL = "gemini-2.5-flash";
const EMBEDDING_MODEL = "text-embedding-005";
const EMBEDDING_DIMENSION = 768;
const SYSTEM_PROMPT_CACHE_TTL = 10 * 60 * 1000;
const TOKEN_CACHE_TTL = 55 * 60 * 1000;
const COMPACTION_THRESHOLD = 8;
const MAX_HISTORY_MESSAGES = 8;
const MAX_TOOL_ITERATIONS = 3;
const MAX_TOOL_RESULT_LENGTH = 8000;
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
// Tool Declarations — 19 tools for Gemini function calling
// ============================================================================

const TOOL_DECLARATIONS = {
  tools: [
    {
      functionDeclarations: [
        {
          name: "search_medicamentos",
          description:
            "Busca medicamentos por similitud semantica. Usa cuando pregunten por productos para un padecimiento, condicion medica, o tipo de tratamiento. IMPORTANTE: cuando recomiendas productos para un medico especifico, SIEMPRE pasa su client_id para excluir productos que ya tiene en botiquin o con historial (M1/M2/M3).",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Texto de busqueda: padecimiento, sintoma, tipo de producto, o nombre de medicamento",
              },
              client_id: {
                type: "string",
                description:
                  "ID del medico para excluir productos que ya tiene en su botiquin (opcional, obtenido via search_clientes)",
              },
            },
            required: ["query"],
          },
        },
        {
          name: "search_fichas_tecnicas",
          description:
            "Busca informacion tecnica de productos (composicion, indicaciones, contraindicaciones, modo de uso). Usa cuando pregunten por detalles tecnicos o fichas de un producto.",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Texto de busqueda: nombre de producto, ingrediente activo, o consulta tecnica",
              },
            },
            required: ["query"],
          },
        },
        {
          name: "search_clientes",
          description:
            "Busca medicos/clientes por nombre. SIEMPRE usa esta herramienta primero cuando mencionen un nombre de medico para obtener su client_id antes de consultar datos del medico.",
          parameters: {
            type: "object",
            properties: {
              name: {
                type: "string",
                description:
                  "Nombre del medico a buscar (puede ser parcial, ej: 'Garcia', 'Dr Lopez')",
              },
            },
            required: ["name"],
          },
        },
        {
          name: "get_doctor_inventory",
          description:
            "Obtiene el inventario actual del botiquin de un medico especifico. Muestra SKUs, cantidades y precios. Requiere client_id (obtenido via search_clientes).",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_doctor_movements",
          description:
            "Obtiene historial de movimientos de un medico: creaciones, ventas, recolecciones del botiquin y/o ventas recurrentes ODV. Util para tendencias y analisis historico.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
              fuente: {
                type: "string",
                enum: ["botiquin", "odv", "ambos"],
                description:
                  "Fuente de datos: 'botiquin' para movimientos de inventario, 'odv' para ventas recurrentes, 'ambos' para todo (default: ambos)",
              },
              limite: {
                type: "integer",
                description:
                  "Numero maximo de resultados (default 30, max 100)",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_clasificacion_cliente",
          description:
            "Obtiene la clasificacion M1/M2/M3 de productos para un medico. M1=venta directa de botiquin (productos vendidos al corte), M2=conversion (productos vendidos en botiquin que luego se volvieron venta recurrente ODV), M3=exposicion (productos que estuvieron en botiquin y luego aparecieron como venta recurrente ODV sin venta directa previa). Util para estrategia comercial.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_user_odv_sales",
          description:
            "Obtiene ventas ODV (recurrentes) de TODOS los clientes del usuario actual. Usa para ver el portafolio completo de ventas recurrentes del asesor.",
          parameters: {
            type: "object",
            properties: {
              sku_filter: {
                type: "string",
                description: "Filtrar por SKU especifico (opcional)",
              },
              limite: {
                type: "integer",
                description:
                  "Numero maximo de resultados (default 50, max 200)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_recolecciones",
          description:
            "Obtiene recolecciones (devoluciones de productos) del usuario. Cada recoleccion es un evento de devolucion que contiene multiples items con sus cantidades en piezas. IMPORTANTE: 'recolecciones' son eventos, 'piezas' son la suma de cantidades de items. No confundir el numero de recolecciones con el numero de piezas.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description:
                  "Filtrar por medico especifico (opcional). Si no se proporciona, devuelve todas las recolecciones del usuario.",
              },
            },
            required: [],
          },
        },
        {
          name: "get_estadisticas_corte",
          description:
            "Obtiene estadisticas generales del corte actual: total ventas, creaciones, recolecciones, con comparacion vs corte anterior. Para preguntas sobre cifras globales del periodo.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_estadisticas_por_medico",
          description:
            "Obtiene estadisticas del corte actual desglosadas por medico: ventas, creaciones, recolecciones por doctor. Para rankings o comparaciones entre medicos.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_ranking_productos",
          description:
            "Obtiene ranking de productos por CONTEO de movimientos (piezas): cuantas piezas se vendieron, crearon, recolectaron, y cuantas hay en stock activo. NO incluye valor monetario — para ingresos/dinero usa get_ranking_ventas.",
          parameters: {
            type: "object",
            properties: {
              limite: {
                type: "integer",
                description: "Numero maximo de productos (default 20)",
              },
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_ranking_ventas",
          description:
            "Ranking de productos por INGRESOS TOTALES (M1+M2+M3). Incluye desglose: ventas_botiquin (M1=ventas directas de botiquin), ventas_conversion (M2=productos que pasaron de botiquin a ODV), ventas_exposicion (M3=productos expuestos en botiquin que luego se vendieron en ODV). USA ESTA HERRAMIENTA cuando pregunten por dinero, ingresos, o valor de ventas de productos.",
          parameters: {
            type: "object",
            properties: {
              limite: {
                type: "integer",
                description: "Maximo de productos (default 20)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_rendimiento_marcas",
          description:
            "Rendimiento por marca con desglose M1/M2/M3: ventas_botiquin (M1), ventas_conversion (M2), ventas_exposicion (M3), y total. USA ESTA HERRAMIENTA para preguntas de ingresos o dinero por marca.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_datos_historicos",
          description:
            "Obtiene datos historicos completos: KPIs globales (ventas M1, creaciones, stock activo, recolecciones) y datos detallados por visita. Sin fechas devuelve TODO el historico. Ideal para preguntas como 'quien ha vendido mas en toda la historia'.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_facturacion_medicos",
          description:
            "Obtiene la facturacion y composicion de ventas POR MEDICO: rango (Diamante, Oro, Plata, Bronce), facturacion actual vs baseline, desglose M1/M2/M3, porcentaje de crecimiento. Ideal para ranking de medicos y analisis de cartera.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_rendimiento_por_padecimiento",
          description:
            "Obtiene rendimiento por padecimiento/condicion medica: valor total e ingresos, piezas vendidas. Para saber que padecimientos generan mas ingresos.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_impacto_botiquin",
          description:
            "Obtiene metricas de impacto del botiquin: adopciones (M1→ODV), conversiones (M2), exposiciones (M3), revenue por categoria, porcentaje del revenue total atribuible al botiquin.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_medication_prices",
          description:
            "Busca precios de medicamentos por nombre, SKU, o descripcion. Incluye fecha de ultima actualizacion del precio.",
          parameters: {
            type: "object",
            properties: {
              busqueda: {
                type: "string",
                description:
                  "Nombre del producto, SKU, o termino de busqueda",
              },
              brand: {
                type: "string",
                description: "Filtrar por marca (opcional)",
              },
            },
            required: ["busqueda"],
          },
        },
        {
          name: "search_pubmed",
          description:
            "Busca el estudio clinico MAS RECIENTE en PubMed sobre un padecimiento dermatologico. Usa cuando el usuario pregunte sobre un padecimiento o tratamiento para enriquecer tu respuesta con evidencia cientifica actualizada. Retorna 1 solo estudio con fecha.",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Padecimiento o tratamiento en ingles (PubMed es en ingles). Ej: 'atopic dermatitis', 'psoriasis treatment'",
              },
            },
            required: ["query"],
          },
        },
      ],
    },
  ],
  toolConfig: { functionCallingConfig: { mode: "AUTO" } },
};

// ============================================================================
// Caches
// ============================================================================

let systemPromptCache: { data: string; expiry: number } | null = null;
let accessTokenCache: { token: string; expiry: number } | null = null;

// ============================================================================
// Vertex AI Auth (Service Account JWT -> OAuth2 Token)
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
// Gemini API — Non-streaming (for compaction only, no tools)
// ============================================================================

async function callGemini(
  systemPrompt: string,
  contents: GeminiContent[]
): Promise<{ text: string; tokensInput: number; tokensOutput: number }> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:generateContent`;

  let res: Response | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    res = await fetch(url, {
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
    if (res.status !== 429 || attempt === 2) break;
    await new Promise((r) => setTimeout(r, 2000 * Math.pow(2, attempt)));
  }

  if (!res!.ok) {
    throw new Error(`Gemini API error (${res!.status}): ${await res!.text()}`);
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
  const isAdmin = user.role === "OWNER" || user.role === "ADMIN";
  return { isAdmin, userId: user.user_id };
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
// Tool Execution — dispatches function calls to Supabase RPCs
// ============================================================================

function truncateResult(text: string): string {
  if (text.length <= MAX_TOOL_RESULT_LENGTH) return text;
  return text.substring(0, MAX_TOOL_RESULT_LENGTH) + "\n... (resultado truncado)";
}

// ============================================================================
// PubMed Search — NCBI E-utilities (esearch + efetch)
// ============================================================================

function extractXmlTag(xml: string, tag: string): string {
  const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = xml.match(regex);
  return match ? match[1].trim() : "";
}

function extractPubDate(articleXml: string): string {
  const pubDate = extractXmlTag(articleXml, "PubDate");
  if (pubDate) {
    const year = extractXmlTag(pubDate, "Year");
    const month = extractXmlTag(pubDate, "Month");
    if (year) return month ? `${month} ${year}` : year;
  }
  return "Fecha no disponible";
}

async function searchPubMed(query: string): Promise<string> {
  try {
    // Step 1: esearch — find top 1 PMID sorted by date
    const searchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(query + " dermatology")}&retmode=json&retmax=1&sort=date`;
    const searchRes = await fetch(searchUrl);
    if (!searchRes.ok) return "Error al buscar en PubMed.";

    const searchData = await searchRes.json();
    const ids: string[] = searchData.esearchresult?.idlist ?? [];
    if (ids.length === 0)
      return "No se encontraron estudios recientes en PubMed para esa busqueda.";

    const pmid = ids[0];

    // Step 2: efetch — get abstract XML
    const fetchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${pmid}&retmode=xml&rettype=abstract`;
    const fetchRes = await fetch(fetchUrl);
    if (!fetchRes.ok) return "Error al obtener articulo de PubMed.";

    const xml = await fetchRes.text();

    // Parse article fields
    const title = extractXmlTag(xml, "ArticleTitle") || "Sin titulo";
    const journal = extractXmlTag(xml, "Title") || "Journal desconocido";
    const pubDate = extractPubDate(xml);

    // Abstract — may have multiple AbstractText sections
    let abstract = "";
    const abstractMatch = xml.match(/<Abstract>([\s\S]*?)<\/Abstract>/i);
    if (abstractMatch) {
      const abstractTexts =
        abstractMatch[1].match(
          /<AbstractText[^>]*>([\s\S]*?)<\/AbstractText>/gi
        ) ?? [];
      abstract = abstractTexts
        .map((t) => t.replace(/<[^>]+>/g, "").trim())
        .join(" ");
    }
    if (abstract.length > 500)
      abstract = abstract.substring(0, 500) + "...";
    if (!abstract) abstract = "Abstract no disponible.";

    // First author
    const authorBlock = xml.match(/<Author[^>]*>([\s\S]*?)<\/Author>/i);
    let author = "Autor desconocido";
    if (authorBlock) {
      const lastName = extractXmlTag(authorBlock[1], "LastName");
      const initials = extractXmlTag(authorBlock[1], "Initials");
      if (lastName) author = `${lastName} ${initials}`.trim() + " et al.";
    }

    // DOI
    const doiMatch = xml.match(
      /<ArticleId IdType="doi">([\s\S]*?)<\/ArticleId>/i
    );
    const doi = doiMatch ? doiMatch[1].trim() : null;

    // Format result
    const doiLine = doi ? `DOI: ${doi} | ` : "";
    return `Estudio reciente (${pubDate}) — ${journal}\n"${title}"\n${author}\nAbstract: ${abstract}\n${doiLine}PMID: ${pmid}`;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("PubMed search error:", msg);
    return "Error al consultar PubMed. Intenta de nuevo.";
  }
}

// deno-lint-ignore no-explicit-any
type AnyRow = Record<string, any>;

async function executeTool(
  name: string,
  args: Record<string, unknown>,
  user: UserInfo
): Promise<string> {
  const { isAdmin, userId } = getUserFilter(user);

  try {
    switch (name) {
      case "search_medicamentos": {
        const query = args.query as string;
        const idCliente = (args.client_id as string) ?? null;
        const embedding = await generateEmbedding(query, "RETRIEVAL_QUERY");
        const { data, error } = await chatbot.rpc("match_medications", {
          query_embedding: JSON.stringify(embedding),
          match_threshold: 0.55,
          match_count: idCliente ? 25 : 10, // fetch more when filtering
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron medicamentos relevantes.";

        let results = data as AnyRow[];

        // When recommending for a specific doctor: exclude existing products + enrich with sales data
        if (idCliente) {
          const excludedSkus = new Set<string>();

          // Fetch exclusion data + global sales in parallel
          const [invRes, clasifRes, rankingRes] = await Promise.all([
            chatbot.rpc("get_doctor_inventory", {
              p_client_id: idCliente,
              p_user_id: userId,
              p_is_admin: true,
            }),
            chatbot.rpc("classification_by_client", {
              p_client_id: idCliente,
            }),
            chatbot.rpc("get_complete_sales_ranking", { p_limite: 200 }),
          ]);

          // 1. Exclude current inventory
          if (invRes.data) {
            for (const item of invRes.data as AnyRow[]) {
              excludedSkus.add(item.sku);
            }
          }

          // 2. Exclude M1/M2/M3 history
          if (clasifRes.data) {
            for (const item of clasifRes.data as AnyRow[]) {
              excludedSkus.add(item.sku);
            }
          }

          const before = results.length;
          results = results.filter((m) => !excludedSkus.has(m.sku));

          if (results.length === 0) {
            return `Se encontraron ${before} medicamentos relevantes pero todos ya estan en el botiquin o tienen historial (M1/M2/M3) con este medico. Considera otros padecimientos o categorias.`;
          }

          // 3. Enrich with global sales performance
          const salesMap = new Map<string, AnyRow>();
          if (rankingRes.data) {
            for (const r of rankingRes.data as AnyRow[]) {
              salesMap.set(r.sku, r);
            }
          }

          results = results.slice(0, 10);

          return results
            .map((m) => {
              const sales = salesMap.get(m.sku);
              const salesInfo = sales
                ? `| Ventas globales: ${sales.piezas_totales}pz $${sales.ventas_totales} (M1:$${sales.ventas_botiquin} M2:$${sales.ventas_conversion} M3:$${sales.ventas_exposicion})`
                : "| Sin historial de ventas global";
              return `${m.sku}: ${m.description} (${m.brand}) $${m.price} | ${m.content ?? ""} | Padecimientos: ${m.conditions || "N/A"} ${salesInfo}`;
            })
            .join("\n");
        }

        return results
          .map(
            (m) =>
              `${m.sku}: ${m.description} (${m.brand}) $${m.price} | ${m.content ?? ""} | Padecimientos: ${m.conditions || "N/A"}`
          )
          .join("\n");
      }

      case "search_fichas_tecnicas": {
        const query = args.query as string;
        const embedding = await generateEmbedding(query, "RETRIEVAL_QUERY");
        const { data, error } = await chatbot.rpc("match_data_sheets", {
          query_embedding: JSON.stringify(embedding),
          match_threshold: 0.60,
          match_count: 3,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No se encontro informacion tecnica relevante.";
        return (data as AnyRow[])
          .map((f) => `[${f.sku}]:\n${f.content}`)
          .join("\n\n");
      }

      case "search_clientes": {
        const nombre = args.name as string;
        const { data, error } = await chatbot.rpc("fuzzy_search_clients", {
          p_search: nombre,
          p_user_id: null,
          p_limit: 5,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron medicos con ese nombre.";
        return (data as AnyRow[])
          .map(
            (c) =>
              `client_id: ${c.client_id} | Nombre: ${c.name} | Similitud: ${(c.similarity * 100).toFixed(0)}%`
          )
          .join("\n");
      }

      case "get_doctor_inventory": {
        const idCliente = args.client_id as string;
        const { data, error } = await chatbot.rpc("get_doctor_inventory", {
          p_client_id: idCliente,
          p_user_id: userId,
          p_is_admin: isAdmin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "El medico no tiene inventario en botiquin actualmente.";
        return (data as AnyRow[])
          .map(
            (item) =>
              `${item.sku}: ${item.description} (${item.brand}) | Cant: ${item.available_quantity} | $${item.price} | ${item.content ?? ""}`
          )
          .join("\n");
      }

      case "get_doctor_movements": {
        const idCliente = args.client_id as string;
        const fuente = (args.fuente as string) ?? "ambos";
        const limite = (args.limite as number) ?? 30;
        const { data, error } = await chatbot.rpc("get_doctor_movements", {
          p_client_id: idCliente,
          p_user_id: userId,
          p_is_admin: true,
          p_fuente: fuente,
          p_limite: limite,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No se encontraron movimientos para este medico.";
        return (data as AnyRow[])
          .map(
            (m) =>
              `[${m.fuente}] ${m.date?.substring(0, 10) ?? "?"} | ${m.type}: ${m.sku} - ${m.description} (${m.brand}) x${m.quantity} @ $${m.price ?? 0}`
          )
          .join("\n");
      }

      case "get_clasificacion_cliente": {
        const idCliente = args.client_id as string;
        const { data, error } = await chatbot.rpc(
          "classification_by_client",
          { p_client_id: idCliente }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay clasificacion disponible para este medico.";
        return (data as AnyRow[])
          .map((c) => `${c.sku}: ${c.clasificacion}`)
          .join("\n");
      }

      case "get_user_odv_sales": {
        const skuFilter = (args.sku_filter as string) ?? null;
        const limite = (args.limite as number) ?? 50;
        const { data, error } = await chatbot.rpc("get_user_odv_sales", {
          p_user_id: userId,
          p_is_admin: true,
          p_sku_filter: skuFilter,
          p_limite: limite,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron ventas ODV.";
        return (data as AnyRow[])
          .map(
            (v) =>
              `${v.date} | ${v.client_name} | ${v.sku}: ${v.description} (${v.brand}) x${v.quantity} @ $${v.price}`
          )
          .join("\n");
      }

      case "get_recolecciones": {
        const idCliente = (args.client_id as string) ?? null;
        const { data, error } = await chatbot.rpc(
          "get_user_collections",
          {
            p_user_id: userId,
            p_client_id: idCliente,
            p_limit: 20,
            p_is_admin: isAdmin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron recolecciones.";
        const rows = data as AnyRow[];
        let totalPiezasGlobal = 0;
        const lines = rows.map((r) => {
          const itemsList: { sku: string; quantity: number }[] = r.items ?? [];
          const piezas = itemsList.reduce((sum, i) => sum + (i.quantity || 0), 0);
          totalPiezasGlobal += piezas;
          const items = itemsList.length > 0
            ? itemsList.map((i) => `${i.sku} x${i.quantity}`).join(", ")
            : "Sin items";
          const obs = r.cedis_observations
            ? ` | Obs: ${r.cedis_observations}`
            : "";
          return `${r.created_at?.substring(0, 10)} | ${r.client_name} | ${r.status} | ${piezas} piezas | ${items}${obs}`;
        });
        const resumen = `Resumen: ${rows.length} recolecciones, ${totalPiezasGlobal} piezas en total`;
        return `${resumen}\n---\n${lines.join("\n")}`;
      }

      case "get_estadisticas_corte": {
        const { data, error } = await admin.rpc(
          "get_cutoff_general_stats_with_comparison"
        );
        if (error) return `Error: ${error.message}`;
        if (!data) return "No hay estadisticas del corte actual.";
        const row = Array.isArray(data) ? data[0] : data;
        if (!row) return "No hay estadisticas del corte actual.";
        return JSON.stringify(row, null, 2);
      }

      case "get_estadisticas_por_medico": {
        const { data: statsData, error: statsError } = await admin.rpc(
          "get_cutoff_stats_by_doctor_with_comparison"
        );
        if (statsError) return `Error: ${statsError.message}`;
        if (!statsData?.length) return "No hay estadisticas por medico.";

        const limited = (statsData as AnyRow[]).slice(0, 30);
        return (
          `Estadisticas por medico (${(statsData as AnyRow[]).length} total, mostrando ${limited.length}):\n` +
          limited.map((m) => JSON.stringify(m)).join("\n")
        );
      }

      case "get_ranking_productos": {
        const limite = (args.limite as number) ?? 20;
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc("get_product_interest", {
          p_limit: limite,
          p_fecha_inicio: fechaInicio,
          p_fecha_fin: fechaFin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de ranking de productos.";
        return (data as AnyRow[])
          .map((p) => JSON.stringify(p))
          .join("\n");
      }

      case "get_ranking_ventas": {
        const limite = (args.limite as number) ?? 20;
        const { data, error } = await chatbot.rpc(
          "get_complete_sales_ranking",
          { p_limite: limite }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de ventas.";
        return (data as AnyRow[])
          .map(
            (p) =>
              `${p.sku}: ${p.description} (${p.brand}) | Botiquin(M1): ${p.piezas_botiquin}pz $${p.ventas_botiquin} | Conversion(M2): ${p.piezas_conversion}pz $${p.ventas_conversion} | Exposicion(M3): ${p.piezas_exposicion}pz $${p.ventas_exposicion} | TOTAL: ${p.piezas_totales}pz $${p.ventas_totales}`
          )
          .join("\n");
      }

      case "get_rendimiento_marcas": {
        const { data, error } = await chatbot.rpc(
          "get_complete_brand_performance"
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de rendimiento por marca.";
        return (data as AnyRow[])
          .map(
            (b) =>
              `${b.brand} | Botiquin(M1): ${b.piezas_botiquin}pz $${b.ventas_botiquin} | Conversion(M2): ${b.piezas_conversion}pz $${b.ventas_conversion} | Exposicion(M3): ${b.piezas_exposicion}pz $${b.ventas_exposicion} | TOTAL: ${b.piezas_totales}pz $${b.ventas_totales}`
          )
          .join("\n");
      }

      case "get_datos_historicos": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc("get_historical_cutoff_data", {
          p_fecha_inicio: fechaInicio,
          p_fecha_fin: fechaFin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data) return "No hay datos historicos disponibles.";
        return truncateResult(JSON.stringify(data, null, 2));
      }

      case "get_facturacion_medicos": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data: facData, error: facError } = await admin.rpc(
          "get_billing_composition",
          {
            p_fecha_inicio: fechaInicio,
            p_fecha_fin: fechaFin,
          }
        );
        if (facError) return `Error: ${facError.message}`;
        if (!facData?.length)
          return "No hay datos de facturacion por medico.";
        const limited = (facData as AnyRow[]).slice(0, 30);
        return (
          `Facturacion por medico (${(facData as AnyRow[]).length} total, mostrando ${limited.length}):\n` +
          limited
            .map(
              (m) =>
                `${m.client_name} | Rango: ${m.current_tier ?? "N/A"} | Fact: $${m.current_billing ?? 0} | Baseline: $${m.baseline ?? 0} | M1: $${m.current_m1 ?? 0} | M2: $${m.current_m2 ?? 0} | M3: $${m.current_m3 ?? 0} | Crec: ${m.growth_pct ?? 0}%`
            )
            .join("\n")
        );
      }

      case "get_rendimiento_por_padecimiento": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc(
          "get_condition_performance",
          {
            p_fecha_inicio: fechaInicio,
            p_fecha_fin: fechaFin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay datos de rendimiento por padecimiento.";
        return (data as AnyRow[])
          .map(
            (p) =>
              `${p.condition}: $${p.value} | ${p.pieces} piezas`
          )
          .join("\n");
      }

      case "get_impacto_botiquin": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc(
          "get_cabinet_impact_summary",
          {
            p_fecha_inicio: fechaInicio,
            p_fecha_fin: fechaFin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay datos de impacto del botiquin.";
        const row = (data as AnyRow[])[0];
        return [
          `Adopciones (M1→ODV): ${row.adoptions} | Revenue: $${row.revenue_adoptions}`,
          `Conversiones (M2): ${row.conversions} | Revenue: $${row.revenue_conversions}`,
          `Exposiciones (M3): ${row.exposures} | Revenue: $${row.revenue_exposures}`,
          `CrossSell: ${row.crosssell_pairs} pares | Revenue: $${row.revenue_crosssell}`,
          `Revenue total impacto: $${row.total_impact_revenue}`,
          `Revenue total ODV: $${row.total_odv_revenue}`,
          `% impacto botiquin: ${row.impact_percentage}%`,
        ].join("\n");
      }

      case "get_medication_prices": {
        const busqueda = args.busqueda as string;
        const marca = (args.brand as string) ?? null;
        const { data, error } = await chatbot.rpc("get_medication_prices", {
          p_busqueda: busqueda,
          p_brand_filter: marca,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No se encontraron medicamentos con ese criterio.";
        return (data as AnyRow[])
          .map(
            (m) =>
              `${m.sku}: ${m.description} (${m.brand}) | $${m.price} | ${m.content ?? ""} | Actualizado: ${m.last_updated?.substring(0, 10) ?? "N/A"}`
          )
          .join("\n");
      }

      case "search_pubmed": {
        const query = args.query as string;
        return await searchPubMed(query);
      }

      default:
        return `Herramienta desconocida: ${name}`;
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`Tool execution error (${name}):`, msg);
    return `Error al ejecutar herramienta: ${msg}`;
  }
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
    .eq("user_id", userId)
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
    const contents: GeminiContent[] = messages.map((m) => ({
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
): GeminiContent[] {
  let historyContents: GeminiContent[] = [];

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
  const merged: GeminiContent[] = [];
  for (const msg of historyContents) {
    if (
      merged.length > 0 &&
      merged[merged.length - 1].role === msg.role
    ) {
      const lastPart = merged[merged.length - 1].parts[0];
      if (lastPart.text && msg.parts[0].text) {
        lastPart.text += "\n" + msg.parts[0].text;
      }
    } else {
      merged.push({ role: msg.role, parts: [...msg.parts] });
    }
  }

  return merged;
}

// ============================================================================
// User Resolution & JWT Verification
// ============================================================================

async function resolveUser(authUserId: string): Promise<UserInfo | null> {
  const { data, error } = await admin
    .from("users")
    .select("user_id, role, auth_user_id")
    .eq("auth_user_id", authUserId)
    .single();

  if (error || !data) return null;
  return {
    user_id: data.user_id,
    role: data.role,
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
    p_user_id: user.user_id,
    p_role: user.role,
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
    .select("user_id")
    .eq("id", msg.conversation_id)
    .single();

  if (!conv || conv.user_id !== user.user_id) {
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
    .select("user_id")
    .eq("id", body.conversation_id)
    .single();

  const isAdmin = user.role === "OWNER" || user.role === "ADMIN";
  if (!conv || (conv.user_id !== user.user_id && !isAdmin)) {
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
    context_client_id?: string;
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
      p_user_id: user.user_id,
      p_role: user.role,
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
        .select("user_id")
        .eq("id", conversationId)
        .single();

      if (!conv || conv.user_id !== user.user_id) {
        conversationId = undefined;
      }
    }

    if (!conversationId) {
      const { data: newConv, error: convError } = await chatbot
        .from("conversations")
        .insert({ user_id: user.user_id })
        .select("id")
        .single();

      if (convError || !newConv) {
        throw new Error("Failed to create conversation");
      }
      conversationId = newConv.id;
    }

    // 3. Build context — NO RAG pre-loading, just system prompt + history
    const [systemPrompt, prevSummaries, history] = await Promise.all([
      getSystemPrompt(),
      getPreviousSummaries(user.user_id, conversationId),
      getConversationHistory(conversationId),
    ]);

    // 4. Build Gemini contents from history + current message
    const mergedContents = buildGeminiHistory(history, body.message);

    // System prompt with user identity (tools provide data on-demand)
    const fullSystemPrompt = [
      systemPrompt,
      `\nUSER: user_id=${user.user_id}, role=${user.role}`,
      prevSummaries ? `\n\n${prevSummaries}` : "",
    ].join("");

    // 5. Stream or non-stream (pass user for tool execution)
    if (useStreaming) {
      return handleStreamingResponse(
        req,
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_client_id || null,
        usage,
        history,
        startTime,
        user
      );
    } else {
      return handleNonStreamingResponse(
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_client_id || null,
        usage,
        history,
        startTime,
        user
      );
    }
  } catch (error) {
    try {
      await chatbot.rpc("rollback_usage", {
        p_user_id: user.user_id,
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
// Streaming Response with Function Calling
// ============================================================================

async function handleStreamingResponse(
  req: Request,
  systemPrompt: string,
  contents: GeminiContent[],
  conversationId: string,
  userMessage: string,
  clienteId: string | null,
  usage: UsageResult,
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  startTime: number,
  user: UserInfo
): Promise<Response> {
  const encoder = new TextEncoder();
  let currentReader: ReadableStreamDefaultReader<Uint8Array> | null = null;

  const stream = new ReadableStream({
    async start(controller) {
      // Safe enqueue that checks for client disconnect before writing
      const safeEnqueue = (chunk: string): boolean => {
        if (req.signal.aborted) return false;
        try {
          controller.enqueue(encoder.encode(chunk));
          return true;
        } catch {
          return false;
        }
      };

      try {
        // Mutable copy of contents for tool calling loop
        const mutableContents = [...contents];
        let fullText = "";
        let tokensInput = 0;
        let tokensOutput = 0;
        let hitMaxTokens = false;

        // Function calling loop: max MAX_TOOL_ITERATIONS rounds
        for (let iter = 0; iter < MAX_TOOL_ITERATIONS; iter++) {
          if (req.signal.aborted) break;

          const token = await getAccessToken();
          const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:streamGenerateContent?alt=sse`;

          let geminiRes: Response | null = null;
          for (let attempt = 0; attempt < 3; attempt++) {
            geminiRes = await fetch(url, {
              method: "POST",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                systemInstruction: { parts: [{ text: systemPrompt }] },
                contents: mutableContents,
                ...TOOL_DECLARATIONS,
                generationConfig: {
                  maxOutputTokens: 1024,
                  temperature: 0.3,
                },
              }),
            });

            if (geminiRes.status !== 429 || attempt === 2) break;
            // Exponential backoff: 2s, 4s
            const delay = 2000 * Math.pow(2, attempt);
            console.warn(`Gemini 429 rate limit, retrying in ${delay}ms (attempt ${attempt + 1}/3)`);
            safeEnqueue(`data: ${JSON.stringify({ t: "", d: false })}\n\n`);
            await new Promise((r) => setTimeout(r, delay));
          }

          if (!geminiRes!.ok) {
            const err = await geminiRes!.text();
            if (geminiRes!.status === 429) {
              throw new Error("El servicio está ocupado. Intenta de nuevo en unos segundos.");
            }
            throw new Error(
              `Gemini streaming error (${geminiRes!.status}): ${err}`
            );
          }

          const reader = geminiRes.body!.getReader();
          currentReader = reader;
          const decoder = new TextDecoder();
          let buffer = "";
          const functionCalls: Array<{
            name: string;
            args: Record<string, unknown>;
          }> = [];
          // Preserve ALL model parts (including thought + thoughtSignature)
          const allModelParts: GeminiPart[] = [];

          // Read the streaming response
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
                const parts = chunk.candidates?.[0]?.content?.parts ?? [];

                for (const part of parts) {
                  // Preserve every part for the model turn (thought signatures, etc.)
                  allModelParts.push(part);

                  if (part.text && !part.thought) {
                    // Stream non-thought text to client
                    fullText += part.text;
                    safeEnqueue(
                      `data: ${JSON.stringify({ t: part.text, d: false })}\n\n`
                    );
                  } else if (part.thought) {
                    // Send keepalive during thinking to reset client timeout
                    safeEnqueue(
                      `data: ${JSON.stringify({ t: "", d: false })}\n\n`
                    );
                  }
                  if (part.functionCall) {
                    functionCalls.push({
                      name: part.functionCall.name,
                      args: part.functionCall.args ?? {},
                    });
                  }
                }

                // Detect max tokens truncation
                const finishReason =
                  chunk.candidates?.[0]?.finishReason;
                if (finishReason === "MAX_TOKENS") {
                  hitMaxTokens = true;
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

          currentReader = null;

          // If function calls detected, execute tools and continue loop
          if (functionCalls.length > 0) {
            // Keepalive before tool execution to reset client timeout
            safeEnqueue(
              `data: ${JSON.stringify({ t: "", d: false })}\n\n`
            );

            // Keepalive every 10s during tool execution to prevent client timeout
            const keepaliveInterval = setInterval(() => {
              safeEnqueue(`data: ${JSON.stringify({ t: "", d: false })}\n\n`);
            }, 10_000);

            let results: unknown[];
            try {
              results = await Promise.all(
                functionCalls.map((fc) => executeTool(fc.name, fc.args, user))
              );
            } finally {
              clearInterval(keepaliveInterval);
            }

            // Add model's turn with ALL original parts (preserves thought_signature)
            mutableContents.push({
              role: "model",
              parts: allModelParts,
            });

            // Add function responses
            mutableContents.push({
              role: "user",
              parts: functionCalls.map((fc, i) => ({
                functionResponse: {
                  name: fc.name,
                  response: { result: truncateResult(results[i]) },
                },
              })),
            });

            continue; // Next iteration — Gemini will process tool results
          }

          break; // Text response, done
        }

        // ── MAX_TOKENS: refund query, don't save, send error ──
        if (hitMaxTokens) {
          // Refund the query that was pre-incremented
          await chatbot.rpc("rollback_usage", {
            p_user_id: user.user_id,
          });

          safeEnqueue(
            `data: ${JSON.stringify({
              d: true,
              e: "RESPONSE_TOO_LONG",
              r: usage.remaining,
              l: usage.queries_limit,
            })}\n\n`
          );

          try { controller.close(); } catch { /* already closed */ }
          return;
        }

        // Store messages in DB
        const latencyMs = Date.now() - startTime;
        const insertResult = await chatbot
          .from("messages")
          .insert([
            {
              conversation_id: conversationId,
              role: "user",
              content: userMessage,
              context_client_id: clienteId,
            },
            {
              conversation_id: conversationId,
              role: "assistant",
              content: fullText,
              context_client_id: clienteId,
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
        safeEnqueue(
          `data: ${JSON.stringify({
            d: true,
            cid: conversationId,
            mid: assistantMsgId,
            r: Math.max((usage.remaining ?? 1) - 1, 0),
            l: usage.queries_limit,
          })}\n\n`
        );

        try { controller.close(); } catch { /* already closed */ }

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

        if (!req.signal.aborted) {
          try {
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ d: true, e: errMsg })}\n\n`
              )
            );
            controller.close();
          } catch {
            // Controller already closed (client disconnected)
          }
        } else {
          try { controller.close(); } catch { /* already closed */ }
        }
      }
    },
    cancel() {
      currentReader?.cancel();
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
// Non-Streaming Response with Function Calling (backward compat)
// ============================================================================

async function handleNonStreamingResponse(
  systemPrompt: string,
  contents: GeminiContent[],
  conversationId: string,
  userMessage: string,
  clienteId: string | null,
  usage: UsageResult,
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  startTime: number,
  user: UserInfo
): Promise<Response> {
  const mutableContents = [...contents];
  let finalText = "";
  let tokensInput = 0;
  let tokensOutput = 0;

  // Function calling loop
  for (let iter = 0; iter < MAX_TOOL_ITERATIONS; iter++) {
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
        contents: mutableContents,
        ...TOOL_DECLARATIONS,
        generationConfig: { maxOutputTokens: 1024, temperature: 0.3 },
      }),
    });

    if (!res.ok) {
      throw new Error(
        `Gemini API error (${res.status}): ${await res.text()}`
      );
    }

    const data = await res.json();
    tokensInput += data.usageMetadata?.promptTokenCount ?? 0;
    tokensOutput += data.usageMetadata?.candidatesTokenCount ?? 0;

    const parts: GeminiPart[] =
      data.candidates?.[0]?.content?.parts ?? [];
    const functionCalls = parts
      .filter((p) => p.functionCall)
      .map((p) => p.functionCall!);

    if (functionCalls.length > 0) {
      const results = await Promise.all(
        functionCalls.map((fc) => executeTool(fc.name, fc.args, user))
      );

      // Use ALL original parts (preserves thought_signature)
      mutableContents.push({
        role: "model",
        parts,
      });

      mutableContents.push({
        role: "user",
        parts: functionCalls.map((fc, i) => ({
          functionResponse: {
            name: fc.name,
            response: { result: truncateResult(results[i]) },
          },
        })),
      });

      continue;
    }

    finalText = parts.find((p) => p.text && !p.thought)?.text ?? "";
    break;
  }

  const latencyMs = Date.now() - startTime;

  const { data: insertedMsgs } = await chatbot
    .from("messages")
    .insert([
      {
        conversation_id: conversationId,
        role: "user",
        content: userMessage,
        context_client_id: clienteId,
      },
      {
        conversation_id: conversationId,
        role: "assistant",
        content: finalText,
        context_client_id: clienteId,
        tokens_input: tokensInput,
        tokens_output: tokensOutput,
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
      { role: "assistant", content: finalText },
    ];
    compactConversation(conversationId, allMsgs).catch(() => {});
  }

  return jsonResponse({
    message: finalText,
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
