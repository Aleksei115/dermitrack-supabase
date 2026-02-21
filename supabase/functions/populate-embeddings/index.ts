import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2.45.4";
import { encodeBase64 } from "jsr:@std/encoding@1/base64";

// ============================================================================
// Constants
// ============================================================================

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

const EMBEDDING_MODEL = "text-embedding-005";
const GEMINI_MODEL = "gemini-2.0-flash";
const EMBEDDING_DIMENSION = 768;
const BATCH_SIZE = 5; // Small batches to stay within memory
const CHUNK_SIZE = 500;
const MAX_PDF_SIZE = 12 * 1024 * 1024; // 12MB (27MB+ files will be skipped)
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 1000;
const PAGE_SIZE = 30; // Meds per invocation

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const admin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const chatbot = admin.schema("chatbot");

// ============================================================================
// Vertex AI Auth
// ============================================================================

let accessTokenCache: { token: string; expiry: number } | null = null;

function base64UrlEncode(data: Uint8Array): string {
  // Safe for small data (JWT parts only)
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Convert Uint8Array to base64 using native Deno encoder (fast) */
function uint8ToBase64(bytes: Uint8Array): string {
  return encodeBase64(bytes);
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
    expiry: Date.now() + 55 * 60 * 1000,
  };
  return data.access_token;
}

// ============================================================================
// Embedding API
// ============================================================================

async function generateEmbeddingsBatch(
  texts: string[],
  taskType: "RETRIEVAL_DOCUMENT" | "RETRIEVAL_QUERY"
): Promise<number[][]> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/us-central1/publishers/google/models/${EMBEDDING_MODEL}:predict`;

  const instances = texts.map((text) => ({
    content: text,
    task_type: taskType,
  }));

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        instances,
        parameters: { outputDimensionality: EMBEDDING_DIMENSION },
      }),
    });

    if (res.ok) {
      const data = await res.json();
      return data.predictions.map(
        (p: { embeddings: { values: number[] } }) => p.embeddings.values
      );
    }

    if (res.status === 429 || res.status >= 500) {
      const wait = BACKOFF_BASE_MS * Math.pow(2, attempt);
      console.warn(
        `Embedding API ${res.status}, retrying in ${wait}ms (attempt ${attempt + 1}/${MAX_RETRIES})`
      );
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }

    throw new Error(
      `Embedding API error (${res.status}): ${await res.text()}`
    );
  }

  throw new Error("Embedding API: max retries exceeded");
}

// ============================================================================
// Document Text Extraction via Gemini (PDF + images)
// ============================================================================

function getMimeType(filePath: string): string {
  const lower = filePath.toLowerCase();
  if (lower.endsWith(".png")) return "image/png";
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
  if (lower.endsWith(".webp")) return "image/webp";
  return "application/pdf";
}

async function extractTextFromDocument(
  fileBytes: Uint8Array,
  mimeType: string
): Promise<string> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${GEMINI_MODEL}:generateContent`;

  const fileBase64 = uint8ToBase64(fileBytes);

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contents: [
        {
          role: "user",
          parts: [
            {
              inlineData: {
                mimeType,
                data: fileBase64,
              },
            },
            {
              text: "Extrae TODO el texto de este documento de ficha tecnica de medicamento. Incluye: composicion, indicaciones, contraindicaciones, dosificacion, presentacion, y cualquier informacion clinica. Responde SOLO con el texto extraido, sin comentarios ni formato adicional.",
            },
          ],
        },
      ],
      generationConfig: {
        maxOutputTokens: 8192,
        temperature: 0.1,
      },
    }),
  });

  if (!res.ok) {
    throw new Error(
      `Gemini extraction error (${res.status}): ${await res.text()}`
    );
  }

  const data = await res.json();
  return data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
}

// ============================================================================
// Text Chunking
// ============================================================================

function chunkText(text: string, chunkSize: number): string[] {
  const words = text.split(/\s+/);
  const chunks: string[] = [];
  let current: string[] = [];

  for (const word of words) {
    current.push(word);
    if (current.length >= chunkSize) {
      chunks.push(current.join(" "));
      current = current.slice(-50);
    }
  }

  if (current.length > 50 || chunks.length === 0) {
    chunks.push(current.join(" "));
  }

  return chunks;
}

// ============================================================================
// Paginated Medicamento Embeddings
// ============================================================================

async function populateMedicamentoEmbeddings(
  offset: number,
  limit: number
): Promise<{
  created: number;
  skipped: number;
  total: number;
  errors: string[];
}> {
  const errors: string[] = [];

  // Get existing SKUs that already have embeddings
  const { data: existing } = await chatbot
    .from("medicamento_embeddings")
    .select("sku");

  const existingSkus = new Set(
    (existing ?? []).map((e: { sku: string }) => e.sku)
  );

  // Get medicamentos page (only those WITHOUT embeddings)
  // We fetch all SKUs and filter, then paginate the filtered list
  const { data: allMeds, error: medsErr } = await admin
    .from("medicamentos")
    .select("sku, marca, descripcion, contenido, precio")
    .order("sku");

  if (medsErr || !allMeds) {
    throw new Error(`Failed to fetch medicamentos: ${medsErr?.message}`);
  }

  const toProcess = allMeds.filter(
    (m: { sku: string }) => !existingSkus.has(m.sku)
  );
  const total = toProcess.length;
  const page = toProcess.slice(offset, offset + limit);

  if (page.length === 0) {
    return { created: 0, skipped: existingSkus.size, total, errors };
  }

  console.log(
    `Processing meds ${offset}–${offset + page.length} of ${total} pending (${existingSkus.size} already done)`
  );

  let created = 0;

  // Process in small batches
  for (let i = 0; i < page.length; i += BATCH_SIZE) {
    const batch = page.slice(i, i + BATCH_SIZE);
    const skus = batch.map((m: { sku: string }) => m.sku);

    // Fetch padecimientos only for this batch
    const { data: padData } = await admin
      .from("medicamento_padecimientos")
      .select("sku, padecimientos(nombre)")
      .in("sku", skus);

    const padMap: Record<string, string[]> = {};
    for (const row of padData ?? []) {
      const pad = (row as { sku: string; padecimientos: { nombre: string } | null }).padecimientos;
      if (pad) {
        if (!padMap[row.sku]) padMap[row.sku] = [];
        padMap[row.sku].push(pad.nombre);
      }
    }

    const texts = batch.map(
      (m: { sku: string; marca: string; descripcion: string; contenido: string | null; precio: number | null }) => {
        const pads = padMap[m.sku] ?? [];
        const padStr =
          pads.length > 0 ? ` Padecimientos: ${pads.join(", ")}` : "";
        return `${m.sku} ${m.marca} ${m.descripcion ?? ""} ${m.contenido ?? ""} Precio: $${m.precio ?? 0}${padStr}`;
      }
    );

    try {
      const embeddings = await generateEmbeddingsBatch(
        texts,
        "RETRIEVAL_DOCUMENT"
      );

      // Upsert one at a time to minimize memory
      for (let j = 0; j < batch.length; j++) {
        const { error: upsertErr } = await chatbot
          .from("medicamento_embeddings")
          .upsert(
            {
              sku: batch[j].sku,
              embedding_text: texts[j],
              embedding: JSON.stringify(embeddings[j]),
              updated_at: new Date().toISOString(),
            },
            { onConflict: "sku" }
          );

        if (upsertErr) {
          errors.push(`${batch[j].sku}: ${upsertErr.message}`);
        } else {
          created++;
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`Batch ${offset + i}: ${msg}`);
      console.error(`Batch ${offset + i} failed:`, msg);
    }

    // Delay between batches
    if (i + BATCH_SIZE < page.length) {
      await new Promise((r) => setTimeout(r, 300));
    }
  }

  return { created, skipped: existingSkus.size, total, errors };
}

// ============================================================================
// Paginated Ficha Tecnica Chunks
// ============================================================================

async function populateFichaTecnicaChunks(
  offset: number,
  limit: number
): Promise<{
  processed: number;
  skipped: number;
  total: number;
  errors: string[];
}> {
  const errors: string[] = [];

  // Get SKUs that already have chunks
  const { data: existingChunks } = await chatbot
    .from("ficha_tecnica_chunks")
    .select("sku");

  const existingSkus = new Set(
    (existingChunks ?? []).map((e: { sku: string }) => e.sku)
  );

  // Get meds with fichas that need processing
  const { data: allMeds } = await admin
    .from("medicamentos")
    .select("sku, ficha_tecnica_url")
    .not("ficha_tecnica_url", "is", null)
    .order("sku");

  if (!allMeds || allMeds.length === 0) {
    return { processed: 0, skipped: 0, total: 0, errors };
  }

  const toProcess = allMeds.filter(
    (m: { sku: string }) => !existingSkus.has(m.sku)
  );
  const total = toProcess.length;
  const page = toProcess.slice(offset, offset + limit);

  if (page.length === 0) {
    return { processed: 0, skipped: existingSkus.size, total, errors };
  }

  console.log(
    `Processing fichas ${offset}–${offset + page.length} of ${total} pending`
  );

  let processed = 0;

  for (const med of page) {
    const sku = med.sku as string;
    const url = med.ficha_tecnica_url as string;

    try {
      let filePath = url;
      if (url.startsWith("http")) {
        const match = url.match(/medicaments-technical-sheet\/(.+)/);
        filePath = match ? match[1] : url;
      } else if (url.startsWith("medicaments-technical-sheet/")) {
        filePath = url.replace("medicaments-technical-sheet/", "");
      }

      // Detect MIME type from file extension
      const mimeType = getMimeType(filePath);

      // Download file
      const { data: fileData, error: dlErr } = await admin.storage
        .from("medicaments-technical-sheet")
        .download(filePath);

      if (dlErr || !fileData) {
        errors.push(`${sku}: Download failed — ${dlErr?.message}`);
        continue;
      }

      const fileBytes = new Uint8Array(await fileData.arrayBuffer());

      if (fileBytes.length > MAX_PDF_SIZE) {
        errors.push(
          `${sku}: File too large (${(fileBytes.length / 1024 / 1024).toFixed(1)}MB)`
        );
        continue;
      }

      // Extract text (works for PDF and images)
      const text = await extractTextFromDocument(fileBytes, mimeType);

      if (!text || text.length < 50) {
        errors.push(
          `${sku}: Extracted text too short (${text.length} chars)`
        );
        continue;
      }

      // Chunk and embed
      const chunks = chunkText(text, CHUNK_SIZE);

      // Process one embedding batch at a time and insert immediately
      for (let i = 0; i < chunks.length; i += BATCH_SIZE) {
        const chunkBatch = chunks.slice(i, i + BATCH_SIZE);
        const chunkTexts = chunkBatch.map(
          (c, idx) => `Ficha tecnica ${sku} (parte ${i + idx + 1}): ${c}`
        );

        const embeddings = await generateEmbeddingsBatch(
          chunkTexts,
          "RETRIEVAL_DOCUMENT"
        );

        const rows = chunkBatch.map((content, idx) => ({
          sku,
          chunk_index: i + idx,
          content,
          embedding: JSON.stringify(embeddings[idx]),
        }));

        const { error: insertErr } = await chatbot
          .from("ficha_tecnica_chunks")
          .upsert(rows, { onConflict: "sku,chunk_index" });

        if (insertErr) {
          errors.push(`${sku} chunk ${i}: ${insertErr.message}`);
        }
      }

      processed++;
      console.log(
        `${sku}: ${chunks.length} chunks from ${text.length} chars`
      );
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`${sku}: ${msg}`);
      console.error(`${sku} failed:`, msg);
    }

    // Delay between PDFs
    await new Promise((r) => setTimeout(r, 500));
  }

  return { processed, skipped: existingSkus.size, total, errors };
}

// ============================================================================
// Main Handler
// ============================================================================

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  // Verify service_role JWT (admin only)
  const authHeader = req.headers.get("authorization");
  const token = authHeader?.replace("Bearer ", "");
  let isServiceRole = false;
  if (token) {
    try {
      const payload = JSON.parse(atob(token.split(".")[1]));
      isServiceRole = payload.role === "service_role";
    } catch {
      /* invalid JWT */
    }
  }
  if (!isServiceRole) {
    return new Response(
      JSON.stringify({ error: "Admin access required (service_role)" }),
      {
        status: 403,
        headers: { "Content-Type": "application/json", ...CORS_HEADERS },
      }
    );
  }

  try {
    const url = new URL(req.url);
    const mode = url.searchParams.get("mode") || "medicamentos";
    const offset = parseInt(url.searchParams.get("offset") || "0", 10);
    const limit = parseInt(
      url.searchParams.get("limit") || String(PAGE_SIZE),
      10
    );

    console.log(`=== populate-embeddings mode=${mode} offset=${offset} limit=${limit} ===`);

    let result: Record<string, unknown>;

    if (mode === "medicamentos") {
      result = await populateMedicamentoEmbeddings(offset, limit);
    } else if (mode === "fichas") {
      result = await populateFichaTecnicaChunks(offset, limit);
    } else {
      return new Response(
        JSON.stringify({
          error: 'Invalid mode. Use ?mode=medicamentos or ?mode=fichas',
        }),
        {
          status: 400,
          headers: { "Content-Type": "application/json", ...CORS_HEADERS },
        }
      );
    }

    return new Response(
      JSON.stringify({ success: true, mode, offset, limit, ...result }),
      { headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("Pipeline error:", msg);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500,
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    });
  }
});
