import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2.45.4";

// ============================================================================
// Types
// ============================================================================

interface Medicamento {
  sku: string;
  marca: string;
  descripcion: string;
  contenido: string | null;
  precio: number | null;
  ficha_tecnica_url: string | null;
}

interface PadecimientoMap {
  [sku: string]: string[];
}

// ============================================================================
// Constants
// ============================================================================

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SERVICE_ROLE_KEY")!;
const GCP_PROJECT_ID = Deno.env.get("GCP_PROJECT_ID")!;
const GCP_SERVICE_ACCOUNT_KEY = Deno.env.get("GCP_SERVICE_ACCOUNT_KEY")!;

const EMBEDDING_MODEL = "text-embedding-005";
const GEMINI_MODEL = "gemini-2.0-flash";
const EMBEDDING_DIMENSION = 768;
const BATCH_SIZE = 10;
const CHUNK_SIZE = 500; // tokens ~= words for Spanish
const MAX_PDF_SIZE = 10 * 1024 * 1024; // 10MB
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 1000;

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
// PDF Text Extraction via Gemini
// ============================================================================

async function extractTextFromPdf(
  pdfBytes: Uint8Array
): Promise<string> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${GEMINI_MODEL}:generateContent`;

  const pdfBase64 = btoa(String.fromCharCode(...pdfBytes));

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
                mimeType: "application/pdf",
                data: pdfBase64,
              },
            },
            {
              text: "Extrae TODO el texto de este documento PDF de ficha tecnica de medicamento. Incluye: composicion, indicaciones, contraindicaciones, dosificacion, presentacion, y cualquier informacion clinica. Responde SOLO con el texto extraido, sin comentarios ni formato adicional.",
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
    throw new Error(`Gemini PDF extraction error (${res.status}): ${await res.text()}`);
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
      // Overlap: keep last 50 words for context continuity
      current = current.slice(-50);
    }
  }

  if (current.length > 50 || chunks.length === 0) {
    chunks.push(current.join(" "));
  }

  return chunks;
}

// ============================================================================
// Main Pipeline
// ============================================================================

async function populateMedicamentoEmbeddings(): Promise<{
  created: number;
  skipped: number;
  errors: string[];
}> {
  const errors: string[] = [];

  // Get all medicamentos
  const { data: meds, error: medsErr } = await admin
    .from("medicamentos")
    .select("sku, marca, descripcion, contenido, precio, ficha_tecnica_url");

  if (medsErr || !meds) {
    throw new Error(`Failed to fetch medicamentos: ${medsErr?.message}`);
  }

  // Get padecimientos mapping
  const { data: padData } = await admin
    .from("medicamento_padecimientos")
    .select("sku, padecimientos(nombre)");

  const padMap: PadecimientoMap = {};
  for (const row of padData ?? []) {
    const pad = row.padecimientos as { nombre: string } | null;
    if (pad) {
      if (!padMap[row.sku]) padMap[row.sku] = [];
      padMap[row.sku].push(pad.nombre);
    }
  }

  // Get existing embeddings
  const { data: existing } = await chatbot
    .from("medicamento_embeddings")
    .select("sku");

  const existingSkus = new Set(
    (existing ?? []).map((e: { sku: string }) => e.sku)
  );

  // Filter to those needing embeddings
  const toProcess = (meds as Medicamento[]).filter(
    (m) => !existingSkus.has(m.sku)
  );

  if (toProcess.length === 0) {
    return { created: 0, skipped: meds.length, errors };
  }

  console.log(
    `Processing ${toProcess.length} medicamentos (${existingSkus.size} already exist)`
  );

  let created = 0;

  // Process in batches
  for (let i = 0; i < toProcess.length; i += BATCH_SIZE) {
    const batch = toProcess.slice(i, i + BATCH_SIZE);

    const texts = batch.map((m) => {
      const pads = padMap[m.sku] ?? [];
      const padStr = pads.length > 0 ? ` Padecimientos: ${pads.join(", ")}` : "";
      return `${m.sku} ${m.marca} ${m.descripcion ?? ""} ${m.contenido ?? ""} Precio: $${m.precio ?? 0}${padStr}`;
    });

    try {
      const embeddings = await generateEmbeddingsBatch(
        texts,
        "RETRIEVAL_DOCUMENT"
      );

      const rows = batch.map((m, idx) => ({
        sku: m.sku,
        embedding_text: texts[idx],
        embedding: JSON.stringify(embeddings[idx]),
        updated_at: new Date().toISOString(),
      }));

      const { error: upsertErr } = await chatbot
        .from("medicamento_embeddings")
        .upsert(rows, { onConflict: "sku" });

      if (upsertErr) {
        errors.push(`Batch ${i}: ${upsertErr.message}`);
      } else {
        created += batch.length;
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`Batch ${i}: ${msg}`);
      console.error(`Batch ${i} failed:`, msg);
    }

    // Small delay between batches to avoid rate limits
    if (i + BATCH_SIZE < toProcess.length) {
      await new Promise((r) => setTimeout(r, 200));
    }
  }

  return { created, skipped: existingSkus.size, errors };
}

async function populateFichaTecnicaChunks(): Promise<{
  processed: number;
  skipped: number;
  errors: string[];
}> {
  const errors: string[] = [];

  // Get medicamentos with ficha_tecnica_url
  const { data: meds } = await admin
    .from("medicamentos")
    .select("sku, ficha_tecnica_url")
    .not("ficha_tecnica_url", "is", null);

  if (!meds || meds.length === 0) {
    return { processed: 0, skipped: 0, errors };
  }

  // Get SKUs that already have chunks
  const { data: existingChunks } = await chatbot
    .from("ficha_tecnica_chunks")
    .select("sku");

  const existingSkus = new Set(
    (existingChunks ?? []).map((e: { sku: string }) => e.sku)
  );

  const toProcess = meds.filter(
    (m: { sku: string }) => !existingSkus.has(m.sku)
  );

  if (toProcess.length === 0) {
    return { processed: 0, skipped: meds.length, errors };
  }

  console.log(
    `Processing ${toProcess.length} fichas técnicas (${existingSkus.size} already exist)`
  );

  let processed = 0;

  for (const med of toProcess) {
    const sku = med.sku as string;
    const url = med.ficha_tecnica_url as string;

    try {
      // Extract filename from URL (stored as path in bucket)
      // ficha_tecnica_url format: "medicaments-technical-sheet/filename.pdf" or full URL
      let filePath = url;
      if (url.startsWith("http")) {
        // Extract path after the bucket name
        const match = url.match(
          /medicaments-technical-sheet\/(.+)/
        );
        filePath = match ? match[1] : url;
      } else if (url.startsWith("medicaments-technical-sheet/")) {
        filePath = url.replace("medicaments-technical-sheet/", "");
      }

      // Download PDF from storage
      const { data: pdfData, error: dlErr } = await admin.storage
        .from("medicaments-technical-sheet")
        .download(filePath);

      if (dlErr || !pdfData) {
        errors.push(`${sku}: Download failed — ${dlErr?.message}`);
        continue;
      }

      const pdfBytes = new Uint8Array(await pdfData.arrayBuffer());

      // Skip oversized PDFs
      if (pdfBytes.length > MAX_PDF_SIZE) {
        errors.push(`${sku}: PDF too large (${(pdfBytes.length / 1024 / 1024).toFixed(1)}MB)`);
        continue;
      }

      // Extract text using Gemini multimodal
      const text = await extractTextFromPdf(pdfBytes);

      if (!text || text.length < 50) {
        errors.push(`${sku}: Extracted text too short (${text.length} chars)`);
        continue;
      }

      // Chunk the text
      const chunks = chunkText(text, CHUNK_SIZE);

      // Generate embeddings for all chunks
      const chunkTexts = chunks.map(
        (c, idx) => `Ficha tecnica ${sku} (parte ${idx + 1}): ${c}`
      );

      // Process chunk embeddings in batches
      const allEmbeddings: number[][] = [];
      for (let i = 0; i < chunkTexts.length; i += BATCH_SIZE) {
        const batch = chunkTexts.slice(i, i + BATCH_SIZE);
        const embs = await generateEmbeddingsBatch(
          batch,
          "RETRIEVAL_DOCUMENT"
        );
        allEmbeddings.push(...embs);
      }

      // Insert chunks with embeddings
      const rows = chunks.map((content, idx) => ({
        sku,
        chunk_index: idx,
        content,
        embedding: JSON.stringify(allEmbeddings[idx]),
      }));

      const { error: insertErr } = await chatbot
        .from("ficha_tecnica_chunks")
        .upsert(rows, { onConflict: "sku,chunk_index" });

      if (insertErr) {
        errors.push(`${sku}: Insert failed — ${insertErr.message}`);
      } else {
        processed++;
        console.log(
          `${sku}: ${chunks.length} chunks created from ${text.length} chars`
        );
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`${sku}: ${msg}`);
      console.error(`${sku} failed:`, msg);
    }

    // Delay between PDFs
    await new Promise((r) => setTimeout(r, 500));
  }

  return { processed, skipped: existingSkus.size, errors };
}

// ============================================================================
// Main Handler
// ============================================================================

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  // Verify service role key (admin only)
  const authHeader = req.headers.get("authorization");
  const apiKey = req.headers.get("apikey");
  if (apiKey !== SERVICE_ROLE_KEY && authHeader !== `Bearer ${SERVICE_ROLE_KEY}`) {
    return new Response(
      JSON.stringify({ error: "Admin access required" }),
      { status: 403, headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  }

  try {
    const url = new URL(req.url);
    const mode = url.searchParams.get("mode") || "all";

    const results: Record<string, unknown> = {};

    if (mode === "all" || mode === "medicamentos") {
      console.log("=== Starting medicamento embeddings ===");
      results.medicamentos = await populateMedicamentoEmbeddings();
    }

    if (mode === "all" || mode === "fichas") {
      console.log("=== Starting ficha técnica chunks ===");
      results.fichas = await populateFichaTecnicaChunks();
    }

    return new Response(JSON.stringify({ success: true, results }), {
      headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("Pipeline error:", msg);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  }
});
