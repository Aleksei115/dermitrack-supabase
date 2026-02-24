import { createClient } from "npm:@supabase/supabase-js@2.45.4";

// --- Types ---

type CallerInfo = {
  auth_user_id: string;
  role: string;
  name: string;
};

type RowError = {
  file: string;
  row: number;
  error: string;
};

type ProcessedFile = {
  botiquin_rows: Record<string, unknown>[];
  ventas_rows: Record<string, unknown>[];
  errors: RowError[];
  unmapped: Set<string>;
};

type ImportSummary = {
  success: boolean;
  files_processed: number;
  summary: {
    total_rows: number;
    botiquin_odv_inserted: number;
    ventas_odv_inserted: number;
    duplicates_skipped: number;
    errors: number;
  };
  unmapped_clients: string[];
  file_errors: { file: string; error: string }[];
  row_errors: RowError[];
};

// --- Constants ---

const MONTHS: Record<string, string> = {
  // Spanish
  ene: "01", feb: "02", mar: "03", abr: "04",
  may: "05", jun: "06", jul: "07", ago: "08",
  sep: "09", oct: "10", nov: "11", dic: "12",
  // English
  jan: "01", apr: "04", aug: "08", dec: "12",
  // Shared: feb, mar, may, jun, jul, sep, oct, nov already covered
};

const REQUIRED_COLUMNS = [
  "Orden de venta nº",
  "Fecha del pedido",
  "SKU",
  "Código Cliente",
  "Piezas Vendidas Fac",
  "Estado de la factura",
  "Precio",
];

const BATCH_SIZE = 100;

// --- Supabase client ---

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// --- Helpers ---

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers":
        "authorization, content-type, x-client-info, apikey",
    },
  });
}

function corsResponse() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers":
        "authorization, content-type, x-client-info, apikey",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
  });
}

async function getCallerInfo(
  authHeader: string | null,
): Promise<CallerInfo | null> {
  if (!authHeader) return null;

  const token = authHeader.replace("Bearer ", "");
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser(token);

  if (error || !user) return null;

  const { data: usuario, error: userError } = await supabase
    .from("users")
    .select("role, name")
    .eq("auth_user_id", user.id)
    .single();

  if (userError || !usuario) return null;

  return {
    auth_user_id: user.id,
    role: usuario.role,
    name: usuario.name,
  };
}

/**
 * Parse date in multiple formats → YYYY-MM-DD
 * Supported: "15-ene-24", "03-dic-25", "29 Apr 2025", "20 Jan 2025"
 */
function parseDateES(raw: string): string | null {
  const trimmed = raw.trim().toLowerCase();

  // Format 1: DD-mmm-YY (e.g. "15-ene-24")
  const match1 = trimmed.match(/^(\d{1,2})-([a-záéíóú]{3})-(\d{2})$/);
  if (match1) {
    const day = match1[1].padStart(2, "0");
    const month = MONTHS[match1[2]];
    if (!month) return null;
    return `20${match1[3]}-${month}-${day}`;
  }

  // Format 2: DD Mon YYYY (e.g. "29 Apr 2025")
  const match2 = trimmed.match(/^(\d{1,2})\s+([a-z]{3})\s+(\d{4})$/);
  if (match2) {
    const day = match2[1].padStart(2, "0");
    const month = MONTHS[match2[2]];
    if (!month) return null;
    return `${match2[3]}-${month}-${day}`;
  }

  return null;
}

function parseCantidad(raw: string): number | null {
  const trimmed = raw.trim().replace(/,/g, "");
  const num = Number(trimmed);
  return isNaN(num) ? null : Math.round(num);
}

function parsePrecio(raw: string): number | null {
  const trimmed = raw.trim().replace(/MXN\s*/gi, "").replace(/,/g, "").replace(/\$/g, "");
  if (!trimmed || trimmed === "") return null;
  const num = Number(trimmed);
  return isNaN(num) ? null : num;
}

/**
 * Simple CSV parser that handles quoted fields.
 * Assumes UTF-8 input (BOM stripped). Delimiter: comma.
 */
function parseCSV(text: string): Record<string, string>[] {
  // Strip BOM if present
  const clean = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const lines = clean.split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = parseCSVLine(lines[0]);
  const rows: Record<string, string>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const values = parseCSVLine(line);
    const row: Record<string, string> = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j].trim()] = (values[j] ?? "").trim();
    }
    rows.push(row);
  }

  return rows;
}

function parseCSVLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (i + 1 < line.length && line[i + 1] === '"') {
          current += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = false;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        fields.push(current);
        current = "";
      } else {
        current += ch;
      }
    }
  }
  fields.push(current);
  return fields;
}

// --- Core logic ---

async function loadClientMapping(): Promise<{
  botiquinMap: Map<string, string>;
  normalMap: Map<string, string>;
}> {
  const botiquinMap = new Map<string, string>();
  const normalMap = new Map<string, string>();

  const { data: clientes, error } = await supabase
    .from("clients")
    .select("client_id, zoho_cabinet_client_id");

  if (error) {
    console.error("[import-odv] Error loading clients:", error.message);
    return { botiquinMap, normalMap };
  }

  for (const c of clientes ?? []) {
    if (c.client_id_zoho_botiquin) {
      botiquinMap.set(c.client_id_zoho_botiquin, c.client_id);
    }
    // client_id IS the zoho normal ID (canonical PK)
    normalMap.set(c.client_id, c.client_id);
  }

  return { botiquinMap, normalMap };
}

function processFile(
  fileName: string,
  rows: Record<string, string>[],
  botiquinMap: Map<string, string>,
  normalMap: Map<string, string>,
): ProcessedFile {
  const result: ProcessedFile = {
    botiquin_rows: [],
    ventas_rows: [],
    errors: [],
    unmapped: new Set(),
  };

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const rowNum = i + 2; // +2 because row 1 is header, data starts at row 2

    const odvId = row["Orden de venta nº"];
    const fechaRaw = row["Fecha del pedido"];
    const sku = row["SKU"];
    const codigoCliente = row["Código Cliente"];
    const cantidadRaw = row["Piezas Vendidas Fac"];
    const estadoFactura = row["Estado de la factura"];
    const precioRaw = row["Precio"];

    // Validate required fields
    if (!odvId || !sku || !codigoCliente) {
      result.errors.push({
        file: fileName,
        row: rowNum,
        error: `Campos requeridos vacíos: odv_id=${odvId}, sku=${sku}, codigo_cliente=${codigoCliente}`,
      });
      continue;
    }

    // Parse date
    const fecha = parseDateES(fechaRaw ?? "");
    if (!fecha) {
      result.errors.push({
        file: fileName,
        row: rowNum,
        error: `Fecha no parseable: '${fechaRaw}'`,
      });
      continue;
    }

    // Parse cantidad
    const cantidad = parseCantidad(cantidadRaw ?? "0");
    if (cantidad === null) {
      result.errors.push({
        file: fileName,
        row: rowNum,
        error: `Cantidad no parseable: '${cantidadRaw}'`,
      });
      continue;
    }

    // Resolve client
    const botiquinClientId = botiquinMap.get(codigoCliente);
    const normalClientId = normalMap.get(codigoCliente);

    if (botiquinClientId) {
      result.botiquin_rows.push({
        odv_id: odvId,
        fecha,
        sku,
        client_id: botiquinClientId,
        quantity: cantidad,
        invoice_status: estadoFactura || null,
      });
    } else if (normalClientId) {
      const precio = parsePrecio(precioRaw ?? "");
      result.ventas_rows.push({
        odv_id: odvId,
        fecha,
        sku,
        client_id: normalClientId,
        quantity: cantidad,
        invoice_status: estadoFactura || null,
        price: precio,
      });
    } else {
      result.unmapped.add(codigoCliente);
      result.errors.push({
        file: fileName,
        row: rowNum,
        error: `Cliente no mapeado: '${codigoCliente}'`,
      });
    }
  }

  return result;
}

async function insertBatch(
  table: "cabinet_odv" | "odv_sales",
  rows: Record<string, unknown>[],
): Promise<{ inserted: number; duplicates: number; errors: string[] }> {
  let inserted = 0;
  let duplicates = 0;
  const errors: string[] = [];

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    const { data, error } = await supabase
      .from(table)
      .upsert(chunk, {
        onConflict: "odv_id,client_id,sku",
        ignoreDuplicates: true,
      })
      .select("id_venta");

    if (error) {
      errors.push(`${table} batch ${Math.floor(i / BATCH_SIZE)}: ${error.message}`);
    } else {
      const returnedCount = data?.length ?? 0;
      inserted += returnedCount;
      duplicates += chunk.length - returnedCount;
    }
  }

  return { inserted, duplicates, errors };
}

// --- Main handler ---

Deno.serve(async (req) => {
  const requestId = crypto.randomUUID();
  const log = (msg: string, extra?: unknown) => {
    if (extra !== undefined) {
      console.log(`[import-odv] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[import-odv] ${requestId} ${msg}`);
  };

  if (req.method === "OPTIONS") {
    return corsResponse();
  }

  try {
    if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
      log("missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
      return jsonResponse({ error: "Missing server config" }, 500);
    }

    if (req.method !== "POST") {
      return jsonResponse({ error: "Método no permitido" }, 405);
    }

    // Auth check
    const authHeader =
      req.headers.get("Authorization") || req.headers.get("authorization");
    const caller = await getCallerInfo(authHeader);

    if (!caller) {
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    if (!["OWNER", "ADMIN"].includes(caller.role)) {
      return jsonResponse(
        {
          error:
            "Acceso denegado. Solo OWNER o ADMINISTRADOR pueden importar ODVs.",
        },
        403,
      );
    }

    log("caller verified", { role: caller.role, name: caller.name });

    // Parse multipart form data
    let formData: FormData;
    try {
      formData = await req.formData();
    } catch {
      return jsonResponse(
        { error: "Body debe ser multipart/form-data con archivos CSV" },
        400,
      );
    }

    // Collect CSV files
    const files: { name: string; text: string }[] = [];
    for (const [_key, value] of formData.entries()) {
      if (value instanceof File && value.name.toLowerCase().endsWith(".csv")) {
        const text = await value.text();
        files.push({ name: value.name, text });
      }
    }

    if (files.length === 0) {
      return jsonResponse(
        { error: "No se encontraron archivos CSV en el FormData" },
        400,
      );
    }

    log(`processing ${files.length} CSV file(s)`);

    // Load client mapping once
    const { botiquinMap, normalMap } = await loadClientMapping();
    log("client mapping loaded", {
      botiquin_codes: botiquinMap.size,
      normal_codes: normalMap.size,
    });

    // Process each file
    const allBotiquinRows: Record<string, unknown>[] = [];
    const allVentasRows: Record<string, unknown>[] = [];
    const allErrors: RowError[] = [];
    const allUnmapped = new Set<string>();
    const fileErrors: { file: string; error: string }[] = [];
    let totalRows = 0;

    for (const file of files) {
      const rows = parseCSV(file.text);

      if (rows.length === 0) {
        fileErrors.push({
          file: file.name,
          error: "Archivo vacío o sin filas de datos",
        });
        continue;
      }

      // Validate required columns
      const firstRow = rows[0];
      const missingCols = REQUIRED_COLUMNS.filter(
        (col) => !(col in firstRow),
      );
      if (missingCols.length > 0) {
        fileErrors.push({
          file: file.name,
          error: `Columnas faltantes: ${missingCols.join(", ")}`,
        });
        continue;
      }

      totalRows += rows.length;
      const processed = processFile(file.name, rows, botiquinMap, normalMap);

      allBotiquinRows.push(...processed.botiquin_rows);
      allVentasRows.push(...processed.ventas_rows);
      allErrors.push(...processed.errors);
      for (const u of processed.unmapped) allUnmapped.add(u);
    }

    log("files processed", {
      botiquin_rows: allBotiquinRows.length,
      ventas_rows: allVentasRows.length,
      errors: allErrors.length,
    });

    // Insert batches
    let botiquinInserted = 0;
    let botiquinDuplicates = 0;
    let ventasInserted = 0;
    let ventasDuplicates = 0;

    if (allBotiquinRows.length > 0) {
      const result = await insertBatch("cabinet_odv", allBotiquinRows);
      botiquinInserted = result.inserted;
      botiquinDuplicates = result.duplicates;
      for (const e of result.errors) {
        allErrors.push({ file: "batch", row: 0, error: e });
      }
    }

    if (allVentasRows.length > 0) {
      const result = await insertBatch("odv_sales", allVentasRows);
      ventasInserted = result.inserted;
      ventasDuplicates = result.duplicates;
      for (const e of result.errors) {
        allErrors.push({ file: "batch", row: 0, error: e });
      }
    }

    // Count errors that are NOT unmapped-client errors (those are already in unmapped_clients)
    const nonClientErrors = allErrors.filter(
      (e) => !e.error.startsWith("Cliente no mapeado"),
    );

    const response: ImportSummary = {
      success: true,
      files_processed: files.length - fileErrors.length,
      summary: {
        total_rows: totalRows,
        botiquin_odv_inserted: botiquinInserted,
        ventas_odv_inserted: ventasInserted,
        duplicates_skipped: botiquinDuplicates + ventasDuplicates,
        errors: nonClientErrors.length,
      },
      unmapped_clients: [...allUnmapped].sort(),
      file_errors: fileErrors,
      row_errors: nonClientErrors.slice(0, 100), // Limit to first 100 errors
    };

    log("import complete", response.summary);
    return jsonResponse(response);
  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500,
    );
  }
});
