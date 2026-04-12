import {
  GCP_SERVICE_ACCOUNT_KEY,
  CLOUD_RUN_URL,
  TOKEN_CACHE_TTL,
} from "./constants.ts";

// ============================================================================
// Service Account Key Parsing
// ============================================================================

function base64UrlEncode(data: Uint8Array): string {
  const base64 = btoa(String.fromCharCode(...data));
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function createSignedJwt(
  serviceAccount: { client_email: string; private_key: string },
  options?: { targetAudience?: string }
): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: "RS256", typ: "JWT" };
  // deno-lint-ignore no-explicit-any
  const payload: Record<string, any> = {
    iss: serviceAccount.client_email,
    aud: "https://oauth2.googleapis.com/token",
    iat: now,
    exp: now + 3600,
  };
  if (options?.targetAudience) {
    payload.sub = serviceAccount.client_email;
    payload.target_audience = options.targetAudience;
  } else {
    payload.scope = "https://www.googleapis.com/auth/cloud-platform";
  }

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

export function parseServiceAccountKey(): { client_email: string; private_key: string } {
  if (!GCP_SERVICE_ACCOUNT_KEY) {
    throw new Error("GCP_SERVICE_ACCOUNT_KEY is not set");
  }
  const raw = GCP_SERVICE_ACCOUNT_KEY.trim();
  if (raw.startsWith("{")) {
    return JSON.parse(raw);
  }
  let b64 = raw.replace(/\s/g, "").replace(/-/g, "+").replace(/_/g, "/");
  while (b64.length % 4 !== 0) b64 += "=";
  return JSON.parse(atob(b64));
}

// ============================================================================
// Circuit Breaker
// ============================================================================

let consecutiveFailures = 0;
let circuitOpenUntil = 0;
const CIRCUIT_THRESHOLD = 3;
const CIRCUIT_RESET_MS = 30_000;

export function checkCircuit(): boolean {
  if (Date.now() < circuitOpenUntil) return false;
  return true;
}

export function recordSuccess(): void {
  consecutiveFailures = 0;
}

export function recordFailure(): void {
  consecutiveFailures++;
  if (consecutiveFailures >= CIRCUIT_THRESHOLD) {
    circuitOpenUntil = Date.now() + CIRCUIT_RESET_MS;
  }
}

// ============================================================================
// Access Token (for Vertex AI APIs)
// ============================================================================

let accessTokenCache: { token: string; expiry: number } | null = null;

export async function getAccessToken(): Promise<string> {
  if (accessTokenCache && Date.now() < accessTokenCache.expiry) {
    return accessTokenCache.token;
  }

  if (!checkCircuit()) {
    throw new Error("Vertex AI circuit breaker open — too many recent failures");
  }

  try {
    const keyJson = parseServiceAccountKey();
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
    recordSuccess();
    return data.access_token;
  } catch (e) {
    recordFailure();
    throw e;
  }
}

// ============================================================================
// Identity Token (for IAM-authenticated Cloud Run services)
// ============================================================================

let identityTokenCache: { token: string; expiry: number } | null = null;

export async function getIdentityToken(): Promise<string> {
  if (identityTokenCache && Date.now() < identityTokenCache.expiry) {
    return identityTokenCache.token;
  }

  if (!checkCircuit()) {
    throw new Error("Vertex AI circuit breaker open — too many recent failures");
  }

  try {
    const keyJson = parseServiceAccountKey();
    const jwt = await createSignedJwt(keyJson, { targetAudience: CLOUD_RUN_URL });

    const res = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`,
    });

    if (!res.ok) {
      throw new Error(`Identity token exchange failed: ${await res.text()}`);
    }

    const data = await res.json();
    identityTokenCache = {
      token: data.id_token,
      expiry: Date.now() + TOKEN_CACHE_TTL,
    };
    recordSuccess();
    return data.id_token;
  } catch (e) {
    recordFailure();
    throw e;
  }
}
