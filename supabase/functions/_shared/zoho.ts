export const ZOHO_ACCOUNTS_DOMAIN = Deno.env.get("ZOHO_ACCOUNTS_DOMAIN") ?? "https://accounts.zoho.com";
export const ZOHO_API_DOMAIN = Deno.env.get("ZOHO_API_DOMAIN") ?? "https://www.zohoapis.com";

export const ZOHO_CLIENT_ID = Deno.env.get("ZOHO_CLIENT_ID") ?? "";
export const ZOHO_CLIENT_SECRET = Deno.env.get("ZOHO_CLIENT_SECRET") ?? "";
export const ZOHO_REDIRECT_URI = Deno.env.get("ZOHO_REDIRECT_URI") ?? "";
export const ZOHO_SCOPE = Deno.env.get("ZOHO_SCOPE") ?? "ZohoCRM.modules.Sales_Orders.CREATE";

export async function exchangeCodeForToken(code: string) {
  const url = new URL(`${ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token`);
  url.searchParams.set("grant_type", "authorization_code");
  url.searchParams.set("client_id", ZOHO_CLIENT_ID);
  url.searchParams.set("client_secret", ZOHO_CLIENT_SECRET);
  url.searchParams.set("redirect_uri", ZOHO_REDIRECT_URI);
  url.searchParams.set("code", code);

  const res = await fetch(url.toString(), { method: "POST" });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data?.error ?? "Zoho token exchange failed");
  }
  return data;
}

export async function refreshAccessToken(refreshToken: string) {
  const url = new URL(`${ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token`);
  url.searchParams.set("grant_type", "refresh_token");
  url.searchParams.set("client_id", ZOHO_CLIENT_ID);
  url.searchParams.set("client_secret", ZOHO_CLIENT_SECRET);
  url.searchParams.set("refresh_token", refreshToken);

  const res = await fetch(url.toString(), { method: "POST" });
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data?.error ?? "Zoho refresh token failed");
  }
  return data;
}

export function buildAuthUrl(state: string) {
  const url = new URL(`${ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/auth`);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("client_id", ZOHO_CLIENT_ID);
  url.searchParams.set("scope", ZOHO_SCOPE);
  url.searchParams.set("redirect_uri", ZOHO_REDIRECT_URI);
  url.searchParams.set("access_type", "offline");
  url.searchParams.set("prompt", "consent");
  url.searchParams.set("state", state);
  return url.toString();
}
