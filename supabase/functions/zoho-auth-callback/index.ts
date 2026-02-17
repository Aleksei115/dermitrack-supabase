import { getServiceClient } from "../_shared/supabaseClient.ts";
import { exchangeCodeForToken } from "../_shared/zoho.ts";

function decodeState(state: string | null) {
  if (!state) return null;
  try {
    return atob(state);
  } catch {
    return null;
  }
}

Deno.serve(async (req) => {
  const url = new URL(req.url);
  const code = url.searchParams.get("code");
  const state = decodeState(url.searchParams.get("state"));

  if (!code || !state) {
    return new Response("Missing code or state", { status: 400 });
  }

  try {
    const tokenData = await exchangeCodeForToken(code);
    const refreshToken = tokenData.refresh_token as string | undefined;
    const accessToken = tokenData.access_token as string | undefined;
    const expiresIn = tokenData.expires_in as number | undefined;

    if (!refreshToken) {
      return new Response("Missing refresh_token from Zoho", { status: 400 });
    }

    const expiresAt = expiresIn
      ? new Date(Date.now() + expiresIn * 1000).toISOString()
      : null;

    const supabase = getServiceClient();
    const { error } = await supabase
      .schema("migration")
      .from("zoho_tokens")
      .upsert({
        auth_user_id: state,
        refresh_token: refreshToken,
        access_token: accessToken ?? null,
        expires_at: expiresAt,
      });

    if (error) {
      return new Response(`Failed to store token: ${error.message}`, {
        status: 500,
      });
    }

    return new Response(JSON.stringify({ connected: true }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    return new Response(message, { status: 500 });
  }
});
