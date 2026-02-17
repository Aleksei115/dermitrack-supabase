import { getServiceClient, getUserClient } from "../_shared/supabaseClient.ts";
import { refreshAccessToken, ZOHO_API_DOMAIN } from "../_shared/zoho.ts";

Deno.serve(async (req) => {
  if (req.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const authHeader = req.headers.get("Authorization");
  if (!authHeader) {
    return new Response("Missing Authorization header", { status: 401 });
  }

  const userClient = getUserClient(authHeader);
  const { data, error } = await userClient.auth.getUser();
  if (error || !data.user) {
    return new Response("Unauthorized", { status: 401 });
  }

  const payload = await req.json();

  const service = getServiceClient();
  const { data: tokenRow, error: tokenError } = await service
    .schema("migration")
    .from("zoho_tokens")
    .select("refresh_token, access_token, expires_at")
    .eq("auth_user_id", data.user.id)
    .maybeSingle();

  if (tokenError || !tokenRow) {
    return new Response("Zoho connection not found", { status: 400 });
  }

  let accessToken = tokenRow.access_token as string | null;
  const expiresAt = tokenRow.expires_at ? new Date(tokenRow.expires_at) : null;
  const isExpired = !expiresAt || expiresAt.getTime() <= Date.now();

  if (!accessToken || isExpired) {
    const refreshed = await refreshAccessToken(tokenRow.refresh_token);
    accessToken = refreshed.access_token as string | null;

    const expiresIn = refreshed.expires_in as number | undefined;
    const newExpiresAt = expiresIn
      ? new Date(Date.now() + expiresIn * 1000).toISOString()
      : null;

    await service
      .schema("migration")
      .from("zoho_tokens")
      .update({ access_token: accessToken, expires_at: newExpiresAt })
      .eq("auth_user_id", data.user.id);
  }

  if (!accessToken) {
    return new Response("Unable to refresh Zoho token", { status: 500 });
  }

  const zohoBody = payload?.data ? payload : { data: [payload] };

  const zohoRes = await fetch(`${ZOHO_API_DOMAIN}/crm/v2/Sales_Orders`, {
    method: "POST",
    headers: {
      Authorization: `Zoho-oauthtoken ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(zohoBody),
  });

  const zohoJson = await zohoRes.json();

  return new Response(JSON.stringify(zohoJson), {
    status: zohoRes.status,
    headers: { "Content-Type": "application/json" },
  });
});
