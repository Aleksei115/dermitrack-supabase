import { getUserClient } from "../_shared/supabaseClient.ts";
import { buildAuthUrl } from "../_shared/zoho.ts";

Deno.serve(async (req) => {
  const authHeader = req.headers.get("Authorization");
  if (!authHeader) {
    return new Response("Missing Authorization header", { status: 401 });
  }

  const supabase = getUserClient(authHeader);
  const { data, error } = await supabase.auth.getUser();
  if (error || !data.user) {
    return new Response("Unauthorized", { status: 401 });
  }

  const state = btoa(data.user.id);
  const url = buildAuthUrl(state);

  return Response.redirect(url, 302);
});
