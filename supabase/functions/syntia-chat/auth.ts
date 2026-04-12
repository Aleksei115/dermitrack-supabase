import { createClient } from "npm:@supabase/supabase-js@2.45.4";
import { SUPABASE_URL, SERVICE_ROLE_KEY, admin } from "./constants.ts";
import type { UserInfo } from "./types.ts";

export async function verifyJwt(req: Request): Promise<string | null> {
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

export async function resolveUser(authUserId: string): Promise<UserInfo | null> {
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
