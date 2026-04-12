import { chatbot } from "../constants.ts";
import { jsonResponse } from "../utils.ts";
import type { UserInfo } from "../types.ts";

export async function handleUsage(user: UserInfo): Promise<Response> {
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
