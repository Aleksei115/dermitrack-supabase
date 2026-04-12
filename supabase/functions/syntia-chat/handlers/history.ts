import { chatbot } from "../constants.ts";
import { jsonResponse } from "../utils.ts";
import type { UserInfo } from "../types.ts";

export async function handleHistory(
  user: UserInfo,
  body: { conversation_id: string }
): Promise<Response> {
  if (!body.conversation_id) {
    return jsonResponse(
      { error: "conversation_id es obligatorio" },
      400
    );
  }

  const { data: conv } = await chatbot
    .from("conversations")
    .select("user_id")
    .eq("id", body.conversation_id)
    .single();

  const isAdmin = user.role === "OWNER" || user.role === "ADMIN";
  if (!conv || (conv.user_id !== user.user_id && !isAdmin)) {
    return jsonResponse({ error: "Conversacion no encontrada" }, 404);
  }

  const { data: msgs, error } = await chatbot
    .from("messages")
    .select("id, role, content, rating, rated_at, created_at")
    .eq("conversation_id", body.conversation_id)
    .order("created_at", { ascending: true });

  if (error) {
    return jsonResponse(
      { error: "Error al obtener historial", details: error.message },
      500
    );
  }

  return jsonResponse({ messages: msgs ?? [] });
}
