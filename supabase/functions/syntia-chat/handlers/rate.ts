import { chatbot } from "../constants.ts";
import { jsonResponse } from "../utils.ts";
import type { UserInfo } from "../types.ts";

export async function handleRate(
  user: UserInfo,
  body: { message_id: string; rating: number }
): Promise<Response> {
  if (
    !body.message_id ||
    !body.rating ||
    body.rating < 1 ||
    body.rating > 5
  ) {
    return jsonResponse(
      { error: "message_id y rating (1-5) son obligatorios" },
      400
    );
  }

  const { data: msg } = await chatbot
    .from("messages")
    .select("id, conversation_id, role")
    .eq("id", body.message_id)
    .single();

  if (!msg || msg.role !== "assistant") {
    return jsonResponse(
      { error: "Mensaje no encontrado o no es de asistente" },
      404
    );
  }

  const { data: conv } = await chatbot
    .from("conversations")
    .select("user_id")
    .eq("id", msg.conversation_id)
    .single();

  if (!conv || conv.user_id !== user.user_id) {
    return jsonResponse(
      { error: "No tienes acceso a este mensaje" },
      403
    );
  }

  const { error } = await chatbot
    .from("messages")
    .update({ rating: body.rating, rated_at: new Date().toISOString() })
    .eq("id", body.message_id);

  if (error) {
    return jsonResponse(
      { error: "Error al calificar", details: error.message },
      500
    );
  }

  return jsonResponse({ success: true, rating: body.rating });
}
