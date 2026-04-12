import { CORS_HEADERS } from "./constants.ts";
import { jsonResponse } from "./utils.ts";
import { verifyJwt, resolveUser } from "./auth.ts";
import { handleUsage } from "./handlers/usage.ts";
import { handleRate } from "./handlers/rate.ts";
import { handleHistory } from "./handlers/history.ts";
import { handleSendMessage } from "./handlers/send-message.ts";

// ============================================================================
// Main Handler
// ============================================================================

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  try {
    const authUserId = await verifyJwt(req);
    if (!authUserId) {
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    const user = await resolveUser(authUserId);
    if (!user) {
      return jsonResponse(
        { error: "Usuario no encontrado en el sistema" },
        403
      );
    }

    if (req.method === "GET") {
      const url = new URL(req.url);
      const action = url.searchParams.get("action");
      if (action === "usage") {
        return await handleUsage(user);
      }
      return jsonResponse({ error: "Accion GET no reconocida" }, 400);
    }

    if (req.method === "POST") {
      const body = await req.json();

      if (body.action === "rate") {
        return await handleRate(user, body);
      }
      if (body.action === "history") {
        return await handleHistory(user, body);
      }

      return await handleSendMessage(req, user, body);
    }

    return jsonResponse({ error: "Metodo no permitido" }, 405);
  } catch (e) {
    console.error("Unhandled error:", e);
    return jsonResponse(
      { error: "Error interno", details: String(e) },
      500
    );
  }
});
