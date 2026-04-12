import {
  chatbot,
  CLOUD_RUN_URL,
  USE_AGENT_ENGINE,
  AGENT_ENGINE_RESOURCE_NAME,
  RATE_LIMIT_MESSAGE,
  MAX_HISTORY_MESSAGES,
} from "../constants.ts";
import { jsonResponse } from "../utils.ts";
import { getAccessToken } from "../vertex-auth.ts";
import { getIdentityToken } from "../vertex-auth.ts";
import {
  getSystemPrompt,
  getConversationHistory,
  getPreviousSummaries,
  buildGeminiHistory,
} from "../conversation.ts";
import { handleStreamingResponse } from "./streaming.ts";
import { handleNonStreamingResponse } from "./non-streaming.ts";
import { handleCloudRunResponse } from "./cloud-run.ts";
import { handleAgentEngineResponse } from "./agent-engine.ts";
import type { UserInfo, UsageResult } from "../types.ts";

export async function handleSendMessage(
  req: Request,
  user: UserInfo,
  body: {
    message: string;
    conversation_id?: string;
    context_client_id?: string;
    stream?: boolean;
  }
): Promise<Response> {
  if (!body.message?.trim()) {
    return jsonResponse({ error: "El mensaje no puede estar vacio" }, 400);
  }

  const wordCount = body.message.trim().split(/\s+/).length;
  if (wordCount > 30) {
    return jsonResponse({ error: "Mensaje demasiado largo (max 30 palabras)" }, 400);
  }

  const startTime = Date.now();
  const useStreaming = body.stream !== false;

  // 1. Rate limit + auth pre-warm in parallel
  const tokenPrewarm = CLOUD_RUN_URL
    ? getIdentityToken().catch(() => null)
    : getAccessToken().catch(() => null);
  const [usageResult, _token] = await Promise.all([
    chatbot.rpc("check_and_increment_usage", {
      p_user_id: user.user_id,
      p_role: user.role,
    }),
    tokenPrewarm,
  ]);

  if (usageResult.error) {
    return jsonResponse(
      {
        error: "Error al verificar limite",
        details: usageResult.error.message,
      },
      500
    );
  }

  const usage: UsageResult = Array.isArray(usageResult.data)
    ? usageResult.data[0]
    : usageResult.data;
  if (!usage?.allowed) {
    return jsonResponse(
      {
        error: "Rate limit exceeded",
        message: RATE_LIMIT_MESSAGE,
        remaining: 0,
        queries_limit: usage?.queries_limit ?? 0,
      },
      429
    );
  }

  try {
    // 2. Get or create conversation
    let conversationId = body.conversation_id;
    if (conversationId) {
      const { data: conv } = await chatbot
        .from("conversations")
        .select("user_id")
        .eq("id", conversationId)
        .single();

      if (!conv || conv.user_id !== user.user_id) {
        conversationId = undefined;
      }
    }

    if (!conversationId) {
      const { data: newConv, error: convError } = await chatbot
        .from("conversations")
        .insert({ user_id: user.user_id })
        .select("id")
        .single();

      if (convError || !newConv) {
        throw new Error("Failed to create conversation");
      }
      conversationId = newConv.id;
    }

    // 3. Build context
    const [systemPrompt, prevSummaries, history] = await Promise.all([
      getSystemPrompt(),
      getPreviousSummaries(user.user_id, conversationId),
      getConversationHistory(conversationId),
    ]);

    // 4. Build Gemini contents from history + current message
    const mergedContents = buildGeminiHistory(history, body.message);

    const fullSystemPrompt = [
      systemPrompt,
      `\nUSER: user_id=${user.user_id}, role=${user.role}`,
      prevSummaries ? `\n\n${prevSummaries}` : "",
    ].join("");

    // 5. Route to Cloud Run ADK, Agent Engine, or legacy Gemini
    if (CLOUD_RUN_URL || (USE_AGENT_ENGINE && AGENT_ENGINE_RESOURCE_NAME)) {
      const contextParts: string[] = [];
      if (history.summary) {
        contextParts.push(`[Resumen previo: ${history.summary}]`);
      }
      const recentMsgs = history.messages.slice(-MAX_HISTORY_MESSAGES);
      for (const m of recentMsgs) {
        contextParts.push(`${m.role === "assistant" ? "Asistente" : "Usuario"}: ${m.content}`);
      }
      if (prevSummaries) {
        contextParts.push(prevSummaries);
      }
      const conversationHistoryStr = contextParts.join("\n");

      if (CLOUD_RUN_URL) {
        return handleCloudRunResponse(
          req,
          conversationHistoryStr,
          body.message,
          conversationId!,
          body.context_client_id || null,
          usage,
          history,
          startTime,
          user
        );
      }

      return handleAgentEngineResponse(
        req,
        conversationHistoryStr,
        body.message,
        conversationId!,
        body.context_client_id || null,
        usage,
        history,
        startTime,
        user
      );
    }

    // Legacy: direct Gemini function calling
    if (useStreaming) {
      return handleStreamingResponse(
        req,
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_client_id || null,
        usage,
        history,
        startTime,
        user
      );
    } else {
      return handleNonStreamingResponse(
        fullSystemPrompt,
        mergedContents,
        conversationId!,
        body.message,
        body.context_client_id || null,
        usage,
        history,
        startTime,
        user
      );
    }
  } catch (error) {
    try {
      await chatbot.rpc("rollback_usage", {
        p_user_id: user.user_id,
      });
    } catch (rollbackErr) {
      console.error("Rollback failed:", rollbackErr);
    }

    const message =
      error instanceof Error ? error.message : "Error desconocido";
    console.error("Send message error:", message);
    return jsonResponse(
      { error: "Error al procesar mensaje", details: message },
      500
    );
  }
}
