import {
  chatbot,
  AGENT_ENGINE_RESOURCE_NAME,
  CORS_HEADERS,
} from "../constants.ts";
import { getAccessToken } from "../vertex-auth.ts";
import { compactConversation, COMPACTION_THRESHOLD } from "../conversation.ts";
import type { UserInfo, UsageResult } from "../types.ts";

export async function handleAgentEngineResponse(
  req: Request,
  conversationHistory: string,
  userMessage: string,
  conversationId: string,
  clienteId: string | null,
  usage: UsageResult,
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  startTime: number,
  user: UserInfo
): Promise<Response> {
  const encoder = new TextEncoder();
  let controllerRef: ReadableStreamDefaultController<Uint8Array> | null = null;

  const safeEnqueue = (data: string): boolean => {
    if (req.signal.aborted || !controllerRef) return false;
    try {
      controllerRef.enqueue(encoder.encode(data));
      return true;
    } catch {
      return false;
    }
  };

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      controllerRef = controller;

      try {
        const token = await getAccessToken();
        const authHeaders = {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        };

        const fullInput = [
          conversationHistory,
          `\nUSER: user_id=${user.user_id}, role=${user.role}`,
          `\nMensaje actual: ${userMessage}`,
        ].join("");

        const streamUrl = `https://us-central1-aiplatform.googleapis.com/v1/${AGENT_ENGINE_RESOURCE_NAME}:streamQuery`;
        const streamRes = await fetch(streamUrl, {
          method: "POST",
          headers: authHeaders,
          body: JSON.stringify({
            class_method: "stream_query",
            input: {
              user_id: user.user_id,
              message: fullInput,
            },
          }),
          signal: req.signal,
        });

        if (!streamRes.ok) {
          const errText = await streamRes.text();
          throw new Error(`Agent Engine stream_query error (${streamRes.status}): ${errText}`);
        }

        const responseText = await streamRes.text();
        console.log("[AE] stream_query raw response (first 2000 chars):", responseText.slice(0, 2000));
        let fullText = "";
        for (const line of responseText.split("\n")) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            const obj = JSON.parse(trimmed);
            const parts = obj?.content?.parts ?? [];
            for (const part of parts) {
              if (part?.text) fullText += part.text;
            }
            if (obj?.output) {
              const output = typeof obj.output === "string" ? obj.output : "";
              if (output && !fullText) fullText = output;
            }
            if (obj?.result?.content?.parts) {
              for (const part of obj.result.content.parts) {
                if (part?.text) fullText += part.text;
              }
            }
          } catch {
            // Skip non-JSON lines
          }
        }

        console.log("[AE] extracted fullText length:", fullText.length, "preview:", fullText.slice(0, 200));

        if (!fullText.trim()) {
          await chatbot.rpc("rollback_usage", { p_user_id: user.user_id });
          safeEnqueue(
            `data: ${JSON.stringify({
              d: true,
              e: "EMPTY_RESPONSE",
              r: usage.remaining,
              l: usage.queries_limit,
            })}\n\n`
          );
          try { controller.close(); } catch { /* already closed */ }
          return;
        }

        // Simulate streaming: send text in chunks
        const CHUNK_SIZE = 80;
        for (let i = 0; i < fullText.length; i += CHUNK_SIZE) {
          if (req.signal.aborted) break;
          const chunk = fullText.slice(i, i + CHUNK_SIZE);
          safeEnqueue(`data: ${JSON.stringify({ t: chunk, d: false })}\n\n`);
        }

        // Store messages in DB
        const latencyMs = Date.now() - startTime;
        const insertResult = await chatbot
          .from("messages")
          .insert([
            {
              conversation_id: conversationId,
              role: "user",
              content: userMessage,
              context_client_id: clienteId,
            },
            {
              conversation_id: conversationId,
              role: "assistant",
              content: fullText,
              context_client_id: clienteId,
              latency_ms: latencyMs,
            },
          ])
          .select("id, role");

        const assistantMsgId =
          insertResult.data?.find(
            (m: { role: string }) => m.role === "assistant"
          )?.id ?? null;

        await chatbot
          .from("conversations")
          .update({ updated_at: new Date().toISOString() })
          .eq("id", conversationId);

        // Final SSE with metadata
        safeEnqueue(
          `data: ${JSON.stringify({
            d: true,
            cid: conversationId,
            mid: assistantMsgId,
            r: Math.max((usage.remaining ?? 1) - 1, 0),
            l: usage.queries_limit,
          })}\n\n`
        );

        try { controller.close(); } catch { /* already closed */ }

        // Compaction (fire and forget)
        const totalMessages = (history.messages.length ?? 0) + 2;
        if (totalMessages >= COMPACTION_THRESHOLD && !history.summary) {
          const allMsgs = [
            ...history.messages,
            { role: "user", content: userMessage },
            { role: "assistant", content: fullText },
          ];
          compactConversation(conversationId, allMsgs).catch(() => {});
        }
      } catch (e) {
        const errMsg = e instanceof Error ? e.message : "Error al procesar";
        console.error("Agent Engine proxy error:", errMsg);

        if (!req.signal.aborted) {
          try {
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ d: true, e: errMsg })}\n\n`
              )
            );
            controller.close();
          } catch {
            // Controller already closed
          }
        } else {
          try { controller.close(); } catch { /* already closed */ }
        }
      }
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      ...CORS_HEADERS,
    },
  });
}
