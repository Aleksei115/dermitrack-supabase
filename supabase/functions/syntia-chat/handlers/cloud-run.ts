import {
  chatbot,
  CLOUD_RUN_URL,
  CORS_HEADERS,
} from "../constants.ts";
import { getIdentityToken } from "../vertex-auth.ts";
import { compactConversation, COMPACTION_THRESHOLD } from "../conversation.ts";
import type { UserInfo, UsageResult, GeminiPart } from "../types.ts";

const ADK_APP_NAME = "syntia_agent";

export async function handleCloudRunResponse(
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
        const idToken = await getIdentityToken();

        // Step 1: Create ephemeral ADK session
        const createSessionUrl = `${CLOUD_RUN_URL}/apps/${ADK_APP_NAME}/users/${user.user_id}/sessions`;
        const sessionRes = await fetch(createSessionUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${idToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            state: {
              user_id: user.user_id,
              role: user.role,
            },
          }),
        });

        if (!sessionRes.ok) {
          const errText = await sessionRes.text();
          throw new Error(
            `ADK session creation failed (${sessionRes.status}): ${errText}`
          );
        }

        const sessionData = await sessionRes.json();
        const sessionId: string = sessionData.id;

        // Step 2: Send message with conversation context
        const messageContent = [
          conversationHistory,
          `\nUSER: user_id=${user.user_id}, role=${user.role}`,
          `\nMensaje actual: ${userMessage}`,
        ].join("");

        const runUrl = `${CLOUD_RUN_URL}/apps/${ADK_APP_NAME}/users/${user.user_id}/sessions/${sessionId}`;
        const runRes = await fetch(runUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${idToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            new_message: {
              role: "user",
              parts: [{ text: messageContent }],
            },
            streaming: true,
          }),
          signal: req.signal,
        });

        if (!runRes.ok) {
          const errText = await runRes.text();
          throw new Error(
            `Cloud Run agent error (${runRes.status}): ${errText}`
          );
        }

        // Step 3: Read SSE stream from Cloud Run and proxy text to client
        let fullText = "";
        const reader = runRes.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        const keepaliveInterval = setInterval(() => {
          safeEnqueue(`data: ${JSON.stringify({ t: "", d: false })}\n\n`);
        }, 10_000);

        try {
          while (true) {
            if (req.signal.aborted) break;
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() ?? "";

            for (const line of lines) {
              if (!line.startsWith("data: ")) continue;
              const jsonStr = line.slice(6).trim();
              if (!jsonStr || jsonStr === "[DONE]") continue;

              try {
                const event = JSON.parse(jsonStr);
                const parts = event.content?.parts ?? [];

                for (const part of parts) {
                  if (part.text && !part.functionCall && !part.functionResponse) {
                    fullText += part.text;
                    safeEnqueue(
                      `data: ${JSON.stringify({ t: part.text, d: false })}\n\n`
                    );
                  }
                }

                if (!parts.some((p: GeminiPart) => p.text && !p.functionCall)) {
                  safeEnqueue(
                    `data: ${JSON.stringify({ t: "", d: false })}\n\n`
                  );
                }
              } catch {
                // Skip unparseable SSE events
              }
            }
          }
        } finally {
          clearInterval(keepaliveInterval);
        }

        // Handle empty response
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
        console.error("Cloud Run proxy error:", errMsg);

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
