import {
  chatbot,
  GCP_PROJECT_ID,
  VERTEX_MODEL,
  MAX_TOOL_ITERATIONS,
  CORS_HEADERS,
} from "../constants.ts";
import { getAccessToken } from "../vertex-auth.ts";
import { executeTool } from "../tools-executor.ts";
import { truncateResult } from "../utils.ts";
import { compactConversation, COMPACTION_THRESHOLD } from "../conversation.ts";
import { TOOL_DECLARATIONS } from "../tools-declarations.ts";
import type { UserInfo, UsageResult, GeminiPart, GeminiSSEChunk } from "../types.ts";

export async function handleStreamingResponse(
  req: Request,
  systemPrompt: string,
  contents: import("../types.ts").GeminiContent[],
  conversationId: string,
  userMessage: string,
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
  let currentReader: ReadableStreamDefaultReader<Uint8Array> | null = null;

  const stream = new ReadableStream({
    async start(controller) {
      const safeEnqueue = (chunk: string): boolean => {
        if (req.signal.aborted) return false;
        try {
          controller.enqueue(encoder.encode(chunk));
          return true;
        } catch {
          return false;
        }
      };

      try {
        const mutableContents = [...contents];
        let fullText = "";
        let tokensInput = 0;
        let tokensOutput = 0;
        let hitMaxTokens = false;

        for (let iter = 0; iter < MAX_TOOL_ITERATIONS; iter++) {
          if (req.signal.aborted) break;

          const token = await getAccessToken();
          const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:streamGenerateContent?alt=sse`;

          let geminiRes: Response | null = null;
          for (let attempt = 0; attempt < 3; attempt++) {
            geminiRes = await fetch(url, {
              method: "POST",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
                systemInstruction: { parts: [{ text: systemPrompt }] },
                contents: mutableContents,
                ...TOOL_DECLARATIONS,
                generationConfig: {
                  maxOutputTokens: 4096,
                  temperature: 0.3,
                  thinkingConfig: { thinkingBudget: 4096 },
                },
              }),
            });

            if (geminiRes.status !== 429 || attempt === 2) break;
            const delay = 2000 * Math.pow(2, attempt);
            console.warn(`Gemini 429 rate limit, retrying in ${delay}ms (attempt ${attempt + 1}/3)`);
            safeEnqueue(`data: ${JSON.stringify({ t: "", d: false })}\n\n`);
            await new Promise((r) => setTimeout(r, delay));
          }

          if (!geminiRes!.ok) {
            const err = await geminiRes!.text();
            if (geminiRes!.status === 429) {
              throw new Error("El servicio está ocupado. Intenta de nuevo en unos segundos.");
            }
            throw new Error(
              `Gemini streaming error (${geminiRes!.status}): ${err}`
            );
          }

          const reader = geminiRes!.body!.getReader();
          currentReader = reader;
          const decoder = new TextDecoder();
          let buffer = "";
          const functionCalls: Array<{
            name: string;
            args: Record<string, unknown>;
          }> = [];
          const allModelParts: GeminiPart[] = [];

          while (true) {
            if (req.signal.aborted) {
              console.warn("Client disconnected during stream");
              break;
            }

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
                const chunk: GeminiSSEChunk = JSON.parse(jsonStr);
                const parts = chunk.candidates?.[0]?.content?.parts ?? [];

                for (const part of parts) {
                  allModelParts.push(part);

                  if (part.text && !part.thought) {
                    fullText += part.text;
                    safeEnqueue(
                      `data: ${JSON.stringify({ t: part.text, d: false })}\n\n`
                    );
                  } else if (part.thought) {
                    safeEnqueue(
                      `data: ${JSON.stringify({ t: "", d: false })}\n\n`
                    );
                  }
                  if (part.functionCall) {
                    functionCalls.push({
                      name: part.functionCall.name,
                      args: part.functionCall.args ?? {},
                    });
                  }
                }

                const finishReason =
                  chunk.candidates?.[0]?.finishReason;
                if (finishReason === "MAX_TOKENS") {
                  hitMaxTokens = true;
                }

                if (chunk.usageMetadata) {
                  tokensInput =
                    chunk.usageMetadata.promptTokenCount ?? tokensInput;
                  tokensOutput =
                    chunk.usageMetadata.candidatesTokenCount ?? tokensOutput;
                }
              } catch {
                // Skip unparseable chunks
              }
            }
          }

          currentReader = null;

          if (functionCalls.length > 0) {
            safeEnqueue(
              `data: ${JSON.stringify({ t: "", d: false })}\n\n`
            );

            const keepaliveInterval = setInterval(() => {
              safeEnqueue(`data: ${JSON.stringify({ t: "", d: false })}\n\n`);
            }, 10_000);

            let results: unknown[];
            try {
              results = await Promise.all(
                functionCalls.map((fc) => executeTool(fc.name, fc.args, user))
              );
            } finally {
              clearInterval(keepaliveInterval);
            }

            mutableContents.push({
              role: "model",
              parts: allModelParts,
            });

            mutableContents.push({
              role: "user",
              parts: functionCalls.map((fc, i) => ({
                functionResponse: {
                  name: fc.name,
                  response: { result: truncateResult(results[i] as string) },
                },
              })),
            });

            continue;
          }

          break;
        }

        // MAX_TOKENS or empty response: refund query, send error
        if (hitMaxTokens || fullText.trim().length === 0) {
          await chatbot.rpc("rollback_usage", {
            p_user_id: user.user_id,
          });

          const errorCode = hitMaxTokens ? "RESPONSE_TOO_LONG" : "EMPTY_RESPONSE";
          console.warn(`Stream ended with ${errorCode} (fullText length: ${fullText.length}, hitMaxTokens: ${hitMaxTokens})`);

          safeEnqueue(
            `data: ${JSON.stringify({
              d: true,
              e: "RESPONSE_TOO_LONG",
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
              tokens_input: tokensInput,
              tokens_output: tokensOutput,
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
        const errMsg =
          e instanceof Error ? e.message : "Error al procesar";
        console.error("Streaming error:", errMsg);

        if (!req.signal.aborted) {
          try {
            controller.enqueue(
              encoder.encode(
                `data: ${JSON.stringify({ d: true, e: errMsg })}\n\n`
              )
            );
            controller.close();
          } catch {
            // Controller already closed (client disconnected)
          }
        } else {
          try { controller.close(); } catch { /* already closed */ }
        }
      }
    },
    cancel() {
      currentReader?.cancel();
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
