import {
  chatbot,
  GCP_PROJECT_ID,
  VERTEX_MODEL,
  MAX_TOOL_ITERATIONS,
} from "../constants.ts";
import { getAccessToken } from "../vertex-auth.ts";
import { executeTool } from "../tools-executor.ts";
import { jsonResponse, truncateResult } from "../utils.ts";
import { compactConversation, COMPACTION_THRESHOLD } from "../conversation.ts";
import { TOOL_DECLARATIONS } from "../tools-declarations.ts";
import type { UserInfo, UsageResult, GeminiPart, GeminiContent } from "../types.ts";

export async function handleNonStreamingResponse(
  systemPrompt: string,
  contents: GeminiContent[],
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
  const mutableContents = [...contents];
  let finalText = "";
  let tokensInput = 0;
  let tokensOutput = 0;

  for (let iter = 0; iter < MAX_TOOL_ITERATIONS; iter++) {
    const token = await getAccessToken();
    const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:generateContent`;

    const res = await fetch(url, {
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

    if (!res.ok) {
      throw new Error(
        `Gemini API error (${res.status}): ${await res.text()}`
      );
    }

    const data = await res.json();
    tokensInput += data.usageMetadata?.promptTokenCount ?? 0;
    tokensOutput += data.usageMetadata?.candidatesTokenCount ?? 0;

    const parts: GeminiPart[] =
      data.candidates?.[0]?.content?.parts ?? [];
    const functionCalls = parts
      .filter((p) => p.functionCall)
      .map((p) => p.functionCall!);

    if (functionCalls.length > 0) {
      const results = await Promise.all(
        functionCalls.map((fc) => executeTool(fc.name, fc.args, user))
      );

      mutableContents.push({
        role: "model",
        parts,
      });

      mutableContents.push({
        role: "user",
        parts: functionCalls.map((fc, i) => ({
          functionResponse: {
            name: fc.name,
            response: { result: truncateResult(results[i]) },
          },
        })),
      });

      continue;
    }

    finalText = parts.find((p) => p.text && !p.thought)?.text ?? "";
    break;
  }

  // Empty response safety: refund and return error
  if (finalText.trim().length === 0) {
    await chatbot.rpc("rollback_usage", { p_user_id: user.user_id });
    return jsonResponse({
      error: "RESPONSE_TOO_LONG",
      message: "Limite de respuesta alcanzado. Intenta hacer una pregunta mas especifica.",
      remaining: usage.remaining,
      queries_limit: usage.queries_limit,
    }, 422);
  }

  const latencyMs = Date.now() - startTime;

  const { data: insertedMsgs } = await chatbot
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
        content: finalText,
        context_client_id: clienteId,
        tokens_input: tokensInput,
        tokens_output: tokensOutput,
        latency_ms: latencyMs,
      },
    ])
    .select("id, role");

  const assistantMsgId =
    insertedMsgs?.find((m: { role: string }) => m.role === "assistant")?.id ??
    null;

  await chatbot
    .from("conversations")
    .update({ updated_at: new Date().toISOString() })
    .eq("id", conversationId);

  const totalMessages = (history.messages.length ?? 0) + 2;
  if (totalMessages >= COMPACTION_THRESHOLD && !history.summary) {
    const allMsgs = [
      ...history.messages,
      { role: "user", content: userMessage },
      { role: "assistant", content: finalText },
    ];
    compactConversation(conversationId, allMsgs).catch(() => {});
  }

  return jsonResponse({
    message: finalText,
    conversation_id: conversationId,
    message_id: assistantMsgId,
    remaining_queries: Math.max((usage.remaining ?? 1) - 1, 0),
    queries_limit: usage.queries_limit,
  });
}
