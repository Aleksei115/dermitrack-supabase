import { chatbot, MAX_HISTORY_MESSAGES, SYSTEM_PROMPT_CACHE_TTL, COMPACTION_THRESHOLD } from "./constants.ts";
import { callGemini } from "./gemini.ts";
import type { GeminiContent } from "./types.ts";

// ============================================================================
// System Prompt Cache
// ============================================================================

let systemPromptCache: { data: string; expiry: number } | null = null;

export async function getSystemPrompt(): Promise<string> {
  if (systemPromptCache && Date.now() < systemPromptCache.expiry) {
    return systemPromptCache.data;
  }

  const { data, error } = await chatbot
    .from("config")
    .select("value")
    .eq("key", "system_prompt")
    .single();

  if (error || !data) {
    throw new Error("System prompt not found in chatbot.config");
  }

  systemPromptCache = {
    data: data.value,
    expiry: Date.now() + SYSTEM_PROMPT_CACHE_TTL,
  };
  return data.value;
}

// ============================================================================
// Conversation History
// ============================================================================

export async function getConversationHistory(conversationId: string): Promise<{
  summary: string | null;
  messages: Array<{ role: string; content: string }>;
}> {
  const { data: conv } = await chatbot
    .from("conversations")
    .select("summary")
    .eq("id", conversationId)
    .single();

  const { data: msgs } = await chatbot
    .from("messages")
    .select("role, content")
    .eq("conversation_id", conversationId)
    .order("created_at", { ascending: true });

  return {
    summary: conv?.summary ?? null,
    messages: msgs ?? [],
  };
}

export async function getPreviousSummaries(
  userId: string,
  excludeConvId?: string
): Promise<string> {
  let query = chatbot
    .from("conversations")
    .select("summary")
    .eq("user_id", userId)
    .not("summary", "is", null)
    .order("created_at", { ascending: false })
    .limit(3);

  if (excludeConvId) {
    query = query.neq("id", excludeConvId);
  }

  const { data } = await query;
  if (!data || data.length === 0) return "";

  return `RESUMEN DE CONVERSACIONES ANTERIORES:\n${data.map((c: { summary: string }) => `- ${c.summary}`).join("\n")}`;
}

// ============================================================================
// Compaction
// ============================================================================

export async function compactConversation(
  conversationId: string,
  messages: Array<{ role: string; content: string }>
): Promise<void> {
  try {
    const prompt =
      "Resume esta conversacion en maximo 200 palabras conservando datos clave, nombres de productos y cifras. Responde SOLO con el resumen.";
    const contents: GeminiContent[] = messages.map((m) => ({
      role: m.role === "assistant" ? "model" : "user",
      parts: [{ text: m.content }],
    }));

    const { text } = await callGemini(prompt, contents);

    await chatbot
      .from("conversations")
      .update({ summary: text, updated_at: new Date().toISOString() })
      .eq("id", conversationId);
  } catch (e) {
    console.error("Compaction failed (non-critical):", e);
  }
}

export { COMPACTION_THRESHOLD };

// ============================================================================
// Build Gemini History
// ============================================================================

export function buildGeminiHistory(
  history: {
    summary: string | null;
    messages: Array<{ role: string; content: string }>;
  },
  currentMessage: string
): GeminiContent[] {
  let historyContents: GeminiContent[] = [];

  if (history.summary && history.messages.length > 4) {
    historyContents.push({
      role: "user",
      parts: [{ text: `[Resumen previo: ${history.summary}]` }],
    });
    historyContents.push({
      role: "model",
      parts: [{ text: "Entendido, tengo el contexto." }],
    });
    const recent = history.messages.slice(-4);
    for (const m of recent) {
      historyContents.push({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.content }],
      });
    }
  } else {
    const recent = history.messages.slice(-MAX_HISTORY_MESSAGES);
    for (const m of recent) {
      historyContents.push({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.content }],
      });
    }
  }

  historyContents.push({ role: "user", parts: [{ text: currentMessage }] });

  // Ensure starts with user role
  if (historyContents.length > 0 && historyContents[0].role === "model") {
    historyContents = historyContents.slice(1);
  }

  // Merge consecutive same-role messages
  const merged: GeminiContent[] = [];
  for (const msg of historyContents) {
    if (
      merged.length > 0 &&
      merged[merged.length - 1].role === msg.role
    ) {
      const lastPart = merged[merged.length - 1].parts[0];
      if (lastPart.text && msg.parts[0].text) {
        lastPart.text += "\n" + msg.parts[0].text;
      }
    } else {
      merged.push({ role: msg.role, parts: [...msg.parts] });
    }
  }

  return merged;
}
