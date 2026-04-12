import { GCP_PROJECT_ID, VERTEX_MODEL } from "./constants.ts";
import { getAccessToken } from "./vertex-auth.ts";
import type { GeminiContent } from "./types.ts";

/**
 * Non-streaming Gemini call (used for compaction only, no tools).
 */
export async function callGemini(
  systemPrompt: string,
  contents: GeminiContent[]
): Promise<{ text: string; tokensInput: number; tokensOutput: number }> {
  const token = await getAccessToken();
  const url = `https://aiplatform.googleapis.com/v1/projects/${GCP_PROJECT_ID}/locations/global/publishers/google/models/${VERTEX_MODEL}:generateContent`;

  let res: Response | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        systemInstruction: { parts: [{ text: systemPrompt }] },
        contents,
        generationConfig: { maxOutputTokens: 1024, temperature: 0.3 },
      }),
    });
    if (res.status !== 429 || attempt === 2) break;
    await new Promise((r) => setTimeout(r, 2000 * Math.pow(2, attempt)));
  }

  if (!res!.ok) {
    throw new Error(`Gemini API error (${res!.status}): ${await res!.text()}`);
  }

  const data = await res!.json();
  const text = data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
  const tokensInput = data.usageMetadata?.promptTokenCount ?? 0;
  const tokensOutput = data.usageMetadata?.candidatesTokenCount ?? 0;

  return { text, tokensInput, tokensOutput };
}
