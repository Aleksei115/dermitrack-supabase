import { CORS_HEADERS, MAX_TOOL_RESULT_LENGTH } from "./constants.ts";

export function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
  });
}

export function truncateResult(text: string): string {
  if (text.length <= MAX_TOOL_RESULT_LENGTH) return text;
  return text.substring(0, MAX_TOOL_RESULT_LENGTH) + "\n... (resultado truncado)";
}
