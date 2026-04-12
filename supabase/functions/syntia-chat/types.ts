// ============================================================================
// Types
// ============================================================================

export interface UserInfo {
  user_id: string;
  role: string;
  auth_user_id: string;
}

export interface UsageResult {
  allowed: boolean;
  queries_used: number;
  queries_limit: number;
  remaining: number;
}

export interface GeminiPart {
  text?: string;
  thought?: boolean;
  thoughtSignature?: string;
  functionCall?: { name: string; args: Record<string, unknown> };
  functionResponse?: { name: string; response: Record<string, unknown> };
}

export interface GeminiContent {
  role: string;
  parts: GeminiPart[];
}

export interface GeminiSSEChunk {
  candidates?: Array<{
    content?: { role?: string; parts?: GeminiPart[] };
    finishReason?: string;
  }>;
  usageMetadata?: {
    promptTokenCount?: number;
    candidatesTokenCount?: number;
  };
}

// deno-lint-ignore no-explicit-any
export type AnyRow = Record<string, any>;
