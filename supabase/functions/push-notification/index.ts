import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

interface NotificationRecord {
  id: string;
  user_id: string;
  type: string;
  title: string;
  body: string;
  data: Record<string, unknown>;
}

interface WebhookPayload {
  type: "INSERT" | "UPDATE" | "DELETE";
  table: string;
  record: NotificationRecord;
  schema: "public";
  old_record: null | NotificationRecord;
}

Deno.serve(async (req) => {
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const expoToken = Deno.env.get("EXPO_ACCESS_TOKEN");

    // Create client with service role - bypasses RLS
    const supabase = createClient(supabaseUrl, supabaseKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false,
      },
    });

    const payload: WebhookPayload = await req.json();
    console.log("[push-notification] Received:", payload.type, payload.record?.id);

    const notification = payload.record;

    // Get all active push tokens for user
    const { data: tokens, error: tokensError } = await supabase
      .from("user_push_tokens")
      .select("token")
      .eq("user_id", notification.user_id)
      .eq("is_active", true);

    if (tokensError) {
      console.error("[push-notification] Token error:", JSON.stringify(tokensError));
      return new Response(JSON.stringify({ error: tokensError.message }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    if (!tokens || tokens.length === 0) {
      console.log("[push-notification] No tokens for:", notification.user_id);
      return new Response(JSON.stringify({ skipped: "no_tokens" }), {
        headers: { "Content-Type": "application/json" },
      });
    }

    console.log("[push-notification] Sending to", tokens.length, "devices");

    // Build messages for all tokens
    const messages = tokens.map((t) => ({
      to: t.token,
      sound: "default",
      title: notification.title,
      body: notification.body,
      data: notification.data || {},
    }));

    // Send to Expo
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (expoToken) {
      headers["Authorization"] = `Bearer ${expoToken}`;
    }

    const res = await fetch("https://exp.host/--/api/v2/push/send", {
      method: "POST",
      headers,
      body: JSON.stringify(messages),
    });

    const expoResponse = await res.json();
    console.log("[push-notification] Expo response:", JSON.stringify(expoResponse));

    // Mark notification as sent
    await supabase
      .from("notifications")
      .update({ push_sent_at: new Date().toISOString() })
      .eq("id", notification.id);

    return new Response(JSON.stringify(expoResponse), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error("[push-notification] Error:", error);
    return new Response(JSON.stringify({ error: String(error) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
