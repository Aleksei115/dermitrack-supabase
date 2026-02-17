import { createClient } from "npm:@supabase/supabase-js@2.45.4";

const EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

interface PushQueueMessage {
  notification_id: string;
  user_id: string;
  title: string;
  body: string;
  data: Record<string, unknown>;
}

interface UserPreferences {
  push_enabled: boolean;
  quiet_hours_start: string | null;
  quiet_hours_end: string | null;
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function isInQuietHours(prefs: UserPreferences): boolean {
  if (!prefs.quiet_hours_start || !prefs.quiet_hours_end) {
    return false;
  }
  const now = new Date();
  const currentTime = `${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}`;
  // Simple comparison - works for most cases
  return currentTime >= prefs.quiet_hours_start && currentTime <= prefs.quiet_hours_end;
}

Deno.serve(async (req) => {
  const requestId = crypto.randomUUID();
  const log = (msg: string, extra?: unknown) => {
    if (extra !== undefined) {
      console.log(`[send-push-notification] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[send-push-notification] ${requestId} ${msg}`);
  };

  try {
    if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
      log("missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
      return jsonResponse({ error: "Missing server config" }, 500);
    }

    // Read messages from the queue
    const { data: messages, error: readError } = await supabase.rpc("pgmq_read", {
      queue_name: "push_notification_queue",
      vt: 30,
      qty: 50,
    });

    if (readError) {
      // Try alternative approach - direct SQL
      log("pgmq_read RPC failed, trying direct", readError);
      const { data: directMessages, error: directError } = await supabase
        .from("pgmq.q_push_notification_queue")
        .select("*")
        .limit(50);

      if (directError) {
        log("Direct queue read also failed", directError);
        return jsonResponse({ error: "Failed to read queue", details: readError }, 500);
      }

      log("Using direct read", { count: directMessages?.length ?? 0 });
    }

    if (!messages?.length) {
      return jsonResponse({ processed: 0, message: "No messages in queue" });
    }

    log("Processing messages", { count: messages.length });

    const results: Array<{ success: boolean; user_id: string; error?: string }> = [];

    for (const msg of messages) {
      const pushMsg: PushQueueMessage = msg.message;

      try {
        // Get active tokens for user
        const { data: tokens } = await supabase
          .from("user_push_tokens")
          .select("token")
          .eq("user_id", pushMsg.user_id)
          .eq("is_active", true);

        if (!tokens?.length) {
          log("No active tokens for user", { user_id: pushMsg.user_id });
          // Delete from queue even if no tokens
          await supabase.rpc("pgmq_delete", {
            queue_name: "push_notification_queue",
            msg_id: msg.msg_id,
          });
          continue;
        }

        // Check user preferences and quiet hours
        const { data: prefs } = await supabase
          .from("user_notification_preferences")
          .select("push_enabled, quiet_hours_start, quiet_hours_end")
          .eq("user_id", pushMsg.user_id)
          .single();

        if (prefs) {
          if (!prefs.push_enabled) {
            log("Push disabled for user", { user_id: pushMsg.user_id });
            await supabase.rpc("pgmq_delete", {
              queue_name: "push_notification_queue",
              msg_id: msg.msg_id,
            });
            continue;
          }

          if (isInQuietHours(prefs as UserPreferences)) {
            log("User in quiet hours, skipping", { user_id: pushMsg.user_id });
            // Don't delete - will retry later
            continue;
          }
        }

        // Build Expo push messages
        const pushMessages = tokens.map((t) => ({
          to: t.token,
          title: pushMsg.title,
          body: pushMsg.body,
          data: pushMsg.data,
          sound: "default",
        }));

        // Send to Expo
        const response = await fetch(EXPO_PUSH_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(pushMessages),
        });

        if (response.ok) {
          // Mark as sent
          await supabase
            .from("notifications")
            .update({ push_sent_at: new Date().toISOString() })
            .eq("id", pushMsg.notification_id);

          // Delete from queue
          await supabase.rpc("pgmq_delete", {
            queue_name: "push_notification_queue",
            msg_id: msg.msg_id,
          });

          results.push({ success: true, user_id: pushMsg.user_id });
          log("Push sent successfully", { user_id: pushMsg.user_id });
        } else {
          const errorText = await response.text();
          log("Expo push failed", { status: response.status, error: errorText });
          results.push({ success: false, user_id: pushMsg.user_id, error: errorText });
        }
      } catch (err) {
        log("Error processing message", err);
        results.push({
          success: false,
          user_id: pushMsg.user_id,
          error: String(err),
        });
      }
    }

    return jsonResponse({ processed: results.length, results });
  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500
    );
  }
});
