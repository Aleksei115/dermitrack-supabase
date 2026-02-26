import { createClient } from "npm:@supabase/supabase-js@2.45.4";

/**
 * reset-user-password
 *
 * Public endpoint (no JWT required) for in-app password reset.
 * Two actions:
 *   - verify_email: checks if email exists in users table
 *   - update_password: changes password via admin API + sends notification email
 */

type VerifyEmailPayload = {
  action: "verify_email";
  email: string;
};

type UpdatePasswordPayload = {
  action: "update_password";
  email: string;
  new_password: string;
};

type Payload = VerifyEmailPayload | UpdatePasswordPayload;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const supabaseAdmin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers":
        "authorization, content-type, x-client-info, apikey",
    },
  });
}

function corsResponse() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers":
        "authorization, content-type, x-client-info, apikey",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
  });
}

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PASSWORD_MIN_LENGTH = 8;

async function logAudit(
  action: string,
  targetUserId: string,
  details: Record<string, unknown>
) {
  try {
    await supabaseAdmin.from("audit_log").insert({
      action,
      target_user_id: targetUserId,
      performed_by: null,
      performed_by_name: "SELF_SERVICE",
      details: JSON.stringify(details),
      created_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error("[audit_log] Failed to log audit:", e);
  }
}

Deno.serve(async (req) => {
  const requestId = crypto.randomUUID();
  const log = (msg: string, extra?: unknown) => {
    if (extra !== undefined) {
      console.log(`[reset-user-password] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[reset-user-password] ${requestId} ${msg}`);
  };

  if (req.method === "OPTIONS") {
    return corsResponse();
  }

  try {
    if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
      log("missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
      return jsonResponse({ error: "Missing server config" }, 500);
    }

    if (req.method !== "POST") {
      return jsonResponse({ error: "Metodo no permitido" }, 405);
    }

    let body: Payload;
    try {
      body = (await req.json()) as Payload;
    } catch {
      return jsonResponse({ error: "JSON invalido" }, 400);
    }

    const { action, email } = body;

    if (!action || !email) {
      return jsonResponse({ error: "action y email son obligatorios" }, 400);
    }

    const trimmedEmail = email.trim().toLowerCase();

    if (!EMAIL_REGEX.test(trimmedEmail)) {
      return jsonResponse({ error: "Formato de email invalido" }, 400);
    }

    // ─── ACTION: verify_email ───────────────────────────────────
    if (action === "verify_email") {
      log("verify_email", { email: trimmedEmail });

      const { data: user } = await supabaseAdmin
        .from("users")
        .select("user_id")
        .eq("email", trimmedEmail)
        .maybeSingle();

      // Always return 200 to prevent enumeration by status code
      return jsonResponse({ exists: !!user });
    }

    // ─── ACTION: update_password ────────────────────────────────
    if (action === "update_password") {
      const { new_password } = body as UpdatePasswordPayload;

      if (!new_password) {
        return jsonResponse({ error: "new_password es obligatorio" }, 400);
      }

      if (new_password.length < PASSWORD_MIN_LENGTH) {
        return jsonResponse(
          {
            error: `La contraseña debe tener al menos ${PASSWORD_MIN_LENGTH} caracteres`,
          },
          400
        );
      }

      log("update_password", { email: trimmedEmail });

      // Find user in users table
      const { data: user, error: userError } = await supabaseAdmin
        .from("users")
        .select("user_id, name, auth_user_id")
        .eq("email", trimmedEmail)
        .maybeSingle();

      if (userError || !user) {
        log("user not found or error", { error: userError });
        // Generic response to prevent enumeration
        return jsonResponse({ success: true });
      }

      if (!user.auth_user_id) {
        log("user has no auth_user_id", { user_id: user.user_id });
        return jsonResponse({ success: true });
      }

      // Update password via admin API
      const { error: updateError } =
        await supabaseAdmin.auth.admin.updateUserById(user.auth_user_id, {
          password: new_password,
        });

      if (updateError) {
        log("failed to update password", {
          message: updateError.message,
        });
        return jsonResponse(
          { error: "No se pudo actualizar la contraseña" },
          500
        );
      }

      log("password updated successfully", {
        user_id: user.user_id,
      });

      // Audit log
      await logAudit("SELF_SERVICE_PASSWORD_RESET", user.user_id, {
        email: trimmedEmail,
        name: user.name,
      });

      // Best-effort notification email via Supabase Auth
      // This sends a "magic link" style email that serves as notification
      try {
        const RESEND_API_KEY = Deno.env.get("RESEND_API_KEY");
        if (RESEND_API_KEY) {
          await fetch("https://api.resend.com/emails", {
            method: "POST",
            headers: {
              Authorization: `Bearer ${RESEND_API_KEY}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              from: "DermiTrack <no-reply@dermitrack.com>",
              to: [trimmedEmail],
              subject: "Tu contraseña ha sido cambiada",
              html: `
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                  <h2 style="color: #1e293b;">Contraseña actualizada</h2>
                  <p style="color: #475569;">Hola ${user.name || ""},</p>
                  <p style="color: #475569;">Tu contraseña de DermiTrack ha sido cambiada exitosamente.</p>
                  <p style="color: #475569;">Si no realizaste este cambio, contacta a tu administrador inmediatamente.</p>
                  <hr style="border: none; border-top: 1px solid #e2e8f0; margin: 20px 0;" />
                  <p style="color: #94a3b8; font-size: 12px;">Este es un mensaje automático de DermiTrack.</p>
                </div>
              `,
            }),
          });
          log("notification email sent");
        } else {
          log("RESEND_API_KEY not configured, skipping notification email");
        }
      } catch (emailError) {
        log("failed to send notification email (non-blocking)", emailError);
      }

      return jsonResponse({ success: true });
    }

    return jsonResponse({ error: "Accion no valida" }, 400);
  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500
    );
  }
});
