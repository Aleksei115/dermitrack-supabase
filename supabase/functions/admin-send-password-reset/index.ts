import { createClient } from "npm:@supabase/supabase-js@2.45.4";

/**
 * admin-send-password-reset
 *
 * Sends a password reset email to an existing user.
 * If the user doesn't have an auth account, creates one first.
 * Only accessible by OWNER or ADMINISTRADOR roles.
 */

type ResetPayload = {
  id_usuario: string;  // ID del usuario en tabla usuarios
};

type CallerInfo = {
  auth_user_id: string;
  rol: string;
  nombre: string;
};

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
      "Access-Control-Allow-Headers": "authorization, content-type, x-client-info, apikey",
    },
  });
}

function corsResponse() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "authorization, content-type, x-client-info, apikey",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
  });
}

async function getCallerInfo(authHeader: string | null, log: (msg: string, extra?: unknown) => void): Promise<CallerInfo | null> {
  if (!authHeader) {
    log("getCallerInfo: no auth header");
    return null;
  }

  const token = authHeader.replace("Bearer ", "");
  log("getCallerInfo: token length", token.length);

  const { data: { user }, error } = await supabaseAdmin.auth.getUser(token);

  if (error) {
    log("getCallerInfo: auth.getUser error", { message: error.message, status: error.status });
    return null;
  }

  if (!user) {
    log("getCallerInfo: no user returned");
    return null;
  }

  log("getCallerInfo: user found", { id: user.id, email: user.email });

  const { data: usuario, error: userError } = await supabaseAdmin
    .from("usuarios")
    .select("rol, nombre")
    .eq("auth_user_id", user.id)
    .single();

  if (userError) {
    log("getCallerInfo: usuarios query error", { message: userError.message, code: userError.code });
    return null;
  }

  if (!usuario) {
    log("getCallerInfo: no usuario found for auth_user_id", user.id);
    return null;
  }

  log("getCallerInfo: caller info found", { rol: usuario.rol, nombre: usuario.nombre });

  return {
    auth_user_id: user.id,
    rol: usuario.rol,
    nombre: usuario.nombre,
  };
}

async function logAudit(
  action: string,
  targetUserId: string,
  performedBy: CallerInfo,
  details: Record<string, unknown>
) {
  try {
    await supabaseAdmin.from("audit_log").insert({
      action,
      target_user_id: targetUserId,
      performed_by: performedBy.auth_user_id,
      performed_by_name: performedBy.nombre,
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
      console.log(`[admin-send-password-reset] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[admin-send-password-reset] ${requestId} ${msg}`);
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

    const headers: Record<string, string> = {};
    req.headers.forEach((value, key) => {
      headers[key] = key.toLowerCase() === 'authorization' ? `Bearer ***${value.slice(-10)}` : value;
    });
    log("received headers", headers);

    const authHeader = req.headers.get("Authorization") || req.headers.get("authorization");
    log("auth header present", { hasAuth: !!authHeader, length: authHeader?.length ?? 0 });

    const caller = await getCallerInfo(authHeader, log);

    if (!caller) {
      log("unauthorized - no valid caller", { authHeaderPresent: !!authHeader });
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    if (!["OWNER", "ADMINISTRADOR"].includes(caller.rol)) {
      log("forbidden - caller is not OWNER or ADMINISTRADOR", { rol: caller.rol });
      return jsonResponse({ error: "Acceso denegado. Solo OWNER o ADMINISTRADOR pueden realizar esta accion." }, 403);
    }

    log("caller verified", { rol: caller.rol, nombre: caller.nombre });

    let body: ResetPayload;
    try {
      body = (await req.json()) as ResetPayload;
    } catch {
      return jsonResponse({ error: "JSON invalido" }, 400);
    }

    const { id_usuario } = body;

    if (!id_usuario) {
      return jsonResponse({ error: "id_usuario es obligatorio" }, 400);
    }

    log("processing request", { id_usuario });

    const { data: targetUser, error: targetError } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, nombre, email, auth_user_id")
      .eq("id_usuario", id_usuario)
      .single();

    if (targetError || !targetUser) {
      log("target user not found", { id_usuario, error: targetError });
      return jsonResponse({ error: "Usuario no encontrado" }, 404);
    }

    if (!targetUser.email) {
      return jsonResponse({ error: "El usuario no tiene email configurado" }, 400);
    }

    log("target user found", { nombre: targetUser.nombre, email: targetUser.email, auth_user_id: targetUser.auth_user_id });

    let authUserId = targetUser.auth_user_id;
    let authAccountCreated = false;

    // If user doesn't have auth account, create one
    if (!authUserId) {
      log("user has no auth account, creating one", { email: targetUser.email });

      // Check if email exists in Auth
      const { data: authCheck } = await supabaseAdmin.auth.admin.listUsers();
      const existingAuthUser = authCheck?.users?.find(
        u => u.email?.toLowerCase() === targetUser.email.toLowerCase()
      );

      if (existingAuthUser) {
        // Email exists in Auth - check if linked to another user
        const { data: linkedUser } = await supabaseAdmin
          .from("usuarios")
          .select("id_usuario, nombre")
          .eq("auth_user_id", existingAuthUser.id)
          .neq("id_usuario", id_usuario)
          .maybeSingle();

        if (linkedUser) {
          return jsonResponse({
            error: "El email ya esta vinculado a otro usuario en el sistema",
            linked_to: linkedUser.nombre
          }, 409);
        }

        // Auth user exists but not linked - link it
        log("auth user exists but not linked, linking now", { authId: existingAuthUser.id });
        authUserId = existingAuthUser.id;
      } else {
        // Create new auth account
        const tempPassword = crypto.randomUUID() + "Aa1!";

        const { data: newAuthUser, error: createError } = await supabaseAdmin.auth.admin.createUser({
          email: targetUser.email,
          password: tempPassword,
          email_confirm: true,
        });

        if (createError) {
          log("failed to create auth account", createError);
          return jsonResponse({
            error: "No se pudo crear la cuenta de autenticacion",
            details: createError.message
          }, 500);
        }

        authUserId = newAuthUser.user?.id;
        authAccountCreated = true;
        log("auth account created", { authUserId });
      }

      if (!authUserId) {
        return jsonResponse({
          error: "No se pudo obtener el ID de autenticacion"
        }, 500);
      }

      // Update usuarios table with auth_user_id
      const { error: updateError } = await supabaseAdmin
        .from("usuarios")
        .update({ auth_user_id: authUserId })
        .eq("id_usuario", id_usuario);

      if (updateError) {
        log("failed to update auth_user_id in usuarios", updateError);
        return jsonResponse({
          error: "No se pudo vincular la cuenta de autenticacion",
          details: updateError.message
        }, 500);
      }

      log("auth_user_id linked to usuario", { id_usuario, authUserId });
    }

    // Send password reset email
    log("sending password reset email", { email: targetUser.email });
    // No especificamos redirectTo para que use el Site URL configurado en Supabase
    const { error: resetError } = await supabaseAdmin.auth.resetPasswordForEmail(
      targetUser.email
    );

    if (resetError) {
      log("failed to send reset email", resetError);
      return jsonResponse({
        error: "No se pudo enviar el email de restablecimiento",
        details: resetError.message
      }, 500);
    }

    log("reset email sent successfully", { email: targetUser.email });

    // Log audit
    await logAudit(
      authAccountCreated ? "CREATE_AUTH_AND_SEND_RESET" : "SEND_PASSWORD_RESET",
      id_usuario,
      caller,
      {
        target_email: targetUser.email,
        target_nombre: targetUser.nombre,
        auth_account_created: authAccountCreated,
      }
    );

    return jsonResponse({
      success: true,
      message: authAccountCreated
        ? `Cuenta de acceso creada y email de restablecimiento enviado a ${targetUser.email}`
        : `Email de restablecimiento enviado a ${targetUser.email}`,
      user: {
        id_usuario: targetUser.id_usuario,
        nombre: targetUser.nombre,
        email: targetUser.email,
      },
      auth_account_created: authAccountCreated,
    }, 200);

  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500
    );
  }
});
