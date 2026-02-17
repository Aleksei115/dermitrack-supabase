import { createClient } from "npm:@supabase/supabase-js@2.45.4";

/**
 * admin-update-user-email
 *
 * Updates the email of an existing user (both in Auth and usuarios table).
 * Optionally sends a password reset email after update.
 *
 * Only accessible by OWNER or ADMINISTRADOR roles.
 */

type UpdateEmailPayload = {
  id_usuario: string;      // ID del usuario en tabla usuarios
  new_email: string;       // Nuevo email (será normalizado)
  send_reset?: boolean;    // Enviar reset password (default: true)
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

function normalizeEmail(email: string): string {
  return email.toLowerCase().trim();
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

  // Get user's role from usuarios table
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
      console.log(`[admin-update-user-email] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[admin-update-user-email] ${requestId} ${msg}`);
  };

  // Handle CORS preflight
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

    // Debug: Log all headers
    const headers: Record<string, string> = {};
    req.headers.forEach((value, key) => {
      headers[key] = key.toLowerCase() === 'authorization' ? `Bearer ***${value.slice(-10)}` : value;
    });
    log("received headers", headers);

    // Verify caller is OWNER or ADMINISTRADOR
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

    // Parse request body
    let body: UpdateEmailPayload;
    try {
      body = (await req.json()) as UpdateEmailPayload;
    } catch {
      return jsonResponse({ error: "JSON invalido" }, 400);
    }

    const { id_usuario, new_email, send_reset = true } = body;

    if (!id_usuario || !new_email) {
      return jsonResponse({ error: "id_usuario y new_email son obligatorios" }, 400);
    }

    const normalizedEmail = normalizeEmail(new_email);
    log("processing request", { id_usuario, new_email: normalizedEmail, send_reset });

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(normalizedEmail)) {
      return jsonResponse({ error: "Formato de email invalido" }, 400);
    }

    // Check if email already exists in usuarios table
    const { data: existingUsuario, error: existingError } = await supabaseAdmin
      .from("usuarios")
      .select("id_usuario, email")
      .eq("email", normalizedEmail)
      .neq("id_usuario", id_usuario)
      .maybeSingle();

    if (existingError) {
      log("error checking existing email", existingError);
      return jsonResponse({ error: "Error al verificar email existente" }, 500);
    }

    if (existingUsuario) {
      return jsonResponse({
        error: "El email ya esta registrado para otro usuario",
        existing_user_id: existingUsuario.id_usuario
      }, 409);
    }

    // Get target user data
    const { data: targetUser, error: targetError } = await supabaseAdmin
      .from("usuarios")
      .select("*")
      .eq("id_usuario", id_usuario)
      .single();

    if (targetError || !targetUser) {
      log("target user not found", { id_usuario, error: targetError });
      return jsonResponse({ error: "Usuario no encontrado" }, 404);
    }

    const oldEmail = targetUser.email;
    log("target user found", { oldEmail, auth_user_id: targetUser.auth_user_id });

    // Variable para guardar el auth_user_id (existente o nuevo)
    let authUserId = targetUser.auth_user_id;
    let authAccountCreated = false;

    if (targetUser.auth_user_id) {
      // Usuario YA tiene cuenta de Auth - actualizar email
      // First check if email exists in auth
      const { data: authCheck } = await supabaseAdmin.auth.admin.listUsers();
      const emailExistsInAuth = authCheck?.users?.some(
        u => u.email?.toLowerCase() === normalizedEmail && u.id !== targetUser.auth_user_id
      );

      if (emailExistsInAuth) {
        return jsonResponse({
          error: "El email ya existe en el sistema de autenticacion"
        }, 409);
      }

      log("updating email in auth", { auth_user_id: targetUser.auth_user_id });
      const { error: authUpdateError } = await supabaseAdmin.auth.admin.updateUserById(
        targetUser.auth_user_id,
        { email: normalizedEmail, email_confirm: true }
      );

      if (authUpdateError) {
        log("failed to update auth email", authUpdateError);
        return jsonResponse({
          error: "No se pudo actualizar el email en Auth",
          details: authUpdateError.message
        }, 500);
      }
    } else {
      // Usuario NO tiene cuenta de Auth - CREAR una nueva
      log("creating new auth account for user", { id_usuario, email: normalizedEmail });

      // Verificar que el email no exista ya en Auth
      const { data: authCheck } = await supabaseAdmin.auth.admin.listUsers();
      const existingAuthUser = authCheck?.users?.find(
        u => u.email?.toLowerCase() === normalizedEmail
      );

      if (existingAuthUser) {
        // El email ya existe en Auth pero no está vinculado a este usuario
        // Verificar si está vinculado a otro usuario en la tabla usuarios
        const { data: linkedUser } = await supabaseAdmin
          .from("usuarios")
          .select("id_usuario, nombre")
          .eq("auth_user_id", existingAuthUser.id)
          .maybeSingle();

        if (linkedUser) {
          return jsonResponse({
            error: "El email ya existe en Auth y esta vinculado a otro usuario",
            linked_to: linkedUser.nombre
          }, 409);
        }

        // Existe en Auth pero no está vinculado - usar ese auth_user_id
        log("auth user exists but not linked, linking now", { authId: existingAuthUser.id });
        authUserId = existingAuthUser.id;
      } else {
        // Crear nueva cuenta de Auth
        // Generar password temporal (el usuario lo cambiará con el reset)
        const tempPassword = crypto.randomUUID() + "Aa1!";

        const { data: newAuthUser, error: createError } = await supabaseAdmin.auth.admin.createUser({
          email: normalizedEmail,
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
    }

    // Update email AND auth_user_id in usuarios table
    const updateData: { email: string; auth_user_id?: string } = { email: normalizedEmail };

    // Si se creó o vinculó una cuenta de Auth, guardar el auth_user_id
    if (authUserId && !targetUser.auth_user_id) {
      updateData.auth_user_id = authUserId;
    }

    const { data: updatedUser, error: updateError } = await supabaseAdmin
      .from("usuarios")
      .update(updateData)
      .eq("id_usuario", id_usuario)
      .select()
      .single();

    if (updateError) {
      log("failed to update usuarios table", updateError);
      // Try to rollback auth email if we updated it
      if (targetUser.auth_user_id) {
        await supabaseAdmin.auth.admin.updateUserById(
          targetUser.auth_user_id,
          { email: oldEmail }
        );
      }
      return jsonResponse({
        error: "No se pudo actualizar el email en la tabla usuarios",
        details: updateError.message
      }, 500);
    }

    log("email updated successfully", { old: oldEmail, new: normalizedEmail });

    // Send password reset if requested
    let resetSent = false;
    let resetError: string | null = null;

    if (send_reset) {
      log("sending password reset email", { email: normalizedEmail });
      // No especificamos redirectTo para que use el Site URL configurado en Supabase
      const { error: resetErr } = await supabaseAdmin.auth.resetPasswordForEmail(
        normalizedEmail
      );

      if (resetErr) {
        log("failed to send reset email", resetErr);
        resetError = resetErr.message;
      } else {
        resetSent = true;
        log("reset email sent successfully");
      }
    }

    // Log audit
    await logAudit(
      authAccountCreated ? "CREATE_AUTH_ACCOUNT" : "UPDATE_USER_EMAIL",
      id_usuario,
      caller,
      {
        old_email: oldEmail,
        new_email: normalizedEmail,
        auth_account_created: authAccountCreated,
        auth_user_id: authUserId,
        reset_sent: resetSent,
        reset_error: resetError,
      }
    );

    return jsonResponse({
      success: true,
      message: authAccountCreated
        ? "Email actualizado y cuenta de autenticacion creada"
        : "Email actualizado correctamente",
      user: {
        id_usuario: updatedUser.id_usuario,
        nombre: updatedUser.nombre,
        email: updatedUser.email,
        rol: updatedUser.rol,
        auth_user_id: updatedUser.auth_user_id,
      },
      old_email: oldEmail,
      new_email: normalizedEmail,
      auth_account_created: authAccountCreated,
      auth_account_linked: !targetUser.auth_user_id && authUserId ? true : false,
      reset_sent: resetSent,
      reset_error: resetError,
    }, 200);

  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500
    );
  }
});
