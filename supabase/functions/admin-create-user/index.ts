import { createClient } from "npm:@supabase/supabase-js@2.45.4";

type CreatePayload = {
  email: string;
  password: string;
  is_usuario?: string;
  user_metadata?: Record<string, unknown>;
  perfil?: {
    user_id?: string;
    name?: string;
    email?: string;
    password?: string;
    role?: string;
    active?: boolean;
    created_date?: string;
    auth_user_id?: string;
    id_zoho?: string;  // ID de Zoho CRM para futura integracion
  };
};

type CallerInfo = {
  auth_user_id: string;
  role: string;
  name: string;
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
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

async function getCallerInfo(authHeader: string | null): Promise<CallerInfo | null> {
  if (!authHeader) return null;

  const token = authHeader.replace("Bearer ", "");
  const { data: { user }, error } = await supabase.auth.getUser(token);

  if (error || !user) return null;

  // Get user's role from users table
  const { data: usuario, error: userError } = await supabase
    .from("users")
    .select("role, name")
    .eq("auth_user_id", user.id)
    .single();

  if (userError || !usuario) return null;

  return {
    auth_user_id: user.id,
    role: usuario.role,
    name: usuario.name,
  };
}

function defaultNameFromEmail(email: string) {
  const local = email.split("@")[0] || "Usuario";
  return local.replace(/[._-]+/g, " ").trim();
}

async function findAuthUserByEmail(email: string) {
  const target = email.toLowerCase();
  const perPage = 1000;
  for (let page = 1; page <= 10; page += 1) {
    const { data, error } = await supabase.auth.admin.listUsers({
      page,
      perPage,
    });
    if (error) return { user: null, error };
    const users = data?.users ?? [];
    const match = users.find((u) => (u.email ?? "").toLowerCase() === target);
    if (match) return { user: match, error: null };
    if (users.length < perPage) break;
  }
  return { user: null, error: null };
}

Deno.serve(async (req) => {
  const requestId = crypto.randomUUID();
  const log = (msg: string, extra?: unknown) => {
    if (extra !== undefined) {
      console.log(`[admin-create-user] ${requestId} ${msg}`, extra);
      return;
    }
    console.log(`[admin-create-user] ${requestId} ${msg}`);
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
      return jsonResponse({ error: "Método no permitido" }, 405);
    }

    // Debug: Log headers
    const headers: Record<string, string> = {};
    req.headers.forEach((value, key) => {
      headers[key] = key.toLowerCase() === 'authorization' ? `Bearer ***${value.slice(-10)}` : value;
    });
    log("received headers", headers);

    // Verify caller is OWNER or ADMINISTRADOR
    const authHeader = req.headers.get("Authorization") || req.headers.get("authorization");
    log("auth header present", { hasAuth: !!authHeader, length: authHeader?.length ?? 0 });

    const caller = await getCallerInfo(authHeader);

    if (!caller) {
      log("unauthorized - no valid caller", { authHeaderPresent: !!authHeader });
      return jsonResponse({ error: "No autorizado" }, 401);
    }

    if (!["OWNER", "ADMIN"].includes(caller.role)) {
      log("forbidden - caller is not OWNER or ADMINISTRADOR", { role: caller.role });
      return jsonResponse({ error: "Acceso denegado. Solo OWNER o ADMINISTRADOR pueden realizar esta accion." }, 403);
    }

    log("caller verified", { role: caller.role, name: caller.name });

    let body: CreatePayload;
    try {
      body = (await req.json()) as CreatePayload;
    } catch {
      return jsonResponse({ error: "JSON inválido" }, 400);
    }

    const { email, password, is_usuario, user_metadata } = body ?? {};
    if (!email || !password) {
      return jsonResponse({ error: "email y password son obligatorios" }, 400);
    }

    const finalMetadata = {
      ...(user_metadata ?? {}),
      ...(is_usuario ? { is_usuario } : {}),
    };

    log("creating auth user", { email });
    const { data: authData, error: authError } =
      await supabase.auth.admin.createUser({
        email,
        password,
        user_metadata: finalMetadata,
        email_confirm: true,
      });

    let authId = authData.user?.id;
    let authEmail = authData.user?.email ?? email;
    if (authError) {
      if (authError.code === "email_exists") {
        log("auth user exists, fetching by email", { email });
        const { user, error: listErr } = await findAuthUserByEmail(email);
        if (listErr) {
          log("auth.listUsers failed", listErr);
          return jsonResponse(
            { error: "No se pudo buscar el usuario en Auth", details: listErr },
            400,
          );
        }
        if (!user?.id) {
          return jsonResponse(
            { error: "Usuario ya existe, pero no se pudo recuperar el id" },
            409,
          );
        }
        authId = user.id;
        authEmail = user.email ?? email;
      } else {
        log("auth.createUser failed", authError);
        return jsonResponse(
          { error: "No se pudo crear el usuario en Auth", details: authError },
          400,
        );
      }
    }

    if (!authId) {
      log("auth.createUser missing id", authData);
      return jsonResponse(
        { error: "Respuesta inválida de Auth (sin id)" },
        502,
      );
    }

    log("checking existing link by auth_user_id", { authId });
    const { data: existingByAuth, error: authLinkErr } = await supabase
      .from("users")
      .select("user_id, email, auth_user_id")
      .eq("auth_user_id", authId)
      .maybeSingle();

    if (authLinkErr) {
      log("users lookup by auth_user_id failed", authLinkErr);
      return jsonResponse(
        {
          id: authId,
          email: authEmail,
          perfil_error: { message: authLinkErr.message, details: authLinkErr.details },
        },
        201,
      );
    }

    if (existingByAuth && existingByAuth.email !== authEmail) {
      log("auth_user_id already linked to another user", existingByAuth);
      return jsonResponse(
        {
          error: "auth_user_id ya está ligado a otro usuario",
          id: authId,
          email: authEmail,
          linked_email: existingByAuth.email,
        },
        409,
      );
    }

    log("looking for existing perfil", { authEmail });
    const { data: existing, error: findErr } = await supabase
      .from("users")
      .select("user_id, auth_user_id")
      .eq("email", authEmail)
      .maybeSingle();

    if (findErr) {
      log("users lookup failed", findErr);
      return jsonResponse(
        {
          id: authId,
          email: authEmail,
          perfil_error: { message: findErr.message, details: findErr.details },
        },
        201,
      );
    }

    if (existing) {
      if (existing.auth_user_id === authId) {
        return jsonResponse({ id: authId, email: authEmail }, 201);
      }
      if (existing.auth_user_id && existing.auth_user_id !== authId) {
        log("email already linked to another auth_user_id", existing);
        return jsonResponse(
          {
            error: "email ya está ligado a otro auth_user_id",
            id: authId,
            email: authEmail,
          },
          409,
        );
      }

      log("linking existing user", { authEmail });
      const { data: updated, error: updErr } = await supabase
        .from("users")
        .update({ auth_user_id: authId })
        .eq("email", authEmail)
        .is("auth_user_id", null)
        .select("user_id, auth_user_id, email")
        .maybeSingle();

      if (updErr) {
        log("users update failed", updErr);
        return jsonResponse(
          { id: authId, email: authEmail, link_error: updErr.message },
          201,
        );
      }
      if (!updated) {
        log("users update affected 0 rows", { authEmail });
        return jsonResponse(
          { id: authId, email: authEmail, link_error: "No se pudo linkear" },
          409,
        );
      }

      return jsonResponse(
        { id: authId, email: authEmail, linked_user_email: authEmail },
        201,
      );
    }

    const perfil = body?.profile ?? body?.perfil ?? {};

    // Validate id_zoho uniqueness if provided
    if (perfil.id_zoho) {
      const { data: existingZoho, error: zohoError } = await supabase
        .from("users")
        .select("user_id")
        .eq("id_zoho", perfil.id_zoho)
        .maybeSingle();

      if (zohoError) {
        log("error checking id_zoho uniqueness", zohoError);
        return jsonResponse(
          { error: "Error al verificar id_zoho", details: zohoError.message },
          500,
        );
      }

      if (existingZoho) {
        log("id_zoho already exists", { id_zoho: perfil.id_zoho });
        return jsonResponse(
          { error: "El id_zoho ya esta registrado para otro usuario", existing_id: existingZoho.user_id },
          409,
        );
      }
    }

    const insertRow = {
      user_id: perfil.user_id ?? authId,
      name: perfil.name ?? defaultNameFromEmail(authEmail),
      email: authEmail,
      password: perfil.password ?? "DISABLED",
      role: perfil.role ?? "USUARIO",
      active: perfil.active ?? true,
      created_date: perfil.created_date ?? new Date().toISOString(),
      auth_user_id: authId,
      id_zoho: perfil.id_zoho ?? null,
    };

    log("inserting new user", { email: authEmail, user_id: insertRow.user_id });
    const { data: inserted, error: insErr } = await supabase
      .from("users")
      .insert(insertRow)
      .select("*")
      .single();

    if (insErr) {
      log("users insert failed", insErr);
      return jsonResponse(
        {
          id: authId,
          email: authEmail,
          perfil_error: { message: insErr.message, details: insErr.details },
        },
        201,
      );
    }

    return jsonResponse({ id: authId, email: authEmail, perfil: inserted }, 201);
  } catch (e) {
    log("unexpected error", e);
    return jsonResponse(
      { error: "Error inesperado", details: String(e) },
      500,
    );
  }
});
