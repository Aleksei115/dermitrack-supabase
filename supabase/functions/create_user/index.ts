import { createClient } from "npm:@supabase/supabase-js@2.45.4";

type CreatePayload = {
  email: string;
  password: string;
  is_usuario?: string;
  user_metadata?: Record<string, unknown>;
  perfil?: {
    user_id?: string;     // opcional para nueva fila
    name?: string;
    email?: string;          // si lo envías, validamos que coincida o lo sobreescribimos con el email principal
    password?: string;       // solo para tabla, no para Auth
    role?: string;
    active?: boolean;
    created_date?: string; // ISO o 'YYYY-MM-DD HH:MM:SS'
    auth_user_id?: string;   // será sobrescrito
    [k: string]: unknown;
  };
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const AUTH_ADMIN_URL = `${SUPABASE_URL}/auth/v1/admin/users`;

const admin = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const USERS_COLUMNS = new Set([
  "user_id",
  "name",
  "email",
  "password",
  "role",
  "active",
  "created_date",
  "auth_user_id",
]);

function sanitizePerfil(perfil: Record<string, unknown> | undefined | null) {
  const out: Record<string, unknown> = {};
  if (!perfil || typeof perfil !== "object") return out;
  for (const [k, v] of Object.entries(perfil)) {
    if (USERS_COLUMNS.has(k)) out[k] = v;
  }
  return out;
}

function defaultNameFromEmail(email: string) {
  const local = email.split("@")[0] || "Usuario";
  return local.replace(/[._-]+/g, " ").trim();
}

Deno.serve(async (req: Request) => {
  try {
    if (req.method !== "POST") {
      return new Response(JSON.stringify({ error: "Método no permitido" }), {
        status: 405,
        headers: { "Content-Type": "application/json" },
      });
    }

    const body = (await req.json()) as CreatePayload;
    const { email, password, is_usuario, user_metadata } = body ?? {};

    if (!email || !password) {
      return new Response(JSON.stringify({ error: "email y password son obligatorios" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Metadata para Auth
    const finalMetadata = {
      ...(user_metadata ?? {}),
      ...(is_usuario ? { is_usuario } : {}),
    };

    // 1) Crear usuario en Auth
    const res = await fetch(AUTH_ADMIN_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        email,
        password,
        user_metadata: finalMetadata,
        email_confirm: true, // opcional según tu flujo
      }),
    });

    const authJson = await res.json();
    if (!res.ok) {
      return new Response(
        JSON.stringify({ error: "No se pudo crear el usuario en Auth", details: authJson }),
        { status: res.status, headers: { "Content-Type": "application/json" } }
      );
    }

    const authId: string | undefined = authJson?.id;
    const authEmail: string = authJson?.email ?? email;
    if (!authId) {
      return new Response(JSON.stringify({ error: "Respuesta inválida de Auth (sin id)" }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }

    // 2) Buscar perfil por email en public.users
    const { data: existing, error: findErr } = await admin
      .from("users")
      .select("user_id, auth_user_id")
      .eq("email", authEmail)
      .maybeSingle();

    if (findErr) {
      // Error inesperado al buscar
      return new Response(
        JSON.stringify({
          id: authId,
          email: authEmail,
          perfil_error: { message: findErr.message, details: findErr.details },
        }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    }

    // Si existe, linkeamos auth_user_id
    if (existing) {
      const { error: updErr } = await admin
        .from("users")
        .update({ auth_user_id: authId })
        .eq("email", authEmail);

      if (updErr) {
        return new Response(
          JSON.stringify({
            id: authId,
            email: authEmail,
            link_error: updErr.message,
          }),
          { status: 201, headers: { "Content-Type": "application/json" } }
        );
      }

      return new Response(
        JSON.stringify({
          id: authId,
          email: authEmail,
          linked_user_email: authEmail,
        }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    }

    // Si no existe, creamos un nuevo perfil
    const safePerfil = sanitizePerfil(body?.perfil);

    // Aplicar defaults
    safePerfil.email = authEmail; // forzamos a coincidir
    if (!("password" in safePerfil) || !safePerfil.password) safePerfil.password = "DISABLED";
    if (!("name" in safePerfil) || !safePerfil.name) safePerfil.name = defaultNameFromEmail(authEmail);
    if (!("role" in safePerfil) || !safePerfil.role) safePerfil.role = "USUARIO";
    if (!("active" in safePerfil)) safePerfil.active = true;
    if (!("created_date" in safePerfil) || !safePerfil.created_date) {
      safePerfil.created_date = new Date().toISOString();
    }
    safePerfil.auth_user_id = authId;

    const { data: inserted, error: insErr } = await admin
      .from("users")
      .insert(safePerfil)
      .select("*")
      .single();

    if (insErr) {
      return new Response(
        JSON.stringify({
          id: authId,
          email: authEmail,
          perfil_error: { message: insErr.message, details: insErr.details },
        }),
        { status: 201, headers: { "Content-Type": "application/json" } }
      );
    }

    return new Response(
      JSON.stringify({
        id: authId,
        email: authEmail,
        perfil: inserted,
      }),
      { status: 201, headers: { "Content-Type": "application/json" } }
    );
  } catch (e) {
    return new Response(JSON.stringify({ error: "Error inesperado", details: String(e) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
