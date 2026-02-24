// Supabase Edge Function: seed-missing-auth-users
// Crea cuentas en auth.users para filas en public.users cuyo auth_user_id no existe en auth.users
// Requisitos:
// - Variables de entorno disponibles: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// - Tabla public.users con columnas: id (pk), email (text), nombre (opcional), auth_user_id (uuid nullable)
// - RLS permitiendo al service role leer/actualizar public.users (service role bypass RLS)

import { createClient } from "npm:@supabase/supabase-js@2.45.4";

interface User {
  id: string | number;
  email: string | null;
  name?: string | null;
  auth_user_id: string | null;
}

console.info("seed-missing-auth-users function started");

Deno.serve(async (req: Request) => {
  try {
    if (req.method !== 'POST') {
      return new Response(JSON.stringify({ error: 'Use POST' }), { status: 405, headers: { 'Content-Type': 'application/json' } });
    }

    const url = Deno.env.get('SUPABASE_URL');
    const serviceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
    if (!url || !serviceKey) {
      return new Response(JSON.stringify({ error: 'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }

    const admin = createClient(url, serviceKey, { auth: { persistSession: false } });

    // 1) Seleccionar solo los usuarios cuyo auth_user_id no existe en auth.users
    const { data: candidates, error: selErr } = await admin
      .from('users')
      .select('id, email, name, auth_user_id')
      .not('auth_user_id', 'is', null);

    if (selErr) throw selErr;

    // Filtrar en memoria los que realmente faltan en auth.users para minimizar roundtrips
    // Construir set de auth_user_id distintos
    const ids = Array.from(new Set((candidates || []).map((u: User) => u.auth_user_id).filter(Boolean))) as string[];

    const missing = new Set<string>();
    if (ids.length > 0) {
      // Consultar auth.users por lotes en páginas si fuera necesario (límite típico 1000)
      const pageSize = 500;
      const existing = new Set<string>();
      for (let i = 0; i < ids.length; i += pageSize) {
        const batch = ids.slice(i, i + pageSize);
        // Supabase no permite seleccionar auth.users directamente con el client por default;
        // pero con service role y schema auth expuesto, se puede usar RPC o vista. Usamos Admin API para verificar individualmente.
        for (const id of batch) {
          try {
            const { data: usr } = await admin.auth.admin.getUserById(id);
            if (usr?.user?.id) existing.add(usr.user.id);
          } catch (_) { /* ignore */ }
        }
      }
      ids.forEach((id) => { if (!existing.has(id)) missing.add(id); });
    }

    // Filtrar candidatos por los realmente faltantes
    const targets: User[] = (candidates || []).filter((u: User) => u.auth_user_id && missing.has(u.auth_user_id));

    if (!targets.length) {
      return new Response(JSON.stringify({ created: 0, message: 'No hay usuarios pendientes' }), { headers: { 'Content-Type': 'application/json' } });
    }

    let created = 0;
    const errors: Array<{ id: User['id']; reason: string }> = [];

    for (const u of targets) {
      // Validar email
      const email = (u.email || '').trim().toLowerCase();
      if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        errors.push({ id: u.id, reason: 'Email inválido o vacío' });
        continue;
      }

      // Crear usuario nuevo en Auth
      const tempPassword = crypto.randomUUID();
      const { data: createdUser, error: cErr } = await admin.auth.admin.createUser({
        email,
        password: tempPassword,
        email_confirm: true,
        user_metadata: { name: u.name ?? null, source: 'seed-missing-auth-users' },
      });
      if (cErr || !createdUser?.user?.id) {
        errors.push({ id: u.id, reason: cErr?.message || 'Fallo al crear usuario' });
        continue;
      }

      const newId = createdUser.user.id;
      // Actualizar public.users con el nuevo auth_user_id
      const { error: upErr } = await admin.from('users').update({ auth_user_id: newId }).eq('id', u.id);
      if (upErr) {
        errors.push({ id: u.id, reason: upErr.message });
        continue;
      }

      created++;
    }

    return new Response(JSON.stringify({ created, attempted: targets.length, errors }), { headers: { 'Content-Type': 'application/json' } });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
});
