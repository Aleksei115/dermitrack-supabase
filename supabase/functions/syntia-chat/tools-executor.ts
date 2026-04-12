import { chatbot, admin } from "./constants.ts";
import { generateEmbedding } from "./embeddings.ts";
import { truncateResult } from "./utils.ts";
import type { UserInfo, AnyRow } from "./types.ts";

// ============================================================================
// Data Helpers
// ============================================================================

function getUserFilter(user: UserInfo): { isAdmin: boolean; userId: string } {
  const isAdmin = user.role === "OWNER" || user.role === "ADMIN";
  return { isAdmin, userId: user.user_id };
}

// ============================================================================
// PubMed Search — NCBI E-utilities (esearch + efetch)
// ============================================================================

function extractXmlTag(xml: string, tag: string): string {
  const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = xml.match(regex);
  return match ? match[1].trim() : "";
}

function extractPubDate(articleXml: string): string {
  const pubDate = extractXmlTag(articleXml, "PubDate");
  if (pubDate) {
    const year = extractXmlTag(pubDate, "Year");
    const month = extractXmlTag(pubDate, "Month");
    if (year) return month ? `${month} ${year}` : year;
  }
  return "Fecha no disponible";
}

async function searchPubMed(query: string): Promise<string> {
  try {
    const searchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=${encodeURIComponent(query + " dermatology")}&retmode=json&retmax=1&sort=date`;
    const searchRes = await fetch(searchUrl);
    if (!searchRes.ok) return "Error al buscar en PubMed.";

    const searchData = await searchRes.json();
    const ids: string[] = searchData.esearchresult?.idlist ?? [];
    if (ids.length === 0)
      return "No se encontraron estudios recientes en PubMed para esa busqueda.";

    const pmid = ids[0];

    const fetchUrl = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${pmid}&retmode=xml&rettype=abstract`;
    const fetchRes = await fetch(fetchUrl);
    if (!fetchRes.ok) return "Error al obtener articulo de PubMed.";

    const xml = await fetchRes.text();

    const title = extractXmlTag(xml, "ArticleTitle") || "Sin titulo";
    const journal = extractXmlTag(xml, "Title") || "Journal desconocido";
    const pubDate = extractPubDate(xml);

    let abstract = "";
    const abstractMatch = xml.match(/<Abstract>([\s\S]*?)<\/Abstract>/i);
    if (abstractMatch) {
      const abstractTexts =
        abstractMatch[1].match(
          /<AbstractText[^>]*>([\s\S]*?)<\/AbstractText>/gi
        ) ?? [];
      abstract = abstractTexts
        .map((t) => t.replace(/<[^>]+>/g, "").trim())
        .join(" ");
    }
    if (abstract.length > 500)
      abstract = abstract.substring(0, 500) + "...";
    if (!abstract) abstract = "Abstract no disponible.";

    const authorBlock = xml.match(/<Author[^>]*>([\s\S]*?)<\/Author>/i);
    let author = "Autor desconocido";
    if (authorBlock) {
      const lastName = extractXmlTag(authorBlock[1], "LastName");
      const initials = extractXmlTag(authorBlock[1], "Initials");
      if (lastName) author = `${lastName} ${initials}`.trim() + " et al.";
    }

    const doiMatch = xml.match(
      /<ArticleId IdType="doi">([\s\S]*?)<\/ArticleId>/i
    );
    const doi = doiMatch ? doiMatch[1].trim() : null;

    const doiLine = doi ? `DOI: ${doi} | ` : "";
    return `Estudio reciente (${pubDate}) — ${journal}\n"${title}"\n${author}\nAbstract: ${abstract}\n${doiLine}PMID: ${pmid}`;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error("PubMed search error:", msg);
    return "Error al consultar PubMed. Intenta de nuevo.";
  }
}

// ============================================================================
// Tool Execution — dispatches function calls to Supabase RPCs
// ============================================================================

export async function executeTool(
  name: string,
  args: Record<string, unknown>,
  user: UserInfo
): Promise<string> {
  const { isAdmin, userId } = getUserFilter(user);

  try {
    switch (name) {
      case "search_medicamentos": {
        const query = args.query as string;
        const idCliente = (args.client_id as string) ?? null;
        const embedding = await generateEmbedding(query, "RETRIEVAL_QUERY");
        const { data, error } = await chatbot.rpc("match_medications", {
          query_embedding: JSON.stringify(embedding),
          match_threshold: 0.55,
          match_count: idCliente ? 25 : 10,
        });
        if (error) return `Error: ${error.message}`;

        let results = (data ?? []) as AnyRow[];

        // Hybrid search: fallback fuzzy if embeddings miss
        if (results.length === 0) {
          const { data: fuzzyData } = await chatbot.rpc(
            "fuzzy_search_medications",
            { p_search: query, p_limit: 10 }
          );

          if (fuzzyData?.length) {
            const fuzzySkus = (fuzzyData as AnyRow[]).map((m) => m.sku);

            const [medsRes, condRes] = await Promise.all([
              chatbot
                .from("medications")
                .select("sku, brand, description, content, price")
                .in("sku", fuzzySkus),
              chatbot
                .from("medication_conditions")
                .select("sku, conditions:conditions(name)")
                .in("sku", fuzzySkus),
            ]);

            if (medsRes.data?.length) {
              const condMap = new Map<string, string[]>();
              if (condRes.data) {
                for (const mc of condRes.data as AnyRow[]) {
                  const list = condMap.get(mc.sku) ?? [];
                  if (mc.conditions?.name) list.push(mc.conditions.name);
                  condMap.set(mc.sku, list);
                }
              }

              results = (medsRes.data as AnyRow[]).map((m) => ({
                ...m,
                conditions: condMap.get(m.sku)?.join(", ") ?? null,
              }));

              console.log(
                `[Fuzzy search] Embedding miss, fuzzy fallback found ${results.length} results for "${query}"`
              );
            }
          }
        }

        if (!results.length) return "No se encontraron medicamentos relevantes.";

        // When recommending for a specific doctor: exclude existing products + enrich with sales data
        if (idCliente) {
          const excludedSkus = new Set<string>();

          const [invRes, clasifRes, rankingRes] = await Promise.all([
            chatbot.rpc("get_doctor_inventory", {
              p_client_id: idCliente,
              p_user_id: userId,
              p_is_admin: true,
            }),
            chatbot.rpc("classification_by_client", {
              p_client_id: idCliente,
            }),
            chatbot.rpc("get_complete_sales_ranking", { p_limit_count: 200 }),
          ]);

          if (invRes.data) {
            for (const item of invRes.data as AnyRow[]) {
              excludedSkus.add(item.sku);
            }
          }

          if (clasifRes.data) {
            for (const item of clasifRes.data as AnyRow[]) {
              excludedSkus.add(item.sku);
            }
          }

          const before = results.length;
          results = results.filter((m) => !excludedSkus.has(m.sku));

          if (results.length === 0) {
            return `Se encontraron ${before} medicamentos relevantes pero todos ya estan en el botiquin o tienen historial (M1/M2/M3) con este medico. Considera otros padecimientos o categorias.`;
          }

          const salesMap = new Map<string, AnyRow>();
          if (rankingRes.data) {
            for (const r of rankingRes.data as AnyRow[]) {
              salesMap.set(r.sku, r);
            }
          }

          results = results.slice(0, 10);

          return results
            .map((m) => {
              const sales = salesMap.get(m.sku);
              const salesInfo = sales
                ? `| Ventas globales: ${sales.piezas_totales}pz $${sales.ventas_totales} (M1:$${sales.ventas_botiquin} M2:$${sales.ventas_conversion} M3:$${sales.ventas_exposicion})`
                : "| Sin historial de ventas global";
              return `${m.sku}: ${m.description} (${m.brand}) $${m.price} | ${m.content ?? ""} | Padecimientos: ${m.conditions || "N/A"} ${salesInfo}`;
            })
            .join("\n");
        }

        return results
          .map(
            (m) =>
              `${m.sku}: ${m.description} (${m.brand}) $${m.price} | ${m.content ?? ""} | Padecimientos: ${m.conditions || "N/A"}`
          )
          .join("\n");
      }

      case "search_fichas_tecnicas": {
        const query = args.query as string;
        let sheets: AnyRow[] = [];

        const { data: medMatches } = await chatbot.rpc(
          "fuzzy_search_medications",
          { p_search: query, p_limit: 5 }
        );

        if (medMatches?.length) {
          const skus = (medMatches as AnyRow[]).map((m) => m.sku);
          const { data: chunkData } = await chatbot
            .from("data_sheet_chunks")
            .select("sku, content, chunk_index")
            .in("sku", skus)
            .order("sku")
            .order("chunk_index");

          if (chunkData?.length) {
            sheets = chunkData as AnyRow[];
            console.log(
              `[Fuzzy search] Resolved ${skus.length} medications, found ${sheets.length} data sheet chunks for "${query}"`
            );
          }
        }

        if (sheets.length === 0) {
          const embedding = await generateEmbedding(query, "RETRIEVAL_QUERY");
          const { data, error } = await chatbot.rpc("match_data_sheets", {
            query_embedding: JSON.stringify(embedding),
            match_threshold: 0.60,
            match_count: 3,
          });
          if (error) return `Error: ${error.message}`;
          sheets = (data ?? []) as AnyRow[];
        }

        if (!sheets.length)
          return "No se encontro informacion tecnica relevante.";
        return sheets
          .map((f) => `[${f.sku}]:\n${f.content}`)
          .join("\n\n");
      }

      case "search_clientes": {
        const nombre = args.name as string;
        const { data, error } = await chatbot.rpc("fuzzy_search_clients", {
          p_search: nombre,
          p_user_id: null,
          p_limit: 5,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron medicos con ese nombre.";
        return (data as AnyRow[])
          .map(
            (c) =>
              `client_id: ${c.client_id} | Nombre: ${c.name} | Similitud: ${(c.similarity * 100).toFixed(0)}%`
          )
          .join("\n");
      }

      case "get_doctor_inventory": {
        const idCliente = args.client_id as string;
        const { data, error } = await chatbot.rpc("get_doctor_inventory", {
          p_client_id: idCliente,
          p_user_id: userId,
          p_is_admin: isAdmin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "El medico no tiene inventario en botiquin actualmente.";
        return (data as AnyRow[])
          .map(
            (item) =>
              `${item.sku}: ${item.description} (${item.brand}) | Cant: ${item.available_quantity} | $${item.price} | ${item.content ?? ""}`
          )
          .join("\n");
      }

      case "get_doctor_movements": {
        const idCliente = args.client_id as string;
        const fuente = (args.fuente as string) ?? "ambos";
        const limite = (args.limite as number) ?? 30;
        const { data, error } = await chatbot.rpc("get_doctor_movements", {
          p_client_id: idCliente,
          p_user_id: userId,
          p_is_admin: true,
          p_source: fuente,
          p_limit_count: limite,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No se encontraron movimientos para este medico.";
        return (data as AnyRow[])
          .map(
            (m) =>
              `[${m.fuente}] ${m.date?.substring(0, 10) ?? "?"} | ${m.type}: ${m.sku} - ${m.description} (${m.brand}) x${m.quantity} @ $${m.price ?? 0}`
          )
          .join("\n");
      }

      case "get_clasificacion_cliente": {
        const idCliente = args.client_id as string;
        const { data, error } = await chatbot.rpc(
          "classification_by_client",
          { p_client_id: idCliente }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay clasificacion disponible para este medico.";
        return (data as AnyRow[])
          .map((c) => `${c.sku}: ${c.clasificacion}`)
          .join("\n");
      }

      case "get_user_odv_sales": {
        const skuFilter = (args.sku_filter as string) ?? null;
        const limite = (args.limite as number) ?? 50;
        const { data, error } = await chatbot.rpc("get_user_odv_sales", {
          p_user_id: userId,
          p_is_admin: true,
          p_sku_filter: skuFilter,
          p_limit_count: limite,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron ventas ODV.";
        return (data as AnyRow[])
          .map(
            (v) =>
              `${v.date} | ${v.client_name} | ${v.sku}: ${v.description} (${v.brand}) x${v.quantity} @ $${v.price}`
          )
          .join("\n");
      }

      case "get_recolecciones": {
        const idCliente = (args.client_id as string) ?? null;
        const { data, error } = await chatbot.rpc(
          "get_user_collections",
          {
            p_user_id: userId,
            p_client_id: idCliente,
            p_limit: 20,
            p_is_admin: isAdmin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No se encontraron recolecciones.";
        const rows = data as AnyRow[];
        let totalPiezasGlobal = 0;
        const lines = rows.map((r) => {
          const itemsList: { sku: string; quantity: number }[] = r.items ?? [];
          const piezas = itemsList.reduce((sum, i) => sum + (i.quantity || 0), 0);
          totalPiezasGlobal += piezas;
          const items = itemsList.length > 0
            ? itemsList.map((i) => `${i.sku} x${i.quantity}`).join(", ")
            : "Sin items";
          const obs = r.cedis_observations
            ? ` | Obs: ${r.cedis_observations}`
            : "";
          return `${r.created_at?.substring(0, 10)} | ${r.client_name} | ${r.status} | ${piezas} piezas | ${items}${obs}`;
        });
        const resumen = `Resumen: ${rows.length} recolecciones, ${totalPiezasGlobal} piezas en total`;
        return `${resumen}\n---\n${lines.join("\n")}`;
      }

      case "get_estadisticas_corte": {
        const { data, error } = await admin.rpc(
          "get_cutoff_general_stats_with_comparison"
        );
        if (error) return `Error: ${error.message}`;
        if (!data) return "No hay estadisticas del corte actual.";
        const row = Array.isArray(data) ? data[0] : data;
        if (!row) return "No hay estadisticas del corte actual.";
        return JSON.stringify(row, null, 2);
      }

      case "get_estadisticas_por_medico": {
        const { data: statsData, error: statsError } = await admin.rpc(
          "get_cutoff_stats_by_doctor_with_comparison"
        );
        if (statsError) return `Error: ${statsError.message}`;
        if (!statsData?.length) return "No hay estadisticas por medico.";

        const limited = (statsData as AnyRow[]).slice(0, 30);
        return (
          `Estadisticas por medico (${(statsData as AnyRow[]).length} total, mostrando ${limited.length}):\n` +
          limited.map((m) => JSON.stringify(m)).join("\n")
        );
      }

      case "get_ranking_productos": {
        const limite = (args.limite as number) ?? 20;
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc("get_product_interest", {
          p_limit: limite,
          p_start_date: fechaInicio,
          p_end_date: fechaFin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de ranking de productos.";
        return (data as AnyRow[])
          .map((p) => JSON.stringify(p))
          .join("\n");
      }

      case "get_ranking_ventas": {
        const limite = (args.limite as number) ?? 20;
        const { data, error } = await chatbot.rpc(
          "get_complete_sales_ranking",
          { p_limit_count: limite }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de ventas.";
        return (data as AnyRow[])
          .map(
            (p) => {
              const desc = String(p.description ?? "").length > 50
                ? String(p.description).substring(0, 50) + "..."
                : String(p.description ?? "");
              return `${p.sku}: ${desc} (${p.brand}) | Botiquin(M1): ${p.piezas_botiquin}pz $${p.ventas_botiquin} | Conversion(M2): ${p.piezas_conversion}pz $${p.ventas_conversion} | Exposicion(M3): ${p.piezas_exposicion}pz $${p.ventas_exposicion} | TOTAL: ${p.piezas_totales}pz $${p.ventas_totales}`;
            }
          )
          .join("\n");
      }

      case "get_rendimiento_marcas": {
        const { data, error } = await chatbot.rpc(
          "get_complete_brand_performance"
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay datos de rendimiento por marca.";
        return (data as AnyRow[])
          .map(
            (b) =>
              `${b.brand} | Botiquin(M1): ${b.piezas_botiquin}pz $${b.ventas_botiquin} | Conversion(M2): ${b.piezas_conversion}pz $${b.ventas_conversion} | Exposicion(M3): ${b.piezas_exposicion}pz $${b.ventas_exposicion} | TOTAL: ${b.piezas_totales}pz $${b.ventas_totales}`
          )
          .join("\n");
      }

      case "get_datos_historicos": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc("get_historical_cutoff_data", {
          p_start_date: fechaInicio,
          p_end_date: fechaFin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data) return "No hay datos historicos disponibles.";
        return truncateResult(JSON.stringify(data, null, 2));
      }

      case "get_facturacion_medicos": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data: facData, error: facError } = await admin.rpc(
          "get_billing_composition",
          {
            p_start_date: fechaInicio,
            p_end_date: fechaFin,
          }
        );
        if (facError) return `Error: ${facError.message}`;
        if (!facData?.length)
          return "No hay datos de facturacion por medico.";
        const limited = (facData as AnyRow[]).slice(0, 30);
        return (
          `Facturacion por medico (${(facData as AnyRow[]).length} total, mostrando ${limited.length}):\n` +
          limited
            .map(
              (m) =>
                `${m.client_name} | Rango: ${m.current_tier ?? "N/A"} | Fact: $${m.current_billing ?? 0} | Baseline: $${m.baseline ?? 0} | M1: $${m.current_m1 ?? 0} | M2: $${m.current_m2 ?? 0} | M3: $${m.current_m3 ?? 0} | Crec: ${m.growth_pct ?? 0}%`
            )
            .join("\n")
        );
      }

      case "get_rendimiento_por_padecimiento": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc(
          "get_condition_performance",
          {
            p_start_date: fechaInicio,
            p_end_date: fechaFin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay datos de rendimiento por padecimiento.";
        return (data as AnyRow[])
          .map(
            (p) =>
              `${p.condition}: $${p.value} | ${p.pieces} piezas`
          )
          .join("\n");
      }

      case "get_impacto_botiquin": {
        const fechaInicio = (args.fecha_inicio as string) ?? null;
        const fechaFin = (args.fecha_fin as string) ?? null;
        const { data, error } = await admin.rpc(
          "get_cabinet_impact_summary",
          {
            p_start_date: fechaInicio,
            p_end_date: fechaFin,
          }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay datos de impacto del botiquin.";
        const row = (data as AnyRow[])[0];
        return [
          `Adopciones (M1→ODV): ${row.adoptions} | Revenue: $${row.revenue_adoptions}`,
          `Conversiones (M2): ${row.conversions} | Revenue: $${row.revenue_conversions}`,
          `Exposiciones (M3): ${row.exposures} | Revenue: $${row.revenue_exposures}`,
          `CrossSell: ${row.crosssell_pairs} pares | Revenue: $${row.revenue_crosssell}`,
          `Revenue total impacto: $${row.total_impact_revenue}`,
          `Revenue total ODV: $${row.total_odv_revenue}`,
          `% impacto botiquin: ${row.impact_percentage}%`,
        ].join("\n");
      }

      case "get_medication_prices": {
        const busqueda = args.busqueda as string;
        const marca = (args.brand as string) ?? null;
        const { data, error } = await chatbot.rpc("get_medication_prices", {
          p_search_term: busqueda,
          p_brand_filter: marca,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No se encontraron medicamentos con ese criterio.";
        return (data as AnyRow[])
          .map(
            (m) =>
              `${m.sku}: ${m.description} (${m.brand}) | $${m.price} | ${m.content ?? ""} | Actualizado: ${m.last_updated?.substring(0, 10) ?? "N/A"}`
          )
          .join("\n");
      }

      case "search_pubmed": {
        const query = args.query as string;
        return await searchPubMed(query);
      }

      case "get_estado_visitas": {
        const { data, error } = await chatbot.rpc("get_visit_status", {
          p_user_id: userId,
          p_is_admin: isAdmin,
        });
        if (error) return `Error: ${error.message}`;
        if (!data?.length) return "No hay visitas en el corte actual.";
        return (data as AnyRow[])
          .map(
            (v) =>
              `${v.client_name} | Visita: ${v.visit_type} | Status: ${v.visit_status} | Saga: ${v.saga_status ?? "N/A"} | Tareas: ${v.tasks_completed}/${v.tasks_total} | Fecha: ${v.created_at?.substring(0, 10) ?? "?"}`
          )
          .join("\n");
      }

      case "get_refill_recommendations": {
        const clientId = args.client_id as string;
        const { data, error } = await chatbot.rpc(
          "get_refill_recommendations",
          { p_client_id: clientId }
        );
        if (error) return `Error: ${error.message}`;
        if (!data?.length)
          return "No hay SKUs asignados sin stock para este medico. Todos los SKUs asignados ya tienen inventario.";
        return (data as AnyRow[])
          .map(
            (r) =>
              `${r.sku}: ${r.description} (${r.brand}) $${r.price} | ${r.was_in_cabinet ? "Estuvo en botiquin" : "Nunca en botiquin"} | Ventas globales: ${r.global_sales_pieces}pz $${r.global_sales_value} | ${r.recommendation}`
          )
          .join("\n");
      }

      default:
        return `Herramienta desconocida: ${name}`;
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    console.error(`Tool execution error (${name}):`, msg);
    return `Error al ejecutar herramienta: ${msg}`;
  }
}
