// ============================================================================
// Tool Declarations — 21 tools for Gemini function calling
// ============================================================================

export const TOOL_DECLARATIONS = {
  tools: [
    {
      functionDeclarations: [
        {
          name: "search_medicamentos",
          description:
            "Busca medicamentos por similitud semantica. Usa cuando pregunten por productos para un padecimiento, condicion medica, o tipo de tratamiento. IMPORTANTE: cuando recomiendas productos para un medico especifico, SIEMPRE pasa su client_id para excluir productos que ya tiene en botiquin o con historial (M1/M2/M3).",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Texto de busqueda: padecimiento, sintoma, tipo de producto, o nombre de medicamento",
              },
              client_id: {
                type: "string",
                description:
                  "ID del medico para excluir productos que ya tiene en su botiquin (opcional, obtenido via search_clientes)",
              },
            },
            required: ["query"],
          },
        },
        {
          name: "search_fichas_tecnicas",
          description:
            "Busca informacion tecnica de productos (composicion, indicaciones, contraindicaciones, modo de uso). Usa cuando pregunten por detalles tecnicos o fichas de un producto.",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Texto de busqueda: nombre de producto, ingrediente activo, o consulta tecnica",
              },
            },
            required: ["query"],
          },
        },
        {
          name: "search_clientes",
          description:
            "Busca medicos/clientes por nombre. SIEMPRE usa esta herramienta primero cuando mencionen un nombre de medico para obtener su client_id antes de consultar datos del medico.",
          parameters: {
            type: "object",
            properties: {
              name: {
                type: "string",
                description:
                  "Nombre del medico a buscar (puede ser parcial, ej: 'Garcia', 'Dr Lopez')",
              },
            },
            required: ["name"],
          },
        },
        {
          name: "get_doctor_inventory",
          description:
            "Obtiene el inventario actual del botiquin de un medico especifico. Muestra SKUs, cantidades y precios. Requiere client_id (obtenido via search_clientes).",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_doctor_movements",
          description:
            "Obtiene historial de movimientos de un medico: creaciones, ventas, recolecciones del botiquin y/o ventas recurrentes ODV. Util para tendencias y analisis historico.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
              fuente: {
                type: "string",
                enum: ["botiquin", "odv", "ambos"],
                description:
                  "Fuente de datos: 'botiquin' para movimientos de inventario, 'odv' para ventas recurrentes, 'ambos' para todo (default: ambos)",
              },
              limite: {
                type: "integer",
                description:
                  "Numero maximo de resultados (default 30, max 100)",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_clasificacion_cliente",
          description:
            "Obtiene la clasificacion M1/M2/M3 de productos para un medico. M1=venta directa de botiquin (productos vendidos al corte), M2=conversion (productos vendidos en botiquin que luego se volvieron venta recurrente ODV), M3=exposicion (productos que estuvieron en botiquin y luego aparecieron como venta recurrente ODV sin venta directa previa). Util para estrategia comercial. IMPORTANTE: Solo muestra clasificaciones que aparezcan EXACTAMENTE en los resultados. Si un SKU del inventario NO aparece en estos resultados, su clasificacion es '-' (sin clasificar). NUNCA inferir o inventar clasificaciones.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description: "ID del cliente/medico",
              },
            },
            required: ["client_id"],
          },
        },
        {
          name: "get_user_odv_sales",
          description:
            "Obtiene ventas ODV (recurrentes) de TODOS los clientes del usuario actual. Usa para ver el portafolio completo de ventas recurrentes del asesor.",
          parameters: {
            type: "object",
            properties: {
              sku_filter: {
                type: "string",
                description: "Filtrar por SKU especifico (opcional)",
              },
              limite: {
                type: "integer",
                description:
                  "Numero maximo de resultados (default 50, max 200)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_recolecciones",
          description:
            "Obtiene recolecciones (devoluciones de productos) del usuario. Cada recoleccion es un evento de devolucion que contiene multiples items con sus cantidades en piezas. IMPORTANTE: 'recolecciones' son eventos, 'piezas' son la suma de cantidades de items. No confundir el numero de recolecciones con el numero de piezas.",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description:
                  "Filtrar por medico especifico (opcional). Si no se proporciona, devuelve todas las recolecciones del usuario.",
              },
            },
            required: [],
          },
        },
        {
          name: "get_estadisticas_corte",
          description:
            "Obtiene estadisticas generales del corte actual: total ventas, creaciones, recolecciones, con comparacion vs corte anterior. Para preguntas sobre cifras globales del periodo.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_estadisticas_por_medico",
          description:
            "Obtiene estadisticas del corte actual desglosadas por medico: ventas, creaciones, recolecciones por doctor. Para rankings o comparaciones entre medicos.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_ranking_productos",
          description:
            "Obtiene ranking de productos por CONTEO de movimientos (piezas): cuantas piezas se vendieron, crearon, recolectaron, y cuantas hay en stock activo. NO incluye valor monetario — para ingresos/dinero usa get_ranking_ventas.",
          parameters: {
            type: "object",
            properties: {
              limite: {
                type: "integer",
                description: "Numero maximo de productos (default 20)",
              },
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_ranking_ventas",
          description:
            "Ranking de productos por INGRESOS TOTALES (M1+M2+M3). Incluye desglose: ventas_botiquin (M1=ventas directas de botiquin), ventas_conversion (M2=productos que pasaron de botiquin a ODV), ventas_exposicion (M3=productos expuestos en botiquin que luego se vendieron en ODV). USA ESTA HERRAMIENTA cuando pregunten por dinero, ingresos, o valor de ventas de productos.",
          parameters: {
            type: "object",
            properties: {
              limite: {
                type: "integer",
                description: "Maximo de productos (default 20)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_rendimiento_marcas",
          description:
            "Rendimiento por marca con desglose M1/M2/M3: ventas_botiquin (M1), ventas_conversion (M2), ventas_exposicion (M3), y total. USA ESTA HERRAMIENTA para preguntas de ingresos o dinero por marca.",
          parameters: {
            type: "object",
            properties: {},
            required: [],
          },
        },
        {
          name: "get_datos_historicos",
          description:
            "Obtiene datos historicos completos: KPIs globales (ventas M1, creaciones, stock activo, recolecciones) y datos detallados por visita. Sin fechas devuelve TODO el historico. Ideal para preguntas como 'quien ha vendido mas en toda la historia'.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_facturacion_medicos",
          description:
            "Obtiene la facturacion y composicion de ventas POR MEDICO: rango (Diamante, Oro, Plata, Bronce), facturacion actual vs baseline, desglose M1/M2/M3, porcentaje de crecimiento. Ideal para ranking de medicos y analisis de cartera.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_rendimiento_por_padecimiento",
          description:
            "Obtiene rendimiento por padecimiento/condicion medica: valor total e ingresos, piezas vendidas. Para saber que padecimientos generan mas ingresos.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_impacto_botiquin",
          description:
            "Obtiene metricas de impacto del botiquin: adopciones (M1→ODV), conversiones (M2), exposiciones (M3), revenue por categoria, porcentaje del revenue total atribuible al botiquin.",
          parameters: {
            type: "object",
            properties: {
              fecha_inicio: {
                type: "string",
                description:
                  "Fecha inicio del periodo en formato YYYY-MM-DD (opcional, sin fecha = toda la historia)",
              },
              fecha_fin: {
                type: "string",
                description:
                  "Fecha fin del periodo en formato YYYY-MM-DD (opcional)",
              },
            },
            required: [],
          },
        },
        {
          name: "get_medication_prices",
          description:
            "Busca precios de medicamentos por nombre, SKU, o descripcion. Incluye fecha de ultima actualizacion del precio.",
          parameters: {
            type: "object",
            properties: {
              busqueda: {
                type: "string",
                description:
                  "Nombre del producto, SKU, o termino de busqueda",
              },
              brand: {
                type: "string",
                description: "Filtrar por marca (opcional)",
              },
            },
            required: ["busqueda"],
          },
        },
        {
          name: "search_pubmed",
          description:
            "Busca el estudio clinico MAS RECIENTE en PubMed sobre un padecimiento dermatologico. Usa cuando el usuario pregunte sobre un padecimiento o tratamiento para enriquecer tu respuesta con evidencia cientifica actualizada. Retorna 1 solo estudio con fecha.",
          parameters: {
            type: "object",
            properties: {
              query: {
                type: "string",
                description:
                  "Padecimiento o tratamiento en ingles (PubMed es en ingles). Ej: 'atopic dermatitis', 'psoriasis treatment'",
              },
            },
            required: ["query"],
          },
        },
        {
          name: "get_estado_visitas",
          description:
            "Estado de las visitas de los medicos en el corte actual. Muestra que medicos tienen visitas pendientes, en progreso o completadas, y el progreso de sus tareas.",
          parameters: { type: "object", properties: {} },
        },
        {
          name: "get_refill_recommendations",
          description:
            "Recomendaciones de rellenado de botiquin para un medico. Muestra SKUs asignados que no tiene en stock, priorizados por tendencias de venta global. Requiere client_id (usar search_clientes primero).",
          parameters: {
            type: "object",
            properties: {
              client_id: {
                type: "string",
                description:
                  "ID del medico (obtenido de search_clientes)",
              },
            },
            required: ["client_id"],
          },
        },
      ],
    },
  ],
  toolConfig: { functionCallingConfig: { mode: "AUTO" } },
};
