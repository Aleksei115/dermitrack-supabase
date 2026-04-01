-- Migration: Text-to-SQL functions for adaptive_analyst sub-agent
-- Adds 3 new chatbot functions and drops 8 redundant RPCs

-- ============================================================
-- 1. Schema context introspection (wrapper over data_dictionary)
-- ============================================================
CREATE OR REPLACE FUNCTION chatbot.get_schema_context(p_tables text[] DEFAULT NULL)
RETURNS TABLE(
    schema_name text,
    table_name text,
    column_name text,
    data_type text,
    is_nullable text,
    column_description text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = metadata, public
AS $$
    SELECT
        d.table_schema::text,
        d.table_name::text,
        d.column_name::text,
        d.data_type::text,
        d.is_nullable::text,
        d.column_description::text
    FROM metadata.data_dictionary d
    WHERE d.table_schema IN ('public', 'metadata')
      AND (p_tables IS NULL OR d.table_name = ANY(p_tables))
    ORDER BY d.table_schema, d.table_name, d.column_name;
$$;

COMMENT ON FUNCTION chatbot.get_schema_context IS
  'Returns column-level schema info from metadata.data_dictionary, optionally filtered by table names.';

GRANT EXECUTE ON FUNCTION chatbot.get_schema_context TO authenticated, anon;


-- ============================================================
-- 2. Business glossary search (wrapper over metadata.search_glossary)
-- ============================================================
CREATE OR REPLACE FUNCTION chatbot.search_glossary(p_query text)
RETURNS TABLE(
    term text,
    definition text,
    category text,
    related_tables text[],
    examples text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = metadata, public
AS $$
    SELECT
        g.term::text,
        g.definition::text,
        g.category::text,
        g.related_tables,
        g.examples::text
    FROM metadata.business_glossary g
    WHERE g.term ILIKE '%' || p_query || '%'
       OR g.definition ILIKE '%' || p_query || '%'
    ORDER BY
        CASE WHEN g.term ILIKE '%' || p_query || '%' THEN 0 ELSE 1 END,
        g.term
    LIMIT 15;
$$;

COMMENT ON FUNCTION chatbot.search_glossary IS
  'Searches business_glossary by term or definition substring match.';

GRANT EXECUTE ON FUNCTION chatbot.search_glossary TO authenticated, anon;


-- ============================================================
-- 3. Safe readonly SQL execution
-- ============================================================
CREATE OR REPLACE FUNCTION chatbot.execute_readonly_query(p_sql text, p_max_rows int DEFAULT 50)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, metadata
AS $$
DECLARE
    v_sql_upper text;
    v_max_rows int;
    v_result jsonb;
    v_columns jsonb;
    v_rows jsonb;
    v_row_count int;
    v_truncated boolean;
BEGIN
    -- Cap max_rows at 200
    v_max_rows := LEAST(COALESCE(p_max_rows, 50), 200);

    -- Normalize for DML/DDL detection
    v_sql_upper := upper(regexp_replace(p_sql, '\s+', ' ', 'g'));

    -- Block DML/DDL statements
    IF v_sql_upper ~ '\m(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|EXECUTE)\M' THEN
        RETURN jsonb_build_object(
            'error', 'Only SELECT queries are allowed. DML/DDL statements are blocked.',
            'blocked_statement', substring(p_sql from 1 for 80)
        );
    END IF;

    -- Execute with timeout and row limit
    BEGIN
        SET LOCAL statement_timeout = '5s';

        EXECUTE format(
            'WITH cte AS (%s) SELECT jsonb_agg(row_to_json(cte)) FROM (SELECT * FROM cte LIMIT %s + 1) cte',
            p_sql, v_max_rows
        ) INTO v_rows;
    EXCEPTION
        WHEN others THEN
            RETURN jsonb_build_object(
                'error', SQLERRM,
                'hint', COALESCE(SQLSTATE, 'unknown'),
                'query_preview', substring(p_sql from 1 for 120)
            );
    END;

    -- Handle empty results
    IF v_rows IS NULL THEN
        RETURN jsonb_build_object(
            'columns', '[]'::jsonb,
            'rows', '[]'::jsonb,
            'row_count', 0,
            'truncated', false
        );
    END IF;

    v_row_count := jsonb_array_length(v_rows);
    v_truncated := v_row_count > v_max_rows;

    -- Trim to max_rows if we fetched max+1
    IF v_truncated THEN
        v_rows := (
            SELECT jsonb_agg(v_rows->i)
            FROM generate_series(0, v_max_rows - 1) AS i
        );
        v_row_count := v_max_rows;
    END IF;

    -- Extract column names from first row
    v_columns := (
        SELECT jsonb_agg(key ORDER BY key)
        FROM jsonb_object_keys(v_rows->0) AS key
    );

    RETURN jsonb_build_object(
        'columns', v_columns,
        'rows', v_rows,
        'row_count', v_row_count,
        'truncated', v_truncated
    );
END;
$$;

COMMENT ON FUNCTION chatbot.execute_readonly_query IS
  'Executes a read-only SQL query with DML/DDL blocking, 5s timeout, and row limit (max 200).';

GRANT EXECUTE ON FUNCTION chatbot.execute_readonly_query TO authenticated, anon;


-- ============================================================
-- 4. Drop redundant chatbot RPCs (replaced by adaptive_analyst)
-- ============================================================
DROP FUNCTION IF EXISTS chatbot.get_complete_sales_ranking(int);
DROP FUNCTION IF EXISTS chatbot.get_complete_brand_performance();
DROP FUNCTION IF EXISTS chatbot.get_doctor_inventory(uuid, uuid, boolean);
DROP FUNCTION IF EXISTS chatbot.classification_by_client(uuid);
DROP FUNCTION IF EXISTS chatbot.get_doctor_movements(uuid, uuid, boolean, text, int);
DROP FUNCTION IF EXISTS chatbot.get_visit_status(uuid, boolean);
DROP FUNCTION IF EXISTS chatbot.get_user_odv_sales(uuid, boolean, text, int);
DROP FUNCTION IF EXISTS chatbot.get_user_collections(uuid, uuid, int, boolean);


-- Reload PostgREST schema cache
NOTIFY pgrst, 'reload schema';
