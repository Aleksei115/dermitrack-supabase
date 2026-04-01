-- Fix: Strip trailing semicolons from SQL before wrapping in CTE
-- LLMs often generate SQL ending with ";" which breaks the WITH cte AS (%s) wrapper.

CREATE OR REPLACE FUNCTION chatbot.execute_readonly_query(p_sql text, p_max_rows int DEFAULT 50)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, metadata
AS $$
DECLARE
    v_sql text;
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

    -- Strip trailing whitespace and semicolons (LLMs add them)
    v_sql := rtrim(rtrim(p_sql), ';');
    v_sql := rtrim(v_sql);

    -- Normalize for DML/DDL detection
    v_sql_upper := upper(regexp_replace(v_sql, '\s+', ' ', 'g'));

    -- Block DML/DDL statements
    IF v_sql_upper ~ '\m(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|EXECUTE)\M' THEN
        RETURN jsonb_build_object(
            'error', 'Only SELECT queries are allowed. DML/DDL statements are blocked.',
            'blocked_statement', substring(v_sql from 1 for 80)
        );
    END IF;

    -- Execute with timeout and row limit
    BEGIN
        SET LOCAL statement_timeout = '5s';

        EXECUTE format(
            'WITH cte AS (%s) SELECT jsonb_agg(row_to_json(cte)) FROM (SELECT * FROM cte LIMIT %s + 1) cte',
            v_sql, v_max_rows
        ) INTO v_rows;
    EXCEPTION
        WHEN others THEN
            RETURN jsonb_build_object(
                'error', SQLERRM,
                'hint', COALESCE(SQLSTATE, 'unknown'),
                'query_preview', substring(v_sql from 1 for 120)
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

NOTIFY pgrst, 'reload schema';
