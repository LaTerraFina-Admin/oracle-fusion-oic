create or replace PACKAGE BODY "LTF_BICC_UTILITY_PKG" AS

    PROCEDURE LTF_orchestrator (
        i_file_content IN CLOB,
        i_mode VARCHAR2
    ) AS

        l_clob_offset          NUMBER := 1;
        l_line                 VARCHAR2(32767);
        l_col_count            NUMBER;
        l_is_first_line        BOOLEAN := TRUE;
        l_delimiter            VARCHAR2(20) := '([^;]*)';
        v_file_name            VARCHAR2(250);
        v_file_prefix          VARCHAR2(250);
        v_stage_table_name     VARCHAR2(250);
        v_interface_table_name VARCHAR2(250);
        v_pk_table_name        VARCHAR2(250);
        l_next_newline_pos     NUMBER;
        l_clob_length          NUMBER;
        l_current_line         NUMBER := 1; -- Track the current line number for debugging
    BEGIN
        l_clob_length := dbms_lob.getlength(i_file_content);

    -- Loop through each line in the CSV CLOB
        LOOP
        -- Find the position of the next newline character
            l_next_newline_pos := dbms_lob.instr(i_file_content, chr(10), l_clob_offset);

        -- Extract the line
            IF l_next_newline_pos > 0 THEN
            -- Newline found, extract the line up to the newline
                l_line := dbms_lob.substr(i_file_content, l_next_newline_pos - l_clob_offset, l_clob_offset);
                l_clob_offset := l_next_newline_pos + 1;
            ELSE
            -- No more newlines found, this is the last line (or the only line)
                l_line := dbms_lob.substr(i_file_content, l_clob_length - l_clob_offset + 1, l_clob_offset);
            -- Set the offset past the end of the CLOB to exit the loop after this iteration
                l_clob_offset := l_clob_length + 1;
            END IF;

        -- Debug: Output line number and content
            dbms_output.put_line('Processing line: ' || l_current_line);
            dbms_output.put_line('Line content: ' || l_line);

        -- Skip header and process data lines
            IF l_is_first_line THEN
                l_is_first_line := FALSE;
                dbms_output.put_line('Skipping header line.');
            ELSE
                l_col_count := 1;
                v_file_name := trim(replace(regexp_substr(l_line, l_delimiter, 1, l_col_count + 0), '"', ''));

                v_file_prefix := regexp_substr(v_file_name, '^([^-]+)');
                dbms_output.put_line('File name: ' || v_file_name);
                dbms_output.put_line('File prefix: ' || v_file_prefix);
                BEGIN
                    SELECT
                        interface_table || to_char(sysdate, 'YYMMDDHH24MISS'),
                        interface_table,
                        pk_table
                    INTO
                        v_stage_table_name,
                        v_interface_table_name,
                        v_pk_table_name
                    FROM
                        LTF_bicc_control
                    WHERE
                        file_prefix = v_file_prefix;

                    dbms_output.put_line('Calling LTF_stage_data for: ' || v_file_name);
                    if i_mode = 'VO_EXTRACT' THEN
                        LTF_stage_data(v_file_name, v_stage_table_name, v_interface_table_name);
                        dbms_output.put_line('LTF_stage_data called successfully.');
                    ELSIF i_mode = 'PRIMARY_KEY_EXTRACT' THEN
                        LTF_SYNC_PRIMARY_KEYS(v_file_name, v_pk_table_name, v_stage_table_name, v_interface_table_name );
                    END IF;
                EXCEPTION
                    WHEN no_data_found THEN
                        dbms_output.put_line('No data found in LTF_bicc_control for file_prefix: ' || v_file_prefix);
                END;

            END IF;

            l_current_line := l_current_line + 1; -- Increment line counter
        -- Exit condition: After processing each line (including the last one)
            EXIT WHEN l_clob_offset > l_clob_length;
        END LOOP;

        dbms_output.put_line('Finished processing file.');
    EXCEPTION
        WHEN OTHERS THEN
            dbms_output.put_line('Error in LTF_orchestrator: ' || sqlerrm);
    END LTF_orchestrator;

    PROCEDURE LTF_stage_data (
        i_file_name            IN VARCHAR2,
        i_stage_table_name     IN VARCHAR2,
        i_interface_table_name IN VARCHAR2
    ) AS
        v_column_list VARCHAR2(32767);
    BEGIN
        SELECT
            LISTAGG(column_name
                    || ' '
                    || data_type
                    ||
                    CASE
                        WHEN data_type = 'VARCHAR2' THEN
                            '('
                            || data_length
                            || ')'
                        ELSE
                            '' -- Handle other data types or leave empty
                    END, ',') WITHIN GROUP(
            ORDER BY
                column_id
            )
        INTO v_column_list
        FROM
            user_tab_columns
        WHERE
            table_name = i_interface_table_name;

        dbms_cloud.create_external_table(table_name => i_stage_table_name, credential_name => 'BUCKET_BICC_CRED', file_uri_list => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/axfkz1wkyran/b/BUCKET_BICC/o/'
        || i_file_name, column_list => v_column_list, format =>
                                                                                                                                JSON_OBJECT
                                                                                                                                (
                                                                                                                                    'delimiter'
                                                                                                                                    VALUE
                                                                                                                                    ','
                                                                                                                                    ,
                                                                                                                                    'skipheaders'
                                                                                                                                    VALUE
                                                                                                                                    '1'
                                                                                                                                    ,
                                                                                                                                    'quote'
                                                                                                                                    VALUE
                                                                                                                                    '"'
                                                                                                                                    ,
                                                                                                                                    'timestampformat'
                                                                                                                                    VALUE
                                                                                                                                    'YYYY-MM-DD HH24:MI:SS.FF6'
                                                                                                                                    ,
                                                                                                                                    'dateformat'
                                                                                                                                    VALUE
                                                                                                                                    'YYYY-MM-DD'
                                                                                                                                    ,
                                                                                                                                            'compression'
                                                                                                                                            VALUE
                                                                                                                                            'gzip'
                                                                                                                                )
        );

        LTF_merge_tables(i_stage_table_name, i_interface_table_name);
        dbms_output.put_line('merging: ' || i_interface_table_name);
        EXECUTE IMMEDIATE 'drop table ' || i_stage_table_name;
        dbms_cloud.delete_object(credential_name => 'BUCKET_BICC_CRED', object_uri => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/axfkz1wkyran/b/BUCKET_BICC/o/'
        || i_file_name);
    END LTF_stage_data;

    PROCEDURE LTF_merge_tables (
        i_source_table IN VARCHAR2,
        i_target_table IN VARCHAR2
    ) AS

        l_update_clause        CLOB;
        l_insert_clause_cols   CLOB;
        l_insert_clause_values CLOB;
        l_merge_stmt           CLOB;
        l_pk_condition         CLOB;
        l_err_msg              VARCHAR2(32767);
        l_offset               PLS_INTEGER := 1;
        l_chunk_size           PLS_INTEGER := 32767;
    BEGIN
    -- Construir a cláusula UPDATE
        SELECT
            LISTAGG(
                CASE
                    WHEN column_name NOT IN(
                        SELECT
                            column_name
                        FROM
                                 user_constraints c
                            JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
                        WHERE
                                c.table_name = upper(i_target_table)
                            AND c.constraint_type = 'P'
                    ) THEN
                        'p."'
                        || column_name
                        || '" = d."'
                        || column_name
                        || '"'
                    ELSE
                        NULL
                END,
                ', ') WITHIN GROUP(
            ORDER BY
                column_id
            )
        INTO l_update_clause
        FROM
            user_tab_columns
        WHERE
            table_name = upper(i_target_table);

    -- Construir as cláusulas INSERT
        SELECT
            rtrim(
                LISTAGG('"'
                        || column_name
                        || '"', ', ') WITHIN GROUP(
                ORDER BY
                    column_id
                ),
                ', '),
            LISTAGG('d."'
                    || column_name
                    || '"', ', ') WITHIN GROUP(
                ORDER BY
                    column_id
                )
        INTO
            l_insert_clause_cols,
            l_insert_clause_values
        FROM
            user_tab_columns
        WHERE
            table_name = upper(i_target_table);

    -- Construir a condição da chave primária
        SELECT
            LISTAGG('p."'
                    || column_name
                    || '" = d."'
                    || column_name
                    || '"', ' AND ') WITHIN GROUP(
            ORDER BY
                position
            )
        INTO l_pk_condition
        FROM
                 user_constraints c
            JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        WHERE
                c.table_name = upper(i_target_table)
            AND c.constraint_type = 'P';

    -- Construir a instrução MERGE
        IF
            l_insert_clause_cols IS NOT NULL
            AND l_insert_clause_values IS NOT NULL
        THEN
            l_merge_stmt := 'MERGE INTO '
                            || i_target_table
                            || ' p USING '
                            || i_source_table
                            || ' d ON ('
                            || l_pk_condition
                            || ') '
                            || 'WHEN MATCHED THEN UPDATE SET '
                            || l_update_clause
                            || ' WHEN NOT MATCHED THEN INSERT VALUES ('
                            || l_insert_clause_values
                            || ')';
        ELSE
            l_merge_stmt := 'MERGE INTO '
                            || i_target_table
                            || ' p USING '
                            || i_source_table
                            || ' d ON ('
                            || l_pk_condition
                            || ') '
                            || 'WHEN MATCHED THEN UPDATE SET '
                            || l_update_clause;
        END IF;

    -- Depurar a instrução gerada (remova ou comente isso em produção)
   -- DBMS_OUTPUT.PUT_LINE('Merge Statement: ' || l_merge_stmt);

    -- Execute a instrução MERGE usando DBMS_LOB
        LOOP
            EXIT WHEN l_offset > dbms_lob.getlength(l_merge_stmt);
            BEGIN
                EXECUTE IMMEDIATE dbms_lob.substr(l_merge_stmt, l_chunk_size, l_offset);
                l_offset := l_offset + l_chunk_size;
            EXCEPTION
                WHEN OTHERS THEN
                    l_err_msg := sqlerrm;
                    dbms_output.put_line('Error at offset '
                                         || l_offset
                                         || ': '
                                         || l_err_msg);
                    RAISE;
            END;

        END LOOP;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_msg := sqlerrm;
            dbms_output.put_line('Error in procedure: ' || l_err_msg);
            RAISE;
    END LTF_merge_tables;

    PROCEDURE LTF_sync_primary_keys (
        i_file_name            IN VARCHAR2,
        i_pk_table_name        IN VARCHAR2,
        i_stage_table_name     IN VARCHAR2,
        i_interface_table_name IN VARCHAR2
    ) AS

        v_constraint_name VARCHAR2(128);
        v_table_owner     VARCHAR2(128) := user; -- Assuming the tables are in the current user's schema
        v_sql             VARCHAR2(32767);
        v_pk_columns      VARCHAR2(32767);
        v_pk_count        NUMBER;
        v_first_pk        BOOLEAN := TRUE;
        v_column_list     VARCHAR2(32767);
        TYPE pk_col_type IS RECORD (
            column_name VARCHAR2(128)
        );
        TYPE pk_col_table IS
            TABLE OF pk_col_type INDEX BY PLS_INTEGER;
        v_pk_col_tab      pk_col_table;
    BEGIN
        SELECT
            LISTAGG(column_name
                    || ' '
                    || data_type
                    ||
                    CASE
                        WHEN data_type = 'VARCHAR2' THEN
                            '('
                            || data_length
                            || ')'
                        ELSE
                            '' -- Handle other data types or leave empty
                    END, ',') WITHIN GROUP(
            ORDER BY
                column_id
            )
        INTO v_column_list
        FROM
            user_tab_columns
        WHERE
            table_name = i_pk_table_name;

        dbms_cloud.create_external_table(table_name => i_stage_table_name, credential_name => 'BUCKET_BICC_CRED', file_uri_list => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/axfkz1wkyran/b/BUCKET_BICC/o/'
        || i_file_name, column_list => v_column_list, format =>
                                                                                                                                JSON_OBJECT
                                                                                                                                (
                                                                                                                                    'delimiter'
                                                                                                                                    VALUE
                                                                                                                                    ','
                                                                                                                                    ,
                                                                                                                                    'skipheaders'
                                                                                                                                    VALUE
                                                                                                                                    '1'
                                                                                                                                    ,
                                                                                                                                    'quote'
                                                                                                                                    VALUE
                                                                                                                                    '"'
                                                                                                                                    ,
                                                                                                                                    'timestampformat'
                                                                                                                                    VALUE
                                                                                                                                    'YYYY-MM-DD HH24:MI:SS.FF6'
                                                                                                                                    ,
                                                                                                                                    'dateformat'
                                                                                                                                    VALUE
                                                                                                                                    'YYYY-MM-DD'
                                                                                                                                    ,
                                                                                                                                            'compression'
                                                                                                                                            VALUE
                                                                                                                                            'gzip'
                                                                                                                                )
        );

    -- 1. Get Primary Key Constraint Name and number of columns 
        SELECT
            c.constraint_name,
            COUNT(*)
        INTO
            v_constraint_name,
            v_pk_count
        FROM
                 user_constraints c
            JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        WHERE
                c.table_name = upper(i_pk_table_name)
            AND c.constraint_type = 'P'
        GROUP BY
            c.constraint_name;

    -- 2. Get Primary Key Column Names
        SELECT
            cc.column_name
        BULK COLLECT
        INTO v_pk_col_tab
        FROM
                 user_constraints c
            JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
        WHERE
                c.table_name = upper(i_pk_table_name)
            AND c.constraint_type = 'P';

    -- 3. Build comma-separated list of PK columns
        v_pk_columns := '';
        FOR i IN v_pk_col_tab.first..v_pk_col_tab.last LOOP
            IF NOT v_first_pk THEN
                v_pk_columns := v_pk_columns || ', ';
            END IF;
            v_pk_columns := v_pk_columns || v_pk_col_tab(i).column_name;
            v_first_pk := FALSE;
        END LOOP;

    -- 4. Create External Table (if not already exists, otherwise just use the existing one)
        BEGIN
            EXECUTE IMMEDIATE 'SELECT 1 FROM '
                              || i_stage_table_name
                              || ' WHERE ROWNUM = 1';
            dbms_output.put_line('External Table already exists: ' || i_stage_table_name);
        EXCEPTION
            WHEN OTHERS THEN
          -- Assuming the external table creation logic is handled elsewhere or was already done previously.
          -- You might want to add error handling here if table creation is part of this procedure.
                dbms_output.put_line('External Table probably not found, was it already created previously?: '
                                     || i_stage_table_name
                                     || ' Error: '
                                     || sqlerrm);
                RETURN;
        END;

    -- 5. Build and Execute Dynamic SQL for Deletion
    -- This SQL deletes rows from the constraint table where the primary key combination does not exist in the external table.
        v_sql := 'DELETE FROM '
                 || i_interface_table_name
                 || ' ct WHERE NOT EXISTS (SELECT 1 FROM '
                 || i_stage_table_name
                 || ' et WHERE ';
        v_first_pk := TRUE;
        FOR i IN v_pk_col_tab.first..v_pk_col_tab.last LOOP
            IF NOT v_first_pk THEN
                v_sql := v_sql || ' AND ';
            END IF;
            v_sql := v_sql
                     || 'ct.'
                     || v_pk_col_tab(i).column_name
                     || ' = et.'
                     || v_pk_col_tab(i).column_name;

            v_first_pk := FALSE;
        END LOOP;

        v_sql := v_sql || ')';
        dbms_output.put_line('Executing SQL: ' || v_sql);
        EXECUTE IMMEDIATE v_sql;
        dbms_output.put_line('Primary key synchronization complete for table: ' || i_interface_table_name);
        dbms_output.put_line('dropping table: ' || i_stage_table_name);
        EXECUTE IMMEDIATE 'drop table ' || i_stage_table_name;
        dbms_cloud.delete_object(credential_name => 'BUCKET_BICC_CRED', object_uri => 'https://objectstorage.us-phoenix-1.oraclecloud.com/n/axfkz1wkyran/b/BUCKET_BICC/o/'
        || i_file_name);
    EXCEPTION
        WHEN no_data_found THEN
            dbms_output.put_line('No primary key found for table: ' || i_interface_table_name);
        WHEN OTHERS THEN
            dbms_output.put_line('Error during primary key synchronization: ' || sqlerrm);
            RAISE;
    END LTF_sync_primary_keys;

END LTF_bicc_utility_pkg;