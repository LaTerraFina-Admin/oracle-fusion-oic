create or replace PACKAGE BODY CMN_LOAD_DATA_TO_ATP_PKG AS

    FUNCTION sql_error_message RETURN VARCHAR2 IS
    BEGIN
        RETURN (SQLERRM||': '||dbms_utility.format_error_backtrace);
    END;

    PROCEDURE create_column_list_from_tbl_prc (
        i_table_name IN VARCHAR2,
        o_column_list OUT CLOB
    ) AS
        CURSOR table_columns_cur(p_table_name IN VARCHAR2) IS
            SELECT
                altc.column_name,
                altc.data_type,
                altc.data_length
            FROM
                all_tab_columns altc
            WHERE 1=1
                AND altc.table_name = p_table_name
            ORDER BY altc.column_id;

        l_column_list CLOB;
        l_column_count PLS_INTEGER := 0;
    BEGIN

        -- init the clob
        DBMS_LOB.CREATETEMPORARY(
            lob_loc => l_column_list,
            cache => TRUE
        );

        -- list all the table columns
        FOR col_rec IN table_columns_cur(i_table_name) LOOP
            l_column_count := l_column_count + 1;

            -- if is the first line, include only the column name and the type
            IF l_column_count > 1 THEN
                l_column_list := l_column_list || ',' || CHR(10);
            END IF;

            l_column_list := l_column_list || col_rec.column_name || ' ' || col_rec.data_type;

            IF col_rec.data_type IN ('VARCHAR2') THEN
                l_column_list := l_column_list || '(' || col_rec.data_length || ')';
            END IF;

        END LOOP;

        o_column_list := l_column_list;
        dbms_output.put_line(l_column_list);

    END create_column_list_from_tbl_prc;

    PROCEDURE load_file_to_ext_table_prc (
        i_credential_name IN VARCHAR2 DEFAULT NULL,
        i_file_uri IN VARCHAR2,
        i_dest_table IN VARCHAR2,
        i_format IN VARCHAR2 DEFAULT NULL,
        o_ext_table_name OUT VARCHAR2,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS
        l_column_list CLOB;
        l_column_list_size INTEGER;
        l_external_table_name VARCHAR2(256);
        l_format CLOB;
        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;
    BEGIN

        -- First get the column list from the dest table
        BEGIN
            create_column_list_from_tbl_prc(
                i_table_name => i_dest_table,
                o_column_list => l_column_list
            );
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_err_reason := 'Not able to create the "column_list" parameter based on the table name.';
                l_err_details := sql_error_message();
        END;

        -- Get the column list size from the CLOB
        l_column_list_size := DBMS_LOB.GETLENGTH(l_column_list);

        -- If the CLOB is empty and don't exists any error early, create a new error message
        IF l_column_list_size = 0 AND l_status != 'ERROR' THEN
            l_status := 'ERROR';
            l_err_reason := 'Not able to create the "column_list" parameter based on the table name.';
            l_err_details := sql_error_message();
        END IF;

        l_external_table_name := i_dest_table || '_EXT';

        -- Then drop the external table if exists
        IF l_status != 'ERROR' THEN
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE ' || l_external_table_name;
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        l_status := 'ERROR';
                        l_err_reason := 'Error while dropping the external table.';
                        l_err_details := sql_error_message();
                    END IF;
            END;
        END IF;

        -- If the format field is not provided, populate with a default value
        IF i_format IS NULL THEN
            l_format := JSON_OBJECT(
                'type' VALUE 'csv',
                'delimiter' VALUE ',',
                'skipheaders' VALUE 1,
                'ignoremissingcolumns' value 'true',
                'rejectlimit' value '10',
                'timestampformat' value 'YYYY-MM-DD HH:MI:SS',
                'dateformat' value 'YYYY-MM-DD',
                'conversionerrors' value 'store_null'
            );
        ELSE
            l_format := utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(i_format)));
        END IF;

        -- If all the steps before are sucessfully validated, create the external table
        IF l_status != 'ERROR' THEN

            BEGIN
                DBMS_CLOUD.CREATE_EXTERNAL_TABLE (
                    table_name => l_external_table_name,
                    credential_name => i_credential_name,
                    file_uri_list => i_file_uri,
                    format => l_format,
                    column_list => l_column_list
                );
            EXCEPTION
                WHEN OTHERS THEN
                    l_status := 'ERROR';
                    l_err_reason := 'Error creating the external table.';
                    l_err_details := sql_error_message();
            END;

        END IF;

        IF l_status != 'ERROR' THEN

            BEGIN
                DBMS_CLOUD.VALIDATE_EXTERNAL_TABLE (
                    table_name => l_external_table_name
                );
            EXCEPTION
                WHEN OTHERS THEN
                    l_status := 'ERROR';
                    l_err_reason := 'Error validating the external table.';
                    l_err_details := sql_error_message();
            END;

        END IF;

        -- Return the values
        o_ext_table_name := l_external_table_name;
        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;

    END;

    PROCEDURE create_merge_statement_prc (
        i_src_table IN VARCHAR2,
        i_dest_table IN VARCHAR2,
        i_contraint_merge_name IN VARCHAR2 DEFAULT NULL,
        o_merge_stm OUT CLOB,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS

        CURSOR all_non_pk_columns(p_src_table VARCHAR2) IS
            SELECT
            altc.column_name
            FROM
            all_tab_columns altc
            WHERE 1=1
            AND altc.table_name = p_src_table
            AND altc.column_name NOT IN (
                SELECT
                alcc.column_name
                FROM
                all_constraints allc,
                all_cons_columns alcc
                WHERE 1=1
                AND allc.table_name = altc.table_name
                AND alcc.table_name = allc.table_name
                AND alcc.constraint_name = allc.constraint_name
                AND (
                    -- If i_contraint_merge_name is NULL, uses the primary key to be the merge match
                    (i_contraint_merge_name IS NULL AND allc.constraint_type = 'P')
                    OR
                    -- If i_contraint_merge_name is not NULL, uses the constraint name provided to be the merge match
                    (i_contraint_merge_name IS NOT NULL AND allc.constraint_name = i_contraint_merge_name)
                )
            )
            ORDER BY altc.column_id;

        CURSOR all_pk_columns(p_src_table VARCHAR2) IS
            SELECT
            altc.column_name
            FROM
            all_tab_columns altc
            WHERE 1=1
            AND altc.table_name = p_src_table
            AND altc.column_name IN (
                SELECT
                alcc.column_name
                FROM
                all_constraints allc,
                all_cons_columns alcc
                WHERE 1=1
                AND allc.table_name = altc.table_name
                AND alcc.table_name = allc.table_name
                AND alcc.constraint_name = allc.constraint_name
                AND (
                    (i_contraint_merge_name IS NULL AND allc.constraint_type = 'P')
                    OR
                    (i_contraint_merge_name IS NOT NULL AND allc.constraint_name = i_contraint_merge_name)
                )
            )
            ORDER BY altc.column_id;

        l_have_pk NUMBER;
        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;
        l_merge_stm CLOB;
        l_merge_condition CLOB;
        l_update_stm CLOB;
        l_insert_columns CLOB;
        l_insert_values CLOB;
        l_loop_count NUMBER;
    BEGIN

        -- Frist checks if exists PK on the table
        BEGIN
            SELECT COUNT(1)
            INTO l_have_pk
            FROM 
            all_constraints allc
            WHERE 1=1
            AND allc.table_name = i_dest_table
            AND (
                (i_contraint_merge_name IS NULL AND allc.constraint_type = 'P')
                OR
                (i_contraint_merge_name IS NOT NULL AND allc.constraint_name = i_contraint_merge_name)
            );
        EXCEPTION
            WHEN OTHERS THEN
                l_have_pk := 0;
        END;

        IF l_have_pk = 0 THEN
            l_status := 'ERROR';
            l_err_reason := 'The table don''t have an unique constraints';
            l_err_details := 'The table ' || i_dest_table || ' don''t have a primary key or the constraint name provided.';
        END IF;

        BEGIN
            DBMS_LOB.CREATETEMPORARY(l_merge_stm, TRUE);
            DBMS_LOB.CREATETEMPORARY(l_merge_condition, TRUE);
            DBMS_LOB.CREATETEMPORARY(l_update_stm, TRUE);
            DBMS_LOB.CREATETEMPORARY(l_insert_columns, TRUE);
            DBMS_LOB.CREATETEMPORARY(l_insert_values, TRUE);

            dbms_lob.append(l_merge_stm, to_clob('MERGE INTO ' || i_dest_table || ' d ' || CHR(10)));
            dbms_lob.append(l_merge_stm, to_clob('USING ' || i_src_table || ' s ON ( 1=1 ' || CHR(10)));

            FOR rec_pk IN all_pk_columns(i_dest_table) LOOP
                dbms_lob.append(l_merge_condition, TO_CLOB('AND d.' || rec_pk.column_name || ' = s.' || rec_pk.column_name || CHR(10)));
                dbms_lob.append(l_insert_columns, TO_CLOB(rec_pk.column_name || ',' || CHR(10)));
                dbms_lob.append(l_insert_values, TO_CLOB('s.' || rec_pk.column_name || ',' || CHR(10)));
            END LOOP;

            l_loop_count := 0;
            FOR rec_non_pk IN all_non_pk_columns(i_dest_table) LOOP
                dbms_lob.append(l_update_stm, TO_CLOB('d.' || rec_non_pk.column_name || ' = s.' || rec_non_pk.column_name || ',' || CHR(10)));
                dbms_lob.append(l_insert_columns, TO_CLOB(rec_non_pk.column_name || ',' || CHR(10)));
                dbms_lob.append(l_insert_values, TO_CLOB('s.' || rec_non_pk.column_name || ',' || CHR(10)));
            END LOOP;

            l_update_stm := rtrim(l_update_stm, ',' || chr(10));
            l_insert_columns := rtrim(l_insert_columns, ',' || chr(10));
            l_insert_values := rtrim(l_insert_values, ',' || chr(10));

            dbms_lob.append(l_merge_stm, l_merge_condition);
            dbms_lob.append(l_merge_stm, TO_CLOB(')' || CHR(10)));

            IF DBMS_LOB.GETLENGTH(l_update_stm) > 0 THEN
                dbms_lob.append(l_merge_stm, TO_CLOB('WHEN MATCHED THEN UPDATE SET' || CHR(10)));
                dbms_lob.append(l_merge_stm, l_update_stm);
                dbms_lob.append(l_merge_stm, TO_CLOB(CHR(10)));
            END IF;

            dbms_lob.append(l_merge_stm, TO_CLOB('WHEN NOT MATCHED THEN INSERT (' || CHR(10)));
            dbms_lob.append(l_merge_stm, l_insert_columns);
            dbms_lob.append(l_merge_stm, TO_CLOB(') VALUES (' || CHR(10)));
            dbms_lob.append(l_merge_stm, l_insert_values);
            dbms_lob.append(l_merge_stm, TO_CLOB(')' || CHR(10)));

        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_err_reason := 'Error while creating the merge statement';
                l_err_details := sql_error_message();
        END;

        o_merge_stm := l_merge_stm;
        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;

    END;

    PROCEDURE merge_file_to_table_prc (
        i_credential_name IN VARCHAR2 DEFAULT NULL,
        i_file_uri IN VARCHAR2,
        i_dest_table IN VARCHAR2,
        i_constraint_name_for_merge IN VARCHAR2 DEFAULT NULL,
        i_format IN VARCHAR2 DEFAULT NULL,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS

        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;
        l_ext_table_name VARCHAR2(256);
        l_merge_stm CLOB;

    BEGIN

        load_file_to_ext_table_prc(
            i_credential_name => i_credential_name,
            i_file_uri => i_file_uri,
            i_dest_table => i_dest_table,
            i_format => i_format,
            o_ext_table_name => l_ext_table_name,
            o_status => l_status,
            o_err_reason => l_err_reason,
            o_err_details => l_err_details
        );

        IF l_status != 'ERROR' THEN
            create_merge_statement_prc (
                i_src_table => l_ext_table_name,
                i_dest_table => i_dest_table,
                o_merge_stm => l_merge_stm,
                o_status => l_status,
                o_err_reason => l_err_reason,
                o_err_details => l_err_details
            );
        END IF;

        IF l_status != 'ERROR' THEN
            BEGIN
                EXECUTE IMMEDIATE l_merge_stm;
            EXCEPTION
                WHEN OTHERS THEN
                    l_status := 'ERROR';
                    l_err_reason := 'Failed executing the merge statement.';
                    l_err_details := sql_error_message();
            END;
        END IF;

        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;
    END;

    PROCEDURE load_file_to_stg_table (
        i_credential_name IN VARCHAR2 DEFAULT NULL,
        i_file_uri IN VARCHAR2,
        i_stg_table_name IN VARCHAR2,
        i_format IN VARCHAR2 DEFAULT NULL,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS

        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;
        l_format CLOB;

    BEGIN

         -- If the format field is not provided, populate with a default value
        IF i_format IS NULL THEN
            l_format := JSON_OBJECT(
                'type' VALUE 'csv',
                'delimiter' VALUE ',',
                'skipheaders' VALUE 1,
                'ignoremissingcolumns' value 'true'
            );
        ELSE
            l_format := utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(i_format)));
        END IF;

        BEGIN
            DBMS_CLOUD.COPY_DATA(
                table_name => i_stg_table_name,
                credential_name => i_credential_name,
                file_uri_list => i_file_uri,
                format => l_format
            );
        EXCEPTION
            WHEN OTHERS THEN
                l_status := 'ERROR';
                l_err_reason := 'Error when copying the data to the stg table.';
                l_err_details := sql_error_message();
        END;

        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;

    END;

    PROCEDURE save_stg_data_into_table_prc (
        i_credential_name IN VARCHAR2 DEFAULT NULL,
        i_file_uri IN VARCHAR2,
        i_dest_table IN VARCHAR2 DEFAULT NULL,
        i_truncate_before_merge IN VARCHAR2 DEFAULT '0',
        i_format IN VARCHAR2 DEFAULT NULL,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS
        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;

        l_dynamic_sql VARCHAR2(4000) := NULL;
    BEGIN

        IF i_truncate_before_merge = '1' THEN
            BEGIN
                EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || i_dest_table;
            EXCEPTION
                WHEN OTHERS THEN
                    l_status := 'ERROR';
                    l_err_reason := 'Error when truncating the stg table.';
                    l_err_details := sql_error_message();
            END;
        END IF;

        IF l_status != 'ERROR' THEN
            load_file_to_stg_table (
                i_credential_name => i_credential_name,
                i_file_uri => i_file_uri,
                i_stg_table_name => i_dest_table,
                i_format => i_format,
                o_status => l_status,
                o_err_reason => l_err_reason,
                o_err_details => l_err_details
            );
        END IF;

        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;

    END;

    PROCEDURE load_file_to_table_prc (
        i_credential_name IN VARCHAR2 DEFAULT NULL,
        i_file_uri IN VARCHAR2,
        i_load_operation IN VARCHAR2,
        i_dest_table IN VARCHAR2,
        i_post_insert_procedure_name IN VARCHAR2 DEFAULT NULL,
        i_format IN VARCHAR2 DEFAULT NULL,
        i_constraint_name_for_merge IN VARCHAR2 DEFAULT NULL,
        o_status OUT VARCHAR2,
        o_err_reason OUT VARCHAR2,
        o_err_details OUT VARCHAR2
    ) AS

        l_status VARCHAR2(20) := 'SUCCESS';
        l_err_reason VARCHAR2(4000) := NULL;
        l_err_details VARCHAR2(4000) := NULL;
        l_dynamic_sql VARCHAR2(4000);

    BEGIN

        IF i_load_operation = 'MERGE' THEN
            merge_file_to_table_prc (
                i_credential_name => i_credential_name,
                i_file_uri => i_file_uri,
                i_dest_table => i_dest_table,
                i_constraint_name_for_merge => i_constraint_name_for_merge,
                i_format => i_format,
                o_status => l_status,
                o_err_reason => l_err_reason,
                o_err_details => l_err_details
            );
        ELSIF i_load_operation = 'INSERT' OR i_load_operation = 'REFRESH' THEN
            save_stg_data_into_table_prc (
                i_credential_name => i_credential_name,
                i_file_uri => i_file_uri,
                i_dest_table => i_dest_table,
                i_truncate_before_merge => CASE WHEN i_load_operation = 'REFRESH' THEN '1' ELSE '0' END,
                i_format => i_format,
                o_status => l_status,
                o_err_reason => l_err_reason,
                o_err_details => l_err_details
            );
        END IF;

        IF i_post_insert_procedure_name IS NOT NULL AND l_status != 'ERROR' THEN
            l_dynamic_sql := 'BEGIN ' || i_post_insert_procedure_name || '(:v_status, :v_reason, :v_message); END;';

            BEGIN
                EXECUTE IMMEDIATE l_dynamic_sql
                USING OUT l_status, OUT l_err_reason, OUT l_err_details;
            EXCEPTION
                WHEN OTHERS THEN
                    l_status := 'ERROR';
                    l_err_reason := 'Error running the procedure to load the data.';
                    l_err_details := sql_error_message();
            END;
        END IF;

        o_status := l_status;
        o_err_reason := l_err_reason;
        o_err_details := l_err_details;

    END load_file_to_table_prc;

END CMN_LOAD_DATA_TO_ATP_PKG;