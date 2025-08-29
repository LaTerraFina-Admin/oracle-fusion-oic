create or replace PROCEDURE LTF_LOAD_TO_BUCKET_PRC(
    p_table_name IN VARCHAR2,
    p_file_uri_list IN VARCHAR2
)
AS
BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || p_table_name;

    DBMS_CLOUD.COPY_DATA(
        table_name => p_table_name,
        credential_name => 'BUCKET_BICC_CRED',
        file_uri_list => p_file_uri_list,
        format => json_object(
            'delimiter' value ',', 
            'skipheaders' value '1',
            'quote' value '"',
            'timestampformat' value 'YYYY-MM-DD HH24:MI:SS.FF6',
            'dateformat' value 'YYYY-MM-DD',
            'compression' value 'gzip'
        )
    );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro ao importar dados: ' || SQLERRM);
        RAISE;
END LTF_LOAD_TO_BUCKET_PRC;