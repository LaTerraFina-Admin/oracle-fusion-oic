create or replace PACKAGE CMN_LOAD_DATA_TO_ATP_PKG AS 

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
    );

END CMN_LOAD_DATA_TO_ATP_PKG;