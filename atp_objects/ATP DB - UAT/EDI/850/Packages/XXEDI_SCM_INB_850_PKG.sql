create or replace PACKAGE XXEDI_SCM_INB_850_PKG AS

    PROCEDURE process_data (
        oic_id IN VARCHAR2
    );

    PROCEDURE update_flag_processed (
        oic_id IN VARCHAR2,
        key_id IN VARCHAR2
    );

    PROCEDURE update_flag_error (
        oic_id     IN VARCHAR2,
        key_id     IN VARCHAR2,
        error_text IN VARCHAR2
    );

    PROCEDURE validation_process (
        oic_id            IN VARCHAR2,
        current_file_name IN VARCHAR2
    );

    PROCEDURE load_xml_process (
        oic_id            IN VARCHAR2,
        current_file_name IN VARCHAR2,
        xml_load          IN CLOB
    );

    PROCEDURE load_order_tables (
        oic_id IN VARCHAR2,
		current_file_name IN VARCHAR2
    );

END XXEDI_SCM_INB_850_PKG;