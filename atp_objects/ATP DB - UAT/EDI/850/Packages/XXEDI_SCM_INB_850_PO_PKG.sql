create or replace PACKAGE XXEDI_SCM_INB_850_PO_PKG AS

    PROCEDURE LOAD_XML_INTO_RAW_STG (
            I_P_OIC_ID          IN VARCHAR2,
            I_P_FILE_NAME       IN VARCHAR2,
            I_P_XML_CONTENT     IN CLOB,
            I_P_SOURCE_SYSTEM   IN VARCHAR2,
            I_P_DOC_TYPE        IN VARCHAR2,
            O_P_RESPONSE        OUT CLOB,
            O_P_STATUS          OUT VARCHAR2
    );

    PROCEDURE PARSE_XML_INTO_STG (
             I_P_OIC_ID     IN VARCHAR2
            ,O_P_RESPONSE   OUT CLOB
            ,O_P_STATUS     OUT VARCHAR2
    );

    PROCEDURE PROCESS_DATA_INTO_INTF (
            I_P_OIC_ID           IN VARCHAR2
            ,O_P_RESPONSE        OUT CLOB
            ,O_P_STATUS          OUT VARCHAR2
            ,O_P_HAS_MORE_COUNT  OUT NUMBER
    );

    PROCEDURE UPDATE_FLAGS (
            I_P_PK            IN NUMBER         -- Single PK
            ,I_P_KEYS         IN VARCHAR2       -- Multiple PKs. String with Primary Keys separeted by commas. like: '1,4,15'
            ,I_P_TABLE_NAME   IN VARCHAR2       -- the table name is used to determine which table to update. A parent table name might update the child table control fields.  check behaviour below.
            ,I_P_FLAG_VALUE   IN VARCHAR2       -- value that goes into the PROCESSED_FLAG field
            ,I_P_ERROR_CODE   IN VARCHAR2       -- value that goes into the ERROR_CODE field  
            ,I_P_ERROR_TEXT   IN CLOB           -- value that goes into the ERROR_MESSAGE field
            ,I_P_OIC_ID       IN VARCHAR2       -- value that goes into the OIC_INSTANCE_ID field
            ,O_P_RESPONSE     OUT CLOB          -- response message with log about the execution of the procedure
    );

    PROCEDURE GET_INTERFACE_TABLES_DATA (
             O_P_RESPONSE       OUT CLOB
            ,O_P_JSON           OUT CLOB
            ,O_P_STATUS         OUT VARCHAR2
    );

END XXEDI_SCM_INB_850_PO_PKG;
