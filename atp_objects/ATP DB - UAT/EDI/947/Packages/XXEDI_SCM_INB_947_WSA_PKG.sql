create or replace PACKAGE XXEDI_SCM_INB_947_WSA_PKG AS

    PROCEDURE LOAD_XML_INTO_RAW_STG (
        I_P_OIC_ID           IN VARCHAR2,
        I_P_FILE_NAME        IN VARCHAR2,
        I_P_XML_CONTENT      IN CLOB,
        I_P_SOURCE_SYSTEM    IN VARCHAR2,
        I_P_DOC_TYPE         IN VARCHAR2,
        O_P_RESPONSE         OUT CLOB,
        O_P_STATUS           OUT VARCHAR2
    );

    PROCEDURE PARSE_XML_INTO_STG (
        I_P_OIC_ID           IN VARCHAR2
        ,O_P_RESPONSE        OUT CLOB
        ,O_P_STATUS          OUT VARCHAR2
    );

   PROCEDURE PROCESS_DATA_INTO_INTF (
            I_P_OIC_ID           IN VARCHAR2
			--,I_P_FILE_NAME        IN VARCHAR2
            ,O_P_RESPONSE        OUT CLOB
            ,O_P_STATUS          OUT VARCHAR2

    );

    PROCEDURE UPDATE_FLAGS (
        I_P_PK               IN NUMBER
        ,I_P_KEYS            IN VARCHAR2
        ,I_P_TABLE_NAME      IN VARCHAR2
        ,I_P_FLAG_VALUE      IN VARCHAR2
        ,I_P_ERROR_CODE      IN VARCHAR2
        ,I_P_ERROR_TEXT      IN CLOB
        ,I_P_OIC_ID          IN VARCHAR2
        ,O_P_RESPONSE        OUT CLOB
        -- ,O_P_STATUS          OUT VARCHAR2
    );

    PROCEDURE GET_INTERFACE_TABLES_DATA (
        O_P_RESPONSE         OUT CLOB
        ,O_P_JSON            OUT CLOB
        ,O_P_STATUS          OUT VARCHAR2
    );

END XXEDI_SCM_INB_947_WSA_PKG;