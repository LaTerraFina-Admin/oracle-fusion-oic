create or replace PACKAGE BODY XXEDI_SCM_INB_944_PKG AS

    g_v_EDI_944_doc_type                    CONSTANT VARCHAR2(100) := 'EDI_944';
    g_v_PRE_VALIDATION_ERROR_CODE           CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_ERROR';
    -- g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE  CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_MISMATCH_ERROR';

    PROCEDURE LOAD_XML_INTO_RAW_STG (
            I_P_OIC_ID          IN VARCHAR2,
            I_P_FILE_NAME       IN VARCHAR2,
            I_P_XML_CONTENT     IN CLOB,
            I_P_SOURCE_SYSTEM   IN VARCHAR2,
            I_P_DOC_TYPE        IN VARCHAR2,
            O_P_RESPONSE        OUT CLOB,
            O_P_STATUS          OUT VARCHAR2
        )
        IS
            L_V_CHILD_PROCEDURE_STATUS VARCHAR2(32);
            L_V_CHILD_PROCEDURE_RESPONSE CLOB;
            L_V_XML_CONTENT_REC_ID NUMBER;
            L_V_ERROR_CODE VARCHAR2(32);
        BEGIN
                O_P_RESPONSE := O_P_RESPONSE || 'LOAD_XML_INTO_RAW_STG Procedure Started' || CHR(10) || CHR(10);
                INSERT INTO XXEDI_SCM_INB_944_XML_DATA_STG (
                        FILE_NAME
                        ,XML_DATA
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                        ,OIC_INSTANCE_ID
                        ,SOURCE_SYSTEM
                        ,DOC_TYPE
                    ) VALUES (
                        I_P_FILE_NAME
                        ,I_P_XML_CONTENT
                        ,'OIC'
                        ,'OIC'
                        ,I_P_OIC_ID
                        ,I_P_SOURCE_SYSTEM
                        ,g_v_EDI_944_doc_type
                ) RETURNING XML_CONTENT_REC_ID INTO L_V_XML_CONTENT_REC_ID;
            COMMIT;
            O_P_RESPONSE := O_P_RESPONSE || '    XML data loaded into XXEDI_SCM_INB_944_XML_DATA_STG. XML_CONTENT_REC_ID: ' || L_V_XML_CONTENT_REC_ID || ' | File_Name: ' || I_P_FILE_NAME || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || 'PARSE_XML_INTO_STG Procedure invoked' || CHR(10) || CHR(10);
            PARSE_XML_INTO_STG (
                                I_P_OIC_ID
                                ,L_V_CHILD_PROCEDURE_RESPONSE
                                ,L_V_CHILD_PROCEDURE_STATUS
            );
            O_P_RESPONSE := O_P_RESPONSE || L_V_CHILD_PROCEDURE_RESPONSE || CHR(10) || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || 'PARSE_XML_INTO_STG Procedure completed' || CHR(10) || CHR(10);

            O_P_STATUS := L_V_CHILD_PROCEDURE_STATUS;
            O_P_RESPONSE := O_P_RESPONSE || 'LOAD_XML_INTO_RAW_STG Procedure completed' || CHR(10) || CHR(10);
        EXCEPTION
            WHEN OTHERS THEN
                O_P_STATUS      := 'ERROR';
                L_V_ERROR_CODE  := TO_CHAR(SQLCODE);
                O_P_RESPONSE    := O_P_RESPONSE
                                   || CHR(10) || 'Error: ' || SQLCODE || ' : ' || SQLERRM
                                   || CHR(10) || 'Trace: ' || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
                                   || CHR(10) || 'Stack: ' || CHR(10) || DBMS_UTILITY.FORMAT_CALL_STACK;
                IF L_V_XML_CONTENT_REC_ID IS NOT NULL THEN
                    UPDATE XXEDI_SCM_INB_944_XML_DATA_STG SET
                        PROCESSED_FLAG        =  'E'
                        ,ERROR_CODE           =  L_V_ERROR_CODE
                        ,ERROR_MESSAGE        =  SUBSTR( O_P_RESPONSE, 1 , 4000 )
                        ,LAST_UPDATE_DATE     =  SYSDATE
                        ,LAST_UPDATE_BY_NAME  =  '944 LOAD_XML_INTO_RAW_STG Procedure'
                    WHERE XML_CONTENT_REC_ID = L_V_XML_CONTENT_REC_ID;
                    COMMIT;
                END IF;
    END LOAD_XML_INTO_RAW_STG;

    PROCEDURE PARSE_XML_INTO_STG (
             I_P_OIC_ID     IN VARCHAR2
            ,O_P_RESPONSE   OUT CLOB
            ,O_P_STATUS     OUT VARCHAR2
        )
        IS
            v_days_to_keep_file_XML_data   NUMBER := 90;

            XML_DATA                XMLTYPE;
            XML_TEST_PAYLOAD        CLOB;
            XML_RAW_DATA_REC        XXEDI_SCM_INB_944_XML_DATA_STG%ROWTYPE;

            l_v_XML_RECEIPT_HEADER                      XMLTYPE;
            l_v_XML_RECEIPT_H_DATES                     XMLTYPE;
            l_v_XML_RECEIPT_H_CARRIER_INFORMATION       XMLTYPE;
            l_v_XML_RECEIPT_H_CONTACTS                  XMLTYPE;
            l_v_XML_RECEIPT_H_ADDRESS                   XMLTYPE;
            l_v_XML_RECEIPT_H_REFERENCES                XMLTYPE;
            l_v_XML_RECEIPT_H_NOTES                     XMLTYPE;
            l_v_XML_RECEIPT_H_REGULATORY_COMPLIANCES    XMLTYPE;

            l_v_XML_RECEIPT_ORDER_LEVEL                 XMLTYPE;
            l_v_XML_RECEIPT_LINE                        XMLTYPE;
            l_v_XML_RECEIPT_L_DETAIL_RESPONSE          XMLTYPE;
            l_v_XML_RECEIPT_L_PRODUCT_OR_ITEM           XMLTYPE;
            l_v_XML_RECEIPT_L_PHYSICAL_DETAILS          XMLTYPE;
            l_v_XML_RECEIPT_L_REFERENCES                XMLTYPE;
            l_v_XML_RECEIPT_L_NOTES                     XMLTYPE;
            l_v_XML_RECEIPT_L_REGULATORY_COMPLIANCES    XMLTYPE;

            l_v_XML_RECEIPT_SUMMARY                     XMLTYPE;


            l_v_RECEIPT_HEADER_REC_ID   NUMBER;
            l_v_RECEIPT_LINES_REC_ID    NUMBER;

            l_v_ERROR_CODE              VARCHAR2(64);
            l_v_ERROR_MESSAGE           VARCHAR2(4000);


        BEGIN
            O_P_RESPONSE := 'PARSE_XML_INTO_STG Procedure Started' || CHR(10);
            -- select xml data from staging table to be processed
            SELECT * INTO XML_RAW_DATA_REC
                FROM XXEDI_SCM_INB_944_XML_DATA_STG
                WHERE OIC_INSTANCE_ID = I_P_OIC_ID AND PROCESSED_FLAG = 'N' AND DOC_TYPE = g_v_EDI_944_doc_type;

            UPDATE XXEDI_SCM_INB_944_XML_DATA_STG
                SET PROCESSED_FLAG = 'P'
                WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            COMMIT;

            BEGIN
                XML_DATA := XMLTYPE(XML_RAW_DATA_REC.XML_DATA);
            EXCEPTION
                WHEN OTHERS THEN
                    l_v_ERROR_CODE    := 'XML cannot be parsed';
                    l_v_ERROR_MESSAGE := Substr( l_v_ERROR_CODE || ' : The XML provided is invalid. Details:' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    Error:' || l_v_ERROR_MESSAGE || ' | File_Name: "' || XML_RAW_DATA_REC.FILE_NAME || '"';
                    UPDATE XXEDI_SCM_INB_944_XML_DATA_STG SET PROCESSED_FLAG = 'E' ,ERROR_CODE = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    RAISE;
            END;

            -- Parse the XML data into the staging tables depending on the source system
            BEGIN

                SELECT 
                    xml_h_ReceiptHeader               
                    ,xml_h_Dates                    
                    ,xml_h_CarrierInformation 
                    ,xml_h_Contacts                   
                    ,xml_h_Address
                    ,xml_h_References
                    ,xml_h_Notes
                    ,xml_h_RegulatoryCompliances                    
                INTO
                    l_v_XML_RECEIPT_HEADER                 
                    ,l_v_XML_RECEIPT_H_DATES
                    ,l_v_XML_RECEIPT_H_CARRIER_INFORMATION
                    ,l_v_XML_RECEIPT_H_CONTACTS
                    ,l_v_XML_RECEIPT_H_ADDRESS
                    ,l_v_XML_RECEIPT_H_REFERENCES
                    ,l_v_XML_RECEIPT_H_NOTES
                    ,l_v_XML_RECEIPT_H_REGULATORY_COMPLIANCES
                FROM XMLTABLE('/WarehouseTransferReceiptAdvice/Header' PASSING XML_DATA 
                    COLUMNS
                        xml_h_ReceiptHeader             XMLTYPE PATH '/Header/HeaderDetail'
                        ,xml_h_Dates                    XMLTYPE PATH '/Header/Dates'
                        ,xml_h_CarrierInformation       XMLTYPE PATH '/Header/CarrierInformation'
                        ,xml_h_Contacts                 XMLTYPE PATH '/Header/Contacts'
                        ,xml_h_Address                  XMLTYPE PATH '/Header/Address'
                        ,xml_h_References               XMLTYPE PATH '/Header/References'
                        ,xml_h_Notes                    XMLTYPE PATH '/Header/Notes'
                        ,xml_h_RegulatoryCompliances    XMLTYPE PATH '/Header/RegulatoryCompliances'

                        ----------------
                        -- ,XML_fragment_ShipmentHeader  XMLTYPE PATH '/Header/ShipmentHeader'
                        -- ,XML_fragment_QuantityTotals XMLTYPE PATH '/Header/QuantityTotals'
                ) AS ReceiptHeader;

                FOR rec IN ( SELECT
                        Receipt_Header.TradingPartnerId                 AS TRADING_PARTNER_ID
                        ,Receipt_Header.WarehouseReceiptId              AS WAREHOUSE_RECEIPT_ID
                        ,Receipt_Header.ShipmentIdentification          AS SHIPMENT_IDENTIFICATION
                        ,Receipt_Header.DepositorOrderNumber            AS DEPOSITOR_ORDER_NUMBER
                        ,Receipt_Header.ShipmentDate                    AS SHIPMENT_DATE
                        ,Receipt_Header.ReportingCode                   AS REPORTING_CODE
                        ,Receipt_Header.MasterLinkNumber                AS MASTER_LINK_NUMBER
                        ,Receipt_Header.LinkSequenceNumber              AS LINK_SEQUENCE_NUMBER
                        ,Receipt_Header.QuantityOfPalletsReceived       AS QUANTITY_OF_PALLETS_RECEIVED
                        ,Receipt_Header.QuantityOfPalletsReturned       AS QUANTITY_OF_PALLETS_RETURNED
                        ,Receipt_Header.QuantityContested               AS QUANTITY_CONTESTED
                        ,Receipt_Header.ReceivingConditionCode          AS RECEIVING_CONDITION_CODE
                        ,Receipt_Header.UnitLoadOptionCode              AS UNIT_LOAD_OPTION_CODE
                        ,Receipt_Header.TemperatureLocationCode         AS TEMPERATURE_LOCATION_CODE
                        ,Receipt_Header.TemperatureUOM                  AS TEMPERATURE_UOM
                        ,Receipt_Header.Temperature                     AS TEMPERATURE
                        ,Receipt_Header.DocumentVersion                 AS DOCUMENT_VERSION
                        ,Receipt_Header.DocumentRevision                AS DOCUMENT_REVISION


                        ,XML_RAW_DATA_REC.XML_CONTENT_REC_ID            AS XML_CONTENT_REC_ID
                        ,XML_RAW_DATA_REC.FILE_NAME                     AS FILE_NAME


                        ,I_P_OIC_ID                                     AS OIC_INSTANCE_ID
                        ,'OIC'                                          AS CREATED_BY_NAME
                        ,'OIC'                                          AS LAST_UPDATE_BY_NAME
                    FROM XMLTABLE('/HeaderDetail' PASSING l_v_XML_RECEIPT_HEADER
                        COLUMNS
                            TradingPartnerId                VARCHAR2(200) PATH 'TradingPartnerId'
                            ,WarehouseReceiptId             VARCHAR2(200) PATH 'WarehouseReceiptId'
                            ,ShipmentIdentification         VARCHAR2(200) PATH 'ShipmentIdentification'
                            ,DepositorOrderNumber           VARCHAR2(200) PATH 'DepositorOrderNumber'
                            ,ShipmentDate                   VARCHAR2(200) PATH 'ShipmentDate'
                            ,ReportingCode                  VARCHAR2(200) PATH 'ReportingCode'
                            ,MasterLinkNumber               VARCHAR2(200) PATH 'MasterLinkNumber'
                            ,LinkSequenceNumber             VARCHAR2(200) PATH 'LinkSequenceNumber'
                            ,QuantityOfPalletsReceived      VARCHAR2(200) PATH 'QuantityOfPalletsReceived'
                            ,QuantityOfPalletsReturned      VARCHAR2(200) PATH 'QuantityOfPalletsReturned'
                            ,QuantityContested              VARCHAR2(200) PATH 'QuantityContested'
                            ,ReceivingConditionCode         VARCHAR2(200) PATH 'ReceivingConditionCode'
                            ,UnitLoadOptionCode             VARCHAR2(200) PATH 'UnitLoadOptionCode'
                            ,TemperatureLocationCode        VARCHAR2(200) PATH 'TemperatureLocationCode'
                            ,TemperatureUOM                 VARCHAR2(200) PATH 'TemperatureUOM'
                            ,Temperature                    VARCHAR2(200) PATH 'Temperature'
                            ,DocumentVersion                VARCHAR2(200) PATH 'DocumentVersion'
                            ,DocumentRevision               VARCHAR2(200) PATH 'DocumentRevision'
                    ) Receipt_Header)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_HEADER_STG ( 
                        TRADING_PARTNER_ID
                        ,WAREHOUSE_RECEIPT_ID
                        ,SHIPMENT_IDENTIFICATION
                        ,DEPOSITOR_ORDER_NUMBER
                        ,SHIPMENT_DATE
                        ,REPORTING_CODE
                        ,MASTER_LINK_NUMBER
                        ,LINK_SEQUENCE_NUMBER
                        ,QUANTITY_OF_PALLETS_RECEIVED
                        ,QUANTITY_OF_PALLETS_RETURNED
                        ,QUANTITY_CONTESTED
                        ,RECEIVING_CONDITION_CODE
                        ,UNIT_LOAD_OPTION_CODE
                        ,TEMPERATURE_LOCATION_CODE
                        ,TEMPERATURE_UOM
                        ,TEMPERATURE
                        ,DOCUMENT_VERSION
                        ,DOCUMENT_REVISION


                        ,XML_CONTENT_REC_ID
                        ,FILE_NAME


                        ,OIC_INSTANCE_ID
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                        rec.TRADING_PARTNER_ID
                        ,rec.WAREHOUSE_RECEIPT_ID
                        ,rec.SHIPMENT_IDENTIFICATION
                        ,rec.DEPOSITOR_ORDER_NUMBER
                        ,rec.SHIPMENT_DATE
                        ,rec.REPORTING_CODE
                        ,rec.MASTER_LINK_NUMBER
                        ,rec.LINK_SEQUENCE_NUMBER
                        ,rec.QUANTITY_OF_PALLETS_RECEIVED
                        ,rec.QUANTITY_OF_PALLETS_RETURNED
                        ,rec.QUANTITY_CONTESTED
                        ,rec.RECEIVING_CONDITION_CODE
                        ,rec.UNIT_LOAD_OPTION_CODE
                        ,rec.TEMPERATURE_LOCATION_CODE
                        ,rec.TEMPERATURE_UOM
                        ,rec.TEMPERATURE
                        ,rec.DOCUMENT_VERSION
                        ,rec.DOCUMENT_REVISION


                        ,rec.XML_CONTENT_REC_ID
                        ,rec.FILE_NAME


                        ,rec.OIC_INSTANCE_ID
                        ,rec.CREATED_BY_NAME
                        ,rec.LAST_UPDATE_BY_NAME
                    ) 
                    RETURNING RECEIPT_HEADER_STG_REC_ID INTO l_v_RECEIPT_HEADER_REC_ID;
                END LOOP;
                COMMIT;

                FOR H_DATES IN ( SELECT
                        Dates.DateTimeQualifier     AS DATE_TIME_QUALIFIER
                        ,Dates."Date"               AS H_DATE
                        ,Dates."Time"               AS H_TIME
                        ,Dates.DateTimePeriod       AS DATE_TIME_PERIOD
                    FROM XMLTABLE('/Dates' PASSING l_v_XML_RECEIPT_H_DATES
                        COLUMNS
                            DateTimeQualifier               VARCHAR2(200) PATH 'DateTimeQualifier'
                            ,"Date"                         VARCHAR2(200) PATH 'Date'
                            ,"Time"                         VARCHAR2(200) PATH 'Time'
                            ,DateTimePeriod                  VARCHAR2(200) PATH 'DateTimePeriod'
                    ) as Dates)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_DATES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,DATE_TIME_QUALIFIER
                            ,H_DATE
                            ,H_TIME
                            ,DATE_TIME_PERIOD


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_DATES.DATE_TIME_QUALIFIER
                            ,H_DATES.H_DATE
                            ,H_DATES.H_TIME
                            ,H_DATES.DATE_TIME_PERIOD


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );

                END LOOP;
                COMMIT;

                FOR H_CARRIER_INFO IN ( SELECT
                        CarrierInfo.StatusCode                  AS STATUS_CODE
                        ,CarrierInfo.CarrierTransMethodCode     AS CARRIER_TRANS_METHOD_CODE
                        ,CarrierInfo.CarrierAlphaCode           AS CARRIER_ALPHA_CODE
                        ,CarrierInfo.CarrierRouting             AS CARRIER_ROUTING
                        ,CarrierInfo.EquipmentDescriptionCode   AS EQUIPMENT_DESCRIPTION_CODE
                        ,CarrierInfo.CarrierEquipmentInitial    AS CARRIER_EQUIPMENT_INITIAL
                        ,CarrierInfo.CarrierEquipmentNumber     AS CARRIER_EQUIPMENT_NUMBER
                        ,CarrierInfo.EquipmentType              AS EQUIPMENT_TYPE
                        ,CarrierInfo.OwnershipCode              AS OWNERSHIP_CODE
                        ,CarrierInfo.RoutingSequenceCode        AS ROUTING_SEQUENCE_CODE
                        ,CarrierInfo.TransitDirectionCode       AS TRANSIT_DIRECTION_CODE
                        ,CarrierInfo.TransitTimeQual            AS TRANSIT_TIME_QUAL
                        ,CarrierInfo.TransitTime                AS TRANSIT_TIME
                        ,CarrierInfo.ServiceLevelCode           AS SERVICE_LEVEL_CODE
                        ,CarrierInfo.SealStatusCode             AS SEAL_STATUS_CODE
                        ,CarrierInfo.SealNumber                 AS SEAL_NUMBER
                        ,CarrierInfo.AddressTypeCode            AS ADDRESS_TYPE_CODE
                        ,CarrierInfo.LocationCodeQualifier      AS LOCATION_CODE_QUALIFIER
                        ,CarrierInfo.AddressLocationNumber      AS ADDRESS_LOCATION_NUMBER
                        ,CarrierInfo.AddressName                AS ADDRESS_NAME
                        ,CarrierInfo.AddressAlternateName       AS ADDRESS_ALTERNATE_NAME
                        ,CarrierInfo.AddressAlternateName2      AS ADDRESS_ALTERNATE_NAME2
                        ,CarrierInfo.Address1                   AS ADDRESS1
                        ,CarrierInfo.Address2                   AS ADDRESS2
                        ,CarrierInfo.Address3                   AS ADDRESS3
                        ,CarrierInfo.Address4                   AS ADDRESS4
                        ,CarrierInfo.City                       AS CITY
                        ,CarrierInfo.State                      AS STATE
                        ,CarrierInfo.PostalCode                 AS POSTAL_CODE
                        ,CarrierInfo.Country                    AS COUNTRY
                        ,CarrierInfo.LocationID                 AS LOCATION_ID
                        ,CarrierInfo.CountrySubDivision         AS COUNTRY_SUB_DIVISION
                        ,CarrierInfo.AddressTaxIdNumber         AS ADDRESS_TAX_ID_NUMBER
                        ,CarrierInfo.AddressTaxExemptNumber     AS ADDRESS_TAX_EXEMPT_NUMBER
                        ,CarrierInfo.DateTimeQualifier          AS DATE_TIME_QUALIFIER
                        ,CarrierInfo."Date"                     AS HC_DATE
                        ,CarrierInfo."Time"                     AS HC_TIME
                        ,CarrierInfo.DateTimePeriod             AS DATE_TIME_PERIOD
                    FROM XMLTABLE('/CarrierInformation' PASSING l_v_XML_RECEIPT_H_CARRIER_INFORMATION
                        COLUMNS
                            StatusCode                              VARCHAR2(200) PATH 'StatusCode'
                            ,CarrierTransMethodCode                 VARCHAR2(200) PATH 'CarrierTransMethodCode'
                            ,CarrierAlphaCode                       VARCHAR2(200) PATH 'CarrierAlphaCode'
                            ,CarrierRouting                         VARCHAR2(200) PATH 'CarrierRouting'
                            ,EquipmentDescriptionCode               VARCHAR2(200) PATH 'EquipmentDescriptionCode'
                            ,CarrierEquipmentInitial                VARCHAR2(200) PATH 'CarrierEquipmentInitial'
                            ,CarrierEquipmentNumber                 VARCHAR2(200) PATH 'CarrierEquipmentNumber'
                            ,EquipmentType                          VARCHAR2(200) PATH 'EquipmentType'
                            ,OwnershipCode                          VARCHAR2(200) PATH 'OwnershipCode'
                            ,RoutingSequenceCode                    VARCHAR2(200) PATH 'RoutingSequenceCode'
                            ,TransitDirectionCode                   VARCHAR2(200) PATH 'TransitDirectionCode'
                            ,TransitTimeQual                        VARCHAR2(200) PATH 'TransitTimeQual'
                            ,TransitTime                            VARCHAR2(200) PATH 'TransitTime'
                            ,ServiceLevelCode                       VARCHAR2(200) PATH '/ServiceLevelCodes/ServiceLevelCode'
                            ,SealStatusCode                         VARCHAR2(200) PATH '/SealNumbers/SealStatusCode'
                            ,SealNumber                             VARCHAR2(200) PATH '/SealNumbers/SealNumber'
                            ,AddressTypeCode                        VARCHAR2(200) PATH '/Address/AddressTypeCode'
                            ,LocationCodeQualifier                  VARCHAR2(200) PATH '/Address/LocationCodeQualifier'
                            ,AddressLocationNumber                  VARCHAR2(200) PATH '/Address/AddressLocationNumber'
                            ,AddressName                            VARCHAR2(200) PATH '/Address/AddressName'
                            ,AddressAlternateName                   VARCHAR2(200) PATH '/Address/AddressAlternateName'
                            ,AddressAlternateName2                  VARCHAR2(200) PATH '/Address/AddressAlternateName2'
                            ,Address1                               VARCHAR2(200) PATH '/Address/Address1'
                            ,Address2                               VARCHAR2(200) PATH '/Address/Address2'
                            ,Address3                               VARCHAR2(200) PATH '/Address/Address3'
                            ,Address4                               VARCHAR2(200) PATH '/Address/Address4'
                            ,City                                   VARCHAR2(200) PATH '/Address/City'
                            ,State                                  VARCHAR2(200) PATH '/Address/State'
                            ,PostalCode                             VARCHAR2(200) PATH '/Address/PostalCode'
                            ,Country                                VARCHAR2(200) PATH '/Address/Country'
                            ,LocationID                             VARCHAR2(200) PATH '/Address/LocationID'
                            ,CountrySubDivision                     VARCHAR2(200) PATH '/Address/CountrySubDivision'
                            ,AddressTaxIdNumber                     VARCHAR2(200) PATH '/Address/AddressTaxIdNumber'
                            ,AddressTaxExemptNumber                 VARCHAR2(200) PATH '/Address/AddressTaxExemptNumber'
                            ,DateTimeQualifier                      VARCHAR2(200) PATH '/Address/Dates/DateTimeQualifier'
                            ,"Date"                                 VARCHAR2(200) PATH '/Address/Dates/Date'
                            ,"Time"                                 VARCHAR2(200) PATH '/Address/Dates/Time'
                            ,DateTimePeriod                         VARCHAR2(200) PATH '/Address/Dates/DateTimePeriod'
                    ) as CarrierInfo)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_CARRIER_INFO_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,STATUS_CODE
                            ,CARRIER_TRANS_METHOD_CODE
                            ,CARRIER_ALPHA_CODE
                            ,CARRIER_ROUTING
                            ,EQUIPMENT_DESCRIPTION_CODE
                            ,CARRIER_EQUIPMENT_INITIAL
                            ,CARRIER_EQUIPMENT_NUMBER
                            ,EQUIPMENT_TYPE
                            ,OWNERSHIP_CODE
                            ,ROUTING_SEQUENCE_CODE
                            ,TRANSIT_DIRECTION_CODE
                            ,TRANSIT_TIME_QUAL
                            ,TRANSIT_TIME
                            ,SERVICE_LEVEL_CODE
                            ,SEAL_STATUS_CODE
                            ,SEAL_NUMBER


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_CARRIER_INFO.STATUS_CODE
                            ,H_CARRIER_INFO.CARRIER_TRANS_METHOD_CODE
                            ,H_CARRIER_INFO.CARRIER_ALPHA_CODE
                            ,H_CARRIER_INFO.CARRIER_ROUTING
                            ,H_CARRIER_INFO.EQUIPMENT_DESCRIPTION_CODE
                            ,H_CARRIER_INFO.CARRIER_EQUIPMENT_INITIAL
                            ,H_CARRIER_INFO.CARRIER_EQUIPMENT_NUMBER
                            ,H_CARRIER_INFO.EQUIPMENT_TYPE
                            ,H_CARRIER_INFO.OWNERSHIP_CODE
                            ,H_CARRIER_INFO.ROUTING_SEQUENCE_CODE
                            ,H_CARRIER_INFO.TRANSIT_DIRECTION_CODE
                            ,H_CARRIER_INFO.TRANSIT_TIME_QUAL
                            ,H_CARRIER_INFO.TRANSIT_TIME
                            ,H_CARRIER_INFO.SERVICE_LEVEL_CODE
                            ,H_CARRIER_INFO.SEAL_STATUS_CODE
                            ,H_CARRIER_INFO.SEAL_NUMBER


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );

                    INSERT INTO XXEDI_SCM_INB_944_H_CARRIER_ADDRESS_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,ADDRESS_TYPE_CODE
                            ,LOCATION_CODE_QUALIFIER
                            ,ADDRESS_LOCATION_NUMBER
                            ,ADDRESS_NAME
                            ,ADDRESS_ALTERNATE_NAME
                            ,ADDRESS_ALTERNATE_NAME2
                            ,ADDRESS1
                            ,ADDRESS2
                            ,ADDRESS3
                            ,ADDRESS4
                            ,CITY
                            ,STATE
                            ,POSTAL_CODE
                            ,COUNTRY
                            ,LOCATION_ID
                            ,COUNTRY_SUB_DIVISION
                            ,ADDRESS_TAX_ID_NUMBER
                            ,ADDRESS_TAX_EXEMPT_NUMBER
                            ,DATE_TIME_QUALIFIER
                            ,HC_DATE
                            ,HC_TIME
                            ,DATE_TIME_PERIOD


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_CARRIER_INFO.ADDRESS_TYPE_CODE
                            ,H_CARRIER_INFO.LOCATION_CODE_QUALIFIER
                            ,H_CARRIER_INFO.ADDRESS_LOCATION_NUMBER
                            ,H_CARRIER_INFO.ADDRESS_NAME
                            ,H_CARRIER_INFO.ADDRESS_ALTERNATE_NAME
                            ,H_CARRIER_INFO.ADDRESS_ALTERNATE_NAME2
                            ,H_CARRIER_INFO.ADDRESS1
                            ,H_CARRIER_INFO.ADDRESS2
                            ,H_CARRIER_INFO.ADDRESS3
                            ,H_CARRIER_INFO.ADDRESS4
                            ,H_CARRIER_INFO.CITY
                            ,H_CARRIER_INFO.STATE
                            ,H_CARRIER_INFO.POSTAL_CODE
                            ,H_CARRIER_INFO.COUNTRY
                            ,H_CARRIER_INFO.LOCATION_ID
                            ,H_CARRIER_INFO.COUNTRY_SUB_DIVISION
                            ,H_CARRIER_INFO.ADDRESS_TAX_ID_NUMBER
                            ,H_CARRIER_INFO.ADDRESS_TAX_EXEMPT_NUMBER
                            ,H_CARRIER_INFO.DATE_TIME_QUALIFIER
                            ,H_CARRIER_INFO.HC_DATE
                            ,H_CARRIER_INFO.HC_TIME
                            ,H_CARRIER_INFO.DATE_TIME_PERIOD


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR H_CONTACTS IN ( SELECT
                        Contacts.ContactTypeCode            AS CONTACT_TYPE_CODE
                        ,Contacts.ContactName               AS CONTACT_NAME
                        ,Contacts.PrimaryPhone              AS PRIMARY_PHONE
                        ,Contacts.PrimaryFax                AS PRIMARY_FAX
                        ,Contacts.PrimaryEmail              AS PRIMARY_EMAIL
                        ,Contacts.ContactQual               AS CONTACT_QUAL
                        ,Contacts.ContactID                 AS CONTACT_ID
                        ,Contacts.ContactReference          AS CONTACT_REFERENCE
                    FROM XMLTABLE('/Contacts' PASSING l_v_XML_RECEIPT_H_CONTACTS
                        COLUMNS
                            ContactTypeCode                 VARCHAR2(200) PATH 'ContactTypeCode'
                            ,ContactName                    VARCHAR2(200) PATH 'ContactName'
                            ,PrimaryPhone                   VARCHAR2(200) PATH 'PrimaryPhone'
                            ,PrimaryFax                     VARCHAR2(200) PATH 'PrimaryFax'
                            ,PrimaryEmail                   VARCHAR2(200) PATH 'PrimaryEmail'
                            ,ContactQual                    VARCHAR2(200) PATH '/AdditionalContactDetails/ContactQual'
                            ,ContactID                      VARCHAR2(200) PATH '/AdditionalContactDetails/ContactID'
                            ,ContactReference               VARCHAR2(200) PATH 'ContactReference'
                    ) as Contacts)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_CONTACTS_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,CONTACT_TYPE_CODE
                            ,CONTACT_NAME
                            ,PRIMARY_PHONE
                            ,PRIMARY_FAX
                            ,PRIMARY_EMAIL
                            ,CONTACT_QUAL
                            ,CONTACT_ID
                            ,CONTACT_REFERENCE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_CONTACTS.CONTACT_TYPE_CODE
                            ,H_CONTACTS.CONTACT_NAME
                            ,H_CONTACTS.PRIMARY_PHONE
                            ,H_CONTACTS.PRIMARY_FAX
                            ,H_CONTACTS.PRIMARY_EMAIL
                            ,H_CONTACTS.CONTACT_QUAL
                            ,H_CONTACTS.CONTACT_ID
                            ,H_CONTACTS.CONTACT_REFERENCE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR H_ADDRESS IN ( SELECT
                        Address.AddressTypeCode             AS ADDRESS_TYPE_CODE
                        ,Address.LocationCodeQualifier      AS LOCATION_CODE_QUALIFIER
                        ,Address.AddressLocationNumber      AS ADDRESS_LOCATION_NUMBER
                        ,Address.AddressName                AS ADDRESS_NAME
                        ,Address.AddressAlternateName       AS ADDRESS_ALTERNATE_NAME
                        ,Address.AddressAlternateName2      AS ADDRESS_ALTERNATE_NAME2
                        ,Address.Address1                   AS ADDRESS1
                        ,Address.Address2                   AS ADDRESS2
                        ,Address.Address3                   AS ADDRESS3
                        ,Address.Address4                   AS ADDRESS4
                        ,Address.City                       AS CITY
                        ,Address.State                      AS STATE
                        ,Address.PostalCode                 AS POSTAL_CODE
                        ,Address.Country                    AS COUNTRY
                        ,Address.LocationID                 AS LOCATION_ID
                        ,Address.CountrySubDivision         AS COUNTRY_SUB_DIVISION
                        ,Address.AddressTaxIdNumber         AS ADDRESS_TAX_ID_NUMBER
                        ,Address.AddressTaxExemptNumber     AS ADDRESS_TAX_EXEMPT_NUMBER
                        ,Address.ReferenceQual              AS REFERENCE_QUAL
                        ,Address.ReferenceID                AS REFERENCE_ID
                        ,Address.Description                AS DESCRIPTION
                        ,Address."Date"                     AS HA_DATE
                        ,Address."Time"                     AS HA_TIME
                        ,Address.ReferenceQual2             AS REFERENCE_QUAL2
                        ,Address.ReferenceID2               AS REFERENCE_ID2
                        ,Address.ContactTypeCode            AS CONTACT_TYPE_CODE
                        ,Address.ContactName                AS CONTACT_NAME
                        ,Address.PrimaryPhone               AS PRIMARY_PHONE
                        ,Address.PrimaryFax                 AS PRIMARY_FAX
                        ,Address.PrimaryEmail               AS PRIMARY_EMAIL
                        ,Address.ContactQual                AS CONTACT_QUAL
                        ,Address.ContactID                  AS CONTACT_ID
                        ,Address.ContactReference           AS CONTACT_REFERENCE
                        ,Address.DateTimeQualifier          AS DATE_TIME_QUALIFIER
                        ,Address.Date2                      AS HA_DATE2
                        ,Address.Time2                      AS HA_TIME2
                        ,Address.DateTimePeriod             AS DATE_TIME_PERIOD
                    FROM XMLTABLE('/Address' PASSING l_v_XML_RECEIPT_H_ADDRESS
                        COLUMNS
                            AddressTypeCode                         VARCHAR2(200) PATH 'AddressTypeCode'
                            ,LocationCodeQualifier                  VARCHAR2(200) PATH 'LocationCodeQualifier'
                            ,AddressLocationNumber                  VARCHAR2(200) PATH 'AddressLocationNumber'
                            ,AddressName                            VARCHAR2(200) PATH 'AddressName'
                            ,AddressAlternateName                   VARCHAR2(200) PATH 'AddressAlternateName'
                            ,AddressAlternateName2                  VARCHAR2(200) PATH 'AddressAlternateName2'
                            ,Address1                               VARCHAR2(200) PATH 'Address1'
                            ,Address2                               VARCHAR2(200) PATH 'Address2'
                            ,Address3                               VARCHAR2(200) PATH 'Address3'
                            ,Address4                               VARCHAR2(200) PATH 'Address4'
                            ,City                                   VARCHAR2(200) PATH 'City'
                            ,State                                  VARCHAR2(200) PATH 'State'
                            ,PostalCode                             VARCHAR2(200) PATH 'PostalCode'
                            ,Country                                VARCHAR2(200) PATH 'Country'
                            ,LocationID                             VARCHAR2(200) PATH 'LocationID'
                            ,CountrySubDivision                     VARCHAR2(200) PATH 'CountrySubDivision'
                            ,AddressTaxIdNumber                     VARCHAR2(200) PATH 'AddressTaxIdNumber'
                            ,AddressTaxExemptNumber                 VARCHAR2(200) PATH 'AddressTaxExemptNumber'
                            ,ReferenceQual                          VARCHAR2(200) PATH '/References/ReferenceQual'
                            ,ReferenceID                            VARCHAR2(200) PATH '/References/ReferenceID'
                            ,Description                            VARCHAR2(200) PATH '/References/Description'
                            ,"Date"                                 VARCHAR2(200) PATH '/References/Dates/Date'
                            ,"Time"                                 VARCHAR2(200) PATH '/References/Dates/Time'
                            ,ReferenceQual2                         VARCHAR2(200) PATH '/References/ReferenceIDs/ReferenceQual'
                            ,ReferenceID2                           VARCHAR2(200) PATH '/References/ReferenceIDs/ReferenceID'
                            ,ContactTypeCode                        VARCHAR2(200) PATH '/Contacts/ContactTypeCode'
                            ,ContactName                            VARCHAR2(200) PATH '/Contacts/ContactName'
                            ,PrimaryPhone                           VARCHAR2(200) PATH '/Contacts/PrimaryPhone'
                            ,PrimaryFax                             VARCHAR2(200) PATH '/Contacts/PrimaryFax'
                            ,PrimaryEmail                           VARCHAR2(200) PATH '/Contacts/PrimaryEmail'
                            ,ContactQual                            VARCHAR2(200) PATH '/Contacts//AdditionalContactDetails/ContactQual'
                            ,ContactID                              VARCHAR2(200) PATH '/Contacts//AdditionalContactDetails/ContactID'
                            ,ContactReference                       VARCHAR2(200) PATH '/Contacts/ContactReference'
                            ,DateTimeQualifier                      VARCHAR2(200) PATH '/Dates/DateTimeQualifier'
                            ,Date2                                  VARCHAR2(200) PATH '/Dates/Date'
                            ,Time2                                  VARCHAR2(200) PATH '/Dates/Time'
                            ,DateTimePeriod                         VARCHAR2(200) PATH '/Dates/DateTimePeriod'
                    ) as Address)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_ADDRESS_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,ADDRESS_TYPE_CODE
                            ,LOCATION_CODE_QUALIFIER
                            ,ADDRESS_LOCATION_NUMBER
                            ,ADDRESS_NAME
                            ,ADDRESS_ALTERNATE_NAME
                            ,ADDRESS_ALTERNATE_NAME2
                            ,ADDRESS1
                            ,ADDRESS2
                            ,ADDRESS3
                            ,ADDRESS4
                            ,CITY
                            ,STATE
                            ,POSTAL_CODE
                            ,COUNTRY
                            ,LOCATION_ID
                            ,COUNTRY_SUB_DIVISION
                            ,ADDRESS_TAX_ID_NUMBER
                            ,ADDRESS_TAX_EXEMPT_NUMBER


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_ADDRESS.ADDRESS_TYPE_CODE
                            ,H_ADDRESS.LOCATION_CODE_QUALIFIER
                            ,H_ADDRESS.ADDRESS_LOCATION_NUMBER
                            ,H_ADDRESS.ADDRESS_NAME
                            ,H_ADDRESS.ADDRESS_ALTERNATE_NAME
                            ,H_ADDRESS.ADDRESS_ALTERNATE_NAME2
                            ,H_ADDRESS.ADDRESS1
                            ,H_ADDRESS.ADDRESS2
                            ,H_ADDRESS.ADDRESS3
                            ,H_ADDRESS.ADDRESS4
                            ,H_ADDRESS.CITY
                            ,H_ADDRESS.STATE
                            ,H_ADDRESS.POSTAL_CODE
                            ,H_ADDRESS.COUNTRY
                            ,H_ADDRESS.LOCATION_ID
                            ,H_ADDRESS.COUNTRY_SUB_DIVISION
                            ,H_ADDRESS.ADDRESS_TAX_ID_NUMBER
                            ,H_ADDRESS.ADDRESS_TAX_EXEMPT_NUMBER


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );

                    INSERT INTO XXEDI_SCM_INB_944_H_ADDRESS_REFERENCES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,REFERENCE_QUAL
                            ,REFERENCE_ID
                            ,DESCRIPTION
                            ,HA_DATE
                            ,HA_TIME
                            ,REFERENCE_QUAL2
                            ,REFERENCE_ID2


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_ADDRESS.REFERENCE_QUAL
                            ,H_ADDRESS.REFERENCE_ID
                            ,H_ADDRESS.DESCRIPTION
                            ,H_ADDRESS.HA_DATE
                            ,H_ADDRESS.HA_TIME
                            ,H_ADDRESS.REFERENCE_QUAL2
                            ,H_ADDRESS.REFERENCE_ID2


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );

                    INSERT INTO XXEDI_SCM_INB_944_H_ADDRESS_CONTACTS_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,CONTACT_TYPE_CODE
                            ,CONTACT_NAME
                            ,PRIMARY_PHONE
                            ,PRIMARY_FAX
                            ,PRIMARY_EMAIL
                            ,CONTACT_QUAL
                            ,CONTACT_ID
                            ,CONTACT_REFERENCE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_ADDRESS.CONTACT_TYPE_CODE
                            ,H_ADDRESS.CONTACT_NAME
                            ,H_ADDRESS.PRIMARY_PHONE
                            ,H_ADDRESS.PRIMARY_FAX
                            ,H_ADDRESS.PRIMARY_EMAIL
                            ,H_ADDRESS.CONTACT_QUAL
                            ,H_ADDRESS.CONTACT_ID
                            ,H_ADDRESS.CONTACT_REFERENCE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );

                    INSERT INTO XXEDI_SCM_INB_944_H_ADDRESS_DATES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,DATE_TIME_QUALIFIER
                            ,HA_DATE
                            ,HA_TIME
                            ,DATE_TIME_PERIOD


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_ADDRESS.DATE_TIME_QUALIFIER
                            ,H_ADDRESS.HA_DATE2
                            ,H_ADDRESS.HA_TIME2
                            ,H_ADDRESS.DATE_TIME_PERIOD

                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR H_REFERENCE IN ( SELECT
                        H_References.ReferenceQual              AS REFERENCE_QUAL
                        ,H_References.ReferenceID               AS REFERENCE_ID
                        ,H_References.Description               AS DESCRIPTION
                        ,H_References."Date"                    AS HR_DATE
                        ,H_References."Time"                    AS HR_TIME
                        ,H_References.ReferenceQual2            AS REFERENCE_QUAL2
                        ,H_References.ReferenceID2              AS REFERENCE_ID2
                    FROM XMLTABLE('/References' PASSING l_v_XML_RECEIPT_H_REFERENCES
                        COLUMNS
                            ReferenceQual                       VARCHAR2(200) PATH 'ReferenceQual'
                            ,ReferenceID                        VARCHAR2(200) PATH 'ReferenceID'
                            ,Description                        VARCHAR2(200) PATH 'Description'
                            ,"Date"                             VARCHAR2(200) PATH 'Date'
                            ,"Time"                             VARCHAR2(200) PATH 'Time'
                            ,ReferenceQual2                     VARCHAR2(200) PATH '/ReferenceIDs/ReferenceQual'
                            ,ReferenceID2                       VARCHAR2(200) PATH '/ReferenceIDs/ReferenceID'
                    ) as H_References)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_REFERENCES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,REFERENCE_QUAL
                            ,REFERENCE_ID
                            ,DESCRIPTION
                            ,HR_DATE
                            ,HR_TIME
                            ,REFERENCE_QUAL2
                            ,REFERENCE_ID2


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_REFERENCE.REFERENCE_QUAL
                            ,H_REFERENCE.REFERENCE_ID
                            ,H_REFERENCE.DESCRIPTION
                            ,H_REFERENCE.HR_DATE
                            ,H_REFERENCE.HR_TIME
                            ,H_REFERENCE.REFERENCE_QUAL2
                            ,H_REFERENCE.REFERENCE_ID2


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR H_NOTES IN ( SELECT
                        H_Notes.NoteCode                      AS NOTE_CODE
                        ,H_Notes.Note                         AS NOTE
                        ,H_Notes.LanguageCode                 AS LANGUAGE_CODE
                    FROM XMLTABLE('/Notes' PASSING l_v_XML_RECEIPT_H_NOTES
                        COLUMNS
                            NoteCode                            VARCHAR2(200) PATH 'NoteCode'
                            ,Note                               VARCHAR2(200) PATH 'Note'
                            ,LanguageCode                       VARCHAR2(200) PATH 'LanguageCode'
                    ) as H_Notes)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_NOTES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,NOTE_CODE
                            ,NOTE
                            ,LANGUAGE_CODE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_NOTES.NOTE_CODE
                            ,H_NOTES.NOTE
                            ,H_NOTES.LANGUAGE_CODE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR H_REGULATORY_COMPLIANCES IN ( SELECT
                        RegulatoryCompliances.RegulatoryComplianceQual      AS REGULATORY_COMPLIANCE_QUAL
                        ,RegulatoryCompliances.YesOrNoResponse              AS YES_OR_NO_RESPONSE
                        ,RegulatoryCompliances.RegulatoryComplianceID       AS REGULATORY_COMPLIANCE_ID
                        ,RegulatoryCompliances.RegulatoryAgency             AS REGULATORY_AGENCY
                        ,RegulatoryCompliances.Description                  AS DESCRIPTION
                    FROM XMLTABLE('/RegulatoryCompliances' PASSING l_v_XML_RECEIPT_H_REGULATORY_COMPLIANCES
                        COLUMNS
                            RegulatoryComplianceQual            VARCHAR2(200) PATH 'RegulatoryComplianceQual'
                            ,YesOrNoResponse                    VARCHAR2(200) PATH 'YesOrNoResponse'
                            ,RegulatoryComplianceID             VARCHAR2(200) PATH 'RegulatoryComplianceID'
                            ,RegulatoryAgency                   VARCHAR2(200) PATH 'RegulatoryAgency'
                            ,Description                        VARCHAR2(200) PATH 'Description'
                    ) as RegulatoryCompliances)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_944_H_REGULATORY_COMPLIANCES_STG ( 
                            RECEIPT_HEADER_STG_REC_ID


                            ,REGULATORY_COMPLIANCE_QUAL
                            ,YES_OR_NO_RESPONSE
                            ,REGULATORY_COMPLIANCE_ID
                            ,REGULATORY_AGENCY
                            ,DESCRIPTION


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                            l_v_RECEIPT_HEADER_REC_ID


                            ,H_REGULATORY_COMPLIANCES.REGULATORY_COMPLIANCE_QUAL
                            ,H_REGULATORY_COMPLIANCES.YES_OR_NO_RESPONSE
                            ,H_REGULATORY_COMPLIANCES.REGULATORY_COMPLIANCE_ID
                            ,H_REGULATORY_COMPLIANCES.REGULATORY_AGENCY
                            ,H_REGULATORY_COMPLIANCES.DESCRIPTION


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                END LOOP;
                COMMIT;

                FOR ORDER_LEVEL IN ( SELECT
                            Order_Level_xml_fragment
                        FROM XMLTABLE('/WarehouseTransferReceiptAdvice/OrderLevel' PASSING XML_DATA
                            COLUMNS
                                Order_Level_xml_fragment    XMLTYPE  PATH '/OrderLevel'
                    ) AS OrderLevel
                ) LOOP
                    l_v_XML_RECEIPT_ORDER_LEVEL := ORDER_LEVEL.Order_Level_xml_fragment;

                    FOR LINE_ITEM IN ( SELECT
                                Line_Item_xml_fragment
                            FROM XMLTABLE('/OrderLevel/LineItem | /OrderLevel/OrderPack/LineItem' PASSING l_v_XML_RECEIPT_ORDER_LEVEL
                                COLUMNS
                                    Line_Item_xml_fragment  XMLTYPE  PATH '/LineItem'
                        ) AS LineItem
                    ) LOOP
                        l_v_XML_RECEIPT_LINE := LINE_ITEM.Line_Item_xml_fragment;


                        FOR ITEM_DETAIL IN ( SELECT
                                ItemDetail.LineSequenceNumber                   AS LINE_SEQUENCE_NUMBER
                                ,ItemDetail.ApplicationId                       AS APPLICATION_ID
                                ,ItemDetail.BuyerPartNumber                     AS BUYER_PART_NUMBER
                                ,ItemDetail.VendorPartNumber                    AS VENDOR_PART_NUMBER
                                ,ItemDetail.ConsumerPackageCode                 AS CONSUMER_PACKAGE_CODE
                                ,ItemDetail.EAN                                 AS EAN
                                ,ItemDetail.GTIN                                AS GTIN
                                ,ItemDetail.UPCCaseCode                         AS UPC_CASE_CODE
                                ,ItemDetail.NatlDrugCode                        AS NATL_DRUG_CODE
                                ,ItemDetail.InternationalStandardBookNumber     AS INTERNATIONAL_STANDARD_BOOK_NUMBER
                                ,ItemDetail.PartNumberQual                      AS PART_NUMBER_QUAL
                                ,ItemDetail.PartNumber                          AS PART_NUMBER
                                ,ItemDetail.ShipQty                             AS SHIP_QTY
                                ,ItemDetail.ShipQtyUOM                          AS SHIP_QTY_UOM
                                ,ItemDetail.WarehouseLotID                      AS WAREHOUSE_LOT_ID
                                ,ItemDetail.WarehouseDetailAdjID                AS WAREHOUSE_DETAIL_ADJ_ID
                                ,ItemDetail.UnitWeight                          AS UNIT_WEIGHT
                                ,ItemDetail.Color                               AS COLOR
                            FROM XMLTABLE('/LineItem/ItemDetail' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    LineSequenceNumber                      VARCHAR2(200) PATH 'LineSequenceNumber'
                                    ,ApplicationId                          VARCHAR2(200) PATH 'ApplicationId'
                                    ,BuyerPartNumber                        VARCHAR2(200) PATH 'BuyerPartNumber'
                                    ,VendorPartNumber                       VARCHAR2(200) PATH 'VendorPartNumber'
                                    ,ConsumerPackageCode                    VARCHAR2(200) PATH 'ConsumerPackageCode'
                                    ,EAN                                    VARCHAR2(200) PATH 'EAN'
                                    ,GTIN                                   VARCHAR2(200) PATH 'GTIN'
                                    ,UPCCaseCode                            VARCHAR2(200) PATH 'UPCCaseCode'
                                    ,NatlDrugCode                           VARCHAR2(200) PATH 'NatlDrugCode'
                                    ,InternationalStandardBookNumber        VARCHAR2(200) PATH 'InternationalStandardBookNumber'
                                    ,PartNumberQual                         VARCHAR2(200) PATH '/ProductID/PartNumberQual'
                                    ,PartNumber                             VARCHAR2(200) PATH '/ProductID/PartNumber'
                                    ,ShipQty                                VARCHAR2(200) PATH 'ShipQty'
                                    ,ShipQtyUOM                             VARCHAR2(200) PATH 'ShipQtyUOM'
                                    ,WarehouseLotID                         VARCHAR2(200) PATH 'WarehouseLotID'
                                    ,WarehouseDetailAdjID                   VARCHAR2(200) PATH 'WarehouseDetailAdjID'
                                    ,UnitWeight                             VARCHAR2(200) PATH 'UnitWeight'
                                    ,Color                                  VARCHAR2(200) PATH 'Color'
                            ) AS ItemDetail
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_LINES_STG (
                                    RECEIPT_HEADER_STG_REC_ID

                                    ,LINE_SEQUENCE_NUMBER
                                    ,APPLICATION_ID
                                    ,BUYER_PART_NUMBER
                                    ,VENDOR_PART_NUMBER
                                    ,CONSUMER_PACKAGE_CODE
                                    ,EAN
                                    ,GTIN
                                    ,UPC_CASE_CODE
                                    ,NATL_DRUG_CODE
                                    ,INTERNATIONAL_STANDARD_BOOK_NUMBER
                                    ,PART_NUMBER_QUAL
                                    ,PART_NUMBER
                                    ,SHIP_QTY
                                    ,SHIP_QTY_UOM
                                    ,WAREHOUSE_LOT_ID
                                    ,WAREHOUSE_DETAIL_ADJ_ID
                                    ,UNIT_WEIGHT
                                    ,COLOR

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                ) VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID

                                    ,ITEM_DETAIL.LINE_SEQUENCE_NUMBER
                                    ,ITEM_DETAIL.APPLICATION_ID
                                    ,ITEM_DETAIL.BUYER_PART_NUMBER
                                    ,ITEM_DETAIL.VENDOR_PART_NUMBER
                                    ,ITEM_DETAIL.CONSUMER_PACKAGE_CODE
                                    ,ITEM_DETAIL.EAN
                                    ,ITEM_DETAIL.GTIN
                                    ,ITEM_DETAIL.UPC_CASE_CODE
                                    ,ITEM_DETAIL.NATL_DRUG_CODE
                                    ,ITEM_DETAIL.INTERNATIONAL_STANDARD_BOOK_NUMBER
                                    ,ITEM_DETAIL.PART_NUMBER_QUAL
                                    ,ITEM_DETAIL.PART_NUMBER
                                    ,ITEM_DETAIL.SHIP_QTY
                                    ,ITEM_DETAIL.SHIP_QTY_UOM
                                    ,ITEM_DETAIL.WAREHOUSE_LOT_ID
                                    ,ITEM_DETAIL.WAREHOUSE_DETAIL_ADJ_ID
                                    ,ITEM_DETAIL.UNIT_WEIGHT
                                    ,ITEM_DETAIL.COLOR

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            ) RETURNING RECEIPT_LINE_STG_REC_ID INTO l_v_RECEIPT_LINES_REC_ID;
                        END LOOP;
                        COMMIT;

                        FOR DETAIL_RESPONSE IN ( SELECT
                                DetailResponse.Qty                          AS QTY
                                ,DetailResponse.QtyUOM                      AS QTY_UOM
                                ,DetailResponse.ReceivingConditionCode      AS RECEIVING_CONDITION_CODE
                                ,DetailResponse.WarehouseLotID              AS WAREHOUSE_LOT_ID
                                ,DetailResponse.DamageReasonCode            AS DAMAGE_REASON_CODE
                                ,DetailResponse.ReferenceQual               AS REFERENCE_QUAL
                                ,DetailResponse.ReferenceID                 AS REFERENCE_ID
                                ,DetailResponse.Description                 AS DESCRIPTION
                                ,DetailResponse."Date"                      AS LD_DATE
                                ,DetailResponse."Time"                      AS LD_TIME
                                ,DetailResponse.ReferenceQual2              AS REFERENCE_QUAL2
                                ,DetailResponse.ReferenceID2                AS REFERENCE_ID2
                            FROM XMLTABLE('/LineItem/DetailResponse' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    Qty                             VARCHAR2(200) PATH 'Qty'
                                    ,QtyUOM                         VARCHAR2(200) PATH 'QtyUOM'
                                    ,ReceivingConditionCode         VARCHAR2(200) PATH 'ReceivingConditionCode'
                                    ,WarehouseLotID                 VARCHAR2(200) PATH 'WarehouseLotID'
                                    ,DamageReasonCode               VARCHAR2(200) PATH 'DamageReasonCode'
                                    ,ReferenceQual                  VARCHAR2(200) PATH 'ReferenceQual'
                                    ,ReferenceID                    VARCHAR2(200) PATH 'ReferenceID'
                                    ,Description                    VARCHAR2(200) PATH 'Description'
                                    ,"Date"                         VARCHAR2(200) PATH 'Date'
                                    ,"Time"                         VARCHAR2(200) PATH 'Time'
                                    ,ReferenceQual2                 VARCHAR2(200) PATH '/ReferenceIDs/ReferenceQual'
                                    ,ReferenceID2                   VARCHAR2(200) PATH '/ReferenceIDs/ReferenceID'
                            ) AS DetailResponse
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_DETAIL_RESPONSE_STG (
                                    RECEIPT_HEADER_STG_REC_ID

                                    ,QTY
                                    ,QTY_UOM
                                    ,RECEIVING_CONDITION_CODE
                                    ,WAREHOUSE_LOT_ID
                                    ,DAMAGE_REASON_CODE
                                    ,REFERENCE_QUAL
                                    ,REFERENCE_ID
                                    ,DESCRIPTION
                                    ,LD_DATE
                                    ,LD_TIME
                                    ,REFERENCE_QUAL2
                                    ,REFERENCE_ID2

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                ) VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID

                                    ,DETAIL_RESPONSE.QTY
                                    ,DETAIL_RESPONSE.QTY_UOM
                                    ,DETAIL_RESPONSE.RECEIVING_CONDITION_CODE
                                    ,DETAIL_RESPONSE.WAREHOUSE_LOT_ID
                                    ,DETAIL_RESPONSE.DAMAGE_REASON_CODE
                                    ,DETAIL_RESPONSE.REFERENCE_QUAL
                                    ,DETAIL_RESPONSE.REFERENCE_ID
                                    ,DETAIL_RESPONSE.DESCRIPTION
                                    ,DETAIL_RESPONSE.LD_DATE
                                    ,DETAIL_RESPONSE.LD_TIME
                                    ,DETAIL_RESPONSE.REFERENCE_QUAL2
                                    ,DETAIL_RESPONSE.REFERENCE_ID2

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            ) RETURNING RECEIPT_LINE_STG_REC_ID INTO l_v_RECEIPT_LINES_REC_ID;
                        END LOOP;
                        COMMIT;

                        FOR PROD_OR_ITEM IN ( SELECT
                                ProdOrItem.ProductCharacteristicCode    AS PRODUCT_CHARACTERISTIC_CODE
                                ,ProdOrItem.AgencyQualifierCode         AS AGENCY_QUALIFIER_CODE
                                ,ProdOrItem.ProductDescriptionCode      AS PRODUCT_DESCRIPTION_CODE
                                ,ProdOrItem.ProductDescription          AS PRODUCT_DESCRIPTION
                                ,ProdOrItem.SurfaceLayerPositionCode    AS SURFACE_LAYER_POSITION_CODE
                                ,ProdOrItem.SourceSubqualifier          AS SOURCE_SUBQUALIFIER
                                ,ProdOrItem.YesOrNoResponse             AS YES_OR_NO_RESPONSE
                                ,ProdOrItem.LanguageCode                AS LANGUAGE_CODE
                            FROM XMLTABLE('/LineItem/ProductOrItemDescription' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    ProductCharacteristicCode   VARCHAR2(200) PATH 'ProductCharacteristicCode'
                                    ,AgencyQualifierCode        VARCHAR2(200) PATH 'AgencyQualifierCode'
                                    ,ProductDescriptionCode     VARCHAR2(200) PATH 'ProductDescriptionCode'
                                    ,ProductDescription         VARCHAR2(200) PATH 'ProductDescription'
                                    ,SurfaceLayerPositionCode   VARCHAR2(200) PATH 'SurfaceLayerPositionCode'
                                    ,SourceSubqualifier         VARCHAR2(200) PATH 'SourceSubqualifier'
                                    ,YesOrNoResponse            VARCHAR2(200) PATH 'YesOrNoResponse'
                                    ,LanguageCode               VARCHAR2(200) PATH 'LanguageCode'
                            ) AS ProdOrItem
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_PRODUCT_OR_ITEM_DESCRIPTION_STG (
                                    RECEIPT_HEADER_STG_REC_ID
                                    ,RECEIPT_LINE_STG_REC_ID

                                    ,PRODUCT_CHARACTERISTIC_CODE
                                    ,AGENCY_QUALIFIER_CODE
                                    ,PRODUCT_DESCRIPTION_CODE
                                    ,PRODUCT_DESCRIPTION
                                    ,SURFACE_LAYER_POSITION_CODE
                                    ,SOURCE_SUBQUALIFIER
                                    ,YES_OR_NO_RESPONSE
                                    ,LANGUAGE_CODE

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                ) VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID
                                    ,l_v_RECEIPT_LINES_REC_ID

                                    ,PROD_OR_ITEM.PRODUCT_CHARACTERISTIC_CODE
                                    ,PROD_OR_ITEM.AGENCY_QUALIFIER_CODE
                                    ,PROD_OR_ITEM.PRODUCT_DESCRIPTION_CODE
                                    ,PROD_OR_ITEM.PRODUCT_DESCRIPTION
                                    ,PROD_OR_ITEM.SURFACE_LAYER_POSITION_CODE
                                    ,PROD_OR_ITEM.SOURCE_SUBQUALIFIER
                                    ,PROD_OR_ITEM.YES_OR_NO_RESPONSE
                                    ,PROD_OR_ITEM.LANGUAGE_CODE

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                        END LOOP;
                        COMMIT;

                        FOR PHYSICAL_DETAILS IN ( SELECT
                                PhysicalDetails.PackQualifier               AS PACK_QUALIFIER
                                ,PhysicalDetails.PackValue                  AS PACK_VALUE
                                ,PhysicalDetails.PackSize                   AS PACK_SIZE
                                ,PhysicalDetails.PackUOM                    AS PACK_UOM
                                ,PhysicalDetails.PackingMedium              AS PACKING_MEDIUM
                                ,PhysicalDetails.PackingMaterial            AS PACKING_MATERIAL
                                ,PhysicalDetails.WeightQualifier            AS WEIGHT_QUALIFIER
                                ,PhysicalDetails.PackWeight                 AS PACK_WEIGHT
                                ,PhysicalDetails.PackWeightUOM              AS PACK_WEIGHT_UOM
                                ,PhysicalDetails.PackVolume                 AS PACK_VOLUME
                                ,PhysicalDetails.PackVolumeUOM              AS PACK_VOLUME_UOM
                                ,PhysicalDetails.PackLength                 AS PACK_LENGTH
                                ,PhysicalDetails.PackWidth                  AS PACK_WIDTH
                                ,PhysicalDetails.PackHeight                 AS PACK_HEIGHT
                                ,PhysicalDetails.DimensionUOM               AS DIMENSION_UOM
                                ,PhysicalDetails.Description                AS DESCRIPTION
                                ,PhysicalDetails.SurfaceLayerPositionCode   AS SURFACE_LAYER_POSITION_CODE
                                ,PhysicalDetails.AssignedID                 AS ASSIGNED_ID
                            FROM XMLTABLE('/LineItem/PhysicalDetails' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    PackQualifier               VARCHAR2(200) PATH 'PackQualifier'
                                    ,PackValue                  VARCHAR2(200) PATH 'PackValue'
                                    ,PackSize                   VARCHAR2(200) PATH 'PackSize'
                                    ,PackUOM                    VARCHAR2(200) PATH 'PackUOM'
                                    ,PackingMedium              VARCHAR2(200) PATH 'PackingMedium'
                                    ,PackingMaterial            VARCHAR2(200) PATH 'PackingMaterial'
                                    ,WeightQualifier            VARCHAR2(200) PATH 'WeightQualifier'
                                    ,PackWeight                 VARCHAR2(200) PATH 'PackWeight'
                                    ,PackWeightUOM              VARCHAR2(200) PATH 'PackWeightUOM'
                                    ,PackVolume                 VARCHAR2(200) PATH 'PackVolume'
                                    ,PackVolumeUOM                 VARCHAR2(200) PATH 'PackVolumeUOM'
                                    ,PackLength                 VARCHAR2(200) PATH 'PackLength'
                                    ,PackWidth                  VARCHAR2(200) PATH 'PackWidth'
                                    ,PackHeight                 VARCHAR2(200) PATH 'PackHeight'
                                    ,DimensionUOM               VARCHAR2(200) PATH 'DimensionUOM'
                                    ,Description                VARCHAR2(200) PATH 'Description'
                                    ,SurfaceLayerPositionCode   VARCHAR2(200) PATH 'SurfaceLayerPositionCode'
                                    ,AssignedID                 VARCHAR2(200) PATH 'AssignedID'
                            ) AS PhysicalDetails
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_PHYSICAL_DETAILS_STG (
                                    RECEIPT_HEADER_STG_REC_ID
                                    ,RECEIPT_LINE_STG_REC_ID

                                    ,PACK_QUALIFIER
                                    ,PACK_VALUE
                                    ,PACK_SIZE
                                    ,PACK_UOM
                                    ,PACKING_MEDIUM
                                    ,PACKING_MATERIAL
                                    ,WEIGHT_QUALIFIER
                                    ,PACK_WEIGHT
                                    ,PACK_WEIGHT_UOM
                                    ,PACK_VOLUME
                                    ,PACK_VOLUME_UOM
                                    ,PACK_LENGTH
                                    ,PACK_WIDTH
                                    ,PACK_HEIGHT
                                    ,DIMENSION_UOM
                                    ,DESCRIPTION
                                    ,SURFACE_LAYER_POSITION_CODE
	                                ,ASSIGNED_ID

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID
                                    ,l_v_RECEIPT_LINES_REC_ID

                                    ,PHYSICAL_DETAILS.PACK_QUALIFIER
                                    ,PHYSICAL_DETAILS.PACK_VALUE
                                    ,PHYSICAL_DETAILS.PACK_SIZE
                                    ,PHYSICAL_DETAILS.PACK_UOM
                                    ,PHYSICAL_DETAILS.PACKING_MEDIUM
                                    ,PHYSICAL_DETAILS.PACKING_MATERIAL
                                    ,PHYSICAL_DETAILS.WEIGHT_QUALIFIER
                                    ,PHYSICAL_DETAILS.PACK_WEIGHT
                                    ,PHYSICAL_DETAILS.PACK_WEIGHT_UOM
                                    ,PHYSICAL_DETAILS.PACK_VOLUME
                                    ,PHYSICAL_DETAILS.PACK_VOLUME_UOM
                                    ,PHYSICAL_DETAILS.PACK_LENGTH
                                    ,PHYSICAL_DETAILS.PACK_WIDTH
                                    ,PHYSICAL_DETAILS.PACK_HEIGHT
                                    ,PHYSICAL_DETAILS.DIMENSION_UOM
                                    ,PHYSICAL_DETAILS.DESCRIPTION
                                    ,PHYSICAL_DETAILS.SURFACE_LAYER_POSITION_CODE
	                                ,PHYSICAL_DETAILS.ASSIGNED_ID

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                        END LOOP;
                        COMMIT;

                        FOR L_REFERENCE IN ( SELECT
                                L_References.ReferenceQual              AS REFERENCE_QUAL
                                ,L_References.ReferenceID               AS REFERENCE_ID
                                ,L_References.Description               AS DESCRIPTION
                                ,L_References."Date"                    AS HR_DATE
                                ,L_References."Time"                    AS HR_TIME
                                ,L_References.ReferenceQual2            AS REFERENCE_QUAL2
                                ,L_References.ReferenceID2              AS REFERENCE_ID2
                            FROM XMLTABLE('/LineItem/References' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    ReferenceQual                       VARCHAR2(200) PATH 'ReferenceQual'
                                    ,ReferenceID                        VARCHAR2(200) PATH 'ReferenceID'
                                    ,Description                        VARCHAR2(200) PATH 'Description'
                                    ,"Date"                             VARCHAR2(200) PATH 'Date'
                                    ,"Time"                             VARCHAR2(200) PATH 'Time'
                                    ,ReferenceQual2                     VARCHAR2(200) PATH '/ReferenceIDs/ReferenceQual'
                                    ,ReferenceID2                       VARCHAR2(200) PATH '/ReferenceIDs/ReferenceID'
                            ) AS L_References
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_REFERENCES_STG (
                                    RECEIPT_HEADER_STG_REC_ID
                                    ,RECEIPT_LINE_STG_REC_ID

                                    ,REFERENCE_QUAL
                                    ,REFERENCE_ID
                                    ,DESCRIPTION
                                    ,HR_DATE
                                    ,HR_TIME
                                    ,REFERENCE_QUAL2
                                    ,REFERENCE_ID2

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID
                                    ,l_v_RECEIPT_LINES_REC_ID

                                    ,L_REFERENCE.REFERENCE_QUAL
                                    ,L_REFERENCE.REFERENCE_ID
                                    ,L_REFERENCE.DESCRIPTION
                                    ,L_REFERENCE.HR_DATE
                                    ,L_REFERENCE.HR_TIME
                                    ,L_REFERENCE.REFERENCE_QUAL2
                                    ,L_REFERENCE.REFERENCE_ID2

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                        END LOOP;
                        COMMIT;

                        FOR L_NOTES IN ( SELECT
                                L_Notes.NoteCode                AS NOTE_CODE
                                ,L_Notes.Note                   AS NOTE
                                ,L_Notes.LanguageCode           AS LANGUAGE_CODE
                            FROM XMLTABLE('/LineItem/Notes' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    NoteCode                    VARCHAR2(200) PATH 'NoteCode'
                                    ,Note                       VARCHAR2(200) PATH 'Note'
                                    ,LanguageCode               VARCHAR2(200) PATH 'LanguageCode'
                            ) AS L_Notes
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_NOTES_STG (
                                    RECEIPT_HEADER_STG_REC_ID
                                    ,RECEIPT_LINE_STG_REC_ID

                                    ,NOTE_CODE
                                    ,NOTE
                                    ,LANGUAGE_CODE

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID
                                    ,l_v_RECEIPT_LINES_REC_ID

                                    ,L_NOTES.NOTE_CODE
                                    ,L_NOTES.NOTE
                                    ,L_NOTES.LANGUAGE_CODE

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                        END LOOP;
                        COMMIT;

                        FOR L_REGULATORY_COMPLIANCE IN ( SELECT
                                L_RegulatoryCompliances.RegulatoryComplianceQual      AS REGULATORY_COMPLIANCE_QUAL
                                ,L_RegulatoryCompliances.YesOrNoResponse              AS YES_OR_NO_RESPONSE
                                ,L_RegulatoryCompliances.RegulatoryComplianceID       AS REGULATORY_COMPLIANCE_ID
                                ,L_RegulatoryCompliances.RegulatoryAgency             AS REGULATORY_AGENCY
                                ,L_RegulatoryCompliances.Description                  AS DESCRIPTION
                            FROM XMLTABLE('/LineItem/RegulatoryCompliances' PASSING l_v_XML_RECEIPT_LINE
                                COLUMNS
                                    RegulatoryComplianceQual            VARCHAR2(200) PATH 'RegulatoryComplianceQual'
                                    ,YesOrNoResponse                    VARCHAR2(200) PATH 'YesOrNoResponse'
                                    ,RegulatoryComplianceID             VARCHAR2(200) PATH 'RegulatoryComplianceID'
                                    ,RegulatoryAgency                   VARCHAR2(200) PATH 'RegulatoryAgency'
                                    ,Description                        VARCHAR2(200) PATH 'Description'
                            ) AS L_RegulatoryCompliances
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_944_L_REGULATORY_COMPLIANCES_STG (
                                    RECEIPT_HEADER_STG_REC_ID
                                    ,RECEIPT_LINE_STG_REC_ID

                                    ,REGULATORY_COMPLIANCE_QUAL
                                    ,YES_OR_NO_RESPONSE
                                    ,REGULATORY_COMPLIANCE_ID
                                    ,REGULATORY_AGENCY
                                    ,DESCRIPTION

                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_RECEIPT_HEADER_REC_ID
                                    ,l_v_RECEIPT_LINES_REC_ID

                                    ,L_REGULATORY_COMPLIANCE.REGULATORY_COMPLIANCE_QUAL
                                    ,L_REGULATORY_COMPLIANCE.YES_OR_NO_RESPONSE
                                    ,L_REGULATORY_COMPLIANCE.REGULATORY_COMPLIANCE_ID
                                    ,L_REGULATORY_COMPLIANCE.REGULATORY_AGENCY
                                    ,L_REGULATORY_COMPLIANCE.DESCRIPTION

                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                        END LOOP;
                        COMMIT;

                    END LOOP;
                    COMMIT;
                END LOOP;
                COMMIT;

            FOR SUMMARY IN ( SELECT
                        Summary.QtyReceived                 AS QTY_RECEIVED
                        ,Summary.UnitsShipped               AS UNITS_SHIPPED
                        ,Summary.QtyDamageOrOnHold          AS QTY_DAMAGE_OR_ON_HOLD
                        ,Summary.LadingQuantityReceived     AS LADING_QUANTITY_RECEIVED
                        ,Summary.LadingQuantity             AS LADING_QUANTITY
                    FROM XMLTABLE('/WarehouseTransferReceiptAdvice/Summary' PASSING XML_DATA
                        COLUMNS
                            QtyReceived                 VARCHAR2(200) PATH 'QtyReceived'
                            ,UnitsShipped               VARCHAR2(200) PATH 'UnitsShipped'
                            ,QtyDamageOrOnHold          VARCHAR2(200) PATH 'QtyDamageOrOnHold'
                            ,LadingQuantityReceived     VARCHAR2(200) PATH 'LadingQuantityReceived'
                            ,LadingQuantity             VARCHAR2(200) PATH 'LadingQuantity'
                ) AS Summary
            ) LOOP
                INSERT INTO XXEDI_SCM_INB_944_SUMMARY_STG (
                        RECEIPT_HEADER_STG_REC_ID

                        ,QTY_RECEIVED
                        ,UNITS_SHIPPED
                        ,QTY_DAMAGE_OR_ON_HOLD
                        ,LADING_QUANTITY_RECEIVED
                        ,LADING_QUANTITY

                        ,OIC_INSTANCE_ID
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                    )
                    VALUES (
                        l_v_RECEIPT_HEADER_REC_ID

                        ,SUMMARY.QTY_RECEIVED
                        ,SUMMARY.UNITS_SHIPPED
                        ,SUMMARY.QTY_DAMAGE_OR_ON_HOLD
                        ,SUMMARY.LADING_QUANTITY_RECEIVED
                        ,SUMMARY.LADING_QUANTITY

                        ,I_P_OIC_ID
                        ,'OIC'
                        ,'OIC'
                );
            END LOOP;
            COMMIT;

            EXCEPTION
                WHEN OTHERS THEN
                    l_v_ERROR_CODE := 'Error when parsing the XML';
                    l_v_ERROR_MESSAGE := Substr( l_v_ERROR_CODE || '. Details: ' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    IF l_v_RECEIPT_HEADER_REC_ID IS NOT NULL THEN
                        DELETE FROM XXEDI_SCM_INB_944_HEADER_STG WHERE RECEIPT_HEADER_STG_REC_ID = l_v_RECEIPT_HEADER_REC_ID;
                        COMMIT;
                    END IF;
                    UPDATE XXEDI_SCM_INB_944_XML_DATA_STG
                        SET PROCESSED_FLAG = 'E' , ERROR_CODE  = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    ' || l_v_ERROR_MESSAGE || CHR(10) || 'File_Name: "' || XML_RAW_DATA_REC.FILE_NAME || '"';
                    RAISE;
            END;
            --

            -- update processed flag to 'Y' for representing that the record was completely processed
            UPDATE XXEDI_SCM_INB_944_XML_DATA_STG SET PROCESSED_FLAG = 'Y' WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            COMMIT;


            -- -- Delete the record from the XML STAGING TABLE after processing
            -- DELETE FROM XXEDI_SCM_INB_944_XML_DATA_STG WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            -- Delete records from the XML STAGING TABLE older than v_days_to_keep_file_XML_data
            DELETE FROM XXEDI_SCM_INB_944_XML_DATA_STG WHERE TRUNC(SYSDATE) - TRUNC(CREATION_DATE) > v_days_to_keep_file_XML_data AND DOC_TYPE = g_v_EDI_944_doc_type;
            COMMIT;

            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || '    XML data has been successfully inserted into the staging tables.'
                              || CHR(10) || CHR(10) || 'PARSE_XML_INTO_STG Procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN

                O_P_RESPONSE := O_P_RESPONSE 
                || CHR(10) || 'Error when parsing the XML.'
                || CHR(10) || 'Details: '  || SQLCODE || ' | ' || SQLERRM
                || CHR(10) || 'trace:   '  || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE 
                || CHR(10) || 'stack:   '  || DBMS_UTILITY.FORMAT_ERROR_STACK;



                O_P_STATUS := 'ERROR';

    END PARSE_XML_INTO_STG;

    PROCEDURE PROCESS_DATA_INTO_INTF (
            I_P_OIC_ID           IN VARCHAR2
            ,O_P_RESPONSE        OUT CLOB
            ,O_P_STATUS          OUT VARCHAR2

        )
        IS
            l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS NUMBER := 10;

            l_v_ERROR_CODE       VARCHAR2(64);
            l_v_ERROR_MESSAGE    VARCHAR2(4000);
        BEGIN            
            O_P_RESPONSE := 'PROCESS_DATA_INTO_INTF procedure started.' || CHR(10) || CHR(10);

            BEGIN -- block to handle reprocessing of records
                FOR receipt_intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                    SELECT
                            ROWNUM
                            ,INTF.RECEIPT_HEADER_INTF_REC_ID
                            ,INTF.RECEIPT_HEADER_STG_REC_ID
                            ,INTF.CREATION_DATE
                            ,INTF.LAST_UPDATE_DATE
                            ,INTF.PROCESSED_FLAG
                            ,INTF.ERROR_CODE
                            ,INTF.ERROR_MESSAGE
                            ,INTF.OIC_INSTANCE_ID
                        FROM XXEDI_SCM_INB_944_HEADER_INTF INTF
                        LEFT JOIN   XXEDI_SCM_INB_944_HEADER_STG    STG ON INTF.RECEIPT_HEADER_STG_REC_ID = STG.RECEIPT_HEADER_STG_REC_ID
                        WHERE
                         INTF.PROCESSED_FLAG = 'E'
                         AND TRUNC(SYSDATE) - TRUNC(STG.CREATION_DATE) <= l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS
                         --AND ERROR_CODE = g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE
                         AND STG.SHIPMENT_IDENTIFICATION NOT IN (  -- ignore if there is a new dropped file with same Shipment
                                SELECT STG_B.SHIPMENT_IDENTIFICATION
                                FROM XXEDI_SCM_INB_944_HEADER_STG STG_B
                                WHERE PROCESSED_FLAG  =  'N'
                            )
                            AND STG.SHIPMENT_IDENTIFICATION NOT IN (  -- ignore in case there is a shipment that got correctly processed
                                SELECT STG_B.SHIPMENT_IDENTIFICATION
                                FROM
                                                XXEDI_SCM_INB_944_HEADER_INTF   INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_944_HEADER_STG    STG_B   ON STG_B.RECEIPT_HEADER_STG_REC_ID = INTF_B.RECEIPT_HEADER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'Y'
                                    AND STG_B.SHIPMENT_IDENTIFICATION  =  STG.SHIPMENT_IDENTIFICATION
                            )
                            AND INTF.RECEIPT_HEADER_INTF_REC_ID = ( -- select only the newest record in error for the same shipment
                                SELECT MAX(INTF_B.RECEIPT_HEADER_INTF_REC_ID)
                                FROM
                                                XXEDI_SCM_INB_944_HEADER_INTF    INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_944_HEADER_STG     STG_B   ON STG_B.RECEIPT_HEADER_STG_REC_ID = INTF_B.RECEIPT_HEADER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'E'
                                    AND STG_B.SHIPMENT_IDENTIFICATION  =  STG.SHIPMENT_IDENTIFICATION
                            )
                ) LOOP
                    UPDATE XXEDI_SCM_INB_944_HEADER_STG SET PROCESSED_FLAG = 'R', ERROR_CODE = NULL, ERROR_MESSAGE = NULL WHERE RECEIPT_HEADER_STG_REC_ID = receipt_intf_rec.RECEIPT_HEADER_STG_REC_ID;
                    UPDATE XXEDI_SCM_INB_944_HEADER_INTF SET PROCESSED_FLAG = 'D'                                          WHERE RECEIPT_HEADER_INTF_REC_ID = receipt_intf_rec.RECEIPT_HEADER_INTF_REC_ID;
                END LOOP;
                COMMIT;
            END;

            FOR STG_REC IN (
                SELECT *
                FROM XXEDI_SCM_INB_944_HEADER_STG
                WHERE PROCESSED_FLAG  IN ('N', 'R') -- N = Not processed, R = Reprocess
            ) LOOP
                O_P_RESPONSE := O_P_RESPONSE || '    Processing RECEIPT_HEADER_STG_REC_ID: ' || TO_CHAR(STG_REC.RECEIPT_HEADER_STG_REC_ID, '999999') || ' from the file name: "' || STG_REC.FILE_NAME || '" | Status: ';

                DECLARE --                
                    l_v_RECEIPT_HEADER_INTF_REC_ID NUMBER;
                    l_v_INTF_ERROR_CODE             VARCHAR2(64);
                    l_v_INTF_ERROR_MESSAGE          VARCHAR2(4000);

                    CURSOR RECEIPT_HEADER_CUR IS
                        SELECT
                            HEADER.RECEIPT_HEADER_STG_REC_ID  
                            ,HEADER.XML_CONTENT_REC_ID         --*
                            ,HEADER.FILE_NAME                  --*
                            ,HEADER.PROCESSED_FLAG
                            ,HEADER.ERROR_CODE
                            ,HEADER.ERROR_MESSAGE
                            ,HEADER.OIC_INSTANCE_ID

                            ,HEADER.SHIPMENT_IDENTIFICATION AS SHIPMENT_NUMBER
                            -- ,TO_CHAR(DATES.H_DATE, 'YYYY/MM/DD') || ' 00:00:00' AS TRANSACTION_DATE
                            ,NVL(DATES.H_DATE, TO_CHAR(SYSDATE, 'YYYY-MM-DD')) || 'T08:00:00+00:00' AS TRANSACTION_DATE
                            --,TO_CHAR(CAST(TO_DATE(DATES.H_DATE, 'YYYY-MM-DD') AS TIMESTAMP) AT TIME ZONE 'US/Central', 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') as TRANSACTION_DATE
                            ,NVL(SF_IOP.ORGANIZATION_CODE, SF_IOP2.ORGANIZATION_CODE) AS SF_ORGANIZATION_CODE
                            ,NVL(ST_IOP.ORGANIZATION_CODE, ST_IOP2.ORGANIZATION_CODE) AS ST_ORGANIZATION_CODE
                        FROM
                            XXEDI_SCM_INB_944_HEADER_STG               HEADER
                            LEFT JOIN WSH_DELIVERY_EXTRACT_PVO_INTF    WND         ON  HEADER.SHIPMENT_IDENTIFICATION = WND.DELIVERY_NAME
                            LEFT JOIN XXEDI_SCM_INB_944_H_DATES_STG    DATES       ON       DATES.RECEIPT_HEADER_STG_REC_ID  =  HEADER.RECEIPT_HEADER_STG_REC_ID  AND  DATES.DATE_TIME_QUALIFIER     = '02'
                            LEFT JOIN XXEDI_SCM_INB_944_H_ADDRESS_STG  ST_ADDRESS  ON  ST_ADDRESS.RECEIPT_HEADER_STG_REC_ID  =  HEADER.RECEIPT_HEADER_STG_REC_ID  AND  ST_ADDRESS.ADDRESS_TYPE_CODE  = 'ST'
                            LEFT JOIN XXEDI_SCM_INB_944_H_ADDRESS_STG  SF_ADDRESS  ON  SF_ADDRESS.RECEIPT_HEADER_STG_REC_ID  =  HEADER.RECEIPT_HEADER_STG_REC_ID  AND  SF_ADDRESS.ADDRESS_TYPE_CODE  = 'SF'
                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF      ST_IOP      ON      ST_IOP.ATTRIBUTE2                 =  HEADER.TRADING_PARTNER_ID         AND  ST_IOP.ATTRIBUTE1             = ST_ADDRESS.ADDRESS_LOCATION_NUMBER 
                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF      SF_IOP      ON      SF_IOP.ATTRIBUTE2                 =  HEADER.TRADING_PARTNER_ID         AND  SF_IOP.ATTRIBUTE1             = SF_ADDRESS.ADDRESS_LOCATION_NUMBER
                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF      ST_IOP2     ON      ST_IOP2.ORGANIZATION_ID           =  WND.ORGANIZATION_ID               AND  ST_IOP2.ATTRIBUTE1            = ST_ADDRESS.ADDRESS_LOCATION_NUMBER
                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF      SF_IOP2     ON      SF_IOP2.ORGANIZATION_ID           =  WND.ORGANIZATION_ID               AND  SF_IOP2.ATTRIBUTE1            = SF_ADDRESS.ADDRESS_LOCATION_NUMBER
                        WHERE
                            HEADER.RECEIPT_HEADER_STG_REC_ID = STG_REC.RECEIPT_HEADER_STG_REC_ID

                    ;--! end of Header Cursor




                    CURSOR RECEIPT_LINES_CUR IS
                        SELECT
                            HEADER.RECEIPT_HEADER_STG_REC_ID  --*
                            ,LINES.RECEIPT_LINE_STG_REC_ID
                            -- ,HEADER.XML_CONTENT_REC_ID         --*
                            -- ,HEADER.PROCESSED_FLAG
                            -- ,HEADER.ERROR_CODE
                            -- ,HEADER.ERROR_MESSAGE

                            ,LINES.VENDOR_PART_NUMBER                          AS  ITEM_NUMBER
                            ,LINES.SHIP_QTY                                    AS  QUANTITY
                            ,CASE
                                WHEN LINES.SHIP_QTY_UOM = 'CA ' THEN 'CS'
                                WHEN LINES.SHIP_QTY_UOM = 'LB ' THEN 'LBS'
                                ELSE LINES.SHIP_QTY_UOM
                            END                                                AS UOM_CODE
                            ,COALESCE(
                                L_REF.REFERENCE_ID,
                                L_REF2.REFERENCE_ID2,
                                LINES.WAREHOUSE_LOT_ID
                            )                                                  AS  LOT_NUMBER
                            ,HEADER.OIC_INSTANCE_ID
                        FROM
                            XXEDI_SCM_INB_944_HEADER_STG                  HEADER
                            LEFT JOIN XXEDI_SCM_INB_944_LINES_STG         LINES   ON   LINES.RECEIPT_HEADER_STG_REC_ID   =  HEADER.RECEIPT_HEADER_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_944_L_REFERENCES_STG  L_REF   ON   L_REF.RECEIPT_LINE_STG_REC_ID     =   LINES.RECEIPT_LINE_STG_REC_ID      AND  L_REF.REFERENCE_QUAL   = 'LT'
                            LEFT JOIN XXEDI_SCM_INB_944_L_REFERENCES_STG  L_REF2  ON  L_REF2.RECEIPT_LINE_STG_REC_ID     =   LINES.RECEIPT_LINE_STG_REC_ID      AND L_REF2.REFERENCE_QUAL2  = 'LT'
                        WHERE
                            HEADER.RECEIPT_HEADER_STG_REC_ID  =  STG_REC.RECEIPT_HEADER_STG_REC_ID

                    ;--! end of Shipment Lines Cursor

                BEGIN -- 

                    UPDATE XXEDI_SCM_INB_944_HEADER_STG SET PROCESSED_FLAG = 'P' WHERE RECEIPT_HEADER_STG_REC_ID = STG_REC.RECEIPT_HEADER_STG_REC_ID;
                    COMMIT;

                    FOR HEADER_REC IN RECEIPT_HEADER_CUR LOOP

                        BEGIN --* PREVALIDATION HEADER
                            l_v_INTF_ERROR_CODE := NULL;
                            l_v_INTF_ERROR_MESSAGE := NULL;
                            -- validation 1: check if the XML contains all the mandatory fields START
                            IF HEADER_REC.SHIPMENT_NUMBER       IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 944 XML Mandatory field SHIPMENT_NUMBER is missing'       || '  |  '; END IF;
                            IF HEADER_REC.TRANSACTION_DATE      IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 944 XML Mandatory field TRANSACTION_DATE is missing.'     || '  |  '; END IF;
                            -- IF HEADER_REC.SF_ORGANIZATION_CODE  IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 944 XML Mandatory field SF_ORGANIZATION_CODE is missing.' || '  |  '; END IF;
                            -- IF HEADER_REC.ST_ORGANIZATION_CODE  IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 944 XML Mandatory field ST_ORGANIZATION_CODE is missing.' || '  |  '; END IF;
                            -- validation 1: check if the XML contains all the mandatory fields END

                        END;

                        IF l_v_INTF_ERROR_MESSAGE IS NOT NULL THEN
                            l_v_INTF_ERROR_CODE    := G_V_PRE_VALIDATION_ERROR_CODE;
                        END IF;

                        l_v_RECEIPT_HEADER_INTF_REC_ID := NULL;
                        INSERT INTO XXEDI_SCM_INB_944_HEADER_INTF (
                            RECEIPT_HEADER_STG_REC_ID
                            ,FILE_NAME

                            --,RECEIPT_SOURCE_CODE
                            --,ASN_TYPE
                            ,SHIPMENT_NUMBER
                            ,TRANSACTION_DATE
                            --,FROM_ORGANIZATION_CODE
                            --,ORGANIZATION_CODE
                            --,EMPLOYEE_NAME 

                            ,ERROR_CODE
                            ,ERROR_MESSAGE
                            ,OIC_INSTANCE_ID
                        ) VALUES (
                            HEADER_REC.RECEIPT_HEADER_STG_REC_ID
                            ,HEADER_REC.FILE_NAME

                            ,HEADER_REC.SHIPMENT_NUMBER
                            ,HEADER_REC.TRANSACTION_DATE

                            ,l_v_INTF_ERROR_CODE
                            ,l_v_INTF_ERROR_MESSAGE
                            ,I_P_OIC_ID
                        ) RETURNING RECEIPT_HEADER_INTF_REC_ID INTO l_v_RECEIPT_HEADER_INTF_REC_ID;
                        COMMIT;

                        FOR RECEIPT_LINE_REC IN RECEIPT_LINES_CUR LOOP

                            BEGIN --* PREVALIDATION SHIPMENT_LINES
                                l_v_INTF_ERROR_CODE := NULL;
                                l_v_INTF_ERROR_MESSAGE := NULL;

                                -- validation 1: check if the XML contains all the mandatory fields start
                                    IF RECEIPT_LINE_REC.ITEM_NUMBER    IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'Mandatory field ITEM_NUMBER is missing.'  || '  |  ';  END IF;
                                    IF RECEIPT_LINE_REC.QUANTITY       IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'Mandatory field QUANTITY is missing.'     || '  |  ';  END IF;
                                    IF RECEIPT_LINE_REC.UOM_CODE       IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'Mandatory field UOM_CODE is missing.'     || '  |  ';  END IF;
                                    IF RECEIPT_LINE_REC.LOT_NUMBER     IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'Mandatory field LOT_NUMBER is missing.'   || '  |  ';  END IF;
                                -- validation 1: check if the XML contains all the mandatory fields end




                                IF l_v_INTF_ERROR_MESSAGE IS NOT NULL AND l_v_INTF_ERROR_CODE IS NULL THEN
                                    l_v_INTF_ERROR_CODE    := G_V_PRE_VALIDATION_ERROR_CODE;
                                END IF;
                            END;

                            INSERT INTO XXEDI_SCM_INB_944_LINES_INTF (
                                RECEIPT_HEADER_INTF_REC_ID
                                ,RECEIPT_LINE_STG_REC_ID
                                ,TRANSACTION_DATE
                                ,ORGANIZATION_CODE
                                ,ITEM_NUMBER
                                ,DOCUMENT_NUMBER
                                --,DOCUMENT_LINE_NUMBER
                                ,QUANTITY
                                ,UOM_CODE
                                ,FROM_ORGANIZATION_CODE
                                ,LOT_NUMBER
                                ,TRANSACTION_QUANTITY
                                ,ERROR_CODE
                                ,ERROR_MESSAGE
                                ,OIC_INSTANCE_ID
                            ) VALUES (
                                l_v_RECEIPT_HEADER_INTF_REC_ID               -- RECEIPT_HEADER_INTF_REC_ID
                                ,RECEIPT_LINE_REC.RECEIPT_LINE_STG_REC_ID    -- ,RECEIPT_LINE_STG_REC_ID
                                ,HEADER_REC.TRANSACTION_DATE                 -- ,TRANSACTION_DATE
                                ,HEADER_REC.ST_ORGANIZATION_CODE             -- ,ORGANIZATION_CODE
                                ,RECEIPT_LINE_REC.ITEM_NUMBER                -- ,ITEM_NUMBER
                                ,HEADER_REC.SHIPMENT_NUMBER                  -- ,DOCUMENT_NUMBER
                                --,HEADER_REC.TRANSACTION_DATE               -- --,DOCUMENT_LINE_NUMBER
                                ,RECEIPT_LINE_REC.QUANTITY                   -- ,QUANTITY
                                ,RECEIPT_LINE_REC.UOM_CODE                   -- ,UOM_CODE
                                ,HEADER_REC.SF_ORGANIZATION_CODE             -- ,FROM_ORGANIZATION_CODE
                                ,RECEIPT_LINE_REC.LOT_NUMBER                 -- ,LOT_NUMBER
                                ,RECEIPT_LINE_REC.QUANTITY                   -- ,TRANSACTION_QUANTITY
                                ,l_v_INTF_ERROR_CODE                         -- ,ERROR_CODE
                                ,l_v_INTF_ERROR_MESSAGE                      -- ,ERROR_MESSAGE
                                ,I_P_OIC_ID                                  -- ,OIC_INSTANCE_ID
                            );
                            COMMIT;
                        END LOOP;
                    END LOOP;


                    UPDATE XXEDI_SCM_INB_944_HEADER_STG SET PROCESSED_FLAG = 'Y' WHERE RECEIPT_HEADER_STG_REC_ID = STG_REC.RECEIPT_HEADER_STG_REC_ID;
                    COMMIT;

                EXCEPTION
                    WHEN OTHERS THEN  
                        L_V_ERROR_CODE := 'PROCESS_DATA_INTO_INTF procedure error';
                        L_V_ERROR_MESSAGE := Substr(SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                        UPDATE XXEDI_SCM_INB_944_HEADER_STG SET PROCESSED_FLAG = 'E', ERROR_CODE = L_V_ERROR_CODE, ERROR_MESSAGE = L_V_ERROR_MESSAGE
                            WHERE RECEIPT_HEADER_STG_REC_ID = STG_REC.RECEIPT_HEADER_STG_REC_ID;
                        IF l_v_RECEIPT_HEADER_INTF_REC_ID IS NOT NULL THEN
                            DELETE FROM XXEDI_SCM_INB_944_HEADER_INTF WHERE RECEIPT_HEADER_INTF_REC_ID = l_v_RECEIPT_HEADER_INTF_REC_ID;
                        END IF;
                        COMMIT;
                        O_P_RESPONSE := O_P_RESPONSE || 'ERROR' || CHR(10) || CHR(10) || L_V_ERROR_MESSAGE || CHR(10)  || 'RECEIPT_HEADER_STG_REC_ID: ' || STG_REC.RECEIPT_HEADER_STG_REC_ID || CHR(10) || 'File name: ' || STG_REC.FILE_NAME || CHR(10) || CHR(10);
                        RAISE;
                END;
                O_P_RESPONSE := O_P_RESPONSE || 'Success.' || CHR(10);
            END LOOP;

            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) || 'PROCESS_DATA_INTO_INTF procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                O_P_STATUS := 'ERROR';
                O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) || 'PROCESS_DATA_INTO_INTF procedure ended with error.' || CHR(10) || CHR(10) || 'Error: ' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
                RAISE;
    END PROCESS_DATA_INTO_INTF;



    PROCEDURE UPDATE_FLAG_PROCESSED (
            oic_id IN VARCHAR2,
            key_id IN VARCHAR2
        ) IS
        BEGIN
            UPDATE XXEDI_SCM_INB_944_HEADER_INTF
            SET
                processed_flag = 'Y',
                error_code = NULL,
                oic_instance_id = oic_id,
                error_message = NULL,
                last_update_date = sysdate,
                last_update_by_name = 'OIC'
            WHERE
                    processed_flag = 'P'
                AND RECEIPT_HEADER_INTF_REC_ID = key_id;

            UPDATE XXEDI_SCM_INB_944_LINES_INTF
            SET
                processed_flag = 'Y',
                error_code = NULL,
                oic_instance_id = oic_id,
                error_message = NULL,
                last_update_date = sysdate,
                last_update_by_name = 'OIC'
            WHERE
                    processed_flag = 'P'
                AND RECEIPT_LINES_INTF_REC_ID = key_id;

            COMMIT;
    END UPDATE_FLAG_PROCESSED;


    PROCEDURE UPDATE_FLAGS (
             I_P_PK           IN NUMBER       -- Single PK
            ,I_P_KEYS         IN VARCHAR2     -- Multiple PKs. String with Primary Keys separeted by commas. like: '1,4,15'
            ,I_P_TABLE_NAME   IN VARCHAR2
            ,I_P_FLAG_VALUE   IN VARCHAR2
            ,I_P_ERROR_CODE   IN VARCHAR2
            ,I_P_ERROR_TEXT   IN CLOB
            ,I_P_OIC_ID       IN VARCHAR2
            ,O_P_RESPONSE     OUT CLOB
            -- ,O_P_STATUS       OUT VARCHAR2
        )
        IS
        v_PK                NUMBER;
        v_count             NUMBER;

        TYPE NumberTable IS TABLE OF NUMBER;
        v_parsed_keys NumberTable := NumberTable();
        v_primary_key NUMBER;
        sql_code_update VARCHAR2(4000);

        v_updated_records_count    NUMBER := 0;
        l_v_error_code             VARCHAR2(4000);
        l_v_ERROR_MESSAGE          VARCHAR2(4000);

        BEGIN
            O_P_RESPONSE := 'UPDATE_FLAGS PROCEDURE started.' || CHR(10) || CHR(10);
            IF (I_P_PK IS NULL AND I_P_KEYS IS NULL)          THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_PK and I_P_KEYS parameters cannot be NULL at the same time.'); END IF;
            IF (I_P_PK IS NOT NULL AND I_P_KEYS IS NOT NULL)  THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_PK and I_P_KEYS parameters cannot be provided at the same time.'); END IF;            
            IF I_P_FLAG_VALUE NOT IN ( 'Y', 'N', 'E' )        THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_FLAG_VALUE parameter argument must be "Y", "N" or "E".'); END IF; 
            IF I_P_FLAG_VALUE = 'E' AND I_P_ERROR_CODE IS NULL  THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: I_P_FLAG_VALUE is "E" but I_P_ERROR_CODE is NULL. When the flag value is "E" the error code must be provided.'); END IF;

            IF I_P_KEYS IS NOT NULL THEN -- populate v_parsed_keys with the keys from the input string
                DECLARE                
                BEGIN
                    IF NOT REGEXP_LIKE(I_P_KEYS, '^\d+(,\d+)*$') THEN
                        RAISE_APPLICATION_ERROR(-20001, 'Invalid input format. The string must contain only numbers separated by commas with no spaces.Example: "1,2,3,4"');
                    END IF;
                    SELECT TO_NUMBER(TRIM(REGEXP_SUBSTR(I_P_KEYS, '[^,]+', 1, LEVEL))) BULK COLLECT INTO v_parsed_keys
                    FROM dual
                    CONNECT BY REGEXP_SUBSTR(I_P_KEYS, '[^,]+', 1, LEVEL) IS NOT NULL;
                    IF v_parsed_keys.COUNT = 0 THEN
                        RAISE_APPLICATION_ERROR(-20001, 'No valid keys found in the input string.');
                    END IF;
                END;
            ELSE
                v_parsed_keys := NumberTable(I_P_PK);
            END IF;

            l_v_ERROR_CODE     := substr(I_P_ERROR_CODE, 1 , 64 );
            l_v_ERROR_MESSAGE  := substr(I_P_ERROR_TEXT, 1 , 4000 );
            IF I_P_FLAG_VALUE IN ('Y', 'N') THEN
                l_v_ERROR_CODE    := NULL;
                l_v_ERROR_MESSAGE := NULL;
            END IF;



            FOR i IN 1 .. v_parsed_keys.COUNT LOOP
                DECLARE
                    l_v_current_preval_error_code VARCHAR2(64);
                    l_b_new_error_code VARCHAR2(64);
                BEGIN
                    v_PK := v_parsed_keys(i);
                    IF UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_944_HEADER_INTF'      THEN
                        SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_944_HEADER_INTF            WHERE RECEIPT_HEADER_INTF_REC_ID = v_PK;
                        IF v_count = 0 THEN RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_944_HEADER_INTF            for the given PK: ' || v_PK); END IF;
                        IF I_P_FLAG_VALUE = 'E' THEN
                                UPDATE XXEDI_SCM_INB_944_HEADER_INTF            SET
                                    PROCESSED_FLAG      = I_P_FLAG_VALUE
                                    ,ERROR_CODE          = l_v_ERROR_CODE
                                    ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                    ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                    ,LAST_UPDATE_DATE    = SYSDATE
                                    ,LAST_UPDATE_BY_NAME = 'OIC'
                                WHERE RECEIPT_HEADER_INTF_REC_ID = v_PK;
                                UPDATE XXEDI_SCM_INB_944_LINES_INTF SET
                                        PROCESSED_FLAG      = I_P_FLAG_VALUE
                                        ,ERROR_CODE          = 'PARENT_ERROR'
                                        ,ERROR_MESSAGE       = 'Some error occured when processing the record. Check parent table (XXEDI_SCM_INB_944_HEADER_INTF) for details. Error code: ' || l_v_ERROR_CODE
                                        ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                        ,LAST_UPDATE_DATE    = SYSDATE
                                        ,LAST_UPDATE_BY_NAME = 'OIC'
                                WHERE RECEIPT_HEADER_INTF_REC_ID = v_PK AND PROCESSED_FLAG != 'E' AND ERROR_CODE IS NULL;
                        ELSIF I_P_FLAG_VALUE IN ('Y', 'N') THEN
                                UPDATE XXEDI_SCM_INB_944_HEADER_INTF        SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE RECEIPT_HEADER_INTF_REC_ID = v_PK;
                                UPDATE XXEDI_SCM_INB_944_LINES_INTF         SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE RECEIPT_HEADER_INTF_REC_ID = v_PK;
                        END IF;
                    ELSE -- Raise invalid I_P_TABLE_NAME parameter
                        RAISE_APPLICATION_ERROR(-20001, 'The value of the I_P_TABLE_NAME parameter is not valid. Provided value: "' || I_P_TABLE_NAME || '"');
                    END IF;
                    v_updated_records_count := v_updated_records_count + SQL%ROWCOUNT;
                    COMMIT;
                    O_P_RESPONSE := O_P_RESPONSE || CHR(10) || '    ' || I_P_TABLE_NAME || ' updated successfully. PK: ' || v_PK || ' | Flag Value: ' || I_P_FLAG_VALUE;
                END;

            END LOOP;

            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) || 'UPDATE_FLAGS PROCEDURE completed successfully. Number of records updated: ' || v_updated_records_count;
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) 
                                             || CHR(10) || 'An error occurred in the UPDATE_FLAGS procedure.' 
                                             || CHR(10) || 'ROLLBACK executed. Error Details: ' 
                                             || CHR(10) || SQLCODE                                                
                                             || CHR(10) || SQLERRM                                                
                                             || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE  
                                             || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_STACK  ;
                --DBMS_OUTPUT.PUT_LINE(O_P_RESPONSE);
                RAISE;

    END UPDATE_FLAGS;


    PROCEDURE GET_INTERFACE_TABLES_DATA (
            O_P_RESPONSE         OUT CLOB
            ,O_P_JSON            OUT CLOB
            ,O_P_STATUS          OUT VARCHAR2
        )
        IS
        BEGIN
            O_P_RESPONSE := 'GET_INTERFACE_TABLES_DATA procedure started.' || CHR(10) || CHR(10);

       SELECT
                JSON_OBJECT(
                    'Unprocessed_Receipts' VALUE ( 
                        SELECT
                            JSON_ARRAYAGG(
                            JSON_OBJECT(
                                header_intf.RECEIPT_HEADER_INTF_REC_ID
                                ,header_intf.RECEIPT_HEADER_STG_REC_ID
                                ,header_intf.FILE_NAME

                                ,header_intf.RECEIPT_SOURCE_CODE
                                ,header_intf.ASN_TYPE
                                ,header_intf.SHIPMENT_NUMBER
                                ,header_intf.TRANSACTION_DATE
                                ,header_intf.FROM_ORGANIZATION_CODE
                                ,header_intf.ORGANIZATION_CODE
                                ,header_intf.EMPLOYEE_NAME

                                ,header_intf.CREATION_DATE
                                ,header_intf.LAST_UPDATE_DATE
                                ,header_intf.PROCESSED_FLAG
                                ,header_intf.ERROR_CODE
                                ,header_intf.ERROR_MESSAGE
                                ,header_intf.OIC_INSTANCE_ID

                                ,'lines' VALUE (
                                    SELECT
                                        JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            line_intf.RECEIPT_LINES_INTF_REC_ID
                                            ,line_intf.TRANSACTION_TYPE
                                            ,line_intf.AUTO_TRANSACT_CODE
                                            ,line_intf.TRANSACTION_DATE
                                            ,line_intf.SOURCE_DOCUMENT_CODE
                                            ,line_intf.RECEIPT_SOURCE_CODE
                                            ,line_intf.ORGANIZATION_CODE
                                            ,line_intf.ITEM_NUMBER
                                            ,line_intf.DOCUMENT_NUMBER
                                            ,line_intf.DOCUMENT_LINE_NUMBER
                                            ,line_intf.SUBINVENTORY
                                            ,'QUANTITY' VALUE line_intf.TRANSACTION_QUANTITY
                                            ,line_intf.UOM_CODE
                                            ,line_intf.INTERFACE_SOURCE_CODE
                                            ,line_intf.FROM_ORGANIZATION_CODE
                                            ,line_intf.PROCESSED_FLAG
                                            ,line_intf.LOT_NUMBER
                                            ,line_intf.TRANSACTION_QUANTITY
                                            ,line_intf.ERROR_CODE
                                            ,line_intf.ERROR_MESSAGE
                                            -- ,'Lots' VALUE ( SELECT
                                            --         JSON_ARRAYAGG(
                                            --         JSON_OBJECT(

                                            --         RETURNING CLOB )
                                            --         RETURNING CLOB )
                                            --     FROM
                                            --         XXEDI_SCM_INB_944_LINES_INTF line_intf_2
                                            --     WHERE
                                            --             line_intf_2.RECEIPT_HEADER_INTF_REC_ID  = header_intf.RECEIPT_HEADER_INTF_REC_ID
                                            --         AND line_intf_2.ITEM_NUMBER                 = line_intf.ITEM_NUMBER
                                            -- )
                                        RETURNING CLOB )
                                        RETURNING CLOB )
                                    FROM
                                        XXEDI_SCM_INB_944_LINES_INTF line_intf
                                    WHERE
                                        line_intf.RECEIPT_HEADER_INTF_REC_ID = header_intf.RECEIPT_HEADER_INTF_REC_ID
                                )
                            RETURNING CLOB ) 
                            RETURNING CLOB ) 
                        FROM
                            XXEDI_SCM_INB_944_HEADER_INTF header_intf
                        WHERE
                            header_intf.PROCESSED_FLAG = 'N'
                    )
                    RETURNING CLOB 
                    --PRETTY
                    STRICT WITH UNIQUE KEYS
                ) AS JSON_OUTPUT
                INTO O_P_JSON
            FROM DUAL;











            O_P_RESPONSE := O_P_RESPONSE || '    JSON generated successfully.' || CHR(10) || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || 'GET_INTERFACE_TABLES_DATA procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                O_P_RESPONSE := O_P_RESPONSE    
                    || '    '  || 'ERROR: An error occurred in the GET_INTERFACE_TABLES_DATA procedure.'
                    || '    '  || 'Error Details: ' || SQLCODE           || CHR(10)
                    || '    '  || SQLERRM                                || CHR(10)
                    || '    '  || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE    || CHR(10)
                    || '    '  || DBMS_UTILITY.FORMAT_ERROR_STACK        || CHR(10)
                ;
                O_P_STATUS := 'ERROR';
    END GET_INTERFACE_TABLES_DATA;


END XXEDI_SCM_INB_944_PKG;