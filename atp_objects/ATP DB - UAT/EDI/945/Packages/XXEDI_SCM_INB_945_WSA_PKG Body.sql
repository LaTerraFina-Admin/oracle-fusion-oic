create or replace PACKAGE BODY XXEDI_SCM_INB_945_WSA_PKG AS


    g_v_EDI_945_doc_type                    CONSTANT VARCHAR2(100) := 'EDI_945';
    g_v_PRE_VALIDATION_ERROR_CODE           CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_ERROR';
    g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE  CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_MISMATCH_ERROR';


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
        BEGIN
                DBMS_OUTPUT.PUT_LINE('LOAD_XML_INTO_RAW_STG Procedure Started');
                INSERT INTO XXEDI_SCM_INB_945_XML_DATA_STG (
                        FILE_NAME
                        ,XML_DATA
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                        ,OIC_INSTANCE_ID
                        ,SOURCE_SYSTEM
                        ,DOC_TYPE
                    )
                        SELECT
                            I_P_FILE_NAME           AS FILE_NAME
                            ,I_P_XML_CONTENT        AS XML_DATA
                            ,'OIC'                  AS CREATED_BY_NAME
                            ,'OIC'                  AS LAST_UPDATE_BY_NAME
                            ,I_P_OIC_ID             AS OIC_INSTANCE_ID
                            ,I_P_SOURCE_SYSTEM      AS SOURCE_SYSTEM
                            ,g_v_EDI_945_doc_type   AS DOC_TYPE
                        FROM DUAL;
                COMMIT;
                O_P_RESPONSE := O_P_RESPONSE || '    XML data loaded into XXEDI_SCM_INB_945_XML_DATA_STG' || CHR(10) || CHR(10);
                O_P_RESPONSE := O_P_RESPONSE || '    Invoking PARSE_XML_INTO_STG Procedure' || CHR(10) || CHR(10);
                PARSE_XML_INTO_STG(
                    I_P_OIC_ID
                    ,L_V_CHILD_PROCEDURE_RESPONSE
                    ,L_V_CHILD_PROCEDURE_STATUS
                );
                O_P_RESPONSE := O_P_RESPONSE || L_V_CHILD_PROCEDURE_RESPONSE || CHR(10) || CHR(10);
                O_P_RESPONSE := O_P_RESPONSE || 'g_v_EDI_945_doc_type VALUE:' || g_v_EDI_945_doc_type || CHR(10) || CHR(10);      --! TEST PRINT
                O_P_RESPONSE :=  '    PARSE_XML_INTO_STG Procedure completed' || CHR(10) || CHR(10);

                O_P_STATUS := L_V_CHILD_PROCEDURE_STATUS;
            O_P_RESPONSE := O_P_RESPONSE || 'LOAD_XML_INTO_RAW_STG Procedure completed' || CHR(10) || CHR(10);
        EXCEPTION
            WHEN OTHERS THEN
                O_P_STATUS      := 'ERROR';
                O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || 'Error: ' || SQLCODE || CHR(10) || SQLERRM || CHR(10) || 'Trace: ' || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    END LOAD_XML_INTO_RAW_STG;


    PROCEDURE PARSE_XML_INTO_STG (
             I_P_OIC_ID     IN VARCHAR2
            ,O_P_RESPONSE   OUT CLOB
            ,O_P_STATUS     OUT VARCHAR2
        )
        IS        
            v_days_to_keep_file_XML_data   NUMBER := 90;

            XML_DATA            XMLTYPE;
            XML_TEST_PAYLOAD    CLOB;
            XML_RAW_DATA_REC    XXEDI_SCM_INB_945_XML_DATA_STG%ROWTYPE;

            l_v_XML_H_SHIPMENT_HEADER                  XMLTYPE; -- .
            l_v_XML_H_ADDRESS                          XMLTYPE; -- ..
            l_v_XML_H_CARRIER_INFORMATION              XMLTYPE; -- ..
            l_v_XML_H_CARRIER_SPECIAL_HANDLING_DETAIL  XMLTYPE; -- ..
            l_v_XML_H_DATES                            XMLTYPE; -- ..
            l_v_XML_H_FOB_RELATED_INSTRUCTION          XMLTYPE; -- ..
            l_v_XML_H_NOTES                            XMLTYPE; -- ..
            l_v_XML_H_QUANTITY_AND_WEIGHT              XMLTYPE; -- ..
            l_v_XML_H_QUANTITY_TOTALS                  XMLTYPE; -- ..
            l_v_XML_H_REFERENCES                       XMLTYPE; -- ..
            l_v_XML_O_ORDER_LEVEL                      XMLTYPE; -- ..
            l_v_XML_O_ORDER_HEADER                     XMLTYPE; -- ...
            l_v_XML_O_QUANTITY_AND_WEIGHT              XMLTYPE; -- ...
            l_v_XML_P_PACK_LEVEL                       XMLTYPE; -- ...
            l_v_XML_P_PACK                             XMLTYPE; -- ....
            l_v_XML_P_PHYSICAL_DETAILS                 XMLTYPE; -- ....
            l_v_XML_I_ITEM_LEVEL                       XMLTYPE; -- ....
            l_v_XML_I_SHIPMENT_LINE                    XMLTYPE; -- .....
            l_v_XML_SL_PRODUCT_ID                      XMLTYPE; -- ......
            l_v_XML_I_PHYSICAL_DETAILS                 XMLTYPE; -- .....
            l_v_XML_I_CARRIER_INFORMATION              XMLTYPE; -- .....
            l_v_XML_I_PRODUCT_OR_ITEM_DESCRIPTION      XMLTYPE; -- .....
            l_v_XML_I_REFERENCES                       XMLTYPE; -- .....


            l_v_SHIPMENT_HEADER_REC_ID   NUMBER;
            l_v_ORDER_LEVEL_REC_ID       NUMBER;
            l_v_ORDER_HEADER_REC_ID      NUMBER;
            l_v_PACK_LEVEL_REC_ID        NUMBER;
            l_v_ITEM_LEVEL_REC_ID        NUMBER;
            l_v_SHIPMENT_LINE_REC_ID     NUMBER;

            l_v_ERROR_CODE       VARCHAR2(64);
            l_v_ERROR_MESSAGE    VARCHAR2(4000);


        BEGIN
            O_P_RESPONSE := 'PARSE_XML_INTO_STG Procedure Started' || CHR(10);
            -- select xml data from staging table to be processed
            SELECT * INTO XML_RAW_DATA_REC
                FROM XXEDI_SCM_INB_945_XML_DATA_STG
                WHERE OIC_INSTANCE_ID = I_P_OIC_ID AND PROCESSED_FLAG = 'N' AND DOC_TYPE = g_v_EDI_945_doc_type;

            UPDATE XXEDI_SCM_INB_945_XML_DATA_STG
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
                    UPDATE XXEDI_SCM_INB_945_XML_DATA_STG SET PROCESSED_FLAG = 'E' ,ERROR_CODE = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    RAISE;
            END;

            -- Parse the XML data into the staging tables depending on the source system
            BEGIN

                SELECT 
                    xml_h_ShipmentHeader                 
                    ,xml_h_Address                       
                    ,xml_h_CarrierInformation            
                    ,xml_h_CarrierSpecialHandlingDetail  
                    ,xml_h_Dates                         
                    ,xml_h_FOBRelatedInstruction         
                    ,xml_h_Notes                         
                    ,xml_h_QuantityAndWeight             
                    ,xml_h_QuantityTotals                
                    ,xml_h_References                    
                INTO
                    l_v_XML_H_SHIPMENT_HEADER                 
                    ,l_v_XML_H_ADDRESS                         
                    ,l_v_XML_H_CARRIER_INFORMATION             
                    ,l_v_XML_H_CARRIER_SPECIAL_HANDLING_DETAIL 
                    ,l_v_XML_H_DATES                           
                    ,l_v_XML_H_FOB_RELATED_INSTRUCTION         
                    ,l_v_XML_H_NOTES                           
                    ,l_v_XML_H_QUANTITY_AND_WEIGHT             
                    ,l_v_XML_H_QUANTITY_TOTALS                 
                    ,l_v_XML_H_REFERENCES                    
                FROM XMLTABLE('/Shipment/Header' PASSING XML_DATA 
                    COLUMNS
                        xml_h_ShipmentHeader               XMLTYPE PATH '/Header/ShipmentHeader'
                        ,xml_h_Address                      XMLTYPE PATH '/Header/Address'
                        ,xml_h_CarrierInformation           XMLTYPE PATH '/Header/CarrierInformation'
                        ,xml_h_CarrierSpecialHandlingDetail XMLTYPE PATH '/Header/CarrierSpecialHandlingDetail'
                        ,xml_h_Dates                        XMLTYPE PATH '/Header/Dates'
                        ,xml_h_FOBRelatedInstruction        XMLTYPE PATH '/Header/FOBRelatedInstruction'
                        ,xml_h_Notes                        XMLTYPE PATH '/Header/Notes'
                        ,xml_h_QuantityAndWeight            XMLTYPE PATH '/Header/QuantityAndWeight'
                        ,xml_h_QuantityTotals               XMLTYPE PATH '/Header/QuantityTotals'
                        ,xml_h_References                   XMLTYPE PATH '/Header/References'
                        ----------------
                        -- ,XML_fragment_ShipmentHeader  XMLTYPE PATH '/Header/ShipmentHeader'
                        -- ,XML_fragment_Dates          XMLTYPE PATH '/Header/Dates'
                        -- ,XML_fragment_References     XMLTYPE PATH '/Header/References'
                        -- ,XML_fragment_Address        XMLTYPE PATH '/Header/Address'
                        -- ,XML_fragment_QuantityTotals XMLTYPE PATH '/Header/QuantityTotals'
                ) AS ShipmentHeader;

                FOR rec IN ( SELECT
                        Shipment_Header.TradingPartnerId              AS TRADING_PARTNER_ID
                        ,Shipment_Header.ShipmentIdentification        AS SHIPMENT_IDENTIFICATION
                        ,Shipment_Header.ShipDate                      AS SHIP_DATE
                        ,Shipment_Header.TsetPurposeCode               AS TSET_PURPOSE_CODE
                        ,Shipment_Header.ShipNoticeDate                AS SHIP_NOTICE_DATE
                        ,Shipment_Header.CarrierProNumber              AS CARRIER_PRO_NUMBER
                        ,Shipment_Header.BillOfLadingNumber            AS BILL_OF_LADING_NUMBER
                        ,Shipment_Header.CurrentScheduledDeliveryDate  AS CURRENT_SCHEDULED_DELIVERY_DATE
                        ,Shipment_Header.AppointmentNumber             AS APPOINTMENT_NUMBER
                        ,Shipment_Header.RequestedPickupDate           AS REQUESTED_PICKUP_DATE

                        ,XML_RAW_DATA_REC.XML_CONTENT_REC_ID           AS XML_CONTENT_REC_ID
                        ,XML_RAW_DATA_REC.FILE_NAME                    AS FILE_NAME


                        ,I_P_OIC_ID                                      AS OIC_INSTANCE_ID
                        ,'OIC'                                         AS CREATED_BY_NAME
                        ,'OIC'                                         AS LAST_UPDATE_BY_NAME
                    FROM XMLTABLE('/ShipmentHeader' PASSING l_v_XML_H_SHIPMENT_HEADER
                        COLUMNS
                            TradingPartnerId               VARCHAR2(200) PATH 'TradingPartnerId'
                        ,ShipmentIdentification         VARCHAR2(200) PATH 'ShipmentIdentification'
                        ,ShipDate                       VARCHAR2(200) PATH 'ShipDate'
                        ,TsetPurposeCode                VARCHAR2(200) PATH 'TsetPurposeCode'
                        ,ShipNoticeDate                 VARCHAR2(200) PATH 'ShipNoticeDate'
                        ,CarrierProNumber               VARCHAR2(200) PATH 'CarrierProNumber'
                        ,BillOfLadingNumber             VARCHAR2(200) PATH 'BillOfLadingNumber'
                        ,CurrentScheduledDeliveryDate   VARCHAR2(200) PATH 'CurrentScheduledDeliveryDate'
                        ,AppointmentNumber              VARCHAR2(200) PATH 'AppointmentNumber'
                        ,RequestedPickupDate            VARCHAR2(200) PATH 'RequestedPickupDate'
                    ) Shipment_Header)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG ( 
                        TRADING_PARTNER_ID
                        ,SHIPMENT_IDENTIFICATION
                        ,SHIP_DATE
                        ,TSET_PURPOSE_CODE
                        ,SHIP_NOTICE_DATE
                        ,CARRIER_PRO_NUMBER
                        ,BILL_OF_LADING_NUMBER
                        ,CURRENT_SCHEDULED_DELIVERY_DATE
                        ,APPOINTMENT_NUMBER
                        ,REQUESTED_PICKUP_DATE

                        ,XML_CONTENT_REC_ID
                        ,FILE_NAME


                        ,OIC_INSTANCE_ID
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                        rec.TRADING_PARTNER_ID
                        ,rec.SHIPMENT_IDENTIFICATION
                        ,rec.SHIP_DATE
                        ,rec.TSET_PURPOSE_CODE
                        ,rec.SHIP_NOTICE_DATE
                        ,rec.CARRIER_PRO_NUMBER
                        ,rec.BILL_OF_LADING_NUMBER
                        ,rec.CURRENT_SCHEDULED_DELIVERY_DATE
                        ,rec.APPOINTMENT_NUMBER
                        ,rec.REQUESTED_PICKUP_DATE

                        ,rec.XML_CONTENT_REC_ID
                        ,rec.FILE_NAME


                        ,rec.OIC_INSTANCE_ID
                        ,rec.CREATED_BY_NAME
                        ,rec.LAST_UPDATE_BY_NAME
                    ) 
                    RETURNING SHIPMENT_HEADER_STG_REC_ID INTO l_v_Shipment_Header_REC_ID;
                END LOOP;
                COMMIT;

                FOR DATES IN ( SELECT 
                        Dates.DateTimeQualifier   AS DATE_TIME_QUALIFIER
                        ,Dates."Date"             AS H_DATE
                        FROM XMLTABLE('/Dates' PASSING l_v_XML_H_DATES
                            COLUMNS
                                DateTimeQualifier   VARCHAR2(32) PATH 'DateTimeQualifier'
                                ,"Date"             VARCHAR2(10) PATH 'Date'
                        ) AS Dates
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_DATES_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,DATE_TIME_QUALIFIER
                            ,H_DATE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        ) VALUES (
                            l_v_Shipment_Header_REC_ID
                            ,DATES.DATE_TIME_QUALIFIER
                            ,DATES.H_DATE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

                FOR REFE IN ( SELECT 
                        refe.H_REF_QUAL
                        ,refe.H_REF_ID
                        ,refe.H_REF_DESCRIPTION
                        ,refe.H_REF_DATE
                        FROM XMLTABLE('/References' PASSING l_v_XML_H_REFERENCES
                            COLUMNS
                                H_REF_QUAL      VARCHAR2(16) PATH 'ReferenceQual'
                                ,H_REF_ID       VARCHAR2(64) PATH 'ReferenceID'
                                ,H_REF_DESCRIPTION    VARCHAR2(64) PATH 'Description'
                                ,H_REF_DATE           VARCHAR2(64) PATH 'Date'
                        ) AS REFE
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_REFERENCES_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,H_REF_QUAL
                            ,H_REF_ID
                            ,H_REF_DESCRIPTION
                            ,H_REF_DATE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,REFE.H_REF_QUAL
                            ,REFE.H_REF_ID
                            ,REFE.H_REF_DESCRIPTION
                            ,REFE.H_REF_DATE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

                FOR NOTE IN ( SELECT
                        NoteCode AS NOTE_CODE
                        ,LISTAGG(ALL Note, ' ' ON OVERFLOW TRUNCATE ' ...' WITHOUT COUNT) WITHIN GROUP (ORDER BY NoteCode, RNK) AS NOTE
                        FROM 
                            (SELECT 
                                    NoteCode
                                    ,Note
                                    ,rownum RNK
                                FROM
                                    XMLTABLE('/Notes' passing l_v_XML_H_NOTES
                                        COLUMNS
                                            NoteCode    VARCHAR2(320) PATH 'NoteCode'
                                            ,Note        VARCHAR2(320) PATH 'Note'
                                    ) Notes
                            ) xml_notes
                        GROUP BY NoteCode
                        ORDER BY NoteCode
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_NOTES_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,NOTE_CODE
                            ,NOTE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,NOTE.NOTE_CODE
                            ,NOTE.NOTE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

                FOR ADDR IN ( SELECT 
                            ADDRESS_TYPE_CODE      
                            ,LOCATION_CODE_QUALIFIER
                            ,ADDRESS_LOCATION_NUMBER
                            ,ADDRESS_NAME           
                            ,ADDRESS1               
                            ,CITY                   
                            ,STATE                  
                            ,POSTAL_CODE            
                            ,COUNTRY                
                        FROM XMLTABLE('/Address' PASSING l_v_XML_H_ADDRESS
                            COLUMNS                                    
                                    ADDRESS_TYPE_CODE         VARCHAR2(32) PATH 'AddressTypeCode'
                                ,LOCATION_CODE_QUALIFIER   VARCHAR2(96) PATH 'LocationCodeQualifier'
                                ,ADDRESS_LOCATION_NUMBER   VARCHAR2(384) PATH 'AddressLocationNumber'
                                ,ADDRESS_NAME              VARCHAR2(384) PATH 'AddressName'
                                ,ADDRESS1                  VARCHAR2(384) PATH 'Address1'
                                ,CITY                      VARCHAR2(384) PATH 'City'
                                ,STATE                     VARCHAR2(384) PATH 'State'
                                ,POSTAL_CODE               VARCHAR2(384) PATH 'PostalCode'
                                ,COUNTRY                   VARCHAR2(384) PATH 'Country'
                        ) AS addr
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_ADDRESS_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,ADDRESS_TYPE_CODE      
                            ,LOCATION_CODE_QUALIFIER
                            ,ADDRESS_LOCATION_NUMBER
                            ,ADDRESS_NAME           
                            ,ADDRESS1               
                            ,CITY                   
                            ,STATE                  
                            ,POSTAL_CODE            
                            ,COUNTRY                


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,ADDR.ADDRESS_TYPE_CODE      
                            ,ADDR.LOCATION_CODE_QUALIFIER
                            ,ADDR.ADDRESS_LOCATION_NUMBER
                            ,ADDR.ADDRESS_NAME           
                            ,ADDR.ADDRESS1               
                            ,ADDR.CITY                   
                            ,ADDR.STATE                  
                            ,ADDR.POSTAL_CODE            
                            ,ADDR.COUNTRY                


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

                FOR CAR_INFO IN ( SELECT
                        CARRIER_TRANS_METHOD_CODE
                        ,CARRIER_ALPHA_CODE
                        ,CARRIER_ROUTING
                    FROM XMLTABLE('/CarrierInformation' PASSING l_v_XML_H_CARRIER_INFORMATION
                            COLUMNS                                    
                                    CARRIER_TRANS_METHOD_CODE VARCHAR2(32)  PATH 'CarrierTransMethodCode'
                                ,CARRIER_ALPHA_CODE        VARCHAR2(96)  PATH 'CarrierAlphaCode'
                                ,CARRIER_ROUTING           VARCHAR2(200) PATH 'CarrierRouting'
                        ) AS CAR_INFO
                ) LOOP 
                    INSERT INTO XXEDI_SCM_INB_945_H_CARRIER_INFORMATION_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,CARRIER_TRANS_METHOD_CODE
                            ,CARRIER_ALPHA_CODE
                            ,CARRIER_ROUTING


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,CAR_INFO.CARRIER_TRANS_METHOD_CODE  
                            ,CAR_INFO.CARRIER_ALPHA_CODE         
                            ,CAR_INFO.CARRIER_ROUTING            


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                    COMMIT;
                END LOOP;

                FOR HANDLING IN ( SELECT
                        SPECIAL_HANDLING_CODE
                    FROM XMLTABLE('/CarrierSpecialHandlingDetail' PASSING l_v_XML_H_CARRIER_SPECIAL_HANDLING_DETAIL
                            COLUMNS                                    
                                    SPECIAL_HANDLING_CODE VARCHAR2(150)  PATH 'SpecialHandlingCode'
                        ) AS HANDLING
                ) LOOP 
                    INSERT INTO XXEDI_SCM_INB_945_H_CARRIER_SPECIAL_HANDLING_DETAIL_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,SPECIAL_HANDLING_CODE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,HANDLING.SPECIAL_HANDLING_CODE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                    COMMIT;
                END LOOP;

                FOR QTY_AND_WEIGHT IN ( SELECT
                        WEIGHT
                        ,WEIGHT_UOM
                    FROM XMLTABLE('/QuantityAndWeight' PASSING l_v_XML_H_QUANTITY_AND_WEIGHT
                            COLUMNS                                    
                                    WEIGHT           VARCHAR2(32)  PATH 'Weight'
                                ,WEIGHT_UOM       VARCHAR2(32)  PATH 'WeightUOM'
                        ) AS QTY_AND_WEIGHT
                ) LOOP 
                    INSERT INTO XXEDI_SCM_INB_945_H_QUANTITY_AND_WEIGHT_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,WEIGHT
                            ,WEIGHT_UOM


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,QTY_AND_WEIGHT.WEIGHT  
                            ,QTY_AND_WEIGHT.WEIGHT_UOM


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                    COMMIT;
                END LOOP;

                FOR FOB IN ( SELECT
                        FOB_PAY_CODE
                    FROM XMLTABLE('/FOBRelatedInstruction' PASSING l_v_XML_H_FOB_RELATED_INSTRUCTION
                            COLUMNS                                    
                                    FOB_PAY_CODE VARCHAR2(150)  PATH 'FOBPayCode'
                        ) AS FOB
                ) LOOP 
                    INSERT INTO XXEDI_SCM_INB_945_H_FOB_RELATED_INSTRUCTION_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,FOB_PAY_CODE


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,FOB.FOB_PAY_CODE


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                    );
                    COMMIT;
                END LOOP;

                FOR TOTALS IN ( SELECT 
                            QUANTITY_TOTALS_QUALIFIER 
                            ,QUANTITY                  
                            ,WEIGHT_QUALIFIER          
                            ,WEIGHT                    
                            ,WEIGHT_UOM                
                        FROM XMLTABLE('/QuantityTotals' PASSING l_v_XML_H_QUANTITY_TOTALS
                            COLUMNS                                    
                                QUANTITY_TOTALS_QUALIFIER     VARCHAR2(32) PATH 'QuantityTotalsQualifier'
                                ,QUANTITY                     VARCHAR2(32) PATH 'Quantity'
                                ,WEIGHT_QUALIFIER             VARCHAR2(32) PATH 'WeightQualifier'
                                ,WEIGHT                       VARCHAR2(32) PATH 'Weight'
                                ,WEIGHT_UOM                   VARCHAR2(32) PATH 'WeightUOM'
                        ) AS TOTALS
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_945_H_QUANTITY_TOTALS_STG (
                            SHIPMENT_HEADER_STG_REC_ID

                            ,QUANTITY_TOTALS_QUALIFIER 
                            ,QUANTITY                  
                            ,WEIGHT_QUALIFIER          
                            ,WEIGHT                    
                            ,WEIGHT_UOM                


                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        )
                        VALUES (
                            l_v_Shipment_Header_REC_ID

                            ,TOTALS.QUANTITY_TOTALS_QUALIFIER
                            ,TOTALS.QUANTITY
                            ,TOTALS.WEIGHT_QUALIFIER
                            ,TOTALS.WEIGHT
                            ,TOTALS.WEIGHT_UOM


                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

                FOR ORDER_LEVEL IN ( SELECT
                            Order_Level_xml_fragment
                        FROM XMLTABLE('/Shipment/OrderLevel' PASSING XML_DATA
                            COLUMNS
                                Order_Level_xml_fragment  XMLTYPE  PATH '/OrderLevel'
                    ) AS OrderLevel
                ) LOOP
                    l_v_XML_O_ORDER_LEVEL := ORDER_LEVEL.Order_Level_xml_fragment;

                    FOR ORDER_HEADER IN ( SELECT
                            DepositorOrderNumber
                            ,PurchaseOrderNumber
                        FROM XMLTABLE('OrderLevel/OrderHeader' PASSING l_v_XML_O_ORDER_LEVEL
                            COLUMNS
                                DepositorOrderNumber VARCHAR2(200) PATH 'DepositorOrderNumber'
                                ,PurchaseOrderNumber VARCHAR2(200) PATH 'PurchaseOrderNumber'
                        ) AS ORDER_HEADER
                    ) LOOP
                        INSERT INTO XXEDI_SCM_INB_945_O_ORDER_LEVEL_STG (
                                SHIPMENT_HEADER_STG_REC_ID


                                ,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_Shipment_Header_REC_ID


                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING ORDER_LEVEL_STG_REC_ID INTO l_v_Order_Level_REC_ID;
                        COMMIT;
                        INSERT INTO XXEDI_SCM_INB_945_O_ORDER_HEADER_STG (
                                SHIPMENT_HEADER_STG_REC_ID
                                ,ORDER_LEVEL_STG_REC_ID

                                ,DEPOSITOR_ORDER_NUMBER
                                ,PURCHASE_ORDER_NUMBER


                                ,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_Shipment_Header_REC_ID
                                ,l_v_Order_Level_REC_ID

                                ,ORDER_HEADER.DepositorOrderNumber
                                ,ORDER_HEADER.PurchaseOrderNumber


                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING ORDER_HEADER_STG_REC_ID INTO l_v_ORDER_HEADER_REC_ID;
                        COMMIT;

                        FOR O_QUANTITY_AND_WEIGHT IN ( SELECT
                                LADING_QUANTITY
                            FROM XMLTABLE('/OrderLevel/QuantityAndWeight' PASSING l_v_XML_O_ORDER_LEVEL
                                COLUMNS
                                    LADING_QUANTITY VARCHAR2(32) PATH 'LadingQuantity'
                            ) AS O_QUANTITY_AND_WEIGHT 
                        ) LOOP
                            INSERT INTO XXEDI_SCM_INB_945_O_QUANTITY_AND_WEIGHT_STG (
                                    SHIPMENT_HEADER_STG_REC_ID
                                    ,ORDER_LEVEL_STG_REC_ID
                                    ,ORDER_HEADER_STG_REC_ID

                                    ,LADING_QUANTITY


                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_Shipment_Header_REC_ID
                                    ,l_v_Order_Level_REC_ID
                                    ,l_v_ORDER_HEADER_REC_ID

                                    ,O_QUANTITY_AND_WEIGHT.LADING_QUANTITY


                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                            );
                            COMMIT;
                        END LOOP;

                        FOR PACK IN ( SELECT 
                                pack.PackLevel
                                ,pack.SHIPPING_SERIAL_ID
                                FROM XMLTABLE('/OrderLevel/PackLevel' PASSING l_v_XML_O_ORDER_LEVEL
                                    COLUMNS
                                        PackLevel             XMLTYPE         PATH '/PackLevel'
                                        ,SHIPPING_SERIAL_ID   VARCHAR2(200)   PATH '/PackLevel/Pack/ShippingSerialID'
                                ) AS Pack
                        ) LOOP
                            l_v_XML_P_PACK_LEVEL := PACK.PackLevel;
                            INSERT INTO XXEDI_SCM_INB_945_P_PACK_LEVEL_STG (
                                    SHIPMENT_HEADER_STG_REC_ID
                                    ,ORDER_LEVEL_STG_REC_ID
                                    ,ORDER_HEADER_STG_REC_ID


                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_Shipment_Header_REC_ID
                                    ,l_v_Order_Level_REC_ID
                                    ,l_v_ORDER_HEADER_REC_ID


                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                                )
                            RETURNING PACK_LEVEL_STG_REC_ID INTO l_v_PACK_LEVEL_REC_ID;
                            COMMIT;
                            INSERT INTO XXEDI_SCM_INB_945_P_PACK_STG (
                                    SHIPMENT_HEADER_STG_REC_ID
                                    ,ORDER_LEVEL_STG_REC_ID
                                    ,ORDER_HEADER_STG_REC_ID
                                    ,PACK_LEVEL_STG_REC_ID

                                    ,SHIPPING_SERIAL_ID


                                    ,OIC_INSTANCE_ID
                                    ,CREATED_BY_NAME
                                    ,LAST_UPDATE_BY_NAME
                                )
                                VALUES (
                                    l_v_Shipment_Header_REC_ID
                                    ,l_v_Order_Level_REC_ID
                                    ,l_v_ORDER_HEADER_REC_ID
                                    ,l_v_PACK_LEVEL_REC_ID

                                    ,PACK.SHIPPING_SERIAL_ID


                                    ,I_P_OIC_ID
                                    ,'OIC'
                                    ,'OIC'
                                );
                            COMMIT;

                            FOR P_PHYSICAL_DETAILS IN ( SELECT
                                    PhysicalDetails.WeightQualifier
                                    ,PhysicalDetails.PackWeight
                                    ,PhysicalDetails.PackWeightUOM
                                    FROM XMLTABLE('/PackLevel/PhysicalDetails' PASSING l_v_XML_P_PACK_LEVEL
                                        COLUMNS
                                            WeightQualifier VARCHAR2(32)    PATH 'WeightQualifier'
                                            ,PackWeight     VARCHAR2(32)    PATH 'PackWeight'
                                            ,PackWeightUOM  VARCHAR2(32)    PATH 'PackWeightUOM'
                                    ) AS PhysicalDetails
                            ) LOOP
                                INSERT INTO XXEDI_SCM_INB_945_P_PHYSICAL_DETAILS_STG (
                                        SHIPMENT_HEADER_STG_REC_ID
                                        ,ORDER_LEVEL_STG_REC_ID
                                        ,ORDER_HEADER_STG_REC_ID
                                        ,PACK_LEVEL_STG_REC_ID

                                        ,WEIGHT_QUALIFIER
                                        ,PACK_WEIGHT
                                        ,PACK_WEIGHT_UOM


                                        ,OIC_INSTANCE_ID
                                        ,CREATED_BY_NAME
                                        ,LAST_UPDATE_BY_NAME
                                    )
                                    VALUES (
                                        l_v_Shipment_Header_REC_ID
                                        ,l_v_Order_Level_REC_ID
                                        ,l_v_ORDER_HEADER_REC_ID
                                        ,l_v_PACK_LEVEL_REC_ID

                                        ,P_PHYSICAL_DETAILS.WeightQualifier
                                        ,P_PHYSICAL_DETAILS.PackWeight
                                        ,P_PHYSICAL_DETAILS.PackWeightUOM


                                        ,I_P_OIC_ID
                                        ,'OIC'
                                        ,'OIC'
                                    );
                                COMMIT;
                            END LOOP;

                            FOR ITEM IN ( SELECT
                                    ITEM.XML_ItemLevel
                                    FROM XMLTABLE('/PackLevel/ItemLevel' PASSING l_v_XML_P_PACK_LEVEL
                                        COLUMNS
                                            XML_ItemLevel   XMLTYPE PATH '/ItemLevel'
                                    ) AS ITEM
                            ) LOOP                                    
                                l_v_XML_I_ITEM_LEVEL := ITEM.XML_ItemLevel;
                                INSERT INTO XXEDI_SCM_INB_945_L_ITEM_LEVEL_STG (
                                        SHIPMENT_HEADER_STG_REC_ID
                                        ,ORDER_LEVEL_STG_REC_ID
                                        ,ORDER_HEADER_STG_REC_ID
                                        ,PACK_LEVEL_STG_REC_ID


                                        ,OIC_INSTANCE_ID
                                        ,CREATED_BY_NAME
                                        ,LAST_UPDATE_BY_NAME
                                    )
                                    VALUES (
                                        L_V_SHIPMENT_HEADER_REC_ID
                                        ,L_V_ORDER_LEVEL_REC_ID
                                        ,l_v_ORDER_HEADER_REC_ID
                                        ,L_V_PACK_LEVEL_REC_ID


                                        ,I_P_OIC_ID
                                        ,'OIC'
                                        ,'OIC'
                                    )
                                RETURNING ITEM_LEVEL_STG_REC_ID INTO l_v_ITEM_LEVEL_REC_ID;
                                COMMIT;

                                FOR SHIPMENT_LINE IN ( SELECT 
                                            LINE_SEQUENCE_NUMBER
                                            ,APPLICATION_ID
                                            ,VENDOR_PART_NUMBER
                                            ,GTIN
                                            ,ITEM_STATUS_CODE
                                            ,ORDER_QTY
                                            ,SHIP_QTY
                                            ,SHIP_QTY_UOM
                                            ,QTY_LEFT_TO_RECEIVE
                                            ,xml_ShipmentLine
                                        FROM XMLTABLE('/ItemLevel/ShipmentLine' PASSING l_v_XML_I_ITEM_LEVEL
                                            COLUMNS
                                                LINE_SEQUENCE_NUMBER     VARCHAR(200)    PATH   'LineSequenceNumber'
                                                ,APPLICATION_ID          VARCHAR(200)    PATH   'ApplicationId'
                                                ,VENDOR_PART_NUMBER      VARCHAR(200)    PATH   'VendorPartNumber'
                                                ,GTIN                    VARCHAR(200)    PATH   'GTIN'
                                                ,ITEM_STATUS_CODE        VARCHAR(200)    PATH   'ItemStatusCode'
                                                ,ORDER_QTY               VARCHAR(200)    PATH   'OrderQty'
                                                ,SHIP_QTY                VARCHAR(200)    PATH   'ShipQty'
                                                ,SHIP_QTY_UOM            VARCHAR(200)    PATH   'ShipQtyUOM'
                                                ,QTY_LEFT_TO_RECEIVE     VARCHAR(200)    PATH   'QtyLeftToReceive'
                                                ,xml_ShipmentLine        XMLTYPE         PATH   '/ShipmentLine'
                                                -- ,xml_PhysicalDetails            XMLTYPE  PATH '/ItemLevel/PhysicalDetails'
                                                -- ,xml_CarrierInformation         XMLTYPE  PATH '/ItemLevel/CarrierInformation'
                                                -- ,xml_ProductOrItemDescription   XMLTYPE  PATH '/ItemLevel/ProductOrItemDescription'
                                                -- ,xml_References                 XMLTYPE  PATH '/ItemLevel/References'
                                        ) AS shipment_line
                                ) LOOP
                                    l_v_XML_I_SHIPMENT_LINE := SHIPMENT_LINE.xml_ShipmentLine;
                                    INSERT INTO XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG (
                                                SHIPMENT_HEADER_STG_REC_ID
                                            ,ORDER_LEVEL_STG_REC_ID
                                            ,ORDER_HEADER_STG_REC_ID
                                            ,PACK_LEVEL_STG_REC_ID
                                            ,ITEM_LEVEL_STG_REC_ID

                                            ,LINE_SEQUENCE_NUMBER
                                            ,APPLICATION_ID
                                            ,VENDOR_PART_NUMBER
                                            ,GTIN
                                            ,ORDER_QTY
                                            ,ITEM_STATUS_CODE
                                            ,SHIP_QTY
                                            ,SHIP_QTY_UOM
                                            ,QTY_LEFT_TO_RECEIVE


                                            ,OIC_INSTANCE_ID
                                            ,CREATED_BY_NAME
                                            ,LAST_UPDATE_BY_NAME
                                        ) VALUES (
                                            l_v_Shipment_Header_REC_ID
                                            ,l_v_Order_Level_REC_ID
                                            ,l_v_ORDER_HEADER_REC_ID
                                            ,l_v_PACK_LEVEL_REC_ID
                                            ,l_v_ITEM_LEVEL_REC_ID

                                            ,SHIPMENT_LINE.LINE_SEQUENCE_NUMBER
                                            ,SHIPMENT_LINE.APPLICATION_ID
                                            ,SHIPMENT_LINE.VENDOR_PART_NUMBER
                                            ,SHIPMENT_LINE.GTIN
                                            ,SHIPMENT_LINE.ITEM_STATUS_CODE
                                            ,SHIPMENT_LINE.ORDER_QTY
                                            ,SHIPMENT_LINE.SHIP_QTY
                                            ,SHIPMENT_LINE.SHIP_QTY_UOM
                                            ,SHIPMENT_LINE.QTY_LEFT_TO_RECEIVE


                                            ,I_P_OIC_ID
                                            ,'OIC'
                                            ,'OIC'
                                    ) RETURNING SHIPMENT_LINE_STG_REC_ID INTO l_v_SHIPMENT_LINE_REC_ID;
                                    COMMIT;
                                    FOR Product_ID IN ( SELECT
                                            PART_NUMBER_QUAL
                                            ,PART_NUMBER
                                            FROM XMLTABLE('/ShipmentLine/ProductID' PASSING l_v_XML_I_SHIPMENT_LINE
                                                COLUMNS
                                                    PART_NUMBER_QUAL  VARCHAR2(64)  PATH 'PartNumberQual'
                                                    ,PART_NUMBER      VARCHAR2(256) PATH 'PartNumber'
                                            ) AS Product_ID
                                    ) LOOP
                                        INSERT INTO XXEDI_SCM_INB_945_L_PRODUCT_ID_STG (
                                                SHIPMENT_HEADER_STG_REC_ID
                                                ,ORDER_LEVEL_STG_REC_ID
                                                ,ORDER_HEADER_STG_REC_ID
                                                ,PACK_LEVEL_STG_REC_ID
                                                ,ITEM_LEVEL_STG_REC_ID
                                                ,SHIPMENT_LINE_STG_REC_ID

                                                ,PART_NUMBER_QUAL
                                                ,PART_NUMBER


                                                ,OIC_INSTANCE_ID
                                                ,CREATED_BY_NAME
                                                ,LAST_UPDATE_BY_NAME
                                            ) VALUES (
                                                l_v_Shipment_Header_REC_ID
                                                ,l_v_Order_Level_REC_ID
                                                ,l_v_ORDER_HEADER_REC_ID
                                                ,l_v_PACK_LEVEL_REC_ID
                                                ,l_v_ITEM_LEVEL_REC_ID
                                                ,l_v_SHIPMENT_LINE_REC_ID

                                                ,Product_ID.PART_NUMBER_QUAL
                                                ,Product_ID.PART_NUMBER


                                                ,I_P_OIC_ID
                                                ,'OIC'
                                                ,'OIC'
                                        );
                                        COMMIT;
                                    END LOOP;
                                END LOOP;

                                FOR PHYSICAL_DETAILS IN ( SELECT 
                                        PHYSICAL_DETAILS.WEIGHT_QUALIFIER
                                        ,PHYSICAL_DETAILS.PACK_WEIGHT
                                        ,PHYSICAL_DETAILS.PACK_WEIGHT_UOM
                                        FROM XMLTABLE('/ItemLevel/PhysicalDetails' PASSING l_v_XML_I_ITEM_LEVEL
                                            COLUMNS
                                                WEIGHT_QUALIFIER  VARCHAR2(32)   PATH 'WeightQualifier'
                                                ,PACK_WEIGHT      VARCHAR2(32)   PATH 'PackWeight'
                                                ,PACK_WEIGHT_UOM  VARCHAR2(32)   PATH 'PackWeightUOM'
                                        ) AS PHYSICAL_DETAILS
                                ) LOOP
                                    INSERT INTO XXEDI_SCM_INB_945_L_PHYSICAL_DETAILS_STG (
                                            SHIPMENT_HEADER_STG_REC_ID
                                            ,ORDER_LEVEL_STG_REC_ID
                                            ,ORDER_HEADER_STG_REC_ID
                                            ,PACK_LEVEL_STG_REC_ID
                                            ,ITEM_LEVEL_STG_REC_ID
                                            ,SHIPMENT_LINE_STG_REC_ID

                                            ,WEIGHT_QUALIFIER
                                            ,PACK_WEIGHT
                                            ,PACK_WEIGHT_UOM                                        


                                            ,OIC_INSTANCE_ID
                                            ,CREATED_BY_NAME
                                            ,LAST_UPDATE_BY_NAME
                                        )
                                        VALUES (
                                            l_v_SHIPMENT_HEADER_REC_ID
                                            ,l_v_ORDER_LEVEL_REC_ID
                                            ,l_v_ORDER_HEADER_REC_ID
                                            ,l_v_PACK_LEVEL_REC_ID
                                            ,l_v_ITEM_LEVEL_REC_ID
                                            ,l_v_SHIPMENT_LINE_REC_ID

                                            ,PHYSICAL_DETAILS.WEIGHT_QUALIFIER
                                            ,PHYSICAL_DETAILS.PACK_WEIGHT
                                            ,PHYSICAL_DETAILS.PACK_WEIGHT_UOM


                                            ,I_P_OIC_ID
                                            ,'OIC'
                                            ,'OIC'
                                        );
                                    COMMIT;
                                END LOOP;

                                FOR CARRIER_INFO IN ( SELECT 
                                        CARRIER_INFO.STATUS_CODE
                                        FROM XMLTABLE('/ItemLevel/CarrierInformation' PASSING l_v_XML_I_ITEM_LEVEL
                                            COLUMNS
                                                STATUS_CODE  VARCHAR2(32)    PATH 'StatusCode'
                                        ) AS CARRIER_INFO
                                ) LOOP
                                    INSERT INTO XXEDI_SCM_INB_945_L_CARRIER_INFORMATION_STG (
                                            SHIPMENT_HEADER_STG_REC_ID
                                            ,ORDER_LEVEL_STG_REC_ID
                                            ,ORDER_HEADER_STG_REC_ID
                                            ,PACK_LEVEL_STG_REC_ID
                                            ,ITEM_LEVEL_STG_REC_ID
                                            ,SHIPMENT_LINE_STG_REC_ID

                                            ,STATUS_CODE


                                            ,OIC_INSTANCE_ID
                                            ,CREATED_BY_NAME
                                            ,LAST_UPDATE_BY_NAME
                                        )
                                        VALUES (
                                            l_v_SHIPMENT_HEADER_REC_ID
                                            ,l_v_ORDER_LEVEL_REC_ID
                                            ,l_v_ORDER_HEADER_REC_ID
                                            ,l_v_PACK_LEVEL_REC_ID
                                            ,l_v_ITEM_LEVEL_REC_ID
                                            ,l_v_SHIPMENT_LINE_REC_ID

                                            ,CARRIER_INFO.STATUS_CODE


                                            ,I_P_OIC_ID
                                            ,'OIC'
                                            ,'OIC'
                                        );
                                    COMMIT;
                                END LOOP;

                                FOR PRODUCT_DESC IN ( SELECT 
                                                PRODUCT_DESC.PRODUCT_CHARACTERISTIC_CODE
                                            ,PRODUCT_DESC.PRODUCT_DESCRIPTION
                                        FROM XMLTABLE('/ItemLevel/ProductOrItemDescription' PASSING l_v_XML_I_ITEM_LEVEL
                                            COLUMNS
                                                    PRODUCT_CHARACTERISTIC_CODE   VARCHAR2(32)   PATH 'ProductCharacteristicCode'
                                                ,PRODUCT_DESCRIPTION           VARCHAR2(512)  PATH 'ProductDescription'
                                        ) AS PRODUCT_DESC
                                ) LOOP
                                    INSERT INTO XXEDI_SCM_INB_945_L_PRODUCT_OR_ITEM_DESCRIPTION_STG (
                                                SHIPMENT_HEADER_STG_REC_ID
                                                ,ORDER_LEVEL_STG_REC_ID
                                                ,ORDER_HEADER_STG_REC_ID
                                                ,PACK_LEVEL_STG_REC_ID
                                                ,ITEM_LEVEL_STG_REC_ID
                                                ,SHIPMENT_LINE_STG_REC_ID

                                                ,PRODUCT_CHARACTERISTIC_CODE
                                                ,PRODUCT_DESCRIPTION


                                                ,OIC_INSTANCE_ID
                                                ,CREATED_BY_NAME
                                                ,LAST_UPDATE_BY_NAME
                                        ) VALUES (
                                            l_v_SHIPMENT_HEADER_REC_ID
                                            ,l_v_ORDER_LEVEL_REC_ID
                                            ,l_v_ORDER_HEADER_REC_ID
                                            ,l_v_PACK_LEVEL_REC_ID
                                            ,l_v_ITEM_LEVEL_REC_ID
                                            ,l_v_SHIPMENT_LINE_REC_ID

                                            ,PRODUCT_DESC.PRODUCT_CHARACTERISTIC_CODE
                                            ,PRODUCT_DESC.PRODUCT_DESCRIPTION


                                            ,I_P_OIC_ID
                                            ,'OIC'
                                            ,'OIC'
                                        );
                                    COMMIT;
                                END LOOP;

                                FOR REFE IN ( SELECT 
                                        refe.L_REF_QUAL
                                        ,refe.L_REF_ID
                                        ,refe.L_REF_DESCRIPTION
                                        ,refe.L_REF_DATE
                                        FROM XMLTABLE('/ItemLevel/References' PASSING l_v_XML_I_ITEM_LEVEL
                                            COLUMNS
                                                L_REF_QUAL           VARCHAR2(32)    PATH 'ReferenceQual'
                                                ,L_REF_ID            VARCHAR2(64)    PATH 'ReferenceID'
                                                ,L_REF_DESCRIPTION   VARCHAR2(512)   PATH 'Description'
                                                ,L_REF_DATE          VARCHAR2(32)    PATH 'Date'
                                        ) AS refe
                                ) LOOP
                                    INSERT INTO XXEDI_SCM_INB_945_L_REFERENCES_STG (
                                            SHIPMENT_HEADER_STG_REC_ID
                                            ,ORDER_LEVEL_STG_REC_ID
                                            ,ORDER_HEADER_STG_REC_ID
                                            ,PACK_LEVEL_STG_REC_ID
                                            ,ITEM_LEVEL_STG_REC_ID
                                            ,SHIPMENT_LINE_STG_REC_ID

                                            ,L_REF_QUAL
                                            ,L_REF_ID
                                            ,L_REF_DESCRIPTION
                                            ,L_REF_DATE


                                            ,OIC_INSTANCE_ID
                                            ,CREATED_BY_NAME
                                            ,LAST_UPDATE_BY_NAME
                                        )
                                        VALUES (
                                            l_v_Shipment_Header_REC_ID
                                            ,l_v_Order_Level_REC_ID
                                            ,l_v_ORDER_HEADER_REC_ID
                                            ,l_v_PACK_LEVEL_REC_ID
                                            ,l_v_ITEM_LEVEL_REC_ID
                                            ,l_v_SHIPMENT_LINE_REC_ID

                                            ,REFE.L_REF_QUAL
                                            ,REFE.L_REF_ID
                                            ,REFE.L_REF_DESCRIPTION
                                            ,REFE.L_REF_DATE


                                            ,I_P_OIC_ID
                                            ,'OIC'
                                            ,'OIC'
                                        );
                                    COMMIT;
                                END LOOP;

                            END LOOP;
                        END LOOP;
                    END LOOP;
                END LOOP;

            EXCEPTION
                WHEN OTHERS THEN
                    l_v_ERROR_CODE := 'Error when parsing the XML';
                    l_v_ERROR_MESSAGE := Substr( l_v_ERROR_CODE || '. Details: ' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    IF l_v_SHIPMENT_HEADER_REC_ID IS NOT NULL THEN
                        DELETE FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG WHERE SHIPMENT_HEADER_STG_REC_ID = l_v_SHIPMENT_HEADER_REC_ID;
                        COMMIT;
                    END IF;
                    UPDATE XXEDI_SCM_INB_945_XML_DATA_STG
                        SET PROCESSED_FLAG = 'E' , ERROR_CODE  = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    ' || l_v_ERROR_MESSAGE || CHR(10) || 'File_Name: "' || XML_RAW_DATA_REC.FILE_NAME || '"';
                    RAISE;
            END;
            --

            -- update processed flag to 'Y' for representing that the record was completely processed
            UPDATE XXEDI_SCM_INB_945_XML_DATA_STG SET PROCESSED_FLAG = 'Y' WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            COMMIT;


            -- -- Delete the record from the XML STAGING TABLE after processing
            -- DELETE FROM XXEDI_SCM_INB_945_XML_DATA_STG WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            -- Delete records from the XML STAGING TABLE older than v_days_to_keep_file_XML_data
            DELETE FROM XXEDI_SCM_INB_945_XML_DATA_STG WHERE TRUNC(SYSDATE) - TRUNC(CREATION_DATE) > v_days_to_keep_file_XML_data AND DOC_TYPE = g_v_EDI_945_doc_type;
            COMMIT;

            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || '    XML data has been successfully inserted into the staging tables.'
                              || CHR(10) || CHR(10) || 'PARSE_XML_INTO_STG Procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error: ');
                DBMS_OUTPUT.PUT_LINE(SQLCODE);
                DBMS_OUTPUT.PUT_LINE(SQLERRM);
                DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
                DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_STACK);
                DBMS_OUTPUT.PUT_LINE(CHR(10) || CHR(10) || CHR(10));
                DBMS_OUTPUT.PUT_LINE(O_P_RESPONSE);
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
                FOR shipment_intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                    SELECT
                            ROWNUM
                            ,INTF.SHIPMENT_HEADER_INTF_REC_ID
                            ,INTF.SHIPMENT_HEADER_STG_REC_ID
                            ,INTF.CREATION_DATE
                            ,INTF.LAST_UPDATE_DATE
                            ,INTF.PROCESSED_FLAG
                            ,INTF.ERROR_CODE
                            ,INTF.ERROR_MESSAGE
                            ,INTF.OIC_INSTANCE_ID
                        FROM
                                        XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF   INTF
                            LEFT JOIN   XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG    STG ON INTF.SHIPMENT_HEADER_STG_REC_ID = STG.SHIPMENT_HEADER_STG_REC_ID
                        WHERE
                            INTF.PROCESSED_FLAG = 'E'
                            AND TRUNC(SYSDATE) - TRUNC(STG.CREATION_DATE) <= l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS
                            AND INTF.ERROR_CODE = g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE
                            AND STG.SHIPMENT_IDENTIFICATION NOT IN (  -- ignore if there is a new dropped file with same Shipment
                                SELECT STG_B.SHIPMENT_IDENTIFICATION
                                FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG STG_B
                                WHERE PROCESSED_FLAG  =  'N'
                            )
                            AND STG.SHIPMENT_IDENTIFICATION NOT IN (  -- ignore in case there is a shipment that got correctly processed
                                SELECT STG_B.SHIPMENT_IDENTIFICATION
                                FROM
                                                XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF   INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG    STG_B   ON STG_B.SHIPMENT_HEADER_STG_REC_ID = INTF_B.SHIPMENT_HEADER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'Y'
                                    AND STG_B.SHIPMENT_IDENTIFICATION  =  STG.SHIPMENT_IDENTIFICATION
                            )
                            AND INTF.SHIPMENT_HEADER_INTF_REC_ID = ( -- select only the newest record in error for the same shipment
                                SELECT MAX(INTF_B.SHIPMENT_HEADER_INTF_REC_ID)
                                FROM
                                                XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF    INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG     STG_B   ON STG_B.SHIPMENT_HEADER_STG_REC_ID = INTF_B.SHIPMENT_HEADER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'E'
                                    AND STG_B.SHIPMENT_IDENTIFICATION  =  STG.SHIPMENT_IDENTIFICATION
                            )
                ) LOOP
                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG  SET PROCESSED_FLAG = 'R', ERROR_CODE = NULL, ERROR_MESSAGE = NULL WHERE SHIPMENT_HEADER_STG_REC_ID  = shipment_intf_rec.SHIPMENT_HEADER_STG_REC_ID;
                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF SET PROCESSED_FLAG = 'D'                                          WHERE SHIPMENT_HEADER_INTF_REC_ID = shipment_intf_rec.SHIPMENT_HEADER_INTF_REC_ID;
                END LOOP;
                COMMIT;
            END;

            FOR STG_REC IN (
                SELECT *
                FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG
                WHERE PROCESSED_FLAG  IN ('N', 'R') -- N = Not processed, R = Reprocess
            ) LOOP
                O_P_RESPONSE := O_P_RESPONSE || '    Processing SHIPMENT_HEADER_STG_REC_ID: ' || TO_CHAR(STG_REC.SHIPMENT_HEADER_STG_REC_ID, '999999') || ' from the file name: "' || STG_REC.FILE_NAME || '" | Status: ';

                DECLARE --                
                    l_v_SHIPMENT_HEADER_INTF_REC_ID NUMBER;
                    l_v_INTF_ERROR_CODE             VARCHAR2(64);
                    l_v_INTF_ERROR_MESSAGE          VARCHAR2(4000);
                    l_v_INTF_LINE_ERROR_CODE        VARCHAR2(4000);
                    l_v_INTF_LINE_ERROR_MESSAGE     VARCHAR2(4000);

                    CURSOR SHIPMENT_HEADER_CUR IS
                        SELECT
                            HEADER.SHIPMENT_HEADER_STG_REC_ID  --*
                            ,HEADER.XML_CONTENT_REC_ID         --*
                            ,HEADER.FILE_NAME                  --*
                            ,HEADER.TRADING_PARTNER_ID
                            ,HEADER.SHIPMENT_IDENTIFICATION
                            ,HEADER.SHIP_DATE
                            ,HEADER.TSET_PURPOSE_CODE
                            ,HEADER.PROCESSED_FLAG
                            ,HEADER.ERROR_CODE
                            ,HEADER.ERROR_MESSAGE
                            ,HEADER.OIC_INSTANCE_ID


                            ,ADDRESS_SF.ADDRESS_LOCATION_NUMBER     AS ADDRESS_SF_ADDRESS_LOCATION_NUMBER --!
                            ,ADDRESS_SF.ADDRESS_NAME                AS ADDRESS_SF_ADDRESS_NAME

                            ,Totals.QUANTITY                       AS H_QT_QUANTITY                            
                            ,Totals.WEIGHT                         AS H_QT_WEIGHT
                            ,Totals.WEIGHT_UOM                     AS H_QT_WEIGHT_UOM

                            ,OH.PURCHASE_ORDER_NUMBER              AS PURCHASE_ORDER_NUMBER
                            ,OH.DEPOSITOR_ORDER_NUMBER             AS DEPOSITOR_ORDER_NUMBER


                            ,IOP.ORGANIZATION_ID                            AS IOP_ORGANIZATION_ID
                            ,IOP.ORGANIZATION_CODE                          AS IOP_ORGANIZATION_CODE

                            -- API FIELDS START
                            ,HEADER.SHIPMENT_IDENTIFICATION || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHHMMSS')                     AS SHIPMENT
                            ,NULL                                  AS Ship_Notice_Date
                            ,NULL                                  AS date_from_qualifier_TEN
                            ,NULL                                  AS date_from_qualifier_TWO
                            ,IOP.ORGANIZATION_CODE                                                                           AS SHIP_FROM_ORG_CODE
                            ,NVL(Totals.WEIGHT,0)                                                                            AS GROSS_WEIGHT        -- 1
                            ,'LBS'                                                                                           AS WEIGHT_UOM          -- 1
                            ,'LBS'                                                                                           AS WEIGHT_UOM_CODE     -- 1
                            -- ,Totals.WEIGHT                                                                                   AS GROSS_WEIGHT     -- 2
                            -- ,CASE WHEN TOTALS.WEIGHT IS NOT NULL THEN 'LBS' ELSE NULL END                                    AS WEIGHT_UOM       -- 2
                            -- ,CASE WHEN TOTALS.WEIGHT IS NOT NULL THEN 'LBS' ELSE NULL END                                    AS WEIGHT_UOM_CODE  -- 2
                            ,TO_CHAR(
                                CAST(TO_DATE(HEADER.SHIP_DATE, 'YYYY-MM-DD') AS TIMESTAMP) 
                                AT TIME ZONE 'US/Central'
                                , 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM')                                                          AS ACTUAL_SHIP_DATE
                            ,TO_CHAR(
                                CAST(TO_DATE(HEADER.SHIP_DATE, 'YYYY-MM-DD') AS TIMESTAMP)
                                AT TIME ZONE 'US/Central'
                                , 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM')                                                          AS ASN_SENT_DATE
                            ,HEADER.SHIPMENT_IDENTIFICATION || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHHMMSS')                     AS EXTERNAL_SYS_TX_REF
                            ,NULL /* HANDLING.SPECIAL_HANDLING_CODE */                                                       AS DFF_SPECIAL_HANDLING_CODE     
                            ,NULL /* TO_CHAR(TO_DATE(Dates_02.H_DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')  */       AS DFF_EDI_945_DFF_date_processed
                            ,OH.PURCHASE_ORDER_NUMBER                                                                        AS DFF_3 -- PO_NUMBER
                            ,NULL                                                                                            AS DFF_4
                            ,NULL                                                                                            AS DFF_5
                            ,HEADER.SHIPMENT_IDENTIFICATION  || '_' || TO_CHAR(SYSDATE, 'YYYYMMDDHHMMSS')                    AS OPU_PACKING_UNIT
                            ,'WMS_OBLPN'                                                                                     AS OPU_PACKING_UNIT_TYPE         
                            ,Totals.WEIGHT                                                                                   AS OPU_GROSS_WEIGHT              
                            ,'LBS'                                                                                           AS OPU_GROSS_WEIGHT_UOM_CODE
                            -- API FIELDS END

                            -- preval start
                            ,(SELECT
                                    'Mismatched Lines (FUSION Line_Number -> XML Line_Sequence_Number) : ' || 
                                    LISTAGG(ALL
                                            '( F: '  
                                            || NVL(to_char(LINE_NUMBER),'NULL')  
                                            || ' -> X: '  
                                            || NVL(LINE_SEQUENCE_NUMBER,'NULL')  
                                            || ')' 
                                        , ' ; '  ON OVERFLOW TRUNCATE ' ...' WITHOUT COUNT)  WITHIN GROUP (ORDER BY LINE_NUMBER, LINE_SEQUENCE_NUMBER )
                                FROM    (
                                        -- DLA - XML_SL
                                            SELECT DLA7.LINE_ID, DLA7.LINE_NUMBER, DLA7.STATUS_CODE, SL7.LINE_SEQUENCE_NUMBER, SL7.SHIPMENT_LINE_STG_REC_ID
                                            FROM          DOO_LINE_EXTRACT_PVO_INTF                 DLA7
                                                LEFT JOIN XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL7   ON  SL7.ORDER_LEVEL_STG_REC_ID  =  OH.ORDER_LEVEL_STG_REC_ID  AND  SL7.LINE_SEQUENCE_NUMBER = DLA7.LINE_NUMBER
                                            WHERE
                                                DLA7.HEADER_ID  =  DHA.HEADER_ID
                                                AND ( DLA7.LINE_ID IS NULL OR SL7.SHIPMENT_LINE_STG_REC_ID IS NULL )
                                        UNION ALL
                                        -- XML_SL - DLA
                                            SELECT DLA8.LINE_ID, DLA8.LINE_NUMBER, DLA8.STATUS_CODE, SL8.LINE_SEQUENCE_NUMBER, SL8.SHIPMENT_LINE_STG_REC_ID
                                            FROM          XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL8 
                                                LEFT JOIN DOO_LINE_EXTRACT_PVO_INTF                 DLA8   ON  DLA8.HEADER_ID  =  DHA.HEADER_ID  AND  DLA8.LINE_NUMBER  =  SL8.LINE_SEQUENCE_NUMBER
                                            WHERE
                                                SL8.ORDER_LEVEL_STG_REC_ID   =  OH.ORDER_LEVEL_STG_REC_ID
                                                AND ( DLA8.LINE_ID IS NULL OR SL8.SHIPMENT_LINE_STG_REC_ID IS NULL )
                                    )
                            )                                                                                            AS MISMATCHED_LINES


                            ,(SELECT
                                    'Matched Lines (Fusion Line Number -> XML Line Sequence Number)    : ' || 
                                    LISTAGG(ALL 
                                            '( F: '  
                                            || NVL(to_char(DLA3.LINE_NUMBER),'NULL')  
                                            || ' -> X: '  
                                            || NVL(SL3.LINE_SEQUENCE_NUMBER,'NULL')  
                                            || ')' 
                                        , ' ; '  ON OVERFLOW TRUNCATE ' ...' WITHOUT COUNT)  WITHIN GROUP (ORDER BY DLA3.LINE_NUMBER, SL3.LINE_SEQUENCE_NUMBER ) MATCHED_LINES
                                FROM 
                                         XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG  SL3
                                    JOIN DOO_LINE_EXTRACT_PVO_INTF              DLA3 ON  DLA3.LINE_NUMBER             =  SL3.LINE_SEQUENCE_NUMBER   AND DLA3.HEADER_ID              =  DHA.HEADER_ID
                                WHERE
                                        SL3.ORDER_LEVEL_STG_REC_ID  =  OH.ORDER_LEVEL_STG_REC_ID
                                    AND DLA3.HEADER_ID              =  DHA.HEADER_ID
                                    AND DLA3.LINE_ID                    IS NOT NULL
                                    AND SL3.SHIPMENT_LINE_STG_REC_ID    IS NOT NULL
                            )                                                                                            AS MATCHED_LINES

                            ,(SELECT COUNT(*) AS MISMATCHED_LINES_COUNT 
                                FROM    (
                                        -- DLA - XML_SL
                                            SELECT DLA7.LINE_ID, DLA7.LINE_NUMBER, DLA7.STATUS_CODE, SL7.LINE_SEQUENCE_NUMBER, SL7.SHIPMENT_LINE_STG_REC_ID
                                            FROM          DOO_LINE_EXTRACT_PVO_INTF                 DLA7
                                                LEFT JOIN XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL7   ON  SL7.ORDER_LEVEL_STG_REC_ID  =  OH.ORDER_LEVEL_STG_REC_ID  AND  SL7.LINE_SEQUENCE_NUMBER = DLA7.LINE_NUMBER
                                            WHERE
                                                DLA7.HEADER_ID  =  DHA.HEADER_ID
                                                AND ( DLA7.LINE_ID IS NULL OR SL7.SHIPMENT_LINE_STG_REC_ID IS NULL )
                                        UNION ALL
                                        -- XML_SL - DLA
                                            SELECT DLA8.LINE_ID, DLA8.LINE_NUMBER, DLA8.STATUS_CODE, SL8.LINE_SEQUENCE_NUMBER, SL8.SHIPMENT_LINE_STG_REC_ID
                                            FROM          XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL8 
                                                LEFT JOIN DOO_LINE_EXTRACT_PVO_INTF                 DLA8   ON  DLA8.HEADER_ID  =  DHA.HEADER_ID  AND  DLA8.LINE_NUMBER  =  SL8.LINE_SEQUENCE_NUMBER
                                            WHERE
                                                SL8.ORDER_LEVEL_STG_REC_ID   =  OH.ORDER_LEVEL_STG_REC_ID
                                                AND ( DLA8.LINE_ID IS NULL OR SL8.SHIPMENT_LINE_STG_REC_ID IS NULL )
                                    )
                            )                                                                                            AS MISMATCHED_LINES_COUNT

                            ,(SELECT 
                                LISTAGG(ALL 'f: '  ||  NVL(TO_CHAR(LINE_NUMBER),'NULL')   ||  ' x: '  ||  nvl(LINE_SEQUENCE_NUMBER,'NULL') , ' ; '  ON OVERFLOW TRUNCATE ' ...' WITHOUT COUNT)  WITHIN GROUP (ORDER BY LINE_NUMBER, LINE_SEQUENCE_NUMBER ) DLA_M_XML
                                            FROM          DOO_LINE_EXTRACT_PVO_INTF                 DLA7
                                                LEFT JOIN XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL7   ON  SL7.ORDER_LEVEL_STG_REC_ID  =  OH.ORDER_LEVEL_STG_REC_ID  AND  SL7.LINE_SEQUENCE_NUMBER = DLA7.LINE_NUMBER
                                            WHERE
                                                DLA7.HEADER_ID  =  DHA.HEADER_ID
                                                AND ( DLA7.LINE_ID IS NULL OR SL7.SHIPMENT_LINE_STG_REC_ID IS NULL )


                            ) as DLA_M_XML
                            ,(SELECT 
                                LISTAGG(ALL 'f: '  ||  NVL(TO_CHAR(LINE_NUMBER),'NULL')   ||  ' x: '  ||  nvl(LINE_SEQUENCE_NUMBER,'NULL') , ' ; '  ON OVERFLOW TRUNCATE ' ...' WITHOUT COUNT)  WITHIN GROUP (ORDER BY LINE_NUMBER, LINE_SEQUENCE_NUMBER ) XML_M_DLA
                                            FROM          XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL8 
                                                LEFT JOIN DOO_LINE_EXTRACT_PVO_INTF                 DLA8   ON  DLA8.HEADER_ID  =  DHA.HEADER_ID  AND  DLA8.LINE_NUMBER  =  SL8.LINE_SEQUENCE_NUMBER
                                            WHERE
                                                SL8.ORDER_LEVEL_STG_REC_ID   =  OH.ORDER_LEVEL_STG_REC_ID
                                                AND ( DLA8.LINE_ID IS NULL OR SL8.SHIPMENT_LINE_STG_REC_ID IS NULL )
                            ) as XML_M_DLA
                            ,DHEB.DOO_HEADERS_ADD_INFO_EDI_EDI945SHIPPINGOVERRIDE                                        AS DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG
                            -- preval end
                        FROM
                            XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG            Header 
                            LEFT JOIN XXEDI_SCM_INB_945_H_ADDRESS_STG          ADDRESS_SF  ON  ADDRESS_SF.SHIPMENT_HEADER_STG_REC_ID  =  header.SHIPMENT_HEADER_STG_REC_ID  AND ADDRESS_SF.ADDRESS_TYPE_CODE               = 'SF'
                            LEFT JOIN XXEDI_SCM_INB_945_H_QUANTITY_TOTALS_STG  Totals      ON      Totals.SHIPMENT_HEADER_STG_REC_ID  =  header.SHIPMENT_HEADER_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_O_ORDER_LEVEL_STG      OL          ON          OL.SHIPMENT_HEADER_STG_REC_ID  =  header.SHIPMENT_HEADER_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_O_ORDER_HEADER_STG     OH          ON          OH.ORDER_LEVEL_STG_REC_ID      =  OL.ORDER_LEVEL_STG_REC_ID

                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF              IOP         ON         IOP.ATTRIBUTE2                  =  Header.TRADING_PARTNER_ID          AND IOP.ATTRIBUTE1                             = ADDRESS_SF.ADDRESS_LOCATION_NUMBER
                            --LEFT JOIN DOO_HEADER_EXTRACT_PVO_INTF              DHA         ON         DHA.ORDER_NUMBER                =      OH.DEPOSITOR_ORDER_NUMBER      AND DHA.STATUS_CODE                            = 'OPEN'
                            LEFT JOIN DOO_HEADER_EXTRACT_PVO_INTF              DHA         ON         DHA.SOURCE_ORDER_NUMBER                =      OH.DEPOSITOR_ORDER_NUMBER      AND DHA.STATUS_CODE                            = 'OPEN' AND DHA.CANCELED_FLAG = 'N' AND DHA.REQUEST_CANCEL_DATE IS NULL
                            LEFT JOIN DOO_HEADER_EFF_EXTRACT_PVO_INTF          DHEB        ON        DHEB.HEADERID                    =     DHA.header_id                   AND DHEB.DOO_HEADERS_ADD_INFO_EDI_CONTEXTCODE  = 'EDI' 
                        WHERE
                            HEADER.SHIPMENT_HEADER_STG_REC_ID = STG_REC.SHIPMENT_HEADER_STG_REC_ID
                    ;--! end of Header Cursor




                    CURSOR Shipment_lines_CUR IS
                        SELECT
                            HEADER.SHIPMENT_HEADER_STG_REC_ID
                            ,HEADER.XML_CONTENT_REC_ID
                            ,HEADER.FILE_NAME

                            ,HEADER.TRADING_PARTNER_ID       --! MANDATORY
                            ,HEADER.SHIPMENT_IDENTIFICATION  --! MANDATORY
                            ,HEADER.SHIP_DATE                --! MANDATORY
                            ,HEADER.TSET_PURPOSE_CODE
                            ,HEADER.SHIP_NOTICE_DATE
                            ,HEADER.CARRIER_PRO_NUMBER
                            ,HEADER.BILL_OF_LADING_NUMBER
                            ,HEADER.CURRENT_SCHEDULED_DELIVERY_DATE
                            ,HEADER.APPOINTMENT_NUMBER
                            ,HEADER.REQUESTED_PICKUP_DATE
                            ,HEADER.CREATION_DATE
                            ,HEADER.LAST_UPDATE_DATE
                            ,HEADER.PROCESSED_FLAG
                            ,HEADER.ERROR_CODE
                            ,HEADER.ERROR_MESSAGE
                            ,HEADER.OIC_INSTANCE_ID

                            ,ADDRESS_SF.ADDRESS_LOCATION_NUMBER     AS ADDRESS_SF_ADDRESS_LOCATION_NUMBER --!
                            ,ADDRESS_SF.ADDRESS_NAME                AS ADDRESS_SF_ADDRESS_NAME






                            ,OH.PURCHASE_ORDER_NUMBER                      AS PURCHASE_ORDER_NUMBER
                            ,OH.DEPOSITOR_ORDER_NUMBER                     AS DEPOSITOR_ORDER_NUMBER

                            -- ,O_QTY.LADING_QUANTITY                         AS O_QTY_LADING_QUANTITY

                            ,PL.PACK_LEVEL_STG_REC_ID                      AS PACK_LEVEL_STG_REC_ID

                            -- ,PACK.SHIPPING_SERIAL_ID                       AS PACK_SHIPPING_SERIAL_ID

                            -- ,P_PS_DT.WEIGHT_QUALIFIER                      AS PACK_WEIGHT_QUALIFIER
                            -- ,P_PS_DT.PACK_WEIGHT                           AS PACK_WEIGHT
                            -- ,P_PS_DT.PACK_WEIGHT_UOM                       AS PACK_WEIGHT_UOM

                            ,IL.ITEM_LEVEL_STG_REC_ID                      AS ITEM_LEVEL_STG_REC_ID

                            ,SL.SHIPMENT_LINE_STG_REC_ID                   AS SHIPMENT_LINE_STG_REC_ID   --! MANDATORY
                            ,SL.LINE_SEQUENCE_NUMBER                       AS SL_LINE_SEQUENCE_NUMBER    --! MANDATORY
                            ,SL.APPLICATION_ID                             AS SL_APPLICATION_ID
                            ,SL.VENDOR_PART_NUMBER                         AS SL_VENDOR_PART_NUMBER      --! MANDATORY
                            ,SL.GTIN                                       AS SL_GTIN
                            ,SL.ORDER_QTY                                  AS SL_ORDER_QTY
                            ,SL.ITEM_STATUS_CODE                           AS SL_ITEM_STATUS_CODE
                            ,SL.SHIP_QTY                                   AS SL_SHIP_QTY                --! MANDATORY
                            ,SL.SHIP_QTY_UOM                               AS SL_SHIP_QTY_UOM            --! MANDATORY
                            ,SL.QTY_LEFT_TO_RECEIVE                        AS SL_QTY_LEFT_TO_RECEIVE

                            ,SL_P_ID.PART_NUMBER_QUAL                      AS SL_PID_PART_NUMBER_QUAL
                            ,SL_P_ID.PART_NUMBER                           AS SL_PID_PART_NUMBER         --* not mandatory anymore
                            ,LT.L_REF_ID                                   AS L_REF_LT_REF_ID            --! MANDATORY

                            ,L_PS_DT_G.WEIGHT_QUALIFIER                    AS IL_L_PS_DT_G_WEIGHT_QUALIFIER
                            ,L_PS_DT_G.PACK_WEIGHT                         AS IL_L_PS_DT_G_PACK_WEIGHT
                            ,L_PS_DT_G.PACK_WEIGHT_UOM                     AS IL_L_PS_DT_G_PACK_WEIGHT_UOM
                            ,L_PS_DT_N.WEIGHT_QUALIFIER                    AS IL_L_PS_DT_N_WEIGHT_QUALIFIER
                            ,L_PS_DT_N.PACK_WEIGHT                         AS IL_L_PS_DT_N_PACK_WEIGHT
                            ,L_PS_DT_N.PACK_WEIGHT_UOM                     AS IL_L_PS_DT_N_PACK_WEIGHT_UOM

                            ---------------------------------------------------------------------------------------------
                            ---------------------------------------------------------------------------------------------
                            ---------------------------------------------------------------------------------------------
                            ---------------------------------------------------------------------------------------------

                            ,IOP.ORGANIZATION_ID                            AS IOP_ORGANIZATION_ID
                            ,IOP.ORGANIZATION_CODE                          AS IOP_ORGANIZATION_CODE

                            ,ESIB.INVENTORY_ITEM_ID                         AS ESIB_INVENTORY_ITEM_ID
                            ,ESIB.ITEM_NUMBER                               AS ESIB_ITEM_NUMBER
                            ,ESIB.PRIMARY_UOM_CODE                          AS ESIB_PRIMARY_UOM_CODE

                            ,DHA.HEADER_ID                                  AS DHA_HEADER_ID
                            ,DHA.ORDER_NUMBER                               AS DHA_ORDER_NUMBER
                            ,DHA.CUSTOMER_PO_NUMBER                         AS DHA_CUSTOMER_PO_NUMBER

                            ,DHEB.DOO_HEADERS_ADD_INFO_EDI_EFFLINEID        AS DHEB_EFF_LINE_ID --* USED IN PRE VALIDATION

                            ,DLA.LINE_ID                                    AS DLA_LINE_ID
                            ,DLA.LINE_NUMBER                                AS DLA_LINE_NUMBER
                            ,DLA.DISPLAY_LINE_NUMBER                        AS DLA_DISPLAY_LINE_NUMBER
                            ,DLA.STATUS_CODE                                AS DLA_STATUS_CODE
                            ,DLA.ORDERED_QTY                                AS DLA_ORDERED_QTY
                            ,DLA.ORDERED_UOM                                AS DLA_ORDERED_UOM
                            ,DLA.ACTUAL_SHIP_DATE                           AS DLA_ACTUAL_SHIP_DATE
                            ,DLA.FULFILLED_QTY                              AS DLA_FULFILLED_QTY
                            ,DLA.SHIPPED_QTY                                AS DLA_SHIPPED_QTY
                            ,DLA.ON_HOLD                                    AS DLA_ON_HOLD
                            ,DLA.INVENTORY_ORGANIZATION_ID                  AS DLA_INVENTORY_ORGANIZATION_ID
                            ,DLA.INVENTORY_ITEM_ID                          AS DLA_INVENTORY_ITEM_ID
                            ,DLA.SOURCE_ORG_ID                              AS DLA_SOURCE_ORG_ID
                            ,DLA.ORG_ID                                     AS DLA_ORG_ID
                            ,DLA.LINE_TYPE_CODE                             AS DLA_LINE_TYPE_CODE


                            ,DFLA.FULFILL_LINE_ID                          AS DFLA_FULFILL_LINE_ID
                            ,DFLA.FULFILL_LINE_NUMBER                      AS DFLA_FULFILL_LINE_NUMBER
                            ,DFLA.STATUS_CODE                              AS DFLA_STATUS_CODE
                            ,DFLA.INVENTORY_ITEM_ID                        AS DFLA_INVENTORY_ITEM_ID
                            ,DFLA.SUBINVENTORY                             AS DFLA_SUBINVENTORY
                            ,DFLA.FULFILL_ORG_ID                           AS DFLA_FULFILL_ORG_ID
                            ,DFLA.SOURCE_ORG_ID                            AS DFLA_SOURCE_ORG_ID
                            ,DFLA.ORG_ID                                   AS DFLA_ORG_ID
                            ,DFLA.REQUISITION_INVENTORY_ORG_ID             AS DFLA_REQUISITION_INVENTORY_ORG_ID
                            ,DFLA.ACTUAL_SHIP_DATE                         AS DFLA_ACTUAL_SHIP_DATE
                            ,DFLA.CUSTOMER_PO_LINE_NUMBER                  AS DFLA_CUSTOMER_PO_LINE_NUMBER
                            ,DFLA.OVERRIDE_SCHEDULE_DATE_FLAG              AS DFLA_OVERRIDE_SCHEDULE_DATE_FLAG
                            ,DFLA.PARENT_FULFILL_LINE_ID                   AS DFLA_PARENT_FULFILL_LINE_ID
                            ,DFLA.SPLIT_FROM_FLINE_ID                      AS DFLA_SPLIT_FROM_FLINE_ID
                            ,DFLA.FULFILLMENT_SPLIT_REF_ID                 AS DFLA_FULFILLMENT_SPLIT_REF_ID
                            ,DFLA.ON_HOLD                                  AS DFLA_ON_HOLD
                            ,DFLA.INVENTORY_ORGANIZATION_ID                AS DFLA_INVENTORY_ORGANIZATION_ID
                            ,DFLA.ORDERED_QTY                              AS DFLA_ORDERED_QTY
                            ,DFLA.SECONDARY_ORDERED_QTY                    AS DFLA_SECONDARY_ORDERED_QTY
                            ,DFLA.ORDERED_UOM                              AS DFLA_ORDERED_UOM
                            ,DFLA.FULFILLED_QTY                            AS DFLA_FULFILLED_QTY
                            ,DFLA.SECONDARY_FULFILLED_QTY                  AS DFLA_SECONDARY_FULFILLED_QTY
                            ,DFLA.SECONDARY_UOM                            AS DFLA_SECONDARY_UOM
                            ,DFLA.SHIPPED_QTY                              AS DFLA_SHIPPED_QTY
                            ,DFLA.SECONDARY_SHIPPED_QTY                    AS DFLA_SECONDARY_SHIPPED_QTY
                            ,DFLA.SHIPPED_UOM                              AS DFLA_SHIPPED_UOM


                            ,WDD.DELIVERY_DETAIL_ID                        AS WDD_DELIVERY_DETAIL_ID
                            ,WDD.RELEASED_STATUS                           AS WDD_RELEASED_STATUS              
                            ,WDD.SOURCE_SHIPMENT_ID                        AS WDD_SOURCE_SHIPMENT_ID              
                            ,WDD.SOURCE_LINE_TYPE                          AS WDD_SOURCE_LINE_TYPE             
                            ,WDD.SOURCE_HEADER_NUMBER                      AS WDD_SOURCE_HEADER_NUMBER         
                            ,WDD.SOURCE_LINE_NUMBER                        AS WDD_SOURCE_LINE_NUMBER           
                            ,WDD.SUBINVENTORY                              AS WDD_SUBINVENTORY                 
                            ,WDD.PICKED_FROM_SUBINVENTORY                  AS WDD_PICKED_FROM_SUBINVENTORY     
                            ,WDD.LOT_NUMBER                                AS WDD_LOT_NUMBER                   
                            ,WDD.INVENTORY_ITEM_ID                         AS WDD_INVENTORY_ITEM_ID            
                            ,WDD.ORGANIZATION_ID                           AS WDD_ORGANIZATION_ID              
                            ,WDD.ORG_ID                                    AS WDD_ORG_ID
                            ,WDD.REQUESTED_QUANTITY                        AS WDD_REQUESTED_QUANTITY
                            ,WDD.REQUESTED_QUANTITY_UOM                    AS WDD_REQUESTED_QUANTITY_UOM
                            ,WDD.REQUESTED_QUANTITY2                       AS WDD_REQUESTED_QUANTITY2
                            ,WDD.REQUESTED_QUANTITY_UOM2                   AS WDD_REQUESTED_QUANTITY_UOM2
                            ,WDD.SRC_REQUESTED_QUANTITY                    AS WDD_SRC_REQUESTED_QUANTITY
                            ,WDD.SRC_REQUESTED_QUANTITY_UOM                AS WDD_SRC_REQUESTED_QUANTITY_UOM
                            ,WDD.SRC_REQUESTED_QUANTITY2                   AS WDD_SRC_REQUESTED_QUANTITY2
                            ,WDD.SRC_REQUESTED_QUANTITY_UOM2               AS WDD_SRC_REQUESTED_QUANTITY_UOM2
                            ,WDD.SHIPPED_QUANTITY                          AS WDD_SHIPPED_QUANTITY
                            ,WDD.SHIPPED_QUANTITY2                         AS WDD_SHIPPED_QUANTITY2
                            ,WDD.CANCELLED_QUANTITY                        AS WDD_CANCELLED_QUANTITY
                            ,WDD.CANCELLED_QUANTITY2                       AS WDD_CANCELLED_QUANTITY2
                            ,WDD.DELIVERED_QUANTITY                        AS WDD_DELIVERED_QUANTITY
                            ,WDD.DELIVERED_QUANTITY2                       AS WDD_DELIVERED_QUANTITY2
                            ,WDD.PICKED_QUANTITY                           AS WDD_PICKED_QUANTITY
                            ,WDD.PICKED_QUANTITY2                          AS WDD_PICKED_QUANTITY2
                            ,WDD.NET_WEIGHT                                AS WDD_NET_WEIGHT
                            ,WDD.WEIGHT_UOM_CODE                           AS WDD_WEIGHT_UOM_CODE
                            ,WDD.VOLUME                                    AS WDD_VOLUME
                            ,WDD.VOLUME_UOM_CODE                           AS WDD_VOLUME_UOM_CODE
                            ,WDD.TRACKING_NUMBER                           AS WDD_TRACKING_NUMBER
                            ,WDD.SPLIT_FROM_DELIVERY_DETAIL_ID             AS WDD_SPLIT_FROM_DELIVERY_DETAIL_ID
                            ,WDD.ORIGINAL_DELIVERY_DETAIL_ID               AS WDD_ORIGINAL_DELIVERY_DETAIL_ID


                            ,NULL                                          AS WDA_DELIVERY_ASSIGNMENT_ID
                            ,NULL                                          AS WDA_DELIVERY_DETAIL_ID

                            ,NULL                                          AS WND_DELIVERY_NAME
                            ,NULL                                          AS WND_DELIVERY_ID



                            -- API fields start
                                --unpacked lines
                                ,WDD.DELIVERY_DETAIL_ID       AS SHIPMENT_LINE_ID
                                ,IOP.ORGANIZATION_CODE        AS ORGANIZATION_CODE
                                -- ,ESI.ITEM_NUMBER              AS ITEM_NUMBER
                                ,SL.VENDOR_PART_NUMBER        AS ITEM_NUMBER          	

                                ,SL.SHIP_QTY                  AS SHIPPED_QUANTITY                  	
                                ,SL.SHIP_QTY_UOM              AS SHIPPED_QUANTITY_UOM              	
                                ,CASE
                                    WHEN SL.SHIP_QTY_UOM = 'CA' THEN 'CS'
                                    WHEN SL.SHIP_QTY_UOM = 'LB' THEN 'LBS'
                                    ELSE SL.SHIP_QTY_UOM
                                END                           AS SHIPPED_QUANTITY_UOM_CODE
                                ,L_PS_DT_G.PACK_WEIGHT        AS GROSS_WEIGHT 
                                ,L_PS_DT_N.PACK_WEIGHT        AS NET_WEIGHT 
                                ,'LBS'                        AS WEIGHT_UOM_CODE 
                                ,'1'                          AS LOADING_SEQUENCE 

                                -- DFFs start
                                ,null                         AS DFF_1                         	    
                                ,null                         AS DFF_2                         	    
                                ,null                         AS DFF_3                         	    
                                ,null                         AS DFF_4                         	    
                                ,null                         AS DFF_5                         	    
                                -- DFFs end

                                -- LOTs start
                                -- ,SL_P_ID.PART_NUMBER          AS LOT_NUMBER                        	
                                ,LT.L_REF_ID                  AS LOT_NUMBER                        	
                                ,SL.SHIP_QTY                  AS LOT_QUANTITY                       
                                ,'AVAILABLE'                  AS LOT_SUB_INVENTORY_CODE
                                ,(SELECT SUM(TO_NUMBER(SL_S.SHIP_QTY))
                                    FROM
                                        XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG      SL_S                                        
                                    WHERE
                                        SL_S.SHIPMENT_HEADER_STG_REC_ID = SL.SHIPMENT_HEADER_STG_REC_ID -- same shipment
                                        AND SL_S.VENDOR_PART_NUMBER     = SL.VENDOR_PART_NUMBER         -- same item

                                )                             AS SUM_QTY_BY_ITEM
                                ,(SELECT SUM(TO_NUMBER(SL_S.SHIP_QTY))
                                    FROM
                                        XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG      SL_S
                                        JOIN XXEDI_SCM_INB_945_L_PRODUCT_ID_STG    SL_S_P_ID ON SL_S_P_ID.SHIPMENT_LINE_STG_REC_ID = SL_S.SHIPMENT_LINE_STG_REC_ID --AND SL_S_P_ID.PART_NUMBER_QUAL = 'LOT qualifier'
                                    WHERE
                                        SL_S.SHIPMENT_HEADER_STG_REC_ID = SL.SHIPMENT_HEADER_STG_REC_ID -- same shipment
                                        AND SL_S.VENDOR_PART_NUMBER     = SL.VENDOR_PART_NUMBER         -- same item
                                        -- AND SL_S_P_ID.PART_NUMBER       = SL_P_ID.PART_NUMBER           -- same lot
                                        AND SL_S_P_ID.PART_NUMBER       = LT.L_REF_ID                   -- same lot

                                )                             AS SUM_QTY_BY_LOT
                                -- LOTs end
                            -- API fields end


                            -- preval start
                            ,DHEB.DOO_HEADERS_ADD_INFO_EDI_EDI945SHIPPINGOVERRIDE   AS DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG
                            ,SL.LINE_SEQUENCE_NUMBER                                AS XML_LINE_SEQUENCE_NUMBER
                            ,SL.SHIP_QTY                                            AS XML_SHIP_QUANTITY
                            ,CASE
                                WHEN TO_NUMBER(SL.SHIP_QTY) = NVL(TO_NUMBER(DLA.ORDERED_QTY),0) THEN 'Y'
                                WHEN TO_NUMBER(SL.SHIP_QTY) < NVL(TO_NUMBER(DLA.ORDERED_QTY),0) THEN 'SHORT_SHIP'
                                ELSE 'OVER_SHIP'
                            END                                                     AS XML_SHIPPED_QTY_MATCH_FUSION_ORDERED_QTY
                            ,CASE
                                WHEN to_char(SL.LINE_SEQUENCE_NUMBER)     = nvl(to_char(DLA.LINE_NUMBER),'null') THEN 'Y'
                                ELSE 'LINE MISMATCH: XML = '  ||  to_char(SL.LINE_SEQUENCE_NUMBER)  ||  ' /  DLA = '  ||  nvl(to_char(DLA.LINE_NUMBER),'null')
                            END                                                     AS XML_LINE_SEQUENCE_NUMBER_MATCH_FUSION_LINE_NUMBER

                            -- preval end




                        FROM
                            XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG             Header 
                            LEFT JOIN XXEDI_SCM_INB_945_H_ADDRESS_STG           ADDRESS_SF  ON  ADDRESS_SF.SHIPMENT_HEADER_STG_REC_ID  =  header.SHIPMENT_HEADER_STG_REC_ID  AND ADDRESS_SF.ADDRESS_TYPE_CODE  =  'SF'
                            LEFT JOIN XXEDI_SCM_INB_945_O_ORDER_LEVEL_STG       OL          ON          OL.SHIPMENT_HEADER_STG_REC_ID  =  header.SHIPMENT_HEADER_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_O_ORDER_HEADER_STG      OH          ON          OH.ORDER_LEVEL_STG_REC_ID      =      OL.ORDER_LEVEL_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_P_PACK_LEVEL_STG        PL          ON          PL.ORDER_LEVEL_STG_REC_ID      =      OL.ORDER_LEVEL_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_L_ITEM_LEVEL_STG        IL          ON          IL.PACK_LEVEL_STG_REC_ID       =      PL.PACK_LEVEL_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG     SL          ON          SL.ITEM_LEVEL_STG_REC_ID       =      IL.ITEM_LEVEL_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_L_PRODUCT_ID_STG        SL_P_ID     ON     SL_P_ID.SHIPMENT_LINE_STG_REC_ID    =      SL.SHIPMENT_LINE_STG_REC_ID
                            LEFT JOIN XXEDI_SCM_INB_945_L_PHYSICAL_DETAILS_STG  L_PS_DT_G   ON   L_PS_DT_G.ITEM_LEVEL_STG_REC_ID       =      IL.ITEM_LEVEL_STG_REC_ID       AND L_PS_DT_G.WEIGHT_QUALIFIER    =  'G'
                            LEFT JOIN XXEDI_SCM_INB_945_L_PHYSICAL_DETAILS_STG  L_PS_DT_N   ON   L_PS_DT_N.ITEM_LEVEL_STG_REC_ID       =      IL.ITEM_LEVEL_STG_REC_ID       AND L_PS_DT_N.WEIGHT_QUALIFIER    =  'N'
                            LEFT JOIN XXEDI_SCM_INB_945_L_REFERENCES_STG        LT          ON          LT.ITEM_LEVEL_STG_REC_ID       =      IL.ITEM_LEVEL_STG_REC_ID       AND        LT.L_REF_QUAL = 'LT'


                            LEFT JOIN INV_ORG_PARAMETERS_PVO_INTF               IOP         ON   IOP.ATTRIBUTE2            =  Header.TRADING_PARTNER_ID      AND   IOP.ATTRIBUTE1                             = ADDRESS_SF.ADDRESS_LOCATION_NUMBER
                            LEFT JOIN INV_ITEM_PVO_INTF                         ESIB        ON  ESIB.ITEM_NUMBER           =      SL.VENDOR_PART_NUMBER      AND  ESIB.ORGANIZATION_ID                        =  IOP.ORGANIZATION_ID
                            -- LEFT JOIN WSH_DELIVERY_LINE_EXTRACT_PVO_INTF        WDD         ON   WDD.SOURCE_HEADER_NUMBER    =      OH.DEPOSITOR_ORDER_NUMBER  AND  WDD.SOURCE_LINE_NUMBER                      =   SL.LINE_SEQUENCE_NUMBER  AND  WDD.RELEASED_STATUS  =  'R'  AND  WDD.SOURCE_LINE_TYPE  =  'SALES_ORDER'
                            --LEFT JOIN DOO_HEADER_EXTRACT_PVO_INTF               DHA         ON   DHA.ORDER_NUMBER          =      OH.DEPOSITOR_ORDER_NUMBER  AND   DHA.STATUS_CODE                            =  'OPEN'
                            LEFT JOIN DOO_HEADER_EXTRACT_PVO_INTF               DHA         ON   DHA.SOURCE_ORDER_NUMBER          =      OH.DEPOSITOR_ORDER_NUMBER  AND   DHA.STATUS_CODE                            =  'OPEN' AND DHA.CANCELED_FLAG = 'N' AND DHA.REQUEST_CANCEL_DATE IS NULL
                            LEFT JOIN DOO_HEADER_EFF_EXTRACT_PVO_INTF           DHEB        ON  DHEB.HEADERID              =     DHA.HEADER_ID               AND  DHEB.DOO_HEADERS_ADD_INFO_EDI_CONTEXTCODE   =  'EDI' 
                            LEFT JOIN DOO_LINE_EXTRACT_PVO_INTF                 DLA         ON   DLA.HEADER_ID             =     DHA.HEADER_ID               AND   DLA.INVENTORY_ITEM_ID                      =  ESIB.INVENTORY_ITEM_ID 
                                                                                                                                                             --AND   DLA.ORG_ID                                 =  ESIB.ORGANIZATION_ID
                            -- LEFT JOIN WSH_DELIVERY_LINE_EXTRACT_PVO_INTF        WDD         ON   WDD.SOURCE_HEADER_NUMBER  =     DHA.ORDER_NUMBER            AND  WDD.SOURCE_LINE_NUMBER                      =  DLA.LINE_NUMBER          AND  WDD.RELEASED_STATUS  =  'R'
                            LEFT JOIN DOO_FULFILL_LINE_EXTRACT_PVO_INTF         DFLA        ON  DFLA.LINE_ID               =     DLA.LINE_ID                 AND DFLA.STATUS_CODE <> 'CANCELED'
                            LEFT JOIN WSH_DELIVERY_LINE_EXTRACT_PVO_INTF        WDD         ON   WDD.SOURCE_SHIPMENT_ID    =    DFLA.FULFILL_LINE_ID         AND   WDD.RELEASED_STATUS                        =  'R'
                            -- LEFT JOIN WSH_DELIVERY_LINE_ASSIGNMENT_EXTRACT_PVO_INTF  WDA    ON WDA.DELIVERY_DETAIL_ID       = WDD.DELIVERY_DETAIL_ID AND WDA.DELIVERY_ID IS NOT NULL
                            -- LEFT JOIN WSH_DELIVERY_EXTRACT_PVO_INTF                  WND    ON WND.DELIVERY_ID              = WDA.DELIVERY_ID

                        WHERE
                            HEADER.SHIPMENT_HEADER_STG_REC_ID = STG_REC.SHIPMENT_HEADER_STG_REC_ID

                    ;--! end of Shipment Lines Cursor

                BEGIN -- 

                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG SET PROCESSED_FLAG = 'P' WHERE SHIPMENT_HEADER_STG_REC_ID = STG_REC.SHIPMENT_HEADER_STG_REC_ID;
                    COMMIT;

                    FOR HEADER_REC IN SHIPMENT_HEADER_CUR LOOP

                        BEGIN --* PREVALIDATION HEADER
                            l_v_INTF_ERROR_CODE := NULL;
                            l_v_INTF_ERROR_MESSAGE := NULL;
                            -- validation 1: check if the XML contains all the mandatory fields START
                            IF HEADER_REC.SHIPMENT_IDENTIFICATION  IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 945 XML Mandatory field Shipment.Header.ShipmentHeader.ShipmentIdentification is missing.'  || '  |  '; END IF;
                            IF HEADER_REC.SHIP_DATE                IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 945 XML Mandatory field Shipment.Header.ShipmentHeader.ShipDate is missing.'                || '  |  '; END IF;
                            IF HEADER_REC.TRADING_PARTNER_ID       IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 945 XML Mandatory field Shipment.Header.ShipmentHeader.TradingPartnerId is missing.'        || '  |  '; END IF;
                            IF HEADER_REC.IOP_ORGANIZATION_CODE    IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'No ORGANIZATION_CODE were found for the provided TRADING_PARTNER_ID: ' 
                                                                                                                                 || HEADER_REC.TRADING_PARTNER_ID 
                                                                                                                                 || ' / ADDRESS_LOCATION_NUMBER: ' || HEADER_REC.ADDRESS_SF_ADDRESS_LOCATION_NUMBER                  || '  |  '; END IF;
                            IF l_v_INTF_ERROR_MESSAGE IS NOT NULL THEN l_v_INTF_ERROR_CODE    := G_V_PRE_VALIDATION_ERROR_CODE; END IF;


                            -- validation 1: check if the XML contains all the mandatory fields END

                            -- validation 2: mismatch header level: check if all the xml lines numbers matches all the Fusion Line numbers START
                            /*IF l_v_INTF_ERROR_CODE IS NULL THEN
                                IF NVL(HEADER_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG,'N') = 'N' AND HEADER_REC.MISMATCHED_LINES_COUNT != 0 THEN
                                    l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE
                                        || 'LINES_MISMATCH_HOLD: ORDER EFF override_shipment_mismatch_flag: ' || NVL(HEADER_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG, 'NULL')
                                        || ' /  Mismatched count: ' || HEADER_REC.MISMATCHED_LINES_COUNT || ' / ' || HEADER_REC.MISMATCHED_LINES || ' / ' || HEADER_REC.MATCHED_LINES              || '  |  ';
                                        l_v_INTF_ERROR_CODE := G_V_PRE_VALIDATION_MISMATCH_ERROR_CODE;
                                END IF;
                            END IF;*/
                            -- validation 2: mismatch header level: check if xml lines numbers matches all the Fusion Line numbers END
                        END;

                        l_v_SHIPMENT_HEADER_INTF_REC_ID := NULL;
                        INSERT INTO XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF (
                             SHIPMENT_HEADER_STG_REC_ID
                            ,FILE_NAME

                            ,ACTION_CODE -- 'CreateAndConfirmShipment'
                            ,SHIPMENT
                            ,Ship_Notice_Date
                            ,date_from_qualifier_TEN
                            ,date_from_qualifier_TWO
                            ,SHIP_FROM_ORG_CODE
                            ,GROSS_WEIGHT
                            ,WEIGHT_UOM
                            ,WEIGHT_UOM_CODE
                            ,ACTUAL_SHIP_DATE
                            ,ASN_SENT_DATE
                            ,EXTERNAL_SYS_TX_REF
                            ,DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG
                            ,DFF_SPECIAL_HANDLING_CODE
                            ,DFF_EDI_945_DFF_date_processed
                            ,DFF_3
                            ,DFF_4
                            ,DFF_5
                            ,OPU_PACKING_UNIT
                            ,OPU_PACKING_UNIT_TYPE
                            ,OPU_GROSS_WEIGHT
                            ,OPU_GROSS_WEIGHT_UOM_CODE

                            ,ERROR_CODE
                            ,ERROR_MESSAGE

                            ,OIC_INSTANCE_ID
                        ) VALUES (
                            HEADER_REC.SHIPMENT_HEADER_STG_REC_ID
                            ,HEADER_REC.FILE_NAME

                            ,'CreateAndConfirmShipment'
                            ,HEADER_REC.SHIPMENT
                            ,HEADER_REC.Ship_Notice_Date
                            ,HEADER_REC.date_from_qualifier_TEN
                            ,HEADER_REC.date_from_qualifier_TWO

                            ,HEADER_REC.SHIP_FROM_ORG_CODE
                            ,HEADER_REC.GROSS_WEIGHT
                            ,HEADER_REC.WEIGHT_UOM
                            ,HEADER_REC.WEIGHT_UOM_CODE
                            ,HEADER_REC.ACTUAL_SHIP_DATE
                            ,HEADER_REC.ASN_SENT_DATE
                            ,HEADER_REC.EXTERNAL_SYS_TX_REF
                            ,HEADER_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG
                            ,HEADER_REC.DFF_SPECIAL_HANDLING_CODE
                            ,HEADER_REC.DFF_EDI_945_DFF_date_processed
                            ,HEADER_REC.DFF_3
                            ,HEADER_REC.DFF_4
                            ,HEADER_REC.DFF_5
                            ,HEADER_REC.OPU_PACKING_UNIT
                            ,HEADER_REC.OPU_PACKING_UNIT_TYPE
                            ,HEADER_REC.OPU_GROSS_WEIGHT
                            ,HEADER_REC.OPU_GROSS_WEIGHT_UOM_CODE

                            ,l_v_INTF_ERROR_CODE
                            ,l_v_INTF_ERROR_MESSAGE

                            ,I_P_OIC_ID
                        ) RETURNING SHIPMENT_HEADER_INTF_REC_ID INTO l_v_SHIPMENT_HEADER_INTF_REC_ID;
                        COMMIT;

                        FOR SHIPMENT_LINE_REC IN Shipment_lines_CUR LOOP

                            BEGIN --* PREVALIDATION SHIPMENT_LINES
                                l_v_INTF_LINE_ERROR_CODE      := NULL;
                                l_v_INTF_LINE_ERROR_MESSAGE   := NULL;

                                -- validation 1: check if the XML contains all the mandatory fields start
                                    --IF SHIPMENT_LINE_REC.SL_LINE_SEQUENCE_NUMBER  IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.LineSequenceNumber is missing.'         || '  |  ';  END IF;
                                    IF SHIPMENT_LINE_REC.SL_VENDOR_PART_NUMBER    IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.VendorPartNumber is missing.'           || '  |  ';  END IF;
                                    IF SHIPMENT_LINE_REC.SL_SHIP_QTY              IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.ShipQty is missing.'                    || '  |  ';  END IF;
                                    IF SHIPMENT_LINE_REC.SL_SHIP_QTY_UOM          IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.ShipQtyUOM is missing.'                 || '  |  ';  END IF;
                                    IF SHIPMENT_LINE_REC.L_REF_LT_REF_ID          IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ItemLevel.References with LT qualifier is missing.'  || '  |  ';  END IF;
                                -- validation 1: check if the XML contains all the mandatory fields end

                                -- validation 2: check if derivated Fusion Fields are not null start
                                    IF SHIPMENT_LINE_REC.IOP_ORGANIZATION_ID IS NULL THEN
                                        l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE 
                                            || 'No ORGANIZATION_ID were found for the provided TRADING_PARTNER_ID: ' || SHIPMENT_LINE_REC.TRADING_PARTNER_ID 
                                            || ' / ADDRESS_LOCATION_NUMBER: ' || SHIPMENT_LINE_REC.ADDRESS_SF_ADDRESS_LOCATION_NUMBER || '  |  ';
                                    END IF;
                                    IF SHIPMENT_LINE_REC.DHA_HEADER_ID IS NULL THEN
                                        l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE 
                                            || 'No Order with STATUS_CODE = "OPEN" found in Oracle Fusion for the provided DEPOSITOR_ORDER_NUMBER: ' || SHIPMENT_LINE_REC.DEPOSITOR_ORDER_NUMBER || '  |  ';
                                    END IF;
                                    IF SHIPMENT_LINE_REC.WDD_DELIVERY_DETAIL_ID IS NULL THEN
                                        l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE 
                                            || 'No Shipment Lines were found for the provided LineSequenceNumber = ' || SHIPMENT_LINE_REC.SL_LINE_SEQUENCE_NUMBER
                                            || ' / VendorPartNumber = ' || SHIPMENT_LINE_REC.SL_VENDOR_PART_NUMBER
                                            || ' / DEPOSITOR_ORDER_NUMBER: ' || SHIPMENT_LINE_REC.DEPOSITOR_ORDER_NUMBER || '  |  ';
                                    END IF;
                                    IF SHIPMENT_LINE_REC.ESIB_INVENTORY_ITEM_ID IS NULL THEN
                                        l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE 
                                            || 'No Inventory Items were found for the provided VendorPartNumber: ' || SHIPMENT_LINE_REC.SL_VENDOR_PART_NUMBER
                                            || ' / ORGANIZATION_CODE: ' || SHIPMENT_LINE_REC.IOP_ORGANIZATION_CODE
                                            || ' / ORGANIZATION_ID: '   || SHIPMENT_LINE_REC.IOP_ORGANIZATION_ID || '  |  ';
                                    END IF;
                                -- validation 2: check if derivated Fusion Fields are not null end

                                IF l_v_INTF_LINE_ERROR_MESSAGE IS NOT NULL THEN l_v_INTF_LINE_ERROR_CODE := G_V_PRE_VALIDATION_ERROR_CODE; END IF;


                                -- validation 3: MISMATCH start
                                IF l_v_INTF_LINE_ERROR_CODE IS NULL THEN
                                    -- A: check if XML qty matches Fusion qty start

                                        IF SHIPMENT_LINE_REC.SUM_QTY_BY_ITEM != SHIPMENT_LINE_REC.DLA_ORDERED_QTY AND NVL(SHIPMENT_LINE_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG,'N') = 'N' THEN
                                            l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE
                                                ||  'QUANTITY_MISMATCH_HOLD: XML Shipped Qty does not match Order Line Ordered_qty in Oracle Fusion.'
                                                ||  ' Order '                        || NVL(TO_CHAR(SHIPMENT_LINE_REC.DHA_ORDER_NUMBER                          ),'NULL') 
                                                ||  ' Line '                         || NVL(TO_CHAR(SHIPMENT_LINE_REC.DLA_LINE_NUMBER                           ),'NULL') 
                                                ||  ' Shipment Override EFF flag: '  || NVL(TO_CHAR(SHIPMENT_LINE_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG  ),'NULL')  ||  ' . ' 
                                                ||  ' Details: '
                                                ||  ' DLA Ordered_QTY: '             || NVL(TO_CHAR(SHIPMENT_LINE_REC.DLA_ORDERED_QTY                           ),'NULL')  ||  '  /  '
                                                ||  ' XML Shipped_QTY: '             || NVL(TO_CHAR(SHIPMENT_LINE_REC.XML_SHIP_QUANTITY                         ),'NULL')  ||  '  /  '
                                                ||  ' XML SUM qty by item: '         || NVL(TO_CHAR(SHIPMENT_LINE_REC.SUM_QTY_BY_ITEM                           ),'NULL')  ||  '  /  '
                                                ||  ' XML SUM qty by item and lot: ' || NVL(TO_CHAR(SHIPMENT_LINE_REC.SUM_QTY_BY_LOT                            ),'NULL')  
                                                ||  '  |  ';
                                            l_v_INTF_LINE_ERROR_CODE := G_V_PRE_VALIDATION_MISMATCH_ERROR_CODE;
                                        END IF;

                                    -- B: check if XML LineSequenceNumber matches Fusion.DLA.LINE_NUMBER Start
                                        /*IF SHIPMENT_LINE_REC.XML_LINE_SEQUENCE_NUMBER_MATCH_FUSION_LINE_NUMBER != 'Y'  AND NVL(SHIPMENT_LINE_REC.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG,'N') = 'N' THEN
                                            l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE
                                                || 'XML LineSequenceNumber "'  ||  NVL(TO_CHAR(SHIPMENT_LINE_REC.SL_LINE_SEQUENCE_NUMBER), 'NULL')  ||  '"'
                                                || ' does not match Fusion'
                                                || ' DLA.LINE_NUMBER "'        ||  NVL(TO_CHAR(SHIPMENT_LINE_REC.DLA_LINE_NUMBER        ), 'NULL')  ||  '"'  
                                                ||  '  |  ';
                                            l_v_INTF_LINE_ERROR_CODE := G_V_PRE_VALIDATION_MISMATCH_ERROR_CODE;
                                        END IF;*/
                                END IF;
                                -- validation 3: MISMATCH end
                            END;

                            INSERT INTO XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF (
                                SHIPMENT_HEADER_INTF_REC_ID
                                ,SHIPMENT_LINES_STG_REC_ID

                                ,ORDER_NUMBER
                                ,HEADER_ID
                                ,LINE_ID
                                ,FULFILL_LINE_ID
                                ,DELIVERY_DETAIL_ID
                                ,WDA_DELIVERY_DETAIL_ID
                                ,WND_DELIVERY_ID
                                ,ORGANIZATION_ID
                                ,INVENTORY_ITEM_ID


                                ,SHIPMENT_LINE_ID
                                ,ORGANIZATION_CODE
                                ,ITEM_NUMBER
                                ,SHIPPED_QUANTITY
                                ,SHIPPED_QUANTITY_UOM
                                ,SHIPPED_QUANTITY_UOM_CODE
                                ,GROSS_WEIGHT
                                ,NET_WEIGHT
                                ,WEIGHT_UOM_CODE
                                ,LOADING_SEQUENCE
                                ,REQUESTED_QUANTITY_TO_CONSUME

                                ,DFF_1
                                ,DFF_2
                                ,DFF_3
                                ,DFF_4
                                ,DFF_5

                                ,LOT_NUMBER
                                ,LOT_QUANTITY
                                ,LOT_SUB_INVENTORY_CODE
                                ,SUM_QTY_BY_LOT

                                ,ERROR_CODE
                                ,ERROR_MESSAGE

                                ,OIC_INSTANCE_ID
                            ) VALUES (
                                l_v_SHIPMENT_HEADER_INTF_REC_ID
                                ,SHIPMENT_LINE_REC.SHIPMENT_LINE_STG_REC_ID

                                ,SHIPMENT_LINE_REC.DHA_ORDER_NUMBER
                                ,SHIPMENT_LINE_REC.DHA_HEADER_ID
                                ,SHIPMENT_LINE_REC.DLA_LINE_ID
                                ,SHIPMENT_LINE_REC.DFLA_FULFILL_LINE_ID
                                ,SHIPMENT_LINE_REC.WDD_DELIVERY_DETAIL_ID
                                ,SHIPMENT_LINE_REC.WDA_DELIVERY_DETAIL_ID
                                ,SHIPMENT_LINE_REC.WND_DELIVERY_ID
                                ,SHIPMENT_LINE_REC.IOP_ORGANIZATION_ID
                                ,SHIPMENT_LINE_REC.ESIB_INVENTORY_ITEM_ID


                                ,SHIPMENT_LINE_REC.SHIPMENT_LINE_ID
                                ,SHIPMENT_LINE_REC.ORGANIZATION_CODE
                                ,SHIPMENT_LINE_REC.ITEM_NUMBER
                                ,SHIPMENT_LINE_REC.SHIPPED_QUANTITY
                                ,SHIPMENT_LINE_REC.SHIPPED_QUANTITY_UOM
                                ,SHIPMENT_LINE_REC.SHIPPED_QUANTITY_UOM_CODE
                                ,SHIPMENT_LINE_REC.GROSS_WEIGHT
                                ,SHIPMENT_LINE_REC.NET_WEIGHT
                                ,SHIPMENT_LINE_REC.WEIGHT_UOM_CODE
                                ,SHIPMENT_LINE_REC.LOADING_SEQUENCE
                                ,SHIPMENT_LINE_REC.DLA_ORDERED_QTY

                                ,SHIPMENT_LINE_REC.DFF_1
                                ,SHIPMENT_LINE_REC.DFF_2
                                ,SHIPMENT_LINE_REC.DFF_3
                                ,SHIPMENT_LINE_REC.DFF_4
                                ,SHIPMENT_LINE_REC.DFF_5

                                ,SHIPMENT_LINE_REC.LOT_NUMBER
                                ,SHIPMENT_LINE_REC.LOT_QUANTITY
                                ,SHIPMENT_LINE_REC.LOT_SUB_INVENTORY_CODE
                                ,SHIPMENT_LINE_REC.SUM_QTY_BY_LOT

                                ,l_v_INTF_LINE_ERROR_CODE
                                ,l_v_INTF_LINE_ERROR_MESSAGE

                                ,I_P_OIC_ID
                            );
                            COMMIT;
                        END LOOP;
                    END LOOP;


                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG SET PROCESSED_FLAG = 'Y' WHERE SHIPMENT_HEADER_STG_REC_ID = STG_REC.SHIPMENT_HEADER_STG_REC_ID;
                    COMMIT;

                EXCEPTION
                    WHEN OTHERS THEN  
                        L_V_ERROR_CODE := 'PROCESS_DATA_INTO_INTF procedure error';
                        L_V_ERROR_MESSAGE := Substr(SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                        UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG SET PROCESSED_FLAG = 'E', ERROR_CODE = L_V_ERROR_CODE, ERROR_MESSAGE = L_V_ERROR_MESSAGE
                            WHERE SHIPMENT_HEADER_STG_REC_ID = STG_REC.SHIPMENT_HEADER_STG_REC_ID;
                        IF l_v_SHIPMENT_HEADER_INTF_REC_ID IS NOT NULL THEN
                            DELETE FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF WHERE SHIPMENT_HEADER_INTF_REC_ID = l_v_SHIPMENT_HEADER_INTF_REC_ID;
                        END IF;
                        COMMIT;
                        O_P_RESPONSE := O_P_RESPONSE || 'ERROR' || CHR(10) || CHR(10) || L_V_ERROR_MESSAGE || CHR(10)  || 'SHIPMENT_HEADER_STG_REC_ID: ' || STG_REC.SHIPMENT_HEADER_STG_REC_ID || CHR(10) || 'File name: ' || STG_REC.FILE_NAME || CHR(10) || CHR(10);
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
                    IF
                        UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF'      THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK;
                            IF v_count = 0 THEN RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF for the given PK: ' || v_PK); END IF;

                            IF I_P_FLAG_VALUE = 'E' THEN

                                SELECT ERROR_CODE INTO l_v_current_preval_error_code FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK;
                                IF 
                                        l_v_ERROR_CODE                =  g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE 
                                    AND l_v_current_preval_error_code IS NOT NULL
                                    AND l_v_current_preval_error_code != g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE
                                THEN
                                    l_v_ERROR_CODE := l_v_current_preval_error_code;
                                END IF;

                                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF SET
                                        PROCESSED_FLAG      = I_P_FLAG_VALUE
                                        ,ERROR_CODE          = l_v_ERROR_CODE
                                        ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                        ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                        ,LAST_UPDATE_DATE    = SYSDATE
                                        ,LAST_UPDATE_BY_NAME = 'OIC'
                                    WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK;
                                    UPDATE XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF SET
                                            PROCESSED_FLAG      = I_P_FLAG_VALUE
                                            ,ERROR_CODE          = 'PARENT_ERROR'
                                            ,ERROR_MESSAGE       = 'Some error occured when processing the record. Check parent table (XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF) for details. Error code: ' || l_v_ERROR_CODE
                                            ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                            ,LAST_UPDATE_DATE    = SYSDATE
                                            ,LAST_UPDATE_BY_NAME = 'OIC'
                                    WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK AND PROCESSED_FLAG != 'E' AND ERROR_CODE IS NULL;
                            ELSIF I_P_FLAG_VALUE IN ('Y', 'N') THEN
                                    UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF   SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK;
                                    UPDATE XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF    SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_INTF_REC_ID = v_PK;
                            END IF;


                        ELSIF UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF'        THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF WHERE SHIPMENT_LINES_INTF_REC_ID = v_PK;
                            IF v_count = 0 THEN
                                RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF for the given PK: ' || v_PK);
                            END IF;
                            UPDATE XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF
                                SET
                                    PROCESSED_FLAG      = I_P_FLAG_VALUE
                                    ,ERROR_CODE          = l_v_ERROR_CODE
                                    ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                    ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                    ,LAST_UPDATE_DATE    = SYSDATE
                                    ,LAST_UPDATE_BY_NAME = 'OIC'
                                WHERE
                                    SHIPMENT_LINES_INTF_REC_ID = v_PK;


                        ELSIF   UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG'   THEN

                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            IF v_count = 0 THEN
                                RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG for the given PK: ' || v_PK);
                            END IF;
                            UPDATE XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_STG                  SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_ADDRESS_STG                          SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_CARRIER_INFORMATION_STG              SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_CARRIER_SPECIAL_HANDLING_DETAIL_STG  SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_DATES_STG                            SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_FOB_RELATED_INSTRUCTION_STG          SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_NOTES_STG                            SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_QUANTITY_AND_WEIGHT_STG              SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_QUANTITY_TOTALS_STG                  SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_H_REFERENCES_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_O_ORDER_LEVEL_STG                      SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_O_ORDER_HEADER_STG                     SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_O_QUANTITY_AND_WEIGHT_STG              SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_P_PACK_LEVEL_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_P_PACK_STG                             SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_P_PHYSICAL_DETAILS_STG                 SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_ITEM_LEVEL_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_SHIPMENT_LINE_STG                    SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_PRODUCT_ID_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_PHYSICAL_DETAILS_STG                 SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_CARRIER_INFORMATION_STG              SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_PRODUCT_OR_ITEM_DESCRIPTION_STG      SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_945_L_REFERENCES_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE SHIPMENT_HEADER_STG_REC_ID = v_PK;


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
                        'Unprocessed_Shipments' VALUE (
                            SELECT
                                JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            header_intf.SHIPMENT_HEADER_INTF_REC_ID

                                            ,header_intf.FILE_NAME
                                            ,header_intf.ACTION_CODE
                                            ,header_intf.SHIPMENT
                                            ,header_intf.Ship_Notice_Date
                                            ,header_intf.date_from_qualifier_TEN
                                            ,header_intf.date_from_qualifier_TWO
                                            ,header_intf.SHIP_FROM_ORG_CODE
                                            ,header_intf.GROSS_WEIGHT
                                            ,header_intf.WEIGHT_UOM
                                            ,header_intf.WEIGHT_UOM_CODE
                                            ,header_intf.ACTUAL_SHIP_DATE
                                            ,header_intf.ASN_SENT_DATE
                                            ,header_intf.EXTERNAL_SYS_TX_REF
                                            ,header_intf.DHEB_DFF_OVERRIDE_SHIPMENT_MISMATCH_FLAG
                                            ,header_intf.DFF_SPECIAL_HANDLING_CODE
                                            ,header_intf.DFF_EDI_945_DFF_date_processed
                                            ,header_intf.DFF_3
                                            ,header_intf.DFF_4
                                            ,header_intf.DFF_5
                                            ,header_intf.OPU_PACKING_UNIT
                                            ,header_intf.OPU_PACKING_UNIT_TYPE
                                            ,header_intf.OPU_GROSS_WEIGHT
                                            ,header_intf.OPU_GROSS_WEIGHT_UOM_CODE

                                            ,header_intf.CREATION_DATE
                                            ,header_intf.LAST_UPDATE_DATE
                                            ,header_intf.PROCESSED_FLAG
                                            ,header_intf.ERROR_CODE
                                            ,header_intf.ERROR_MESSAGE
                                            ,header_intf.OIC_INSTANCE_ID
                                            ,'Shipment_Lines' VALUE
                                                (
                                                    SELECT
                                                        JSON_ARRAYAGG(
                                                            JSON_OBJECT(
                                                                line_intf.SHIPMENT_LINES_INTF_REC_ID

                                                                ,line_intf.ORDER_NUMBER
                                                                ,line_intf.HEADER_ID
                                                                ,line_intf.LINE_ID
                                                                ,line_intf.FULFILL_LINE_ID
                                                                ,line_intf.DELIVERY_DETAIL_ID
                                                                ,line_intf.WDA_DELIVERY_DETAIL_ID
                                                                ,line_intf.WND_DELIVERY_ID
                                                                ,line_intf.ORGANIZATION_ID
                                                                ,line_intf.INVENTORY_ITEM_ID
                                                                ,line_intf.SHIPMENT_LINE_ID
                                                                ,line_intf.ORGANIZATION_CODE
                                                                ,line_intf.ITEM_NUMBER
                                                                ,line_intf.SHIPPED_QUANTITY
                                                                ,line_intf.SHIPPED_QUANTITY_UOM
                                                                ,line_intf.SHIPPED_QUANTITY_UOM_CODE
                                                                ,line_intf.GROSS_WEIGHT
                                                                ,line_intf.NET_WEIGHT
                                                                ,line_intf.WEIGHT_UOM_CODE
                                                                ,line_intf.LOADING_SEQUENCE
                                                                ,line_intf.REQUESTED_QUANTITY_TO_CONSUME
                                                                ,line_intf.DFF_1
                                                                ,line_intf.DFF_2
                                                                ,line_intf.DFF_3
                                                                ,line_intf.DFF_4
                                                                ,line_intf.DFF_5
                                                                ,line_intf.LOT_NUMBER
                                                                ,line_intf.LOT_QUANTITY
                                                                ,line_intf.LOT_SUB_INVENTORY_CODE
                                                                ,line_intf.SUM_QTY_BY_LOT
                                                                ,line_intf.SUM_QTY_BY_ITEM

                                                                ,line_intf.CREATION_DATE
                                                                ,line_intf.LAST_UPDATE_DATE
                                                                ,line_intf.PROCESSED_FLAG
                                                                ,line_intf.ERROR_CODE
                                                                ,line_intf.ERROR_MESSAGE
                                                                ,line_intf.OIC_INSTANCE_ID

                                                                RETURNING CLOB
                                                            )

                                                        RETURNING CLOB
                                                    )
                                                    FROM XXEDI_SCM_INB_945_L_SHIPMENT_LINES_INTF line_intf
                                                    WHERE line_intf.SHIPMENT_HEADER_INTF_REC_ID = header_intf.SHIPMENT_HEADER_INTF_REC_ID
                                                )                                
                                            RETURNING CLOB
                                        )

                                    RETURNING CLOB
                                )
                            FROM
                                XXEDI_SCM_INB_945_H_SHIPMENT_HEADER_INTF header_intf
                            WHERE
                                header_intf.PROCESSED_FLAG = 'N'                
                        )
                    RETURNING CLOB
                    -- PRETTY
                    STRICT WITH UNIQUE KEYS
                ) as Json_output
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


END XXEDI_SCM_INB_945_WSA_PKG;