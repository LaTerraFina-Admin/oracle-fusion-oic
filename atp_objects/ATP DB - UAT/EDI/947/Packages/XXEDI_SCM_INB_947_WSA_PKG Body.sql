create or replace PACKAGE BODY XXEDI_SCM_INB_947_WSA_PKG AS


    g_v_EDI_947_doc_type                    CONSTANT VARCHAR2(100) := 'EDI_947';
    g_v_PRE_VALIDATION_ERROR_CODE           CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_ERROR';
    g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE  CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_MISMATCH_ERROR';
	g_v_file_name   VARCHAR2(400):= NULL;


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
                INSERT INTO XXEDI_SCM_INB_947_XML_DATA_STG (
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
                            ,g_v_EDI_947_doc_type   AS DOC_TYPE
                        FROM DUAL;
                COMMIT;
				g_v_file_name:=I_P_FILE_NAME;
               O_P_RESPONSE := O_P_RESPONSE || '    XML data loaded into XXEDI_SCM_INB_947_XML_DATA_STG' || CHR(10) || CHR(10);
                O_P_RESPONSE := O_P_RESPONSE || '    Invoking PARSE_XML_INTO_STG Procedure' || CHR(10) || CHR(10);
                PARSE_XML_INTO_STG(
                    I_P_OIC_ID
                    ,L_V_CHILD_PROCEDURE_RESPONSE
                    ,L_V_CHILD_PROCEDURE_STATUS
                );
             O_P_RESPONSE := O_P_RESPONSE || L_V_CHILD_PROCEDURE_RESPONSE || CHR(10) || CHR(10);
              O_P_RESPONSE := O_P_RESPONSE || 'g_v_EDI_947_doc_type VALUE:' || g_v_EDI_947_doc_type || CHR(10) || CHR(10);      --! TEST PRINT
            --  O_P_RESPONSE :=  '    PARSE_XML_INTO_STG Procedure completed' || CHR(10) || CHR(10);

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
            XML_RAW_DATA_REC    XXEDI_SCM_INB_947_XML_DATA_STG%ROWTYPE;

            l_v_XML_H_HEADER_ORDER                  XMLTYPE; -- .
            l_v_XML_H_ADDRESS                       XMLTYPE; -- ..
			l_v_XML_L_LINE_ITEM                     XMLTYPE; -- ....
            l_v_XML_L_ITEM_DETAIL                   XMLTYPE; -- ..
            l_v_XML_L_PRODUCTORITEMDESCRIPTION      XMLTYPE; -- ..
            l_v_XML_L_REFERENCES                    XMLTYPE; -- ..
			l_v_XML_L_SUMMARY                       XMLTYPE; -- ..


            l_v_HEADER_ORDER_REC_ID      NUMBER;
            l_v_ADDRESS_REC_ID           NUMBER;
			l_v_LINE_ITEM_REC_ID         NUMBER;
            l_v_ITEM_DETAIL_REC_ID       NUMBER;
            l_v_PROD_ITEM_DESC_REC_ID    NUMBER;
            l_v_REFERENCES_REC_ID        NUMBER;
			l_v_SUMMARY_REC_ID           NUMBER;

            l_v_ERROR_CODE       VARCHAR2(64);
            l_v_ERROR_MESSAGE    VARCHAR2(4000);


        BEGIN
            O_P_RESPONSE := 'PARSE_XML_INTO_STG Procedure Started' || CHR(10);
            -- select xml data from staging table to be processed
            SELECT * INTO XML_RAW_DATA_REC
                FROM XXEDI_SCM_INB_947_XML_DATA_STG
                WHERE OIC_INSTANCE_ID = I_P_OIC_ID AND PROCESSED_FLAG = 'N' AND DOC_TYPE = G_V_EDI_947_DOC_TYPE;

            UPDATE XXEDI_SCM_INB_947_XML_DATA_STG
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
                    UPDATE XXEDI_SCM_INB_947_XML_DATA_STG SET PROCESSED_FLAG = 'E' ,ERROR_CODE = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    RAISE;
            END;

            -- Parse the XML data into the staging tables depending on the source system
            BEGIN

                SELECT 
                    xml_h_HeaderOrder                 
                    ,xml_h_Address                                  
                INTO
                    l_v_XML_H_HEADER_ORDER              
                    ,l_v_XML_H_ADDRESS                                            
                FROM XMLTABLE('/WarehouseInventoryAdjustmentAdvice/Header' PASSING XML_DATA 
                    COLUMNS
                        xml_h_HeaderOrder                   XMLTYPE PATH '/Header/HeaderOrder'
                        ,xml_h_Address                      XMLTYPE PATH '/Header/Address'
                ) AS WarehouseInvAdjHeader;

                FOR rec IN ( SELECT
                        Header_Order.TradingPartnerId              AS TRADING_PARTNER_ID
                        ,Header_Order.DocumentId                   AS DOCUMENT_ID
                        ,Header_Order."Date"                       AS H_DATE
                        ,Header_Order.AdjustmentNumber1            AS ADJUSTMENT_NUMBER1
                        ,Header_Order.AdjustmentNumber2            AS ADJUSTMENT_NUMBER2                       
                        ,XML_RAW_DATA_REC.XML_CONTENT_REC_ID       AS XML_CONTENT_REC_ID
                        ,XML_RAW_DATA_REC.FILE_NAME                AS FILE_NAME
                        ,I_P_OIC_ID                                AS OIC_INSTANCE_ID
                        ,'OIC'                                     AS CREATED_BY_NAME
                        ,'OIC'                                     AS LAST_UPDATE_BY_NAME
                    FROM XMLTABLE('/HeaderOrder' PASSING l_v_XML_H_HEADER_ORDER
                        COLUMNS
                         TradingPartnerId               VARCHAR2(200) PATH 'TradingPartnerId'
                        ,DocumentId                     VARCHAR2(200) PATH 'DocumentId'
                        ,"Date"                          VARCHAR2(200) PATH 'Date'
                        ,AdjustmentNumber1              VARCHAR2(200) PATH 'AdjustmentNumber1'
                        ,AdjustmentNumber2              VARCHAR2(200) PATH 'AdjustmentNumber2'                       
                    ) Header_Order)
                LOOP
                    INSERT INTO XXEDI_SCM_INB_947_H_HEADER_ORDER_STG ( 
                        TRADING_PARTNER_ID
                        ,DOCUMENT_ID
                        ,H_DATE
                        ,ADJUSTMENT_NUMBER1
                        ,ADJUSTMENT_NUMBER2
                        ,XML_CONTENT_REC_ID
                        ,FILE_NAME
                        ,OIC_INSTANCE_ID
                        ,CREATED_BY_NAME
                        ,LAST_UPDATE_BY_NAME
                    ) VALUES (
                        rec.TRADING_PARTNER_ID
                        ,rec.DOCUMENT_ID
                        ,rec.H_DATE
                        ,rec.ADJUSTMENT_NUMBER1
                        ,rec.ADJUSTMENT_NUMBER2
                        ,rec.XML_CONTENT_REC_ID
                        ,rec.FILE_NAME
                        ,rec.OIC_INSTANCE_ID
                        ,rec.CREATED_BY_NAME
                        ,rec.LAST_UPDATE_BY_NAME
                    ) 
                    RETURNING HEADER_ORDER_STG_REC_ID INTO l_v_HEADER_ORDER_REC_ID;
                END LOOP;
                COMMIT;

                FOR ADDRS IN ( SELECT 
                        Address.AddressTypeCode   AS ADDRESS_TYPE_CODE
                        ,Address.AddressName      AS ADDRESS_NAME
						,Address.Address1         AS ADDRESS1
                        ,Address.City             AS CITY
						,Address."State"           AS "STATE"
                        ,Address.PostalCode       AS POSTAL_CODE
						,Address.LocationCodeQualifier AS LOCATION_CODE_QUALIFIER                      
						,Address.AddressLocationNumber AS ADDRESS_LOCATION_NUMBER    
                        FROM XMLTABLE('/Address' PASSING l_v_XML_H_ADDRESS
                            COLUMNS
                                AddressTypeCode   VARCHAR2(32) PATH 'AddressTypeCode'
                                ,AddressName      VARCHAR2(200) PATH 'AddressName'
								,Address1         VARCHAR2(200) PATH 'Address1'
                                ,City             VARCHAR2(100) PATH 'City'
								,"State"          VARCHAR2(10) PATH 'State'
                                ,PostalCode       VARCHAR2(10) PATH 'PostalCode'
								,LocationCodeQualifier VARCHAR2(100) PATH 'LocationCodeQualifier'
								,AddressLocationNumber  VARCHAR2(250) PATH 'AddressLocationNumber'

                        ) AS Address
                ) LOOP
                    INSERT INTO XXEDI_SCM_INB_947_H_ADDRESS_STG (
					        -- ADDRESS_REC_ID
                            HEADER_ORDER_STG_REC_ID
							,ADDRESSTYPECODE
							,LOCATION_CODE_QUALIFIER
							,ADDRESS_LOCATION_NUMBER
							,ADDRESS_NAME
							,ADDRESS1                                
							,CITY                                        
							,STATE                                    
							,POSTAL_CODE             
                            ,OIC_INSTANCE_ID
                            ,CREATED_BY_NAME
                            ,LAST_UPDATE_BY_NAME
                        ) VALUES (
                            l_v_HEADER_ORDER_REC_ID
                            ,ADDRS.ADDRESS_TYPE_CODE
							,ADDRS.LOCATION_CODE_QUALIFIER
							,ADDRS.ADDRESS_LOCATION_NUMBER
                            ,ADDRS.ADDRESS_NAME
							,ADDRS.ADDRESS1
                            ,ADDRS.CITY
							,ADDRS."STATE"
							,ADDRS.POSTAL_CODE
                            ,I_P_OIC_ID
                            ,'OIC'
                            ,'OIC'
                        );
                    COMMIT;
                END LOOP;

				 FOR LINE_ITEM IN ( SELECT
                            Line_Item_xml_fragment
                        FROM XMLTABLE('/WarehouseInventoryAdjustmentAdvice/LineItem' PASSING XML_DATA
                            COLUMNS
                                Line_Item_xml_fragment  XMLTYPE  PATH '/LineItem'
                    ) AS LineItem
                ) LOOP
                    l_v_XML_L_LINE_ITEM := LINE_ITEM.Line_Item_xml_fragment;
					 INSERT INTO XXEDI_SCM_INB_947_LINE_ITEM_STG (
                                HEADER_ORDER_STG_REC_ID


                                ,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_HEADER_ORDER_REC_ID


                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING LINE_ITEM_STG_REC_ID INTO l_v_LINE_ITEM_REC_ID;
                        COMMIT;

               FOR ITEM_DETAIL IN ( SELECT
                            VendorPartNumber
                            ,AdjReasonCode
							,QtyAdjusted
                            ,QtyAdjustedUOM
							,Weight1
							,WeightQualifier1
							,WeightUOM1
							,InventoryTransaction
							,UPCCaseCode
                        FROM XMLTABLE('LineItem/ItemDetail' PASSING l_v_XML_L_LINE_ITEM
                            COLUMNS
							 VendorPartNumber VARCHAR2(200) PATH 'VendorPartNumber'
                            ,AdjReasonCode VARCHAR2(200) PATH 'AdjReasonCode'
							,QtyAdjusted VARCHAR2(200) PATH 'QtyAdjusted'
                            ,QtyAdjustedUOM VARCHAR2(200) PATH 'QtyAdjustedUOM'
							,Weight1 VARCHAR2(200) PATH 'Weight1'
							,WeightQualifier1 VARCHAR2(200) PATH 'WeightQualifier1'
							,WeightUOM1  VARCHAR2(200) PATH 'WeightUOM1'
							,InventoryTransaction VARCHAR2(200) PATH   'InventoryTransaction'
							,UPCCaseCode VARCHAR2(200) PATH 'UPCCaseCode'
                        ) AS ITEM_DETAIL
                    ) LOOP



						   INSERT INTO XXEDI_SCM_INB_947_ITEM_DETAIL_STG (
                                HEADER_ORDER_STG_REC_ID
                                ,LINE_ITEM_STG_REC_ID
                                ,VENDOR_PART_NUMBER
								,ADJREASONCODE
								,QTYADJUSTED
								,QTYADJUSTEDUOM
								,WEIGHT1
								,WEIGHTQUALIFIER1
								,WEIGHTUOM1
								,INVENTORYTRANSACTION
								,UPCCASECODE
                                ,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_HEADER_ORDER_REC_ID
                                ,l_v_LINE_ITEM_REC_ID
									,ITEM_DETAIL.VendorPartNumber
                            ,ITEM_DETAIL.AdjReasonCode
							,ITEM_DETAIL.QtyAdjusted
                            ,ITEM_DETAIL.QtyAdjustedUOM
							,ITEM_DETAIL.Weight1
							,ITEM_DETAIL.WeightQualifier1
							,ITEM_DETAIL.WeightUOM1
							,ITEM_DETAIL.InventoryTransaction
							,ITEM_DETAIL.UPCCaseCode
                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING ITEM_DETAIL_STG_REC_ID INTO l_v_ITEM_DETAIL_REC_ID;
                        COMMIT;
        END LOOP;
		  O_P_RESPONSE := 'Inseerted into XXEDI_SCM_INB_947_ITEM_DETAIL_STG' || CHR(10);
		  FOR PRODORITEMDESC IN ( SELECT
                            ProductCharacteristicCode
                            ,ProductDescription
                        FROM XMLTABLE('LineItem/ProductOrItemDescription' PASSING l_v_XML_L_LINE_ITEM
                            COLUMNS
							 ProductCharacteristicCode VARCHAR2(200) PATH 'ProductCharacteristicCode'
                            ,ProductDescription VARCHAR2(200) PATH 'ProductDescription'
                        ) AS PRODORITEMDESC
                    ) LOOP



						   INSERT INTO XXEDI_SCM_INB_947_L_PRODUCT_OR_ITEM_DESC_STG (
                                HEADER_ORDER_STG_REC_ID
                                ,LINE_ITEM_STG_REC_ID
                                ,PRODUCT_CHARACTERISTIC_CODE
								,PRODUCT_DESCRIPTION
								,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_HEADER_ORDER_REC_ID
                                ,l_v_LINE_ITEM_REC_ID
									,PRODORITEMDESC.ProductCharacteristicCode
                            ,PRODORITEMDESC.ProductDescription
                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING PRODUCT_OR_ITEM_DESC_STG_REC_ID INTO l_v_PROD_ITEM_DESC_REC_ID;
                        COMMIT;
		  END LOOP;
		 FOR REFERS IN ( SELECT
                            ReferenceQual
                            ,ReferenceID
                        FROM XMLTABLE('LineItem/References' PASSING l_v_XML_L_LINE_ITEM
                            COLUMNS
							 ReferenceQual VARCHAR2(200) PATH 'ReferenceQual'
                            ,ReferenceID VARCHAR2(200) PATH 'ReferenceID'
                        ) AS REFERS
                    ) LOOP



						   INSERT INTO XXEDI_SCM_INB_947_L_REFERENCE_STG (
                                HEADER_ORDER_STG_REC_ID
                                ,LINE_ITEM_STG_REC_ID
                                ,REFERENCE_QUAL
								,REFERENCEID
								,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_HEADER_ORDER_REC_ID
                                ,l_v_LINE_ITEM_REC_ID
									,REFERS.ReferenceQual
                            ,REFERS.ReferenceID
                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING REFERENCE_STG_REC_ID INTO l_v_REFERENCES_REC_ID;
                        COMMIT;
		  END LOOP;
		  END LOOP;
		   FOR SUMMARY IN ( SELECT
                            Summary_xml_fragment
                        FROM XMLTABLE('/WarehouseInventoryAdjustmentAdvice/Summary' PASSING XML_DATA
                            COLUMNS
                                Summary_xml_fragment  XMLTYPE  PATH '/Summary'
                    ) AS SUMMARY
                ) LOOP
                    l_v_XML_L_SUMMARY := SUMMARY.Summary_xml_fragment;
					 INSERT INTO XXEDI_SCM_INB_947_SUMMARY_STG (
                                HEADER_ORDER_STG_REC_ID


                                ,OIC_INSTANCE_ID
                                ,CREATED_BY_NAME
                                ,LAST_UPDATE_BY_NAME
                            )
                            VALUES (
                                l_v_HEADER_ORDER_REC_ID


                                ,I_P_OIC_ID
                                ,'OIC'
                                ,'OIC'
                        ) RETURNING SUMMARY_STG_REC_ID INTO l_v_SUMMARY_REC_ID;
                        COMMIT;
						END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    l_v_ERROR_CODE := 'Error when parsing the XML';
                    l_v_ERROR_MESSAGE := Substr( l_v_ERROR_CODE || '. Details: ' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    IF l_v_HEADER_ORDER_REC_ID IS NOT NULL THEN
                        DELETE FROM XXEDI_SCM_INB_947_H_HEADER_ORDER_STG WHERE HEADER_ORDER_STG_REC_ID = l_v_HEADER_ORDER_REC_ID;
                        COMMIT;
                    END IF;
                    UPDATE XXEDI_SCM_INB_947_XML_DATA_STG
                        SET PROCESSED_FLAG = 'E' , ERROR_CODE  = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE
                        WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
                    COMMIT;
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    ' || l_v_ERROR_MESSAGE || CHR(10) || 'File_Name: "' || XML_RAW_DATA_REC.FILE_NAME || '"';
                    RAISE;
            END;
            --

            -- update processed flag to 'Y' for representing that the record was completely processed
            UPDATE XXEDI_SCM_INB_947_XML_DATA_STG SET PROCESSED_FLAG = 'Y' WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            COMMIT;


            -- -- Delete the record from the XML STAGING TABLE after processing
            -- DELETE FROM XXEDI_SCM_INB_947_XML_DATA_STG WHERE XML_CONTENT_REC_ID = XML_RAW_DATA_REC.XML_CONTENT_REC_ID;
            -- Delete records from the XML STAGING TABLE older than v_days_to_keep_file_XML_data
            DELETE FROM XXEDI_SCM_INB_947_XML_DATA_STG WHERE TRUNC(SYSDATE) - TRUNC(CREATION_DATE) > v_days_to_keep_file_XML_data AND DOC_TYPE = G_V_EDI_947_DOC_TYPE;
            COMMIT;

          --  O_P_RESPONSE := O_P_RESPONSE || CHR(10) || '    XML data has been successfully inserted into the staging tables.'
           --                   || CHR(10) || CHR(10) || 'PARSE_XML_INTO_STG Procedure completed successfully.';
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
						 l_v_total_trx_qty NUMBER := 0;

            l_v_ERROR_CODE       VARCHAR2(64);
            l_v_ERROR_MESSAGE    VARCHAR2(4000);
        BEGIN            
          --  O_P_RESPONSE := 'PROCESS_DATA_INTO_INTF procedure started.' || CHR(10) || CHR(10);
		--	l_v_total_trx_qty NUMBER := 0;


			--to get total transaction Qty


select sum(to_number(item_details.QTYADJUSTED))
			into l_v_total_trx_qty
from XXEDI_SCM_INB_947_ITEM_DETAIL_STG item_details,
XXEDI_SCM_INB_947_L_REFERENCE_STG lots,
XXEDI_SCM_INB_947_H_HEADER_ORDER_STG hdr
where 1=1
and item_details.line_item_stg_rec_id=lots.line_item_stg_rec_id
and hdr.HEADER_ORDER_STG_REC_ID=item_details.HEADER_ORDER_STG_REC_ID
and lots.REFERENCE_QUAL='LT';
--and hdr.file_name=I_P_FILE_NAME;

O_P_RESPONSE := 'l_v_total_trx_qty.' || CHR(10) || CHR(10)||l_v_total_trx_qty;


            BEGIN -- block to handle reprocessing of records
                FOR header_order_intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                    SELECT
                            ROWNUM
                            ,INTF.HEADER_ORDER_INTF_REC_ID
                            ,INTF.HEADER_ORDER_STG_REC_ID
                            ,INTF.CREATION_DATE
                            ,INTF.LAST_UPDATE_DATE
                            ,INTF.PROCESSED_FLAG
                            ,INTF.ERROR_CODE
                            ,INTF.ERROR_MESSAGE
                            ,INTF.OIC_INSTANCE_ID
                        FROM
                                        XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF   INTF
                            LEFT JOIN   XXEDI_SCM_INB_947_H_HEADER_ORDER_STG    STG ON INTF.HEADER_ORDER_STG_REC_ID = STG.HEADER_ORDER_STG_REC_ID
                        WHERE
                            INTF.PROCESSED_FLAG = 'E'
                            AND TRUNC(SYSDATE) - TRUNC(INTF.CREATION_DATE) <= l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS
                            AND INTF.ERROR_CODE = g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE
                            AND STG.DOCUMENT_ID NOT IN ( 
                                SELECT STG_B.DOCUMENT_ID
                                FROM XXEDI_SCM_INB_947_H_HEADER_ORDER_STG STG_B
                                WHERE PROCESSED_FLAG  =  'N'
                            )
                            AND STG.DOCUMENT_ID NOT IN ( 
                                SELECT STG_B.DOCUMENT_ID
                                FROM
                                                XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF   INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_947_H_HEADER_ORDER_STG    STG_B   ON STG_B.HEADER_ORDER_STG_REC_ID = INTF_B.HEADER_ORDER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'Y'
                                    AND STG_B.DOCUMENT_ID  =  STG.DOCUMENT_ID
                            )
                            AND INTF.HEADER_ORDER_INTF_REC_ID = ( 
                                SELECT MAX(INTF_B.HEADER_ORDER_INTF_REC_ID)
                                FROM
                                                XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF    INTF_B
                                    LEFT JOIN   XXEDI_SCM_INB_947_H_HEADER_ORDER_STG     STG_B   ON STG_B.HEADER_ORDER_STG_REC_ID = INTF_B.HEADER_ORDER_STG_REC_ID
                                WHERE
                                        INTF_B.PROCESSED_FLAG          =   'E'
                                    AND STG_B.DOCUMENT_ID  =  STG.DOCUMENT_ID
                            )

                ) LOOP
                    UPDATE XXEDI_SCM_INB_947_H_HEADER_ORDER_STG  SET PROCESSED_FLAG = 'R', ERROR_CODE = NULL, ERROR_MESSAGE = NULL WHERE HEADER_ORDER_STG_REC_ID  = header_order_intf_rec.HEADER_ORDER_STG_REC_ID;
                    UPDATE XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF SET PROCESSED_FLAG = 'D'                                          WHERE HEADER_ORDER_INTF_REC_ID = header_order_intf_rec.HEADER_ORDER_INTF_REC_ID;

			  END LOOP;
                COMMIT;
            END;
--O_P_RESPONSE:=O_P_RESPONSE||'Testing at 536';
            FOR STG_REC IN (
                SELECT *
                FROM XXEDI_SCM_INB_947_H_HEADER_ORDER_STG
                WHERE PROCESSED_FLAG  IN ('N', 'R') -- N = Not processed, R = Reprocess
            ) LOOP
               -- O_P_RESPONSE := O_P_RESPONSE || 'Processing HEADER_ORDER_STG_REC_ID at 542: ' || TO_CHAR(STG_REC.HEADER_ORDER_STG_REC_ID, '999999') || ' from the file name: "' || STG_REC.FILE_NAME || '" | Status: ';

                DECLARE --                
                    l_v_HEADER_ORDER_INTF_REC_ID NUMBER;
					l_v_trx_intf_id NUMBER;
                    l_v_INTF_ERROR_CODE             VARCHAR2(64);
                    l_v_INTF_ERROR_MESSAGE          VARCHAR2(4000);
                    l_v_INTF_LINE_ERROR_CODE        VARCHAR2(4000);
                    l_v_INTF_LINE_ERROR_MESSAGE     VARCHAR2(4000);

					CURSOR header_cur IS
                        SELECT Header.TRADING_PARTNER_ID                          
,Header.DOCUMENT_ID                               
,Header.H_DATE                                     
,Header.ADJUSTMENT_NUMBER1                         
,Header.ADJUSTMENT_NUMBER2  

						from
						 XXEDI_SCM_INB_947_H_HEADER_ORDER_STG             Header 
					LEFT JOIN XXEDI_SCM_INB_947_H_ADDRESS_STG           ADDRESS  ON  ADDRESS.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID  --AND 
where 1=1;


cursor TRANSACTION_CURSOR
 IS SELECT
    header.trading_partner_id,
    header.file_name,
    line_item.line_item_stg_rec_id,
    header.header_order_stg_rec_id,
    'EDI'                                     AS sourcecode,
    1                                         AS transactionmode,
    (
        SELECT
            SUM(TO_NUMBER(item_details.qtyadjusted))--SL_S.SHIP_QTY))
        FROM
                 xxedi_scm_inb_947_item_detail_stg item_details
            JOIN xxedi_scm_inb_947_l_reference_stg lots ON lots.line_item_stg_rec_id = item_details.line_item_stg_rec_id --AND SL_S_P_ID.PART_NUMBER_QUAL = 'LOT qualifier'
        WHERE
                lots.header_order_stg_rec_id = item_details.header_order_stg_rec_id -- same shipment
          --  AND item_details.line_item_stg_rec_id = lots.line_item_stg_rec_id         -- same item
                                        -- AND SL_S_P_ID.PART_NUMBER       = SL_P_ID.PART_NUMBER           -- same lot
                                       -- AND SL_S_P_ID.PART_NUMBER       = LT.L_REF_ID                   -- same lot
            AND header.header_order_stg_rec_id = lots.header_order_stg_rec_id
            AND line_item.line_item_stg_rec_id = item_details.line_item_stg_rec_id
            AND lots.reference_qual = 'LT'
    )                                         AS transactionquantity,
    (
        CASE
            WHEN item_detail.qtyadjusteduom = 'CA' THEN
                'CS'
            WHEN item_detail.qtyadjusteduom = 'LB' THEN
                'LBS'
            ELSE
                item_detail.qtyadjusteduom
        END
    )                                         transactionuom,
    header.H_date || 'T08:00:00-05:00'   AS transactiondate,
    'AVAILABLE'                               AS subinventorycode,
    (
        CASE
            WHEN item_detail.qtyadjusted > 0 THEN
                'Account Alias Receipt'
            WHEN item_detail.qtyadjusted < 0 THEN
                'Account Alias Issue'
        END
    )                                         AS transactiontypename,
    item_detail.adjreasoncode                 adjreasoncode,
    --   'SPOILS' as      AccountAliasCombination,
 --'4393310'  as ItemNumber,-- 
    item_detail.vendor_part_number            itemnumber,
 hr_org.NAME as   OrganizationName,
  --  'Atlantic Street Union City'              AS organizationname,--Need to derive
    'true'                                    AS usecurrentcostflag,
    header.document_id                        AS externalsystemtransactionreference
FROM
    xxedi_scm_inb_947_h_header_order_stg         header
    LEFT JOIN xxedi_scm_inb_947_h_address_stg              address_sf ON address_sf.header_order_stg_rec_id = header.header_order_stg_rec_id  --AND 
							--ADDRESS_SF.ADDRESS_TYPE_CODE  =  'SF'
    LEFT JOIN xxedi_scm_inb_947_line_item_stg              line_item ON line_item.header_order_stg_rec_id = header.header_order_stg_rec_id
    LEFT JOIN xxedi_scm_inb_947_item_detail_stg            item_detail ON item_detail.header_order_stg_rec_id = header.header_order_stg_rec_id
                                                               AND item_detail.line_item_stg_rec_id = line_item.line_item_stg_rec_id
    LEFT JOIN xxedi_scm_inb_947_l_product_or_item_desc_stg product_item ON product_item.header_order_stg_rec_id = header.header_order_stg_rec_id
                                                                           AND product_item.line_item_stg_rec_id = line_item.line_item_stg_rec_id
    LEFT JOIN xxedi_scm_inb_947_l_reference_stg            lt ON lt.header_order_stg_rec_id = header.header_order_stg_rec_id
                                                      AND lt.line_item_stg_rec_id = line_item.line_item_stg_rec_id
    LEFT JOIN inv_org_parameters_pvo_intf                  iop ON iop.attribute2 = header.trading_partner_id
                                                 AND iop.attribute1 = address_sf.address_location_number
    LEFT JOIN inv_item_pvo_intf                            esib ON esib.item_number = item_detail.vendor_part_number
                                        AND esib.organization_id = iop.organization_id
    LEFT JOIN hr_organization_unit_translation_pvo_intf    hr_org ON hr_org.organization_id = iop.organization_id
                                                                  AND iop.attribute1 = address_sf.address_location_number
WHERE
        1 = 1
    AND header.header_order_stg_rec_id = stg_rec.header_order_stg_rec_id
						--	and LINE_ITEM_STG_REC_ID
    AND lt.reference_qual = 'LT';

		   CURSOR LOT_CUR IS
                       SELECT
					 header.header_order_stg_rec_id,
					 LINE_ITEM.LINE_ITEM_STG_REC_ID,
					 LT.REFERENCE_STG_REC_ID,
                 (CASE WHEN LT.REFERENCE_QUAL ='LT'
 THEN LT.REFERENCEID    
ELSE LT.REFERENCE_QUAL
END) AS LotNumber,
                     header.H_date || 'T08:00:00-05:00' AS originationDate,
              ITEM_DETAIL.qtyadjusted     TransactionQuantity
			 FROM 
 XXEDI_SCM_INB_947_H_HEADER_ORDER_STG             Header 
                           -- LEFT JOIN XXEDI_SCM_INB_947_H_ADDRESS_STG           ADDRESS_SF  ON  ADDRESS_SF.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID  --AND 
							--ADDRESS_SF.ADDRESS_TYPE_CODE  =  'SF'
                            LEFT JOIN XXEDI_SCM_INB_947_LINE_ITEM_STG LINE_ITEM ON LINE_ITEM.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID 
							LEFT JOIN XXEDI_SCM_INB_947_ITEM_DETAIL_STG ITEM_DETAIL ON ITEM_DETAIL.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID 
							AND ITEM_DETAIL.LINE_ITEM_STG_REC_ID = LINE_ITEM.LINE_ITEM_STG_REC_ID
							LEFT JOIN XXEDI_SCM_INB_947_L_PRODUCT_OR_ITEM_DESC_STG PRODUCT_ITEM ON PRODUCT_ITEM.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID 
							  AND PRODUCT_ITEM.LINE_ITEM_STG_REC_ID = LINE_ITEM.LINE_ITEM_STG_REC_ID
							LEFT JOIN XXEDI_SCM_INB_947_L_REFERENCE_STG LT ON LT.HEADER_ORDER_STG_REC_ID  =  header.HEADER_ORDER_STG_REC_ID 
							AND LT.LINE_ITEM_STG_REC_ID=LINE_ITEM.LINE_ITEM_STG_REC_ID
							  WHERE 1=1
                       and    HEADER.HEADER_ORDER_STG_REC_ID = STG_REC.HEADER_ORDER_STG_REC_ID
							AND LT.REFERENCE_QUAL='LT';


                BEGIN -- 

                    UPDATE XXEDI_SCM_INB_947_H_HEADER_ORDER_STG SET PROCESSED_FLAG = 'P' WHERE HEADER_ORDER_STG_REC_ID = STG_REC.HEADER_ORDER_STG_REC_ID;
                    COMMIT;

                    FOR HEADER_REC IN TRANSACTION_CURSOR LOOP

                        BEGIN --* PREVALIDATION HEADER
                            l_v_INTF_ERROR_CODE := NULL;
                            l_v_INTF_ERROR_MESSAGE := NULL;
                            -- validation 1: check if the XML contains all the mandatory fields START
                           IF HEADER_REC.externalsystemtransactionreference  IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 947 XML Mandatory field Header.document id is missing.'  || '  |  '; END IF;
                           IF HEADER_REC.TransactionDate                IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 947 XML Mandatory field Header.Date is missing.'                || '  |  '; END IF;
                            IF HEADER_REC.TRADING_PARTNER_ID       IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'EDI 947 XML Mandatory field Header.TradingPartnerId is missing.'        || '  |  '; END IF;
                           IF HEADER_REC.OrganizationName    IS NULL THEN l_v_INTF_ERROR_MESSAGE := l_v_INTF_ERROR_MESSAGE || 'No OrganizationName were found for the provided TRADING_PARTNER_ID: '         || '  |  '; END IF;
                             --                                                                                                    || HEADER_REC.TRADING_PARTNER_ID 
                              --                                                                                                   || ' / ADDRESS_LOCATION_NUMBER: ' || HEADER_REC.ADDRESS_SF_ADDRESS_LOCATION_NUMBER                  || '  |  '; END IF;
                            IF l_v_INTF_ERROR_MESSAGE IS NOT NULL THEN l_v_INTF_ERROR_CODE    := G_V_PRE_VALIDATION_ERROR_CODE; END IF;



                        END;

                        l_v_HEADER_ORDER_INTF_REC_ID := NULL;
						l_v_trx_intf_id :=NULL;




                       INSERT INTO XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF
            (HEADER_ORDER_STG_REC_ID ,
			TRADING_PARTNER_ID,
  --  XML_CONTENT_REC_ID    ,
    FILE_NAME      ,    
LINE_ITEM_STG_REC_ID,    
  --  TRANSACTIONINTERFACEID  ,
   -- TRANSACTIONHEADERID    ,
  --  SOURCECODE              ,
  --  SOURCEHEADERID        ,
  --  SOURCELINEID           ,
    TRANSACTIONMODE        ,
    TRANSACTIONQUANTITY     ,
    TRANSACTIONUOM         ,
    TRANSACTIONDATE         ,
    SUBINVENTORYCODE       ,
    TRANSACTIONTYPENAME    ,
	ADJREASONCODE,
   -- ACCOUNTALIASCOMBINATION,
    ITEMNUMBER             ,
    ORGANIZATIONNAME       ,
    USECURRENTCOSTFLAG      ,
	ExternalSystemTransactionReference,
  --  LOTNUMBER             ,
	--LOTQTY,
  --  LOTEXPIRATIONDATE,
	ERROR_CODE,
	ERROR_MESSAGE,
OIC_INSTANCE_ID	)
VALUES      ( HEADER_REC.HEADER_ORDER_STG_REC_ID
              ,HEADER_REC.TRADING_PARTNER_ID
              ,HEADER_REC.FILE_NAME
			  ,HEADER_REC.LINE_ITEM_STG_REC_ID
             ,1
             ,HEADER_REC.TransactionQuantity
             ,HEADER_REC.TRANSACTIONUOM
             ,HEADER_REC.TransactionDate
             ,HEADER_REC.SUBINVENTORYCODE
			 ,HEADER_REC.TransactionTypeName
			 ,HEADER_REC.AdjReasonCode
			-- ,HEADER_REC.ACCOUNTALIASCOMBINATION
			 ,HEADER_REC.ITEMNUMBER
			 ,HEADER_REC.OrganizationName
			 ,'true'
			 --,'14131-1'
			 ,HEADER_REC.externalsystemtransactionreference
			-- ,HEADER_REC.LotNumber
			 --  ,HEADER_REC.LotQuantity
			 --  ,HEADER_REC.originationDate
			 --,l_v_total_trx_qty
             ,L_V_INTF_ERROR_CODE
             ,L_V_INTF_ERROR_MESSAGE
			 ,I_P_OIC_ID 
            --  ,HEADER_REC.DOCUMENT_ID
             ) 
   RETURNING HEADER_ORDER_INTF_REC_ID INTO l_v_HEADER_ORDER_INTF_REC_ID;
  -- RETURNING TRANSACTIONINTERFACEID INTO l_v_trx_intf_id;
                        COMMIT;
						--	O_P_RESPONSE := 'After header insert.' || CHR(10) || CHR(10)||;
					--	O_P_RESPONSE:='HEADER_REC.TRADING_PARTNER_ID'||HEADER_REC.TRADING_PARTNER_ID;
					FOR LOTS_REC IN LOT_CUR LOOP
BEGIN --* PREVALIDATION lots
                                l_v_INTF_LINE_ERROR_CODE      := NULL;
                                l_v_INTF_LINE_ERROR_MESSAGE   := NULL;

                                -- validation 1: check if the XML contains all the mandatory fields start
                                 --   IF SHIPMENT_LINE_REC.SL_LINE_SEQUENCE_NUMBER  IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.LineSequenceNumber is missing.'         || '  |  ';  END IF;
                                 --   IF SHIPMENT_LINE_REC.SL_VENDOR_PART_NUMBER    IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.VendorPartNumber is missing.'           || '  |  ';  END IF;
                                 --   IF SHIPMENT_LINE_REC.SL_SHIP_QTY              IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.ShipQty is missing.'                    || '  |  ';  END IF;
                                 --   IF SHIPMENT_LINE_REC.SL_SHIP_QTY_UOM          IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ShipmentLine.ShipQtyUOM is missing.'                 || '  |  ';  END IF;
                                    IF LOTS_REC.LotNumber          IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ItemLevel.Lot Number is missing.'  || '  |  ';  END IF;
									IF LOTS_REC.originationDate          IS NULL THEN    l_v_INTF_LINE_ERROR_MESSAGE := l_v_INTF_LINE_ERROR_MESSAGE || 'Mandatory field ItemLevel.Lot Expiration date is missing.'  || '  |  ';  END IF;
                 END;

				 INSERT INTO XXEDI_SCM_INB_947_L_LOTS_INTF
					(REFERENCE_STG_REC_ID        
,LINE_ITEM_STG_REC_ID         
,HEADER_ORDER_STG_REC_ID   
,HEADER_ORDER_INTF_REC_ID  
, LotNumber                   
, OriginationDate                    
, TransactionQuantity
--,TransactionInterfaceId
					)
					values(
					LOTS_REC.REFERENCE_STG_REC_ID
					,LOTS_REC.LINE_ITEM_STG_REC_ID
					,LOTS_REC.HEADER_ORDER_STG_REC_ID
					,l_v_HEADER_ORDER_INTF_REC_ID
					--,'MODINT-5'--
					,LOTS_REC.LotNumber
					,LOTS_REC.originationDate
					,LOTS_REC.TransactionQuantity
					--,l_v_trx_intf_id
					);
COMMIT;
				  END LOOP;
 END LOOP;

                    UPDATE XXEDI_SCM_INB_947_H_HEADER_ORDER_STG SET PROCESSED_FLAG = 'Y' WHERE HEADER_ORDER_STG_REC_ID = STG_REC.HEADER_ORDER_STG_REC_ID;
                    COMMIT;

                EXCEPTION
                    WHEN OTHERS THEN  
                        L_V_ERROR_CODE := 'PROCESS_DATA_INTO_INTF procedure error';
                        L_V_ERROR_MESSAGE := Substr(SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                        UPDATE XXEDI_SCM_INB_947_H_HEADER_ORDER_STG SET PROCESSED_FLAG = 'E', ERROR_CODE = L_V_ERROR_CODE, ERROR_MESSAGE = L_V_ERROR_MESSAGE
                            WHERE HEADER_ORDER_STG_REC_ID = STG_REC.HEADER_ORDER_STG_REC_ID;
                        IF l_v_HEADER_ORDER_INTF_REC_ID IS NOT NULL THEN
                            DELETE FROM XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF WHERE HEADER_ORDER_INTF_REC_ID = l_v_HEADER_ORDER_INTF_REC_ID;
                        END IF;
                        COMMIT;
                       O_P_RESPONSE := O_P_RESPONSE || 'ERROR' || CHR(10) || CHR(10) || L_V_ERROR_MESSAGE || CHR(10)  || 'HEADER_ORDER_STG_REC_ID: ' || STG_REC.HEADER_ORDER_STG_REC_ID || CHR(10) || 'File name: ' || STG_REC.FILE_NAME || CHR(10) || CHR(10);
                        RAISE;
                END;

              -- O_P_RESPONSE := O_P_RESPONSE || 'Success.' || CHR(10);
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
                        UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF'      THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF WHERE TransactionInterfaceId = v_PK;
                            IF v_count = 0 THEN RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF for the given PK: ' || v_PK); END IF;

                            IF I_P_FLAG_VALUE = 'E' THEN

                                SELECT ERROR_CODE INTO l_v_current_preval_error_code FROM XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF WHERE TransactionInterfaceId = v_PK;
                                IF 
                                        l_v_ERROR_CODE                =  g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE 
                                    AND l_v_current_preval_error_code IS NOT NULL
                                    AND l_v_current_preval_error_code != g_v_PRE_VALIDATION_MISMATCH_ERROR_CODE
                                THEN
                                    l_v_ERROR_CODE := l_v_current_preval_error_code;
                                END IF;

                                    UPDATE XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF SET
                                        PROCESSED_FLAG      = I_P_FLAG_VALUE
                                        ,ERROR_CODE          = l_v_ERROR_CODE
                                        ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                        ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                        ,LAST_UPDATE_DATE    = SYSDATE
                                        ,LAST_UPDATE_BY_NAME = 'OIC'
                                    WHERE TransactionInterfaceId = v_PK;
                                 /*   UPDATE XXEDI_SCM_INB_947_LINES_INTF SET
                                            PROCESSED_FLAG      = I_P_FLAG_VALUE
                                            ,ERROR_CODE          = 'PARENT_ERROR'
                                            ,ERROR_MESSAGE       = 'Some error occured when processing the record. Check parent table (XXEDI_SCM_INB_947_H_HEADER_ORDER_INTF) for details. Error code: ' || l_v_ERROR_CODE
                                            ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                            ,LAST_UPDATE_DATE    = SYSDATE
                                            ,LAST_UPDATE_BY_NAME = 'OIC'
                                    WHERE HEADER_ORDER_INTF_REC_ID = v_PK AND PROCESSED_FLAG != 'E' AND ERROR_CODE IS NULL;*/
                            ELSIF I_P_FLAG_VALUE IN ('Y', 'N') THEN
                                    UPDATE XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF   SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE TransactionInterfaceId = v_PK;
                                  --  UPDATE XXEDI_SCM_INB_947_LINES_INTF    SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_INTF_REC_ID = v_PK;
                            END IF;


                       /* ELSIF UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_947_LINES_INTF'        THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_947_LINES_INTF WHERE LINES_INTF_REC_ID = v_PK;
                            IF v_count = 0 THEN
                                RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_947_LINES_INTF for the given PK: ' || v_PK);
                            END IF;
                            UPDATE XXEDI_SCM_INB_947_LINES_INTF
                                SET
                                    PROCESSED_FLAG      = I_P_FLAG_VALUE
                                    ,ERROR_CODE          = l_v_ERROR_CODE
                                    ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                    ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                    ,LAST_UPDATE_DATE    = SYSDATE
                                    ,LAST_UPDATE_BY_NAME = 'OIC'
                                WHERE
                                    LINES_INTF_REC_ID = v_PK;*/


                        ELSIF   UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF'   THEN

                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF WHERE TransactionInterfaceId = v_PK;
                            IF v_count = 0 THEN
                                RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_947_H_HEADER_ORDER_INTF for the given PK: ' || v_PK);
                            END IF;
                            UPDATE XXEDI_SCM_INB_947_H_HEADER_ORDER_STG                  SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = l_v_ERROR_CODE, ERROR_MESSAGE = l_v_ERROR_MESSAGE, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_H_ADDRESS_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_LINE_ITEM_STG                       SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_ITEM_DETAIL_STG                     SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_L_PRODUCT_OR_ITEM_DESC_STG          SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_L_REFERENCE_STG                     SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;
                            UPDATE XXEDI_SCM_INB_947_SUMMARY_STG                         SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL          , ERROR_MESSAGE = NULL             , OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_ORDER_STG_REC_ID = v_PK;



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
                        'UnprocessedInventoryTransactions' VALUE (
                            SELECT
                                JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                            header_intf.TransactionInterfaceId
                                            ,header_intf.FILE_NAME
                                            ,header_intf.TransactionHeaderId
											 ,header_intf.SourceCode,
            header_intf.SourceLineId,
            header_intf.SourceHeaderId,
            header_intf.TransactionMode,
            header_intf.TransactionQuantity,
            header_intf.TransactionUOM,
            header_intf.TransactionDate,
            header_intf.SubinventoryCode,
            header_intf.TransactionTypeName,
            header_intf.AdjReasonCode,
            header_intf.ItemNumber,
            header_intf.OrganizationName,
            header_intf.UseCurrentCostFlag,
            header_intf.ExternalSystemTransactionReference
                                            ,'lots' VALUE
                                                (
                                                    SELECT
                                                        JSON_ARRAYAGG(
                                                            JSON_OBJECT(
                                                                header_intf.TransactionInterfaceId
                                            ,line_intf.LotNumber
                                            ,line_intf.OriginationDate
											 ,line_intf.TransactionQuantity
                                                                RETURNING CLOB
                                                            )

                                                        RETURNING CLOB
                                                    )
                                                    FROM XXEDI_SCM_INB_947_L_LOTS_INTF line_intf
                                                    WHERE line_intf.HEADER_ORDER_INTF_REC_ID = header_intf.HEADER_ORDER_INTF_REC_ID
													  and  line_intf.LINE_ITEM_STG_REC_ID= header_intf.LINE_ITEM_STG_REC_ID
                                                )                                
                                            RETURNING CLOB
                                        )

                                    RETURNING CLOB
                                )
                            FROM
                                XXEDI_SCM_INB_947_INV_TRANSACTIONS_INTF header_intf
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


END XXEDI_SCM_INB_947_WSA_PKG;