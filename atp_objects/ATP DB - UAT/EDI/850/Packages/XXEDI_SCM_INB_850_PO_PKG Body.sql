create or replace PACKAGE BODY XXEDI_SCM_INB_850_PO_PKG AS
-- v17 - 

    g_v_EDI_850_doc_type                CONSTANT VARCHAR2(100) := 'EDI_850';
    g_v_maximum_transform_rows_per_exec CONSTANT NUMBER        := 10;


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
            O_P_RESPONSE := 'XXEDI_SCM_INB_850_XML_DATA_STG Procedure Started' || CHR(10) || CHR(10);
            INSERT INTO XXEDI_SCM_INB_850_XML_DATA_STG (
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
                        ,g_v_EDI_850_doc_type   AS DOC_TYPE
                    FROM DUAL;
            COMMIT;
            O_P_RESPONSE := O_P_RESPONSE || '    XML data loaded into XXEDI_SCM_INB_850_XML_DATA_STG' || CHR(10) || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || '    Invoking PARSE_XML_INTO_STG Procedure'               || CHR(10) || CHR(10);
            -- INVOKE PARSE_XML_INTO_STG PROCEDURE
            PARSE_XML_INTO_STG(
                I_P_OIC_ID
                ,L_V_CHILD_PROCEDURE_RESPONSE
                ,L_V_CHILD_PROCEDURE_STATUS
            );
            O_P_RESPONSE := O_P_RESPONSE || L_V_CHILD_PROCEDURE_RESPONSE                 || CHR(10) || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || '    PARSE_XML_INTO_STG Procedure completed' || CHR(10) || CHR(10);
            O_P_RESPONSE := O_P_RESPONSE || 'LOAD_XML_INTO_RAW_STG Procedure completed'  || CHR(10) || CHR(10);
            O_P_STATUS := L_V_CHILD_PROCEDURE_STATUS;
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
            v_days_to_keep_file_XML_data NUMBER := 90;

            v_xpath                     VARCHAR2(4000);
            v_ERROR_CODE                VARCHAR2(64);
            v_ERROR_MESSAGE             VARCHAR2(4000);
            v_RESPONSE                  VARCHAR2(4000);

            XML_TEST_PAYLOAD                    CLOB;
            XML_DATA_STG_REC                    XXEDI_SCM_INB_850_XML_DATA_STG%ROWTYPE;
            XML_DATA                            XMLTYPE;
            XML_H                               XMLTYPE;
            XML_H_Header                        XMLTYPE;
            XML_H_PAYMENT_TERMS                 XMLTYPE;
            XML_H_DATES                         XMLTYPE;
            XML_H_ADDRESS                       XMLTYPE;
            XML_H_ADDRESS_Contacts              XMLTYPE;
            XML_H_CARRIER_INFORMATION           XMLTYPE;
            XML_H_NOTES                         XMLTYPE;
            XML_H_CHARGES_ALLOWANCES            XMLTYPE;
            XML_H_QUANTITY_TOTALS               XMLTYPE;
            XML_H_FOB_RELATED_INSTRUCTION       XMLTYPE;
            XML_H_REFERENCES                    XMLTYPE;
            XML_H_CONTACTS                      XMLTYPE;
            XML_L                               XMLTYPE;
            XML_L_order_line                    XMLTYPE;
            XML_L_product_or_item_desc          XMLTYPE;
            XML_L_physical_details              XMLTYPE;
            XML_L_charges_allowances            XMLTYPE;
            XML_L_notes                         XMLTYPE;
            XML_S_Summary                       XMLTYPE;

            l_v_HEADER_REC_ID                   NUMBER;
            l_v_LINE_REC_ID                     NUMBER;

            l_trading_partner_id                VARCHAR2(150);    --  
            l_purchase_order_number             VARCHAR2(150);    --  
            l_xml                               CLOB;             --  
            l_xml_type                          XMLTYPE;          --  
            L_FILE_NAME                         VARCHAR2(255);    --  
            l_sequence_number                   NUMBER;           --  
            l_sequence_number_allowance         NUMBER;           --  
            l_exists_allowance                  VARCHAR2(1);      --  

        BEGIN
            O_P_RESPONSE := 'PARSE_XML_INTO_STG Procedure Started' || CHR(10) || O_P_RESPONSE;

            SELECT * INTO XML_DATA_STG_REC FROM XXEDI_SCM_INB_850_XML_DATA_STG WHERE OIC_INSTANCE_ID = I_P_OIC_ID AND PROCESSED_FLAG = 'N' AND DOC_TYPE = g_v_EDI_850_doc_type; --!

            UPDATE XXEDI_SCM_INB_850_XML_DATA_STG SET PROCESSED_FLAG = 'P' WHERE XML_CONTENT_REC_ID = XML_DATA_STG_REC.XML_CONTENT_REC_ID; COMMIT;
            BEGIN -- block that will check if the XML can be parsed >>
                -- XML_DATA := XMLTYPE(XML_DATA_STG_REC.XML_DATA);
                l_xml_type := XMLTYPE(XML_DATA_STG_REC.XML_DATA);
                L_FILE_NAME := XML_DATA_STG_REC.FILE_NAME;
            EXCEPTION
                WHEN OTHERS THEN
                    v_ERROR_CODE    := 'XML cannot be parsed';
                    v_ERROR_MESSAGE := Substr( v_ERROR_CODE || ' : The XML provided is invalid. Details:' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    Error:' || v_ERROR_MESSAGE || ' | File_Name: "' || XML_DATA_STG_REC.FILE_NAME || '"';
                    UPDATE XXEDI_SCM_INB_850_XML_DATA_STG SET PROCESSED_FLAG = 'E' ,ERROR_CODE = v_ERROR_CODE, ERROR_MESSAGE = v_ERROR_MESSAGE WHERE XML_CONTENT_REC_ID = XML_DATA_STG_REC.XML_CONTENT_REC_ID; COMMIT;
                    RAISE;
            END; -- block that will check if the XML can be parsed <<

            BEGIN -- block to insert XML data into stage tables >>

                    SELECT  x.trading_partner_id,  x.purchase_order_number
                    INTO    l_trading_partner_id,  l_purchase_order_number
                    FROM  XMLTABLE ( '/Order/Header/OrderHeader'  PASSING l_xml_type
                            COLUMNS
                                trading_partner_id VARCHAR2(150)    PATH  'TradingPartnerId',
                                purchase_order_number VARCHAR2(150) PATH  'PurchaseOrderNumber'
                        ) x;
                        
                    FOR header_rec IN ( SELECT
                                    l_file_name,
                                    l_trading_partner_id,
                                    l_purchase_order_number,
                                    XXEDI_SCM_INB_850_SOURCE_ORDER_S.NEXTVAL,
                                    x.t_set_purpose_code,
                                    x.primary_po_type_code,
                                    x.release_number,
                                    x.purchase_order_date,
                                    x.purchase_order_time,
                                    x.contract_type,
                                    x.ship_complete_code,
                                    x.buyers_currency,
                                    x.exchange_rate,
                                    x.department,
                                    x.department_description,
                                    x.vendor,
                                    x.division,
                                    x.customer_account_number,
                                    x.customer_order_number
                                FROM
                                    XMLTABLE ( '/Order/Header/OrderHeader'
                                            PASSING l_xml_type
                                        COLUMNS
                                            t_set_purpose_code VARCHAR2(50) PATH 'TsetPurposeCode',
                                            primary_po_type_code VARCHAR2(50) PATH 'PrimaryPOTypeCode',
                                            release_number VARCHAR2(50) PATH 'ReleaseNumber',
                                            purchase_order_date VARCHAR2(50) PATH 'PurchaseOrderDate',
                                            purchase_order_time VARCHAR2(50) PATH 'PurchaseOrderTime',
                                            contract_type VARCHAR2(50) PATH 'ContractType',
                                            ship_complete_code VARCHAR2(50) PATH 'ShipCompleteCode',
                                            buyers_currency VARCHAR2(50) PATH 'BuyersCurrency',
                                            exchange_rate VARCHAR2(50) PATH 'ExchangeRate',
                                            department VARCHAR2(50) PATH 'Department',
                                            department_description VARCHAR2(50) PATH 'DepartmentDescription',
                                            vendor VARCHAR2(50) PATH 'Vendor',
                                            division VARCHAR2(50) PATH 'Division',
                                            customer_account_number VARCHAR2(50) PATH 'CustomerAccountNumber',
                                            customer_order_number VARCHAR2(50) PATH 'CustomerOrderNumber'
                                    ) x )
                    LOOP
                    -- Insert into orders table
                    INSERT INTO XXEDI_SCM_INB_850_HEADERS_STG2 (
                        file_name,
                        trading_partner_id,
                        purchase_order_number,
                        source_header_id,
                        t_set_purpose_code,
                        primary_po_type_code,
                        release_number,
                        purchase_order_date,
                        purchase_order_time,
                        contract_type,
                        ship_complete_code,
                        buyers_currency,
                        exchange_rate,
                        department,
                        department_description,
                        vendor,
                        division,
                        customer_account_number,
                        customer_order_number,
                        creation_date,
                        created_by_name,
                        last_update_date,
                        last_update_by_name,
                        processed_flag,
                        oic_instance_id,
                        target_system_document_type,
                        target_system_document_number
                    ) VALUES (
                            l_file_name,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            XXEDI_SCM_INB_850_SOURCE_ORDER_S.NEXTVAL,
                            header_rec.t_set_purpose_code,
                            header_rec.primary_po_type_code,
                            header_rec.release_number,
                            header_rec.purchase_order_date,
                            header_rec.purchase_order_time,
                            header_rec.contract_type,
                            header_rec.ship_complete_code,
                            header_rec.buyers_currency,
                            header_rec.exchange_rate,
                            header_rec.department,
                            header_rec.department_description,
                            header_rec.vendor,
                            header_rec.division,
                            header_rec.customer_account_number,
                            header_rec.customer_order_number,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder',
                            l_file_name
                    )RETURNING HEADER_REC_ID INTO l_v_HEADER_REC_ID;
                    
                    END LOOP;

                    FOR rec_payments IN (
                        SELECT
                            x.v_termstype,
                            x.v_termsbasisdatecode,
                            x.v_termsdiscountpercentage,
                            x.v_termsdiscountdate,
                            x.v_termsdiscountduedays,
                            x.v_termsnetduedate,
                            x.v_termsnetduedays,
                            x.v_termsdiscountamount,
                            x.v_percentofinvoicepayable,
                            x.v_termsdescription,
                            x.v_termsdueday,
                            x.v_paymentmethodcode,
                            x.v_termsduedatequal,
                            x.v_amountsubjecttodiscount,
                            x.v_discountamountdue
                        FROM
                            XMLTABLE ( '/Order/Header/PaymentTerms'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_termstype VARCHAR2(50) PATH 'TermsType',
                                    v_termsbasisdatecode VARCHAR2(50) PATH 'TermsBasisDateCode',
                                    v_termsdiscountpercentage VARCHAR2(50) PATH 'TermsDiscountPercentage',
                                    v_termsdiscountdate VARCHAR2(50) PATH 'TermsDiscountDate',
                                    v_termsdiscountduedays VARCHAR2(50) PATH 'TermsDiscountDueDays',
                                    v_termsnetduedate VARCHAR2(50) PATH 'TermsNetDueDate',
                                    v_termsnetduedays VARCHAR2(50) PATH 'TermsNetDueDays',
                                    v_termsdiscountamount VARCHAR2(50) PATH 'TermsDiscountAmount',
                                    v_percentofinvoicepayable VARCHAR2(50) PATH 'PercentOfInvoicePayable',
                                    v_termsdescription VARCHAR2(50) PATH 'TermsDescription',
                                    v_termsdueday VARCHAR2(50) PATH 'TermsDueDay',
                                    v_paymentmethodcode VARCHAR2(50) PATH 'PaymentMethodCode',
                                    v_termsduedatequal VARCHAR2(50) PATH 'TermsDueDateQual',
                                    v_amountsubjecttodiscount VARCHAR2(50) PATH 'AmountSubjectToDiscount',
                                    v_discountamountdue VARCHAR2(50) PATH 'DiscountAmountDue'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_payment_terms_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            terms_type,
                            terms_basis_date_code,
                            terms_discount_percentage,
                            terms_discount_date,
                            terms_discount_due_days,
                            terms_net_due_date,
                            terms_net_due_days,
                            terms_discount_amount,
                            percent_of_invoice_payable,
                            terms_description,
                            terms_due_day,
                            payment_method_code,
                            terms_due_date_qual,
                            amount_subject_to_discount,
                            discount_amount_due,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_payments.v_termstype,
                            rec_payments.v_termsbasisdatecode,
                            rec_payments.v_termsdiscountpercentage,
                            rec_payments.v_termsdiscountdate,
                            rec_payments.v_termsdiscountduedays,
                            rec_payments.v_termsnetduedate,
                            rec_payments.v_termsnetduedays,
                            rec_payments.v_termsdiscountamount,
                            rec_payments.v_percentofinvoicepayable,
                            rec_payments.v_termsdescription,
                            rec_payments.v_termsdueday,
                            rec_payments.v_paymentmethodcode,
                            rec_payments.v_termsduedatequal,
                            rec_payments.v_amountsubjecttodiscount,
                            rec_payments.v_discountamountdue,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    FOR rec_dates IN (
                        SELECT
                            x.v_datetimequalifier,
                            x.v_date,
                            x.v_time,
                            x.v_datetimeperiod
                        FROM
                            XMLTABLE ( '/Order/Header/Dates'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_datetimequalifier VARCHAR2(50) PATH 'DateTimeQualifier',
                                    v_date VARCHAR2(50) PATH 'Date',
                                    v_time VARCHAR2(50) PATH 'Time',
                                    v_datetimeperiod VARCHAR2(50) PATH 'DateTimePeriod'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_dates_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            date_time_qualifier,
                            date_order,
                            time_order,
                            date_time_period,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_dates.v_datetimequalifier,
                            rec_dates.v_date,
                            rec_dates.v_time,
                            rec_dates.v_datetimeperiod,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;
                    
                    FOR rec_addresses IN (
                        SELECT
                            x.v_addresstypecode,
                            x.v_locationcodequalifier,
                            x.v_addresslocationnumber,
                            x.v_addressname,
                            x.v_addressalternatename,
                            x.v_addressalternatename2,
                            x.v_address1,
                            x.v_address2,
                            x.v_address3,
                            x.v_address4,
                            x.v_city,
                            x.v_state,
                            x.v_postalcode,
                            x.v_country,
                            x.v_locationid
                        FROM
                            XMLTABLE ( '/Order/Header/Address'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_addresstypecode VARCHAR2(50) PATH 'AddressTypeCode',
                                    v_locationcodequalifier VARCHAR2(50) PATH 'LocationCodeQualifier',
                                    v_addresslocationnumber VARCHAR2(50) PATH 'AddressLocationNumber',
                                    v_addressname VARCHAR2(100) PATH 'AddressName',
                                    v_addressalternatename VARCHAR2(100) PATH 'AddressAlternateName',
                                    v_addressalternatename2 VARCHAR2(100) PATH 'AddressAlternateName2',
                                    v_address1 VARCHAR2(100) PATH 'Address1',
                                    v_address2 VARCHAR2(100) PATH 'Address2',
                                    v_address3 VARCHAR2(100) PATH 'Address3',
                                    v_address4 VARCHAR2(100) PATH 'Address4',
                                    v_city VARCHAR2(50) PATH 'City',
                                    v_state VARCHAR2(50) PATH 'State',
                                    v_postalcode VARCHAR2(20) PATH 'PostalCode',
                                    v_country VARCHAR2(50) PATH 'Country',
                                    v_locationid VARCHAR2(50) PATH 'LocationID'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_addresses_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            address_typecode,
                            location_code_qualifier,
                            address_location_number,
                            address_name,
                            address_alternate_name,
                            address_alternate_name2,
                            address1,
                            address2,
                            address3,
                            address4,
                            city,
                            state_address,
                            postalcode,
                            country,
                            location_id,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_header_rec_id,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_addresses.v_addresstypecode,
                            rec_addresses.v_locationcodequalifier,
                            rec_addresses.v_addresslocationnumber,
                            rec_addresses.v_addressname,
                            rec_addresses.v_addressalternatename,
                            rec_addresses.v_addressalternatename2,
                            rec_addresses.v_address1,
                            rec_addresses.v_address2,
                            rec_addresses.v_address3,
                            rec_addresses.v_address4,
                            rec_addresses.v_city,
                            rec_addresses.v_state,
                            rec_addresses.v_postalcode,
                            rec_addresses.v_country,
                            rec_addresses.v_locationid,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    COMMIT;

                    /*FOR rec_address_contacts IN (
                        SELECT
                            x.v_contacttypecode,
                            x.v_contactname,
                            x.v_primaryphone,
                            x.v_primaryfax,
                            x.v_primaryemail
                        FROM
                            XMLTABLE ( '/Order/Header/Address/Contacts'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_contacttypecode VARCHAR2(50) PATH 'ContactTypeCode',
                                    v_contactname VARCHAR2(100) PATH 'ContactName',
                                    v_primaryphone VARCHAR2(50) PATH 'PrimaryPhone',
                                    v_primaryfax VARCHAR2(50) PATH 'PrimaryFax',
                                    v_primaryemail VARCHAR2(100) PATH 'PrimaryEmail'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_contacts_stg (
                            trading_partner_id,
                            purchase_order_number,
                            contact_type_code,
                            contac_tname,
                            primary_phone,
                            primary_fax,
                            primary_email,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_address_contacts.v_contacttypecode,
                            rec_address_contacts.v_contactname,
                            rec_address_contacts.v_primaryphone,
                            rec_address_contacts.v_primaryfax,
                            rec_address_contacts.v_primaryemail,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            oic_id,
                            'SalesOrder'
                        );

                    END LOOP;*/

                    FOR rec_address_refs IN (
                        SELECT
                            x.v_referencequal,
                            x.v_referenceid,
                            x.v_description,
                            x.v_date,
                            x.v_time
                        FROM
                            XMLTABLE ( '/Order/Header/References' --alterado de acordo com estrutura de arq enviado
                                    PASSING l_xml_type
                                COLUMNS
                                    v_referencequal VARCHAR2(50) PATH 'ReferenceQual',
                                    v_referenceid VARCHAR2(100) PATH 'ReferenceID',
                                    v_description VARCHAR2(200) PATH 'Description',
                                    v_date VARCHAR2(50) PATH 'Date',
                                    v_time VARCHAR2(50) PATH 'Time'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_references_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            reference_qual,
                            reference_id,
                            description,
                            date_reference,
                            time_reference,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_address_refs.v_referencequal,
                            rec_address_refs.v_referenceid,
                            rec_address_refs.v_description,
                            rec_address_refs.v_date,
                            rec_address_refs.v_time,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    FOR rec_fob IN (
                        SELECT
                            x.v_fobpaycode,
                            x.v_foblocationqualifier,
                            x.v_foblocationdescription,
                            x.v_fobtitlepassagecode,
                            x.v_fobtitlepassagelocation,
                            x.v_transportationtermstype,
                            x.v_transportationterms,
                            x.v_description
                        FROM
                            XMLTABLE ( '/Order/Header/FOBRelatedInstruction'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_fobpaycode VARCHAR2(50) PATH 'FOBPayCode',
                                    v_foblocationqualifier VARCHAR2(50) PATH 'FOBLocationQualifier',
                                    v_foblocationdescription VARCHAR2(200) PATH 'FOBLocationDescription',
                                    v_fobtitlepassagecode VARCHAR2(50) PATH 'FOBTitlePassageCode',
                                    v_fobtitlepassagelocation VARCHAR2(200) PATH 'FOBTitlePassageLocation',
                                    v_transportationtermstype VARCHAR2(50) PATH 'TransportationTermsType',
                                    v_transportationterms VARCHAR2(200) PATH 'TransportationTerms',
                                    v_description VARCHAR2(200) PATH 'Description'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_fob_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            fob_pay_code,
                            fob_location_qualifier,
                            fob_location_description,
                            fob_title_passage_code,
                            fob_title_passage_location,
                            transportation_terms_type,
                            transportation_terms,
                            description_fob,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_fob.v_fobpaycode,
                            rec_fob.v_foblocationqualifier,
                            rec_fob.v_foblocationdescription,
                            rec_fob.v_fobtitlepassagecode,
                            rec_fob.v_fobtitlepassagelocation,
                            rec_fob.v_transportationtermstype,
                            rec_fob.v_transportationterms,
                            rec_fob.v_description,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    COMMIT;

                    FOR rec_carrier IN (
                        SELECT
                            x.v_carriertransmethodcode,
                            x.v_carrieralphacode,
                            x.v_carrierrouting,
                            x.v_routingsequencecode,
                            x.v_servicelevelcodes
                        FROM
                            XMLTABLE ( '/Order/Header/CarrierInformation'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_carriertransmethodcode VARCHAR2(50) PATH 'CarrierTransMethodCode',
                                    v_carrieralphacode VARCHAR2(50) PATH 'CarrierAlphaCode',
                                    v_carrierrouting VARCHAR2(200) PATH 'CarrierRouting',
                                    v_routingsequencecode VARCHAR2(50) PATH 'RoutingSequenceCode',
                                    v_servicelevelcodes VARCHAR2(50) PATH 'ServiceLevelCodes/ServiceLevelCode'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_carrier_info_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            carrier_trans_method_code,
                            carrier_alpha_code,
                            carrier_routing,
                            routing_sequence_code,
                            service_level_code,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_carrier.v_carriertransmethodcode,
                            rec_carrier.v_carrieralphacode,
                            rec_carrier.v_carrierrouting,
                            rec_carrier.v_routingsequencecode,
                            rec_carrier.v_servicelevelcodes,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    COMMIT;

                    FOR rec_refs IN (
                        SELECT
                            x.v_referencequal,
                            x.v_referenceid,
                            x.v_description,
                            x.v_date,
                            x.v_time
                        FROM
                            XMLTABLE ( '/Order/Header/References'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_referencequal VARCHAR2(50) PATH 'ReferenceQual',
                                    v_referenceid VARCHAR2(50) PATH 'ReferenceID',
                                    v_description VARCHAR2(200) PATH 'Description',
                                    v_date VARCHAR2(50) PATH 'Date',
                                    v_time VARCHAR2(50) PATH 'Time'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_references_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            reference_qual,
                            reference_id,
                            description,
                            date_reference,
                            time_reference,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_refs.v_referencequal,
                            rec_refs.v_referenceid,
                            rec_refs.v_description,
                            rec_refs.v_date,
                            rec_refs.v_time,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    COMMIT;

                    FOR rec_notes_refs IN (
                        SELECT
                            x.v_referencequal,
                            x.v_referenceid,
                            x.v_description
                        FROM
                            XMLTABLE ( '/Order/Header/Notes'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_referencequal VARCHAR2(50) PATH 'NoteCode',
                                    v_referenceid VARCHAR2(200) PATH 'Note',
                                    v_description VARCHAR2(50) PATH 'LanguageCode'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_references_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            reference_qual,
                            reference_id,
                            description,
                            date_reference,
                            time_reference,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_notes_refs.v_referencequal,
                            rec_notes_refs.v_referenceid,
                            rec_notes_refs.v_description,
                            NULL,
                            NULL,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    l_sequence_number_allowance := 0;

                    FOR rec_line_item IN (
                                SELECT
                                    x.*
                                FROM
                                    XMLTABLE ( '/Order/LineItem'
                                            PASSING l_xml_type

                                    ) x 
                            ) LOOP

                            l_sequence_number_allowance := l_sequence_number_allowance + 1;

                            FOR rec_lines_charge_allowance IN (
                                SELECT
                                    x.v_AllowChrgRate,
                                    x.v_AllowChrgPercent,
                                    x.v_AllowChrgAmt
                                FROM
                                    XMLTABLE ( 'Order/LineItem/ChargesAllowances'
                                            PASSING l_xml_type
                                        COLUMNS v_AllowChrgAmt VARCHAR2(50) PATH 'AllowChrgAmt',
                                                v_AllowChrgPercent VARCHAR2(50) PATH 'AllowChrgPercent',
                                                v_AllowChrgRate VARCHAR2(50) PATH 'AllowChrgRate'

                                    ) x 
                            ) LOOP

                                INSERT INTO XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG2 (
                                    header_rec_id,
                                    line_rec_id,
                                    trading_partner_id,
                                    purchase_order_number,
                                    line_sequence_number,
                                    ALLOW_CHRG_AMT,
                                    ALLOW_CHRG_PERCENT, 
                                    ALLOW_CHRG_RATE,						
                                    --date_time_period,
                                    creation_date,
                                    created_by_name,
                                    last_update_date,
                                    last_update_by_name,
                                    processed_flag,
                                    oic_instance_id,
                                    target_system_document_type
                                ) VALUES (
                                    l_v_HEADER_REC_ID,
                                    l_v_LINE_REC_ID,
                                    l_trading_partner_id,
                                    l_purchase_order_number,
                                    l_sequence_number_allowance, --rec_lines.v_linesequencenumber,
                                    rec_lines_charge_allowance.v_AllowChrgAmt,
                                    rec_lines_charge_allowance.v_AllowChrgPercent,
                                    rec_lines_charge_allowance.v_AllowChrgRate,
                                    --rec_lines_dates.v_datetimeperiod,
                                    sysdate,
                                    'INTEGRATION',
                                    sysdate,
                                    'INTEGRATION',
                                    'N',
                                    I_P_OIC_ID,
                                    'SalesOrder'
                                );

                            END LOOP;	

                            COMMIT;

                    END LOOP;

                    l_sequence_number := 0;

                    FOR rec_lines IN (
                        SELECT
                            x.v_linesequencenumber,
                            x.v_buyerpartnumber,
                            x.v_vendorpartnumber,
                            x.v_consumerpackagecode,
                            x.v_allowchrgamt,
                            x.v_allowchrgpercent,
                            x.v_ean,
                            x.v_gtin,
                            x.v_upccasecode,
                            x.v_natldrugcode,
                            x.v_internationalstandardbooknumber,
                            x.v_orderqty,
                            x.v_orderqtyuom,
                            x.v_purchasepricetype,
                            x.v_purchaseprice,
                            x.v_purchasepricebasis,
                            x.v_extendeditemtotal,
                            x.v_productsizecode,
                            x.v_productsizedescription,
                            x.v_productcolorcode,
                            x.v_productcolordescription,
                            x.v_productprocesscode,
                            x.v_productprocessdescription,
                            x.v_department,
                            x.XML_fragment_ChargesAllowances
                        FROM
                            XMLTABLE ( '/Order/LineItem'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_linesequencenumber VARCHAR2(50) PATH '/LineItem/OrderLine/LineSequenceNumber',
                                    v_buyerpartnumber VARCHAR2(50) PATH '/LineItem/OrderLine/BuyerPartNumber',
                                    v_vendorpartnumber VARCHAR2(50) PATH '/LineItem/OrderLine/VendorPartNumber',
                                    v_consumerpackagecode VARCHAR2(50) PATH '/LineItem/OrderLine/ConsumerPackageCode',
                                    v_allowchrgamt VARCHAR2(50) PATH '/LineItem/OrderLine/AllowChrgAmt',
                                    v_allowchrgpercent VARCHAR2(50) PATH '/LineItem/OrderLine/AllowChrgPercent',
                                    v_ean VARCHAR2(50) PATH '/LineItem/OrderLine/EAN',
                                    v_gtin VARCHAR2(50) PATH '/LineItem/OrderLine/GTIN',
                                    v_upccasecode VARCHAR2(50) PATH '/LineItem/OrderLine/UPCCaseCode',
                                    v_natldrugcode VARCHAR2(50) PATH '/LineItem/OrderLine/NatlDrugCode',
                                    v_internationalstandardbooknumber VARCHAR2(50) PATH '/LineItem/OrderLine/InternationalStandardBookNumber',
                                    v_orderqty NUMBER PATH '/LineItem/OrderLine/OrderQty',
                                    v_orderqtyuom VARCHAR2(50) PATH '/LineItem/OrderLine/OrderQtyUOM',
                                    v_purchasepricetype VARCHAR2(50) PATH '/LineItem/OrderLine/PurchasePriceType',
                                    v_purchaseprice NUMBER PATH '/LineItem/OrderLine/PurchasePrice',
                                    v_purchasepricebasis VARCHAR2(50) PATH '/LineItem/OrderLine/PurchasePriceBasis',
                                    v_extendeditemtotal NUMBER PATH '/LineItem/OrderLine/ExtendedItemTotal',
                                    v_productsizecode VARCHAR2(50) PATH '/LineItem/OrderLine/ProductSizeCode',
                                    v_productsizedescription VARCHAR2(200) PATH '/LineItem/OrderLine/ProductSizeDescription',
                                    v_productcolorcode VARCHAR2(50) PATH '/LineItem/OrderLine/ProductColorCode',
                                    v_productcolordescription VARCHAR2(200) PATH '/LineItem/OrderLine/ProductColorDescription',
                                    v_productprocesscode VARCHAR2(50) PATH '/LineItem/OrderLine/ProductProcessCode',
                                    v_productprocessdescription VARCHAR2(200) PATH '/LineItem/OrderLine/ProductProcessDescription',
                                    v_department VARCHAR2(50) PATH '/LineItem/OrderLine/Department',

                                    XML_fragment_ChargesAllowances                         XMLTYPE         PATH '/LineItem/ChargesAllowances'
                            ) x
                    ) LOOP

                        l_sequence_number := l_sequence_number + 1;
                        INSERT INTO xxedi_scm_inb_850_lines_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            line_sequence_number,
                            buyer_part_number,
                            vendor_part_number,
                            consumer_package_code,
                            allow_chrg_amt,
                            allow_chrg_percent,
                            ean,
                            gtin,
                            upc_case_code,
                            natl_drug_code,
                            international_standard_book_number,
                            order_qty,
                            order_qty_uom,
                            purchase_price_type,
                            purchase_price,
                            purchase_price_basis,
                            extended_item_total,
                            product_size_code,
                            product_size_description,
                            product_color_code,
                            product_color_description,
                            product_process_code,
                            product_process_description,
                            department,
                            ATTRIBUTE_CHAR3,
                            ATTRIBUTE_CHAR1,
                            ATTRIBUTE_CHAR2,
                            --ATTRIBUTE_NUMBER1,
                            ATTRIBUTE_NUMBER2,
                            ATTRIBUTE_CHAR4,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,                                 --trading_partner_id,
                            l_purchase_order_number,                              --purchase_order_number,
                            l_sequence_number, /*rec_lines.v_linesequencenumber,*/                       --line_sequence_number,
                            rec_lines.v_buyerpartnumber,                          --buyer_part_number,
                            rec_lines.v_vendorpartnumber,                         --vendor_part_number,
                            rec_lines.v_consumerpackagecode,                      --consumer_package_code,
                            rec_lines.v_allowchrgamt,                             --allow_chrg_amt,
                            rec_lines.v_allowchrgpercent,                         --allow_chrg_percent,
                            rec_lines.v_ean,                                      --ean,
                            rec_lines.v_gtin,                                     --gtin,
                            rec_lines.v_upccasecode,                              --upc_case_code,
                            rec_lines.v_natldrugcode,                             --natl_drug_code,
                            rec_lines.v_internationalstandardbooknumber,          --international_standard_book_number,
                            rec_lines.v_orderqty,                                 --order_qty,
                            rec_lines.v_orderqtyuom,                              --order_qty_uom,
                            rec_lines.v_purchasepricetype,                        --purchase_price_type,
                            rec_lines.v_purchaseprice,                            --purchase_price,
                            rec_lines.v_purchasepricebasis,                       --purchase_price_basis,
                            rec_lines.v_extendeditemtotal,                        --extended_item_total,
                            rec_lines.v_productsizecode,                          --product_size_code,
                            rec_lines.v_productsizedescription,                   --product_size_description,
                            rec_lines.v_productcolorcode,                         --product_color_code,
                            rec_lines.v_productcolordescription,                  --product_color_description,
                            rec_lines.v_productprocesscode,                       --product_process_code,
                            rec_lines.v_productprocessdescription,                --product_process_description,
                            rec_lines.v_department,                               --department,
                            rec_lines.v_gtin,                                     --ATTRIBUTE_CHAR3,
                            rec_lines.v_buyerpartnumber,                          --ATTRIBUTE_CHAR1,
                            rec_lines.v_consumerpackagecode,                      --ATTRIBUTE_CHAR2,
                            --rec_lines.v_purchaseprice                             --ATTRIBUTE_NUMBER1,
                            '',                                                   --ATTRIBUTE_NUMBER2,
                            rec_lines.v_upccasecode,                              --ATTRIBUTE_CHAR4,
                            sysdate,                                              --creation_date,
                            'INTEGRATION',                                        --created_by_name,                       
                            sysdate,                                              --last_update_date,                       
                            'INTEGRATION',                                        --last_update_by_name,                       
                            'N',                                                  --processed_flag,                       
                            I_P_OIC_ID,                                               --oic_instance_id,                       
                            'SalesOrder'                                          --target_system_document_type                       

                        );         

                        COMMIT;

                        FOR rec_lines_charge_allowance IN (
                            SELECT
                                x.v_AllowChrgRate,
                                x.v_AllowChrgPercent,
                                x.v_AllowChrgAmt
                            FROM
                                XMLTABLE ( '/ChargesAllowances' PASSING rec_lines.XML_fragment_ChargesAllowances
                                    COLUMNS v_AllowChrgAmt VARCHAR2(50) PATH 'AllowChrgAmt',
                                            v_AllowChrgPercent VARCHAR2(50) PATH 'AllowChrgPercent',
                                            v_AllowChrgRate VARCHAR2(50) PATH 'AllowChrgRate'

                                ) x 
                        ) LOOP

                            INSERT INTO XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG2 (
                                header_rec_id,
                                line_rec_id,
                                trading_partner_id,
                                purchase_order_number,
                                line_sequence_number,
                                ALLOW_CHRG_AMT,
                                ALLOW_CHRG_PERCENT, 
                                ALLOW_CHRG_RATE,						
                                --date_time_period,
                                creation_date,
                                created_by_name,
                                last_update_date,
                                last_update_by_name,
                                processed_flag,
                                oic_instance_id,
                                target_system_document_type
                            ) VALUES (
                                l_v_HEADER_REC_ID,
                                l_v_LINE_REC_ID,
                                l_trading_partner_id,
                                l_purchase_order_number,
                                l_sequence_number, --rec_lines.v_linesequencenumber,
                                rec_lines_charge_allowance.v_AllowChrgAmt,
                                rec_lines_charge_allowance.v_AllowChrgPercent,
                                rec_lines_charge_allowance.v_AllowChrgRate,
                                --rec_lines_dates.v_datetimeperiod,
                                sysdate,
                                'INTEGRATION',
                                sysdate,
                                'INTEGRATION',
                                'N',
                                I_P_OIC_ID,
                                'SalesOrder'
                            );

                        COMMIT;

                        FOR rec_lines_dates IN (                                
                            SELECT
                                x.v_datetimequalifier,
                                x.v_date,
                                x.v_time,
                                x.v_datetimeperiod
                            FROM
                                XMLTABLE ( '/Order/LineItem/Dates'
                                        PASSING l_xml_type
                                    COLUMNS
                                        v_datetimequalifier VARCHAR2(50) PATH 'DateTimeQualifier',
                                        v_date VARCHAR2(50) PATH 'Date',
                                        v_time VARCHAR2(50) PATH 'Time',
                                        v_datetimeperiod VARCHAR2(50) PATH 'DateTimePeriod'
                                ) x
                        ) LOOP
                            INSERT INTO xxedi_scm_inb_850_line_dates_stg2 (
                                header_rec_id,
                                line_rec_id,
                                trading_partner_id,
                                purchase_order_number,
                                line_sequence_number,
                                date_time_qualifier,
                                date_order,
                                time_order,
                                date_time_period,
                                creation_date,
                                created_by_name,
                                last_update_date,
                                last_update_by_name,
                                processed_flag,
                                oic_instance_id,
                                target_system_document_type
                            ) VALUES (
                                l_v_HEADER_REC_ID,
                                l_v_LINE_REC_ID,
                                l_trading_partner_id,
                                l_purchase_order_number,
                                l_sequence_number, --rec_lines.v_linesequencenumber,
                                rec_lines_dates.v_datetimequalifier,
                                rec_lines_dates.v_date,
                                rec_lines_dates.v_time,
                                rec_lines_dates.v_datetimeperiod,
                                sysdate,
                                'INTEGRATION',
                                sysdate,
                                'INTEGRATION',
                                'N',
                                I_P_OIC_ID,
                                'SalesOrder'
                            );

                            COMMIT;

                            FOR rec_lines_items IN (
                                SELECT
                                    x.v_productcharacteristiccode,
                                    x.v_agencyqualifiercode,
                                    x.v_productdescriptioncode,
                                    x.v_productdescription,
                                    x.v_yesornoresponse
                                FROM
                                    XMLTABLE ( '/Order/LineItem/ProductOrItemDescription'
                                            PASSING l_xml_type
                                        COLUMNS
                                            v_productcharacteristiccode VARCHAR2(50) PATH 'ProductCharacteristicCode',
                                            v_agencyqualifiercode VARCHAR2(50) PATH 'AgencyQualifierCode',
                                            v_productdescriptioncode VARCHAR2(50) PATH 'ProductDescriptionCode',
                                            v_productdescription VARCHAR2(200) PATH 'ProductDescription',
                                            v_yesornoresponse VARCHAR2(50) PATH 'YesOrNoResponse'
                                    ) x
                            ) LOOP
                                INSERT INTO xxedi_scm_inb_850_line_items_stg2 (
                                    header_rec_id,
                                    line_rec_id,
                                    trading_partner_id,
                                    purchase_order_number,
                                    line_sequence_number,
                                    product_characteristic_code,
                                    agency_qualifier_code,
                                    product_description_code,
                                    product_description,
                                    yes_or_no_response,
                                    --date_time_period,
                                    creation_date,
                                    created_by_name,
                                    last_update_date,
                                    last_update_by_name,
                                    processed_flag,
                                    oic_instance_id,
                                    target_system_document_type
                                ) VALUES (
                                    l_v_HEADER_REC_ID,
                                    l_v_LINE_REC_ID,
                                    l_trading_partner_id,
                                    l_purchase_order_number,
                                    l_sequence_number, --rec_lines.v_linesequencenumber,
                                    rec_lines_items.v_productcharacteristiccode,
                                    rec_lines_items.v_agencyqualifiercode,
                                    rec_lines_items.v_productdescriptioncode,
                                    rec_lines_items.v_productdescription,
                                    rec_lines_items.v_yesornoresponse,
                                    --rec_lines_dates.v_datetimeperiod,
                                    sysdate,
                                    'INTEGRATION',
                                    sysdate,
                                    'INTEGRATION',
                                    'N',
                                    I_P_OIC_ID,
                                    'SalesOrder'
                                );

                            COMMIT;

                        END LOOP;		

                        END LOOP;			  

                    END LOOP;		

                    END LOOP;

                    COMMIT;

                    FOR rec_summary IN (
                        SELECT
                            x.v_totalamount,
                            x.v_totallineitemnumber,
                            x.v_description
                        FROM
                            XMLTABLE ( '/Order/Summary'
                                    PASSING l_xml_type
                                COLUMNS
                                    v_totalamount NUMBER PATH 'TotalAmount',
                                    v_totallineitemnumber NUMBER PATH 'TotalLineItemNumber',
                                    v_description VARCHAR2(200) PATH 'Description'
                            ) x
                    ) LOOP
                        INSERT INTO xxedi_scm_inb_850_summary_stg2 (
                            header_rec_id,
                            trading_partner_id,
                            purchase_order_number,
                            total_amount,
                            total_line_item_number,
                            description,
                            creation_date,
                            created_by_name,
                            last_update_date,
                            last_update_by_name,
                            processed_flag,
                            oic_instance_id,
                            target_system_document_type
                        ) VALUES (
                            l_v_HEADER_REC_ID,
                            l_trading_partner_id,
                            l_purchase_order_number,
                            rec_summary.v_totalamount,
                            rec_summary.v_totallineitemnumber,
                            rec_summary.v_description,
                            sysdate,
                            'INTEGRATION',
                            sysdate,
                            'INTEGRATION',
                            'N',
                            I_P_OIC_ID,
                            'SalesOrder'
                        );

                    END LOOP;

                    COMMIT;
                    /*DELETE FROM xxedi_scm_inb_850_xml_load_stg
                    WHERE
                        oic_instance_id = oic_id;

                    COMMIT;*/
                    --

            EXCEPTION
                WHEN OTHERS THEN
                    V_ERROR_CODE := 'Error when parsing the XML';
                    V_ERROR_MESSAGE := Substr( V_ERROR_CODE || '. Details: ' || SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE, 1 , 4000 );
                    IF l_v_HEADER_REC_ID IS NOT NULL THEN
                         DELETE FROM XXEDI_SCM_INB_850_HEADERS_STG2 WHERE HEADER_REC_ID = HEADER_REC_ID; 
                         COMMIT;
                     END IF;
                    UPDATE XXEDI_SCM_INB_850_XML_DATA_STG
                        SET PROCESSED_FLAG = 'E'
                          , ERROR_CODE  = V_ERROR_CODE
                          , ERROR_MESSAGE = V_ERROR_MESSAGE 
                     WHERE XML_CONTENT_REC_ID = XML_DATA_STG_REC.XML_CONTENT_REC_ID; COMMIT;
                    O_P_RESPONSE    := O_P_RESPONSE || CHR(10) || '    ' || v_ERROR_MESSAGE || CHR(10) || 'File_Name: "' || XML_DATA_STG_REC.FILE_NAME || '"';
                    RAISE;
            END; -- block to insert XML data into stage tables <<

            UPDATE      XXEDI_SCM_INB_850_XML_DATA_STG  SET  PROCESSED_FLAG  =  'Y'  WHERE XML_CONTENT_REC_ID = XML_DATA_STG_REC.XML_CONTENT_REC_ID; COMMIT;
            DELETE FROM XXEDI_SCM_INB_850_XML_DATA_STG                               WHERE PROCESSED_FLAG = 'Y' AND TRUNC(SYSDATE) - TRUNC(CREATION_DATE) > NVL(v_days_to_keep_file_XML_data, 0) AND DOC_TYPE = g_v_EDI_850_doc_type; COMMIT;
            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || '    XML data has been successfully inserted into the staging tables.' || CHR(10) || CHR(10) || 'PARSE_XML_INTO_STG Procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';

        EXCEPTION
            WHEN OTHERS THEN
                O_P_STATUS := 'ERROR';
    END PARSE_XML_INTO_STG;


    PROCEDURE PROCESS_DATA_INTO_INTF (
              I_P_OIC_ID           IN VARCHAR2
            , O_P_RESPONSE        OUT CLOB
            , O_P_STATUS          OUT VARCHAR2
            , O_P_HAS_MORE_COUNT  OUT NUMBER
        )
        IS
            L_V_ERROR_CODE                   VARCHAR2(50);
            L_V_ERROR_MESSAGE                VARCHAR2(4000);
            L_V_INSERTED_ORDER_HEADER_COUNT  NUMBER := 0;
            L_V_INSERTED_ORDER_LINE_COUNT    NUMBER := 0;
            L_V_HEADER_INTF_REC_ID           NUMBER;
            
            l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS NUMBER := 30;
        BEGIN
            O_P_RESPONSE := 'PROCESS_DATA_INTO_INTF procedure started.' || CHR(10) || CHR(10);
         
            BEGIN --! REVIEW --* block to handle reprocessing of records  >>>>>>>>>>>
              /*
                    --TODO atualizar esses updates acima para usar a primary key da stg / intf
                    será necessario alterar a tabela e a lógica
                   não deveria ser necessario atualizar a XXEDI_SCM_INB_850_LINES_STG
            */   
             FOR intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                       SELECT intf.header_rec_id
                            , intf.transaction_id                            
                            , stg.creation_date
                            , intf.processed_flag
                            , intf.error_code                            
                            , intf.oic_instance_id
                         FROM xxedi_scm_inb_850_headers_intf intf
                         LEFT JOIN xxedi_scm_inb_850_headers_stg2 stg 
                                ON intf.header_rec_id = stg.header_rec_id
                        WHERE intf.processed_flag = 'E'
                          AND TRUNC(sysdate) - TRUNC(intf.creation_date) <= l_v_max_file_age_in_days_for_reprocess
                            --AND ERROR_CODE = g_v_PRE_VALIDATION_ERROR_CODE
                ) LOOP
                    UPDATE xxedi_scm_inb_850_headers_stg2   
                       SET processed_flag   = 'R'
                         , error_code       = NULL
                         , Error_Message    = NULL  
                     WHERE header_rec_id  =  intf_rec.header_rec_id;
                     
                    UPDATE xxedi_scm_inb_850_lines_stg2     
                       SET processed_flag   = 'R'
                         , error_code       = NULL
                         , error_message    = NULL   
                     WHERE header_rec_id  =  intf_rec.header_rec_id;
                     
                    UPDATE xxedi_scm_inb_850_headers_intf   
                       SET processed_flag = 'D'                                            
                     WHERE header_rec_id = intf_rec.header_rec_id;
                     
                END LOOP;
                COMMIT;
            END; -- block to handle reprocessing of records  <<<<<<<<<<<
            BEGIN -- block to handle reprocessing of records  
                FOR intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                        SELECT
                                 rownum
                                , header_rec_id
                                , transaction_id
                                , creation_date
                                , last_update_date
                                , processed_flag
                                , error_code
                                , error_message
                                , oic_instance_id
                            FROM xxedi_scm_inb_850_headers_intf
                            WHERE
                                processed_flag = 'E'
                                AND TRUNC(sysdate) - TRUNC(creation_date) <= l_v_max_file_age_in_days_for_reprocess
                                --AND ERROR_CODE = g_v_PRE_VALIDATION_ERROR_CODE
                    ) LOOP
                        UPDATE xxedi_scm_inb_850_headers_stg2   
                            SET processed_flag = 'R'
                              , error_code = NULL
                              , error_message = NULL  
                          WHERE  header_rec_id  = intf_rec.header_rec_id;
                          
                        UPDATE xxedi_scm_inb_850_lines_stg2     
                           SET processed_flag = 'R'
                             , error_code = NULL
                             , error_message = NULL  
                        WHERE header_rec_id  =  intf_rec.header_rec_id;
                        
                        UPDATE xxedi_scm_inb_850_headers_intf   
                           SET processed_flag = 'D'                                            
                         WHERE header_rec_id = intf_rec.header_rec_id;
                END LOOP;
                COMMIT;
			END; -- block to handle reprocessing of records  
           
           
            FOR stg_rec IN  ( 
                SELECT * 
                  FROM xxedi_scm_inb_850_headers_stg2
                 WHERE processed_flag IN  ('N' , 'R')
            ) LOOP
				O_P_RESPONSE := O_P_RESPONSE 
							 -- || '    Processing headers_stg_REC_ID: ' || TO_CHAR(STG_REC.headers_stg, '999999') 
							  || ' from the file name: "' || STG_REC.FILE_NAME || '" | Status: ';

                DECLARE
                    lv_header_pk NUMBER;
                    lv_lines_pk  NUMBER;
                    
                    CURSOR header_cur IS                       
                     SELECT subquery.file_name,
                            subquery.transaction_id,
                            subquery.source_transaction_number,
                            subquery.source_transaction_system,
                            subquery.source_transaction_id,
                            subquery.business_unit_name,
                            subquery.buying_party_number,
                            subquery.customer_po_number,
                            subquery.transactional_currency_code,
                            subquery.transaction_on,
                            subquery.transaction_type_code,
                            subquery.requesting_legal_entity,
                            subquery.orig_system_document_reference,
                            subquery.partial_ship_allowed_flag,
                            subquery.priced_on,
                            subquery.freeze_price_flag,
                            subquery.freeze_shipping_charge_flag,
                            subquery.freeze_tax_flag,
                            subquery.submitted_flag,
                            subquery.requested_fulfillment_organization_code,
                            subquery.payment_terms,
                            --subquery.shipping_carrier,
                            --subquery.shipping_service_level_code,
                            --subquery.shipping_mode,
                            --subquery.shipsetflag,
                            subquery.requested_ship_date,
                            subquery.requested_arrival_date,
                            subquery.latest_acceptable_ship_date,
                            subquery.earliest_acceptable_ship_date,
                            subquery.party_name,
                            subquery.account_number,
                            subquery.party_number,
                            subquery.party_id,
                            subquery.site_use_id,
                            subquery.test_purposecode,
                            subquery.primary_po_type_code,
                            subquery.customer_order_number,
                            subquery.dept_number,
                            subquery.department,
                            --subquery.contact_name,
                            --subquery.contact_phone,
                            --subquery.contact_email,
                            subquery.creation_date,
                            subquery.created_by_name,
                            subquery.processed_flag,
                            i_p_oic_id oic_instance_id,
                            subquery.target_system_document_number,
                            subquery.header_rec_id
                        FROM
                            (
                                SELECT DISTINCT
                                    -- fields
                                        header.file_name,
                                        header.trading_partner_id || header.purchase_order_number transaction_id,
                                        --header.purchase_order_number                              source_transaction_number,
                                        header.source_header_id                                   source_transaction_number,
                                        'EDI'                                                     source_transaction_system,
                                        --nvl(reference.reference_id, header.purchase_order_number) source_transaction_id,
                                        header.source_header_id                                   source_transaction_id,
                                        'LTF US BU'                                                  business_unit_name,
                                        (
                                            SELECT
                                            hz.party_number
                                            FROM
                                                hz_customer_account_pvo_intf       hca,
                                                hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                                hz_customer_pvo_intf               hz,
                                                hz_cust_acct_dff_pvo_intf hcadff
                                            WHERE hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                            AND hcasa.cust_account_id = hca.cust_account_id
                                            AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                            AND hz.party_id = hca.party_id
                                            AND hcadff.cust_account_id = hca.cust_account_id
                                            --AND hcasa.site_use_code = 'BILL_TO'
                                            AND ROWNUM = 1
                                        )                                                       buying_party_number,
                                        header.purchase_order_number                              customer_po_number,
                                        'USD'                                                   transactional_currency_code,
                                        header.purchase_order_date                                transaction_on,
                                        'EDI_ORDER'                                               transaction_type_code,
                                        'La Terra Fina USA, LLC'                                           requesting_legal_entity,
                                        (SELECT
                                            LISTAGG(ref.reference_qual || '_' || ref.reference_id, ' / ')
                                                WITHIN GROUP (ORDER BY NULL)
                                            FROM XXEDI_SCM_INB_850_REFERENCES_STG2 ref
                                            WHERE 
                                                    ref.trading_partner_id     = HEADER.trading_partner_id
                                                AND ref.purchase_order_number  = HEADER.purchase_order_number
                                                AND ref.reference_id           IS NOT NULL
                                                AND ref.reference_qual         IS NOT NULL
                                        )                                                         AS orig_system_document_reference, -- Added by Joao @2025-05-07 to remove the block below with cursors and update
                                        'false'                                                   partial_ship_allowed_flag,
                                        header.purchase_order_date                                priced_on,
                                        'false'                                                   freeze_price_flag,
                                        'false'                                                   freeze_shipping_charge_flag,
                                        'false'                                                   freeze_tax_flag,
                                        'true'                                                    submitted_flag,
                                        ''                                                        requested_fulfillment_organization_code,
                                        NULL                                                      payment_terms,
                                        --'CUSTOMER PICK UP'                                        shipping_carrier,
                                        --'Ground'                                                  shipping_service_level_code,
                                        --'Truckload'                                               shipping_mode, 
                                        --'true'                                                    shipsetflag,
                                        (
                                            SELECT
                                                so_dates.date_order
                                            FROM
                                                XXEDI_SCM_INB_850_DATES_STG2 so_dates
                                            WHERE
                                                1 = 1
                                                AND so_dates.trading_partner_id = header.trading_partner_id
                                                AND so_dates.purchase_order_number = header.purchase_order_number
                                                AND so_dates.processed_flag = 'N'
                                                AND so_dates.date_time_qualifier IN ('010', '10', '037', '37', '118')
                                                AND ROWNUM = 1
                                        ) requested_ship_date,
                                        (
                                            SELECT
                                                so_dates.date_order
                                            FROM
                                                XXEDI_SCM_INB_850_DATES_STG2 so_dates
                                            WHERE
                                                    1 = 1
                                                AND so_dates.trading_partner_id = header.trading_partner_id
                                                AND so_dates.purchase_order_number = header.purchase_order_number
                                                AND so_dates.processed_flag = 'N'
                                                AND so_dates.date_time_qualifier in ('002','02', '064', '64')
                                                AND ROWNUM = 1
                                        )                                                         requested_arrival_date,						
                                        (
                                            SELECT
                                                so_dates.date_order
                                            FROM
                                                XXEDI_SCM_INB_850_DATES_STG2 so_dates
                                            WHERE
                                                    1 = 1
                                                AND so_dates.trading_partner_id = header.trading_partner_id
                                                AND so_dates.purchase_order_number = header.purchase_order_number
                                                AND so_dates.processed_flag = 'N'
                                                AND so_dates.date_time_qualifier in('038','38')
                                                AND ROWNUM = 1
                                        )                                                         latest_acceptable_ship_date,
                                        (
                                            SELECT
                                                so_dates.date_order
                                            FROM
                                                XXEDI_SCM_INB_850_DATES_STG2 so_dates
                                            WHERE
                                                    1 = 1
                                                AND so_dates.trading_partner_id = header.trading_partner_id
                                                AND so_dates.purchase_order_number = header.purchase_order_number
                                                AND so_dates.processed_flag = 'N'
                                                AND so_dates.date_time_qualifier in('037','37')
                                                AND ROWNUM = 1
                                        )                                                         earliest_acceptable_ship_date,
                                        (
                                        SELECT	 
                                            hz.party_name	
                                            FROM
                                                hz_customer_account_pvo_intf       hca,
                                                hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                                hz_customer_pvo_intf               hz,
                                                hz_cust_acct_dff_pvo_intf          hcadff
                                            WHERE hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                            AND hcasa.cust_account_id = hca.cust_account_id
                                            AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                            AND hz.party_id = hca.party_id
                                            AND hcadff.cust_account_id = hca.cust_account_id
                                            --AND hcasa.site_use_code = 'BILL_TO'
                                            AND ROWNUM = 1							
                                        )                                                               party_name,
                                        (
                                        SELECT	 
                                            hca.account_number	
                                            FROM
                                            hz_customer_account_pvo_intf       hca,
                                            hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                            hz_customer_pvo_intf               hz,
                                            hz_cust_acct_dff_pvo_intf          hcadff
                                        WHERE hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                            AND hcasa.cust_account_id = hca.cust_account_id
                                            --AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                            AND hz.party_id = hca.party_id
                                            AND hcadff.cust_account_id = hca.cust_account_id
                                            AND hcasa.site_use_code = 'BILL_TO'
                                            AND ROWNUM = 1									
                                        )                                                            account_number,
                                        (
                                            SELECT	
                                            hz.party_number	
                                            FROM
                                                hz_customer_account_pvo_intf       hca,
                                                hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                                hz_customer_pvo_intf               hz,
                                                hz_cust_acct_dff_pvo_intf          hcadff
                                            WHERE hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                            AND hcasa.cust_account_id = hca.cust_account_id
                                            AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                            AND hz.party_id = hca.party_id
                                            AND hcadff.cust_account_id = hca.cust_account_id
                                            --AND hcasa.site_use_code = 'BILL_TO'
                                            AND ROWNUM = 1
                                        )                                                            party_number,
                                        COALESCE((
                                            SELECT
                                                hps.party_site_id
                                            FROM
                                                hz_party_site_pvo_intf hps,
                                                lookup_values_pvo_intf flv,
                                                xxedi_scm_inb_850_addresses_stg2 addr,
                                                xxedi_scm_inb_850_references_stg2 ref
                                            WHERE
                                                1 = 1
                                                AND hps.party_site_number = flv.attribute2
                                                AND upper(flv.lookup_type) = 'LTF EDI 850 MULTIPLE LOCATION'
                                                AND (addr.address_location_number || ref.reference_id) = flv.attribute1
                                                AND addr.address_typecode IN ('ST', 'BS', 'BY')
                                                AND addr.trading_partner_id(+) = header.trading_partner_id
                                                AND addr.purchase_order_number(+) = header.purchase_order_number
                                                AND addr.processed_flag(+) = 'N'
                                                --AND REF.REFERENCE_QUAL = 'ZZ'
                                                AND ref.description = 'BUYER VENDOR SUFFIX'
                                                AND ref.trading_partner_id(+) = header.trading_partner_id
                                                AND ref.purchase_order_number(+) = header.purchase_order_number
                                                AND ref.processed_flag(+) = 'N'
                                                AND ROWNUM = 1
                                        ),
                                        (
                                            SELECT
                                                hps.party_site_id
                                            FROM
                                                hz_party_site_pvo_intf hps,
                                                lookup_values_pvo_intf flv,
                                                xxedi_scm_inb_850_addresses_stg2 addr
                                            WHERE
                                                1 = 1
                                                AND hps.party_site_number = flv.attribute2
                                                AND UPPER(flv.lookup_type) = 'LTF EDI 850 MULTIPLE LOCATION'
                                                AND (ADDR.ADDRESS_LOCATION_NUMBER || SUBSTR(header.vendor, -3)) = flv.attribute1
                                                AND addr.address_typecode IN ('ST', 'BS', 'BY')
                                                AND addr.trading_partner_id(+) = header.trading_partner_id
                                                AND addr.purchase_order_number(+) = header.purchase_order_number
                                                AND addr.processed_flag(+) = 'N'
                                                AND ROWNUM = 1
                                        ),
                                        (
                                            SELECT
                                                hps.party_site_id
                                            FROM
                                                hz_party_site_pvo_intf hps,
                                                lookup_values_pvo_intf flv,
                                                xxedi_scm_inb_850_addresses_stg2 addr
                                            WHERE
                                                1 = 1
                                                AND HPS.PARTY_SITE_NUMBER = FLV.ATTRIBUTE2
                                                AND UPPER(FLV.LOOKUP_TYPE) = 'LTF EDI 850 MULTIPLE LOCATION'
                                                AND ADDR.ADDRESS_LOCATION_NUMBER = FLV.ATTRIBUTE1
                                                AND ADDR.ADDRESS_TYPECODE IN ('ST', 'BS', 'BY')
                                                AND ADDR.TRADING_PARTNER_ID(+) = HEADER.TRADING_PARTNER_ID
                                                AND ADDR.PURCHASE_ORDER_NUMBER(+) = HEADER.PURCHASE_ORDER_NUMBER
                                                AND ADDR.PROCESSED_FLAG(+) = 'N'
                                                AND ROWNUM = 1
                                        ),
                                        (
                                            SELECT
                                                hcasa.party_site_id
                                            FROM
                                                hz_customer_account_pvo_intf       hca,
                                                hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                                hz_cust_acct_dff_pvo_intf          hcadff,
                                                hz_cust_acct_site_dff_pvo_intf     hcasadff
                                            WHERE
                                                hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                                AND hcasadff.ltf_edi_location_code_ = addresses.address_location_number
                                                AND hcasa.cust_account_id = hca.cust_account_id
                                                --AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                                AND hcasa.Site_Use_Code = 'SHIP_TO'
                                                AND hcasa.cust_acct_site_id = hcasadff.custacctsiteid
                                                AND hcadff.cust_account_id = hca.cust_account_id
                                                AND ROWNUM = 1
                                            ),                                                     
                                            (
                                            SELECT
                                                hcasa.party_site_id
                                            FROM
                                                HZ_CUSTOMER_ACCOUNT_PVO_INTF       hca,
                                                HZ_CUST_ACCT_SITE_USE_LOC_PVO_INTF hcasa,
                                                hz_cust_acct_dff_pvo_intf          hcadff,
                                                hz_cust_acct_site_dff_pvo_intf     hcasadff,
                                                XXEDI_SCM_INB_850_ADDRESSES_STG2    addressesBS
                                            WHERE
                                                hcadff.ltf_buying_party_id_ = header.trading_partner_id							
                                                AND hcasadff.ltf_edi_location_code_ = addressesBS.address_location_number								
                                                AND addressesBS.trading_partner_id (+) = header.trading_partner_id
                                                AND addressesBS.purchase_order_number (+) = header.purchase_order_number
                                                AND addressesBS.processed_flag (+) = 'N'
                                                AND addressesBS.address_typecode (+) IN ('BS','BY')
                                                AND hcasa.cust_account_id = hca.cust_account_id
                                                --AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                                AND hcasa.Site_Use_Code = 'SHIP_TO'
                                                AND hcasa.cust_acct_site_id = hcasadff.custacctsiteid
                                                AND hcadff.cust_account_id = hca.cust_account_id
                                                AND ROWNUM = 1
                                            )) party_id, --siteid
                                        nvl((
                                        SELECT
                                                hcasa.site_use_id
                                            FROM
                                                hz_customer_account_pvo_intf       hca,
                                                hz_cust_acct_site_use_loc_pvo_intf hcasa,
                                                hz_cust_acct_dff_pvo_intf          hcadff,
                                                hz_cust_acct_site_dff_pvo_intf     hcasadff
                                            WHERE
                                                hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                                AND hcasadff.ltf_edi_location_code_ = addressesBillTo.address_location_number
                                                AND hcasa.cust_account_id = hca.cust_account_id
                                                --AND hcasa.ship_to_flag IN ( 'P', 'Y' )
                                                AND hcasa.Site_Use_Code = 'BILL_TO'
                                                AND hcasa.cust_acct_site_id = hcasadff.custacctsiteid
                                                AND hcadff.cust_account_id = hca.cust_account_id
                                                AND ROWNUM = 1
                                            ),
                                            (
                                            SELECT
                                                    hcasa.site_use_id
                                                FROM
                                                    HZ_CUSTOMER_ACCOUNT_PVO_INTF       hca,
                                                    HZ_CUST_ACCT_SITE_USE_LOC_PVO_INTF hcasa,
                                                    hz_cust_acct_dff_pvo_intf          hcadff,
                                                    hz_cust_acct_site_dff_pvo_intf     hcasadff
                                                WHERE
                                                    hcadff.ltf_buying_party_id_ = header.trading_partner_id
                                                    AND hcasa.cust_account_id = hca.cust_account_id
                                                    AND hcasa.Site_Use_Code = 'BILL_TO'
                                                    AND hcasa.cust_acct_site_id = hcasadff.custacctsiteid
                                                    AND hcadff.cust_account_id = hca.cust_account_id
                                                    AND ROWNUM = 1
                                                ))                                                 site_use_id,	
                                        header.t_set_purpose_code                                 test_purposecode,
                                        header.primary_po_type_code                               primary_po_type_code,
                                        header.purchase_order_number                              customer_order_number,
                                        addresses.address_location_number                         dept_number,
                                        header.department,
                                        sysdate                                                   creation_date,
                                        'INTEGRATION'                                             created_by_name,
                                        'P'                                                       processed_flag,
                                        header.target_system_document_number                      target_system_document_number,
                                        ROW_NUMBER()
                                        OVER(PARTITION BY header.trading_partner_id, header.purchase_order_number
                                            ORDER BY
                                                header.last_update_date DESC
                                        )                                                         AS rnk,
                                        header.header_rec_id
                                FROM 
                                              xxedi_scm_inb_850_headers_stg2        header
                                              
                                    LEFT JOIN xxedi_scm_inb_850_payment_terms_stg2  Terms            
                                          ON  terms.trading_partner_id            = header.trading_partner_id   
                                          AND terms.purchase_order_number         = header.purchase_order_number
                                          
                                    LEFT JOIN xxedi_scm_inb_850_carrier_info_stg2   carrier          
                                           ON carrier.trading_partner_id          = header.trading_partner_id   
                                          AND carrier.purchase_order_number       = header.purchase_order_number
                                          
                                    LEFT JOIN xxedi_scm_inb_850_addresses_stg2      addresses        
                                           ON addresses.trading_partner_id        = header.trading_partner_id   
                                          AND addresses.purchase_order_number     = header.purchase_order_number  
                                          AND addresses.address_typecode          = 'ST'
                                          
                                    LEFT JOIN xxedi_scm_inb_850_addresses_stg2      addressesbillto  
                                           ON addressesbillto.trading_partner_id     = header.trading_partner_id   
                                          AND addressesbillto.purchase_order_number  = header.purchase_order_number  
                                          AND addressesbillto.address_typecode       = 'BT'
                                          
                                    LEFT JOIN xxedi_scm_inb_850_references_stg2     REFERENCE        
                                           ON reference.trading_partner_id        = header.trading_partner_id   
                                          AND reference.purchase_order_number     = header.purchase_order_number  
                                          AND reference.REFERENCE_QUAL            = 'GEN'
                                          
                                WHERE 1=1 -- header.processed_flag IN  ('N' , 'R')
                            ) subquery
                        WHERE
                            subquery.rnk = 1;  --TODO: Replace with actual cursor to fetch headers
                    
                BEGIN
                
                    FOR header_rec IN header_cur LOOP

                        BEGIN --* PREVALIDATION BLOCK - HEADER
                           
                            L_V_ERROR_MESSAGE := NULL; 

                            IF  header_rec.requested_ship_date           IS NULL 
                            AND header_rec.requested_arrival_date        IS NULL 
                            AND header_rec.latest_acceptable_ship_date   IS NULL 
                            AND header_rec.earliest_acceptable_ship_date IS NULL 
							THEN                  --! EXAMPLE
                                L_V_ERROR_MESSAGE := 'RequestedShipDate, RequestedArrivalDate, LatestAcceptableShipDate and EarliestAcceptableShipDate can be not null';  
                            END IF; 
							
                            IF ( L_V_ERROR_MESSAGE IS NOT NULL ) 
							THEN  
								L_V_ERROR_CODE := 'PRE_VALIDATION_ERROR';  
							END IF;
                           
                        END;  --* PREVALIDATION BLOCK - HEADER

                        BEGIN --* UPDATE DUPLICATES INTF BLOCK - HEADER
                             UPDATE xxedi_scm_inb_850_headers_intf 
                                SET processed_flag 		= 'D'
								  , last_update_date 	= SYSDATE 
								  , last_update_by_name = 'PROCESS_DATA_INTO_INTF' 
                              WHERE processed_flag IN ( 'P','N' )
								AND header_rec_id = header_rec.header_rec_id ;
								
							UPDATE xxedi_scm_inb_850_lines_intf     intf_lines
							   SET intf_lines.processed_flag 		= 'D'
								 , intf_lines.last_update_date 		= systimestamp
								 , intf_lines.last_update_by_name 	= 'process_data'
							 WHERE intf_lines.transaction_id   =  header_rec.transaction_id 
							   AND intf_lines.processed_flag  IN  ( 'P', 'N' );
                
                            COMMIT;
                        END;  --* UPDATE DUPLICATES INTF BLOCK - HEADER

                        BEGIN  --* INSERT INTO HEADER INTF BLOCK                        
                            LV_HEADER_PK := NULL;
                            INSERT INTO XXEDI_SCM_INB_850_HEADERS_INTF (
											  file_name
											, transaction_id
											, source_transaction_number
											, source_transaction_system
											, source_transaction_id
											, business_unit_name
											, buying_party_number
											, customer_po_number
											, transactional_currency_code
											, transaction_on
											, transaction_type_code
											, requesting_legal_entity
											, orig_system_document_reference
											, partial_ship_allowed_flag
											, priced_on
											, freeze_price_flag
											, freeze_shipping_charge_flag
											, freeze_tax_flag
											, submitted_flag
											, requested_fulfillment_organization_code
											, payment_terms
											-- , shipping_carrier
											-- , shipping_service_level_code
											-- , shipping_mode
											-- , shipsetflag
											, requested_ship_date
											, requested_arrival_date
											, latest_acceptable_ship_date
											, earliest_acceptable_ship_date
											, party_name
											, account_number
											, party_number
											, party_id
											, site_use_id
											, test_purposecode
											, primary_po_type_code
											, customer_order_number
											, dept_number
											, department
											-- , contact_name
											-- , contact_phone
											-- , contact_email
											, creation_date
											, created_by_name
											, processed_flag
											, oic_instance_id
											, target_system_document_number
											, error_code
											, error_message
											
								   ) VALUES (
											  header_rec.file_name
											, header_rec.transaction_id
											, header_rec.source_transaction_number
											, header_rec.source_transaction_system
											, header_rec.source_transaction_id
											, header_rec.business_unit_name
											, header_rec.buying_party_number
											, header_rec.customer_po_number
											, header_rec.transactional_currency_code
											, header_rec.transaction_on
											, header_rec.transaction_type_code
											, header_rec.requesting_legal_entity
											, header_rec.orig_system_document_reference
											, header_rec.partial_ship_allowed_flag
											, header_rec.priced_on
											, header_rec.freeze_price_flag
											, header_rec.freeze_shipping_charge_flag
											, header_rec.freeze_tax_flag
											, header_rec.submitted_flag
											, header_rec.requested_fulfillment_organization_code
											, header_rec.payment_terms
											-- , header_rec.shipping_carrier
											-- , header_rec.shipping_service_level_code
											-- , header_rec.shipping_mode
											-- , header_rec.shipsetflag
											, header_rec.requested_ship_date
											, header_rec.requested_arrival_date
											, header_rec.latest_acceptable_ship_date
											, header_rec.earliest_acceptable_ship_date
											, header_rec.party_name
											, header_rec.account_number
											, header_rec.party_number
											, header_rec.party_id
											, header_rec.site_use_id
											, header_rec.test_purposecode
											, header_rec.primary_po_type_code
											, header_rec.customer_order_number
											, header_rec.dept_number
											, header_rec.department
											-- , header_rec.contact_name
											-- , header_rec.contact_phone
											-- , header_rec.contact_email
											, header_rec.creation_date
											, header_rec.created_by_name
											, header_rec.processed_flag
											, i_p_oic_id --oic_instance_id
											, header_rec.target_system_document_number
											
											, L_V_ERROR_CODE
											, L_V_ERROR_MESSAGE

											) RETURNING HEADER_INTF_REC_ID INTO LV_HEADER_PK;
                            COMMIT;
                        END;   --* INSERT INTO HEADER INTF BLOCK
                   /* UPDATE xxedi_scm_inb_850_headers_stg2 
                       SET processed_flag   = 'P'
                         , last_update_date = SYSDATE 
                     WHERE header_rec_id    = stg_rec.header_rec_id; 
                    */

                        DECLARE --* INSERT INTO LINES INTF BLOCK
                            CURSOR LINES_CUR IS
                                SELECT
									linesubquery.transaction_id,
									linesubquery.source_transaction_line_id,
									linesubquery.source_transaction_line_number,
									linesubquery.source_transaction_schedule_id,
									linesubquery.source_schedule_number,
									linesubquery.transaction_category_code,
									linesubquery.product_number,
									linesubquery.ordered_quantity,
									linesubquery.ordered_uom_code,
									linesubquery.customer_po_number,
									linesubquery.customer_po_line_number,
									linesubquery.ATTRIBUTE_DATE1,
									linesubquery.ATTRIBUTE_DATE2,
									linesubquery.ATTRIBUTE_CHAR3,
									linesubquery.ATTRIBUTE_CHAR1,
									linesubquery.ATTRIBUTE_CHAR2,
									linesubquery.ATTRIBUTE_NUMBER1,
									SUBSTR(linesubquery.ATTRIBUTE_NUMBER2, 1, 150) ATTRIBUTE_NUMBER2,
									linesubquery.ATTRIBUTE_CHAR4,
									linesubquery.allow_chrg_amt,
									linesubquery.allow_chrg_percent,				
									linesubquery.creation_date,
									linesubquery.created_by_name,
									linesubquery.processed_flag,
									i_p_oic_id oic_instance_id
								FROM
									(
										SELECT DISTINCT
											lines.trading_partner_id || lines.purchase_order_number                    transaction_id,
											lines.purchase_order_number
											|| to_char(sysdate, 'MMSS')
											|| lines.line_sequence_number                                              source_transaction_line_id,
											lines.line_sequence_number                                                 source_transaction_line_number,
											lines.purchase_order_number
											|| to_char(sysdate, 'MMSS')
											|| lines.line_sequence_number                                              source_transaction_schedule_id,
											'1'                                                                        source_schedule_number,
											'ORDER'                                                                    transaction_category_code,
											nvl((
												SELECT
													item.item_number
												FROM
													INV_ITEM_PVO_INTF item
												WHERE
														item.item_number = lines.vendor_part_number
													AND ROWNUM = 1
											),
												nvl((
												SELECT
													item.item_number
												FROM
													INV_ITEM_PVO_INTF        item,
													INV_ITEM_RELATIONSHIP_PVO_INTF  eirb,
													INV_TRANDING_PARTNER_ITEMS_PVO_INTF etpi,
													hz_customer_account_pvo_intf        hca,
													hz_cust_acct_site_use_loc_pvo_intf  hcasa,
													hz_customer_pvo_intf                hz,
													hz_cust_acct_dff_pvo_intf           hcadff
												WHERE
														etpi.tp_item_number = lines.buyer_part_number
													AND etpi.tp_item_id = eirb.tp_item_id
													AND eirb.inventory_item_id = item.inventory_item_id
													AND hz.party_id = etpi.trading_partner_id
													AND hcadff.ltf_buying_party_id_ = lines.trading_partner_id
													AND hcasa.cust_account_id = hca.cust_account_id
													AND hcasa.ship_to_flag IN ( 'P', 'Y' )
													AND hz.party_id = hca.party_id
													AND hcadff.cust_account_id = hca.cust_account_id
													AND ROWNUM = 1						
											),
													nvl((
													select item.item_number
														from INV_ITEM_RELATIONSHIP_PVO_INTF eirb
															,INV_ITEM_PVO_INTF item
													where eirb.sub_type = 'GTIN'
														and eirb.CROSS_REFERENCE  =  lines.gtin
														and item.INVENTORY_ITEM_ID  = eirb.INVENTORY_ITEM_ID
														AND ROWNUM = 1
											),
											nvl((
												select item.item_number
													from INV_ITEM_RELATIONSHIP_PVO_INTF eirb
														,INV_ITEM_PVO_INTF item
												where eirb.sub_type = 'UPC'
													and eirb.CROSS_REFERENCE  =  lines.upc_case_code
													and item.INVENTORY_ITEM_ID  = eirb.INVENTORY_ITEM_ID
													AND ROWNUM = 1								 
											),
														nvl((
													select item.item_number
														from INV_ITEM_RELATIONSHIP_PVO_INTF eirb
															,INV_ITEM_PVO_INTF item
													where eirb.sub_type = 'UPC'
														and eirb.CROSS_REFERENCE  =  lines.consumer_package_code
														and item.INVENTORY_ITEM_ID  = eirb.INVENTORY_ITEM_ID
														AND ROWNUM = 1
											),		
											nvl((
														SELECT
														item.item_number
													FROM
														INV_ITEM_PVO_INTF        item,
														INV_ITEM_RELATIONSHIP_PVO_INTF  eirb,
														INV_TRANDING_PARTNER_ITEMS_PVO_INTF etpi
													WHERE
															etpi.tp_item_number = lines.upc_case_code
														AND etpi.tp_item_id = eirb.tp_item_id
														AND eirb.inventory_item_id = item.inventory_item_id
														AND ROWNUM = 1						
												),						
													nvl((
														SELECT
														item.item_number
													FROM
														INV_ITEM_PVO_INTF        item,
														INV_ITEM_RELATIONSHIP_PVO_INTF  eirb,
														INV_TRANDING_PARTNER_ITEMS_PVO_INTF etpi
													WHERE
															etpi.tp_item_number = lines.consumer_package_code
														AND etpi.tp_item_id = eirb.tp_item_id
														AND eirb.inventory_item_id = item.inventory_item_id
														AND ROWNUM = 1						
												),
													nvl((
														SELECT
														item.item_number
													FROM
														INV_ITEM_PVO_INTF        item,
														INV_ITEM_RELATIONSHIP_PVO_INTF  eirb,
														INV_TRANDING_PARTNER_ITEMS_PVO_INTF etpi
													WHERE
															etpi.tp_item_number = lines.gtin
														AND etpi.tp_item_id = eirb.tp_item_id
														AND eirb.inventory_item_id = item.inventory_item_id
														AND ROWNUM = 1						
												),
															nvl(lines.vendor_part_number, lines.buyer_part_number))))))))) product_number,
											lines.order_qty                                                            ordered_quantity,
											decode(lines.order_qty_uom, 'CA', 'CS', lines.order_qty_uom)               ordered_uom_code,
											lines.purchase_order_number                                                customer_po_number,
											lines.line_sequence_number                                                 customer_po_line_number,
											(
												SELECT
													so_dates.date_order
												FROM
													XXEDI_SCM_INB_850_DATES_STG2 so_dates
												WHERE
														1 = 1
													AND so_dates.trading_partner_id = lines.trading_partner_id
													AND so_dates.purchase_order_number = lines.purchase_order_number
													AND so_dates.processed_flag = 'N'
													AND so_dates.date_time_qualifier IN ('118')
													AND ROWNUM = 1
											)                                                                          ATTRIBUTE_DATE1,
											(
												SELECT
													so_dates.date_order
												FROM
													XXEDI_SCM_INB_850_DATES_STG2 so_dates
												WHERE
														1 = 1
													AND so_dates.trading_partner_id = lines.trading_partner_id
													AND so_dates.purchase_order_number = lines.purchase_order_number
													AND so_dates.processed_flag = 'N'
													AND so_dates.date_time_qualifier IN ('001')
													AND ROWNUM = 1
											)                                                                         ATTRIBUTE_DATE2,                                                     
											lines.ATTRIBUTE_CHAR3                                                     ATTRIBUTE_CHAR3,
											lines.ATTRIBUTE_CHAR1                                                     ATTRIBUTE_CHAR1,
											lines.ATTRIBUTE_CHAR2                                                     ATTRIBUTE_CHAR2,
											lines.purchase_price                                                      ATTRIBUTE_NUMBER1,                                                
											( SELECT LISTAGG(
													   DECODE(ca.allow_chrg_amt    , '', '', ca.allow_chrg_amt)
													|| DECODE(ca.allow_chrg_percent, '', '', DECODE(ca.allow_chrg_amt    , '', ca.allow_chrg_percent, ' / ' || ca.allow_chrg_percent))
													|| DECODE(ca.allow_chrg_rate   , '', '', DECODE(ca.allow_chrg_percent, '', ca.allow_chrg_rate   , ' / ' || ca.allow_chrg_rate))
													,' / '
												) WITHIN GROUP (ORDER BY NULL)
												FROM XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG2 ca
												WHERE 
														ca.processed_flag         = 'N'
													AND ca.line_sequence_number   = lines.line_sequence_number
													AND ca.trading_partner_id     = lines.trading_partner_id
													AND ca.purchase_order_number  = lines.purchase_order_number
											)                                                                         ATTRIBUTE_NUMBER2, -- added by Joao  @2025-05-07. To remove the block below with cursors and update
											lines.ATTRIBUTE_CHAR4                                                     ATTRIBUTE_CHAR4,
											lines.purchase_price                                                       edi_price,
											lines.allow_chrg_amt,
											lines.allow_chrg_percent,
											sysdate                                                                    creation_date,
											'INTEGRATION'                                                              created_by_name,
											'P'                                                                        processed_flag,
											ROW_NUMBER()
											OVER(PARTITION BY lines.trading_partner_id, lines.purchase_order_number, lines.line_sequence_number
												ORDER BY
													lines.last_update_date DESC
											)                                                                          AS rnk
										FROM
											XXEDI_SCM_INB_850_LINES_STG2 lines
										WHERE
											lines.processed_flag IN ('N' , 'R')
									) linesubquery
								WHERE
									linesubquery.rnk = 1
								ORDER BY
									linesubquery.transaction_id,
									linesubquery.source_transaction_line_id; --TODO: Replace with actual cursor to fetch lines
		
						BEGIN
						
							BEGIN   --* INSERT INTO LINES INTF BLOCK
								FOR lines_rec IN lines_cur LOOP 
								INSERT INTO XXEDI_SCM_INB_850_LINES_INTF (
                                              HEADER_INTF_REC_ID
                                            , transaction_id
											, source_transaction_line_id
											, source_transaction_line_number
											, source_transaction_schedule_id
											, source_schedule_number
											, transaction_category_code
											, product_number
											, ordered_quantity
											, ordered_uom_code
											, customer_po_number
											, customer_po_line_number
											, attribute_date1
											, attribute_date2
											, attribute_char3
											, attribute_char1
											, attribute_char2
											, attribute_number1
											, attribute_number2
											, attribute_char4			
											-- , original_cases
											-- , original_pounds
											-- , edi_price
											, allow_chrg_amt
											, allow_chrg_percent
											, creation_date
											, created_by_name
											, processed_flag
											, oic_instance_id

											) VALUES (
											  LV_HEADER_PK
                                            , lines_rec.transaction_id
											, lines_rec.source_transaction_line_id
											, lines_rec.source_transaction_line_number
											, lines_rec.source_transaction_schedule_id
											, lines_rec.source_schedule_number
											, lines_rec.transaction_category_code
											, lines_rec.product_number
											, lines_rec.ordered_quantity
											, lines_rec.ordered_uom_code
											, lines_rec.customer_po_number
											, lines_rec.customer_po_line_number
											, lines_rec.ATTRIBUTE_DATE1
											, lines_rec.ATTRIBUTE_DATE2
											, lines_rec.ATTRIBUTE_CHAR3
											, lines_rec.ATTRIBUTE_CHAR1
											, lines_rec.ATTRIBUTE_CHAR2
											, lines_rec.ATTRIBUTE_NUMBER1
											, lines_rec.ATTRIBUTE_NUMBER2
											, lines_rec.ATTRIBUTE_CHAR4
											, lines_rec.allow_chrg_amt
											, lines_rec.allow_chrg_percent				
											, lines_rec.creation_date
											, lines_rec.created_by_name
											, lines_rec.processed_flag
											, i_p_oic_id
											) ;
								COMMIT;
								END LOOP;
							END;    --* INSERT INTO LINES INTF BLOCK
						END;	
						END LOOP;
					
                    UPDATE XXEDI_SCM_INB_850_HEADERS_STG2 
                       SET PROCESSED_FLAG = 'Y'
                         , LAST_UPDATE_DATE = SYSDATE 
                     WHERE header_rec_id = STG_REC.header_rec_id;
                    
                    COMMIT;
                   

                    O_P_RESPONSE := O_P_RESPONSE || '    STG # ' || STG_REC.header_rec_id || ' -> FileName: "' || STG_REC.FILE_NAME || '" -> Status: ' || 'Success.' || CHR(10);
				

                EXCEPTION
                    WHEN OTHERS THEN
                        L_V_ERROR_CODE := 'PROCESS_DATA_INTO_INTF procedure error';
                        L_V_ERROR_MESSAGE := Substr(SQLCODE || ' | ' || SQLERRM || ' | Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' | Stack: ' || DBMS_UTILITY.FORMAT_CALL_STACK, 1 , 4000);
                     --   UPDATE XXEDI_SCM_INB_850_HEADERS_STG SET PROCESSED_FLAG = 'E', ERROR_CODE = L_V_ERROR_CODE, ERROR_MESSAGE = L_V_ERROR_MESSAGE WHERE Order_REC_ID = STG_REC.Order_REC_ID;
                     --   DELETE FROM XXEDI_SCM_INB_850_HEADERS_INT WHERE Order_REC_ID = STG_REC.Order_REC_ID;
                        COMMIT;
                        RAISE;
                        
                END;
               
            END LOOP;

            SELECT COUNT(*) INTO O_P_HAS_MORE_COUNT FROM XXEDI_SCM_INB_850_HEADERS_STG2 WHERE PROCESSED_FLAG IN ( 'N', 'R' ); 

            O_P_RESPONSE := 'PROCESS_DATA_INTO_INTF procedure started.' || CHR(10) || CHR(10) || O_P_RESPONSE || CHR(10) || CHR(10) || 'PROCESS_DATA_INTO_INTF procedure completed successfully.';
            O_P_STATUS := 'SUCCESS';
            
        EXCEPTION
            WHEN OTHERS THEN
                O_P_STATUS := 'ERROR';
                RAISE;
    END PROCESS_DATA_INTO_INTF;

    PROCEDURE UPDATE_FLAGS (
            I_P_PK            IN NUMBER         -- Single PK
            ,I_P_KEYS         IN VARCHAR2       -- Multiple PKs. String with Primary Keys separeted by commas. like: '1,4,15'
            ,I_P_TABLE_NAME   IN VARCHAR2       -- the table name is used to determine which table to update. A parent table name might update the child table control fields.  check behaviour below.
            ,I_P_FLAG_VALUE   IN VARCHAR2       -- value that goes into the PROCESSED_FLAG field
            ,I_P_ERROR_CODE   IN VARCHAR2       -- value that goes into the ERROR_CODE field  
            ,I_P_ERROR_TEXT   IN CLOB           -- value that goes into the ERROR_MESSAGE field
            ,I_P_OIC_ID       IN VARCHAR2       -- value that goes into the OIC_INSTANCE_ID field
            ,O_P_RESPONSE     OUT CLOB          -- response message with log about the execution of the procedure
        )
        IS
            v_PK                NUMBER;
            v_count             NUMBER;
            v_aux_txt           VARCHAR2(4000);
            l_v_ERROR_CODE      VARCHAR2(256);
            l_v_ERROR_MESSAGE   VARCHAR2(4000);

            TYPE NumberTable IS TABLE OF NUMBER;
            v_parsed_keys NumberTable := NumberTable(); -- table that holds the primary keys of the records to be updated

        BEGIN
                null;
                
                O_P_RESPONSE := 'UPDATE_FLAGS PROCEDURE started.' || CHR(10) || CHR(10);
                IF (I_P_PK IS NULL AND I_P_KEYS IS NULL)            THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_PK and I_P_KEYS parameters cannot be NULL at the same time.'); END IF;
                IF (I_P_PK IS NOT NULL AND I_P_KEYS IS NOT NULL)    THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_PK and I_P_KEYS parameters cannot be provided at the same time.'); END IF;            
                IF I_P_FLAG_VALUE NOT IN ( 'Y', 'N', 'E' )          THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: The I_P_FLAG_VALUE parameter argument must be "Y", "N" or "E".'); END IF; 
                IF I_P_FLAG_VALUE = 'E' AND I_P_ERROR_CODE IS NULL  THEN RAISE_APPLICATION_ERROR(-20000, 'ERROR: I_P_FLAG_VALUE is "E" but I_P_ERROR_CODE is NULL. When the flag value is "E" the error code must be provided.'); END IF;

                IF I_P_KEYS IS NOT NULL THEN -- populate with PKs from I_P_KEYS and use the keys to update the each record
                    DECLARE                
                    BEGIN
                        IF NOT REGEXP_LIKE(I_P_KEYS, '^\d+(,\d+)*$') THEN
                            RAISE_APPLICATION_ERROR(-20001, 'Invalid input format. The string must contain only numbers separated by commas with no spaces.Example: "1,2,3,4"');
                        END IF;
                        SELECT TO_NUMBER(TRIM(REGEXP_SUBSTR(I_P_KEYS, '[^,]+', 1, LEVEL))) BULK COLLECT INTO v_parsed_keys
                        FROM dual
                        CONNECT BY REGEXP_SUBSTR(I_P_KEYS, '[^,]+', 1, LEVEL) IS NOT NULL;
                        IF v_parsed_keys.COUNT = 0 THEN
                            RAISE_APPLICATION_ERROR(-20001, 'No valid keys found in the I_P_KEYS parameter string.');
                        END IF;
                    END;
                ELSE -- populate v_parsed_keys to update a record for single PK from I_P_PK
                    v_parsed_keys := NumberTable(I_P_PK);
                END IF;

                IF I_P_FLAG_VALUE IN ('Y' , 'N') THEN
                    l_v_ERROR_CODE    := NULL;
                    l_v_ERROR_MESSAGE := NULL;
                ELSE
                    l_v_ERROR_CODE    := substr(I_P_ERROR_CODE, 1 , 256 );
                    l_v_ERROR_MESSAGE := substr(I_P_ERROR_TEXT, 1 , 4000 );
                END IF;
            --
            FOR i IN 1 .. v_parsed_keys.COUNT LOOP
                BEGIN
                    v_PK := v_parsed_keys(i);
                    IF  UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_850_HEADERS_INT'      THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_850_HEADERS_INTF WHERE HEADER_INTF_REC_ID = v_PK;
                            IF v_count = 0 THEN  RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_850_HEADERS_INT for the given PK: ' || v_PK);  END IF;
                            IF I_P_FLAG_VALUE = 'E' THEN
                                    UPDATE XXEDI_SCM_INB_850_HEADERS_INTF SET
                                            PROCESSED_FLAG       = I_P_FLAG_VALUE
                                            ,ERROR_CODE          = I_P_ERROR_CODE
                                            ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                            ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                            ,LAST_UPDATE_DATE    = SYSDATE
                                            ,LAST_UPDATE_BY_NAME = 'OIC'
                                        WHERE HEADER_INTF_REC_ID = v_PK;
                                    UPDATE XXEDI_SCM_INB_850_LINES_INTF SET
                                                PROCESSED_FLAG      = I_P_FLAG_VALUE
                                                ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                                ,LAST_UPDATE_DATE    = SYSDATE
                                                ,LAST_UPDATE_BY_NAME = 'OIC'
                                            WHERE HEADER_INTF_REC_ID = v_PK AND PROCESSED_FLAG != 'E' AND ERROR_CODE IS NULL;
                            ELSIF I_P_FLAG_VALUE = 'Y' OR I_P_FLAG_VALUE = 'N' THEN
                                    UPDATE XXEDI_SCM_INB_850_HEADERS_INTF   SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_INTF_REC_ID = v_PK;
                                    UPDATE XXEDI_SCM_INB_850_LINES_INTF     SET PROCESSED_FLAG = I_P_FLAG_VALUE, ERROR_CODE = NULL, ERROR_MESSAGE = NULL, OIC_INSTANCE_ID = I_P_OIC_ID, LAST_UPDATE_DATE = SYSDATE, LAST_UPDATE_BY_NAME = 'OIC' WHERE HEADER_INTF_REC_ID = v_PK;
                            END IF;
                        ELSIF UPPER(I_P_TABLE_NAME) = 'XXEDI_SCM_INB_850_LINES_INT'        THEN
                            SELECT COUNT(*) INTO v_count FROM XXEDI_SCM_INB_850_LINES_INTF WHERE LINES_INTF_REC_ID = v_PK;
                            IF v_count = 0 THEN  RAISE_APPLICATION_ERROR(-20000, 'No record found in XXEDI_SCM_INB_850_LINES_INT for the given PK: ' || v_PK);  END IF;
                            UPDATE XXEDI_SCM_INB_850_LINES_INTF
                                SET
                                    PROCESSED_FLAG      = I_P_FLAG_VALUE
                                    ,ERROR_CODE          = I_P_ERROR_CODE
                                    ,ERROR_MESSAGE       = l_v_ERROR_MESSAGE
                                    ,OIC_INSTANCE_ID     = I_P_OIC_ID
                                    ,LAST_UPDATE_DATE    = SYSDATE
                                    ,LAST_UPDATE_BY_NAME = 'OIC'
                                WHERE
                                    LINES_INTF_REC_ID = v_PK;
                        ELSE -- Raise invalid I_P_TABLE_NAME parameter
                            RAISE_APPLICATION_ERROR(-20001, 'The value of the I_P_TABLE_NAME parameter is not valid. Provided value: "' || I_P_TABLE_NAME || '"');
                    END IF;                    
                    COMMIT;
                    O_P_RESPONSE := O_P_RESPONSE || CHR(10) || I_P_TABLE_NAME || ' updated successfully. PK: ' || v_PK || ' | Flag Value: ' || I_P_FLAG_VALUE;
                END;

            END LOOP;

            O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) || 'UPDATE_FLAGS PROCEDURE completed successfully.';
           
           
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                O_P_RESPONSE := O_P_RESPONSE || CHR(10) || CHR(10) 
                                             || CHR(10) || 'An error occurred in the UPDATE_FLAGS procedure.' 
                                             || CHR(10) || 'ROLLBACK executed. Error Details: ' 
                                             || CHR(10) || SQLCODE                                                
                                             || CHR(10) || SQLERRM                                                
                                             || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE  
                                             || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_STACK;
                RAISE;
                
    END UPDATE_FLAGS;


    PROCEDURE GET_INTERFACE_TABLES_DATA
        (
             O_P_RESPONSE       OUT CLOB
            ,O_P_JSON           OUT CLOB
            ,O_P_STATUS         OUT VARCHAR2
        )
        IS
        BEGIN
        
            O_P_RESPONSE := 'GET_INTERFACE_TABLES_DATA procedure started.' || CHR(10) || CHR(10);


            SELECT JSON_OBJECT( 
                'Unprocessed_Orders' VALUE (
                        SELECT
                            JSON_ARRAYAGG(
                            JSON_OBJECT(
                                header_intf.HEADER_INTF_REC_ID
                                ,header_intf.HEADER_REC_ID --AS SOURCE_HEADER_ID
                                ,header_intf.FILE_NAME
                                ,header_intf.TRANSACTION_ID
                                ,header_intf.SOURCE_TRANSACTION_NUMBER
                                ,header_intf.SOURCE_TRANSACTION_SYSTEM
                                ,header_intf.SOURCE_TRANSACTION_ID
                                ,header_intf.BUSINESS_UNIT_NAME
                                ,header_intf.BUYING_PARTY_NUMBER
                                ,header_intf.CUSTOMER_PO_NUMBER
                                ,header_intf.TRANSACTIONAL_CURRENCY_CODE
                                ,header_intf.TRANSACTION_ON
                                ,header_intf.TRANSACTION_TYPE_CODE
                                ,header_intf.DEPARTMENT
                                ,header_intf.REQUESTING_LEGAL_ENTITY
                                ,header_intf.ORIG_SYSTEM_DOCUMENT_REFERENCE
                                ,header_intf.PARTIAL_SHIP_ALLOWED_FLAG
                                ,header_intf.PRICED_ON
                                ,header_intf.FREEZE_PRICE_FLAG
                                ,header_intf.FREEZE_SHIPPING_CHARGE_FLAG
                                ,header_intf.FREEZE_TAX_FLAG
                                ,header_intf.SUBMITTED_FLAG
                                ,header_intf.REQUESTED_FULFILLMENT_ORGANIZATION_CODE
                                ,header_intf.PAYMENT_TERMS
                                ,header_intf.SHIPPING_CARRIER
                                ,header_intf.SHIPPING_SERVICE_LEVEL_CODE
                                ,header_intf.SHIPPING_MODE
                                ,header_intf.REQUESTED_SHIP_DATE
                                ,header_intf.REQUESTED_ARRIVAL_DATE
                                ,header_intf.LATEST_ACCEPTABLE_SHIP_DATE
                                ,header_intf.EARLIEST_ACCEPTABLE_SHIP_DATE
                                ,header_intf.PARTY_NAME
                                ,header_intf.ACCOUNT_NUMBER
                                ,header_intf.PARTY_NUMBER
                                ,header_intf.PARTY_ID
                                ,header_intf.SITE_USE_ID
                                ,header_intf.TEST_PURPOSECODE
                                ,header_intf.PRIMARY_PO_TYPE_CODE
                                ,header_intf.CUSTOMER_ORDER_NUMBER
                                ,header_intf.DEPT_NUMBER
                                ,header_intf.CONTACT_NAME
                                ,header_intf.CONTACT_DETAIL
                                ,header_intf.CREATION_DATE
                                ,header_intf.CREATED_BY_ID
                                ,header_intf.CREATED_BY_NAME
                                ,header_intf.LAST_UPDATE_DATE
                                ,header_intf.LAST_UPDATE_BY_ID
                                ,header_intf.LAST_UPDATE_BY_NAME
                                ,header_intf.PROCESSED_FLAG
                                ,header_intf.ERROR_CODE
                                ,header_intf.ERROR_MESSAGE
                                ,header_intf.OIC_INSTANCE_ID
                                ,header_intf.SEQUENCE_ID
                                ,header_intf.TARGET_SYSTEM_DOCUMENT_TYPE
                                ,header_intf.TARGET_SYSTEM_DOCUMENT_NUMBER
                                ,header_intf.TARGET_SYSTEM_DOCUMENT_LINE_NUMBER
                                ,'Order_Lines' VALUE
                                    (
                                        SELECT
                                            JSON_ARRAYAGG(
                                            JSON_OBJECT(
                                                line_intf.LINES_INTF_REC_ID
                                                ,line_intf.HEADER_INTF_REC_ID
                                                ,line_intf.TRANSACTION_ID
                                                ,line_intf.SOURCE_TRANSACTION_LINE_ID
                                                ,line_intf.SOURCE_TRANSACTION_LINE_NUMBER
                                                ,line_intf.SOURCE_TRANSACTION_SCHEDULE_ID
                                                ,line_intf.SOURCE_SCHEDULE_NUMBER
                                                ,line_intf.TRANSACTION_CATEGORY_CODE
                                                ,line_intf.PRODUCT_NUMBER
                                                ,line_intf.ORDERED_QUANTITY
                                                ,line_intf.ORDERED_UOM_CODE
                                                ,line_intf.CUSTOMER_PO_NUMBER
                                                ,line_intf.CUSTOMER_PO_LINE_NUMBER
                                                ,line_intf.ORIGINAL_CASES
                                                ,line_intf.ORIGINAL_POUNDS
                                                ,line_intf.EDI_PRICE
                                                ,line_intf.ALLOW_CHRG_AMT
                                                ,line_intf.ALLOW_CHRG_PERCENT
                                                ,line_intf.ATTRIBUTE_DATE1
                                                ,line_intf.ATTRIBUTE_DATE2
                                                ,line_intf.ATTRIBUTE_NUMBER1
                                                ,line_intf.ATTRIBUTE_CHAR1
                                                ,line_intf.ATTRIBUTE_CHAR3
                                                ,line_intf.ATTRIBUTE_CHAR2
                                                ,line_intf.ATTRIBUTE_NUMBER2
                                                ,line_intf.ATTRIBUTE_CHAR4
                                                ,line_intf.CREATION_DATE
                                                ,line_intf.CREATED_BY_ID
                                                ,line_intf.CREATED_BY_NAME
                                                ,line_intf.LAST_UPDATE_DATE
                                                ,line_intf.LAST_UPDATE_BY_ID
                                                ,line_intf.LAST_UPDATE_BY_NAME
                                                ,line_intf.PROCESSED_FLAG
                                                ,line_intf.ERROR_CODE
                                                ,line_intf.ERROR_MESSAGE
                                                ,line_intf.OIC_INSTANCE_ID
                                                ,line_intf.SEQUENCE_ID
                                                ,line_intf.TARGET_SYSTEM_DOCUMENT_TYPE
                                                ,line_intf.TARGET_SYSTEM_DOCUMENT_NUMBER
                                                ,line_intf.TARGET_SYSTEM_DOCUMENT_LINE_NUMBER
                                            RETURNING CLOB )                                            
                                            RETURNING CLOB )
                                        FROM XXEDI_SCM_INB_850_LINES_INTF line_intf
                                        WHERE line_intf.HEADER_INTF_REC_ID = header_intf.HEADER_INTF_REC_ID
                                    )
                            RETURNING CLOB )                    
                            ORDER BY  header_intf.HEADER_INTF_REC_ID  RETURNING CLOB )
                        FROM XXEDI_SCM_INB_850_HEADERS_INTF header_intf
                        WHERE header_intf.PROCESSED_FLAG IN ('N','P')
                    )
                    RETURNING CLOB
                    -- PRETTY
                    STRICT WITH UNIQUE KEYS
                ) AS JSON_OUTPUT
            INTO O_P_JSON
            FROM DUAL;


            O_P_RESPONSE := O_P_RESPONSE || '    JSON generated successfully.' || CHR(10) || CHR(10) || 'GET_INTERFACE_TABLES_DATA procedure completed successfully.';
            O_P_STATUS   := 'SUCCESS';
            
            
        EXCEPTION
            WHEN OTHERS THEN
                O_P_RESPONSE := O_P_RESPONSE    
                    || '    '  || 'ERROR: An error occurred in the GET_INTERFACE_TABLES_DATA procedure.'
                    || '    '  || 'Error Details: ' || SQLCODE           || CHR(10)
                    || '    '  || SQLERRM                                || CHR(10)
                    || '    '  || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE    || CHR(10)
                    || '    '  || DBMS_UTILITY.FORMAT_ERROR_STACK        || CHR(10)
                ;
                O_P_STATUS   := 'ERROR';

    END GET_INTERFACE_TABLES_DATA;


END XXEDI_SCM_INB_850_PO_PKG;