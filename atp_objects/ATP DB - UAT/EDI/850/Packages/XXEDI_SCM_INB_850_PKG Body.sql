create or replace PACKAGE BODY XXEDI_SCM_INB_850_PKG AS


    -- g_v_EDI_doc_type                        CONSTANT VARCHAR2(100) := 'EDI_850';
    -- g_v_PRE_VALIDATION_ERROR_CODE           CONSTANT VARCHAR2(64)  := 'PRE_VALIDATION_ERROR';


    PROCEDURE load_xml_process ( -- LOAD_XML_INTO_RAW_STG
            oic_id            IN VARCHAR2,
            current_file_name IN VARCHAR2,
            xml_load          IN CLOB
        ) IS
        BEGIN
            INSERT INTO XXEDI_SCM_INB_850_XML_LOAD_STG (
                file_name,
                xml_data_load,
                creation_date,
                created_by_id,
                created_by_name,
                last_update_date,
                last_update_by_id,
                last_update_by_name,
                processed_flag,
                oic_instance_id
            )
                SELECT
                    current_file_name file_name,
                    xml_load          xml_data_load,
                    sysdate           creation_date,
                    NULL              created_by_id,
                    NULL              created_by_name,
                    sysdate           last_update_date,
                    NULL              last_update_by_id,
                    NULL              last_update_by_name,
                    'N'               processed_flag,
                    oic_id            oic_instance_id
                FROM
                    dual;

            COMMIT;
        --
    END load_xml_process;


    PROCEDURE load_order_tables (  --PARSE_XML_INTO_STG
            oic_id IN VARCHAR2,
            current_file_name IN VARCHAR2
        ) IS

            l_trading_partner_id    VARCHAR2(150);
            l_purchase_order_number VARCHAR2(150);
            l_xml                   CLOB;
            l_xml_type              XMLTYPE;
            L_FILE_NAME             VARCHAR2(255);
            l_sequence_number       NUMBER;
            l_sequence_number_allowance NUMBER;
            l_exists_allowance VARCHAR2(1);
        BEGIN

            SELECT  XML_DATA_LOAD,   FILE_NAME  
            INTO    L_XML        , L_FILE_NAME
            FROM  XXEDI_SCM_INB_850_XML_LOAD_STG
            WHERE  OIC_INSTANCE_ID  =  OIC_ID  AND  FILE_NAME = CURRENT_FILE_NAME;

            l_xml_type := xmltype(l_xml);

            -- Extract Key for tables. trading_partner_id and purchase_order_number are used as primary key in the tables.
            SELECT  x.trading_partner_id,  x.purchase_order_number
            INTO    l_trading_partner_id,  l_purchase_order_number
            FROM  XMLTABLE ( '/Order/Header/OrderHeader'  PASSING l_xml_type
                    COLUMNS
                        trading_partner_id VARCHAR2(150) PATH 'TradingPartnerId',
                        purchase_order_number VARCHAR2(150) PATH 'PurchaseOrderNumber'
                ) x;

            -- Insert into orders table
            INSERT INTO XXEDI_SCM_INB_850_HEADERS_STG (
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
            )
                SELECT
                    L_FILE_NAME,
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
                    x.customer_order_number,
                    sysdate,
                    'INTEGRATION',
                    sysdate,
                    'INTEGRATION',
                    'N',
                    oic_id,
                    'SalesOrder',
                    l_file_name
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
                    ) x;

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
                INSERT INTO xxedi_scm_inb_850_payment_terms_stg (
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
                    oic_id,
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
                INSERT INTO xxedi_scm_inb_850_dates_stg (
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
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;

            /*FOR rec_contacts IN (
                SELECT
                    x.v_contacttypecode,
                    x.v_contactname,
                    x.v_primaryphone,
                    x.v_primaryfax,
                    x.v_primaryemail
                FROM
                    XMLTABLE ( '/Order/Header/Contacts'
                            PASSING l_xml_type
                        COLUMNS
                            v_contacttypecode VARCHAR2(50) PATH 'ContactTypeCode',
                            v_contactname VARCHAR2(50) PATH 'ContactName',
                            v_primaryphone VARCHAR2(50) PATH 'PrimaryPhone',
                            v_primaryfax VARCHAR2(50) PATH 'PrimaryFax',
                            v_primaryemail VARCHAR2(50) PATH 'PrimaryEmail'
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
                    rec_contacts.v_contacttypecode,
                    rec_contacts.v_contactname,
                    rec_contacts.v_primaryphone,
                    rec_contacts.v_primaryfax,
                    rec_contacts.v_primaryemail,
                    sysdate,
                    'INTEGRATION',
                    sysdate,
                    'INTEGRATION',
                    'N',
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;*/

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
                INSERT INTO xxedi_scm_inb_850_addresses_stg (
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
                    oic_id,
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

            /*FOR rec_address_refs IN (
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
                INSERT INTO xxedi_scm_inb_850_references_stg (
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
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;*/

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
                INSERT INTO xxedi_scm_inb_850_fob_stg (
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
                    oic_id,
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
                INSERT INTO xxedi_scm_inb_850_carrier_info_stg (
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
                    oic_id,
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
                INSERT INTO xxedi_scm_inb_850_references_stg (
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
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;

            COMMIT;

            /*FOR rec_notes_refs IN (
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
                INSERT INTO xxedi_scm_inb_850_references_stg (
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
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;*/

            l_sequence_number_allowance := 0;

            /*FOR rec_line_item IN (
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

                        INSERT INTO XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG (
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
                            oic_id,
                            'SalesOrder'
                        );

                    END LOOP;	

                    COMMIT;

            END LOOP;*/

            --END IF;

            --END LOOP;	

            --COMMIT;

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
                INSERT INTO xxedi_scm_inb_850_lines_stg (
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
                    oic_id,                                               --oic_instance_id,                       
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

                    INSERT INTO XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG (
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
                        oic_id,
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
                    INSERT INTO xxedi_scm_inb_850_line_dates_stg (
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
                        oic_id,
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
                        INSERT INTO xxedi_scm_inb_850_line_items_stg (
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
                            oic_id,
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
                INSERT INTO xxedi_scm_inb_850_summary_stg (
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
                    oic_id,
                    'SalesOrder'
                );

            END LOOP;

            COMMIT;
            /*DELETE FROM xxedi_scm_inb_850_xml_load_stg
            WHERE
                oic_instance_id = oic_id;

            COMMIT;*/
        --
    END load_order_tables;


    PROCEDURE process_data (  --PROCESS_DATA_INTO_INTF
        oic_id IN VARCHAR2
        ) IS
            l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS NUMBER := 30; -- Number of days to reprocess the records in the interface table
        BEGIN
            BEGIN -- block to handle reprocessing of records  >>>>>>>>>>>
                FOR intf_rec IN ( -- update the matching stage tables records with PROCESSED_FLAG to 'R' to reprocess
                    SELECT
                            ROWNUM
                            ,transaction_id
                            ,CREATION_DATE
                            ,LAST_UPDATE_DATE
                            ,PROCESSED_FLAG
                            ,ERROR_CODE
                            ,ERROR_MESSAGE
                            ,OIC_INSTANCE_ID
                        FROM XXEDI_SCM_INB_850_HEADERS_INT
                        WHERE
                            PROCESSED_FLAG = 'E'
                            AND TRUNC(SYSDATE) - TRUNC(CREATION_DATE) <= l_v_MAX_FILE_AGE_IN_DAYS_FOR_REPROCESS
                            --AND ERROR_CODE = g_v_PRE_VALIDATION_ERROR_CODE
                ) LOOP
                    UPDATE XXEDI_SCM_INB_850_HEADERS_STG   SET PROCESSED_FLAG = 'R', ERROR_CODE = NULL, ERROR_MESSAGE = NULL   WHERE  trading_partner_id  ||  purchase_order_number  =  intf_rec.transaction_id;
                    UPDATE XXEDI_SCM_INB_850_LINES_STG     SET PROCESSED_FLAG = 'R', ERROR_CODE = NULL, ERROR_MESSAGE = NULL   WHERE  trading_partner_id  ||  purchase_order_number  =  intf_rec.transaction_id;
                    UPDATE XXEDI_SCM_INB_850_HEADERS_INT   SET PROCESSED_FLAG = 'D'                                            WHERE  transaction_id = intf_rec.transaction_id;
                END LOOP;
                COMMIT;
            END; -- block to handle reprocessing of records  <<<<<<<<<<<

            -- Update field with 'D' where the record is already in the interface table
            FOR STG_REC IN ( SELECT trading_partner_id || purchase_order_number AS COMPOSED_PK
                                FROM  XXEDI_SCM_INB_850_HEADERS_STG 
                                WHERE processed_flag = 'N'
            ) LOOP 
                UPDATE XXEDI_SCM_INB_850_HEADERS_INT  INTF_HEADER
                    SET  INTF_HEADER.processed_flag = 'D',  INTF_HEADER.last_update_date = systimestamp,  INTF_HEADER.last_update_by_name = 'process_data'  
                    WHERE  INTF_HEADER.transaction_id = STG_REC.COMPOSED_PK  AND  INTF_HEADER.processed_flag  IN  ( 'P', 'N' )
                ;
                UPDATE XXEDI_SCM_INB_850_LINES_INT    INTF_LINES
                    SET  INTF_LINES.processed_flag = 'D',  INTF_LINES.last_update_date = systimestamp,  INTF_LINES.last_update_by_name = 'process_data'
                    WHERE  INTF_LINES.transaction_id = STG_REC.COMPOSED_PK  AND  INTF_LINES.processed_flag  IN  ( 'P', 'N' )
                ;
            END LOOP;

            INSERT INTO XXEDI_SCM_INB_850_HEADERS_INT (
                file_name,
                transaction_id,
                source_transaction_number,
                source_transaction_system,
                source_transaction_id,
                business_unit_name,
                buying_party_number,
                customer_po_number,
                transactional_currency_code,
                transaction_on,
                transaction_type_code,
                requesting_legal_entity,
                orig_system_document_reference,
                partial_ship_allowed_flag,
                priced_on,
                freeze_price_flag,
                freeze_shipping_charge_flag,
                freeze_tax_flag,
                submitted_flag,
                requested_fulfillment_organization_code,
                payment_terms,
                --shipping_carrier,
                --shipping_service_level_code,
                --shipping_mode,
                --shipsetflag,
                requested_ship_date,
                requested_arrival_date,
                latest_acceptable_ship_date,
                earliest_acceptable_ship_date,
                party_name,
                account_number,
                party_number,
                party_id,
                site_use_id,
                test_purposecode,
                primary_po_type_code,
                customer_order_number,
                dept_number,
                department,
                --contact_name,
                --contact_phone,
                --contact_email,
                creation_date,
                created_by_name,
                processed_flag,
                oic_instance_id,
                target_system_document_number
            )  SELECT
                    subquery.file_name,
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
                    (
                        CASE
                            WHEN subquery.requested_arrival_date IS NOT NULL THEN ''
                            ELSE subquery.requested_ship_date
                        END
                    ) AS requested_ship_date,
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
                    oic_id oic_instance_id,
                    subquery.target_system_document_number
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
                                    FROM XXEDI_SCM_INB_850_REFERENCES_STG ref
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
                                        XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                        XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                        XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                        XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                        HPS.PARTY_SITE_ID
                                    FROM
                                        HZ_PARTY_SITE_PVO_INTF HPS,
                                        LOOKUP_VALUES_PVO_INTF FLV,
                                        XXEDI_SCM_INB_850_ADDRESSES_STG ADDR,
                                        XXEDI_SCM_INB_850_REFERENCES_STG REF
                                    WHERE
                                        1 = 1
                                        AND HPS.PARTY_SITE_NUMBER = FLV.ATTRIBUTE2
                                        AND UPPER(FLV.LOOKUP_TYPE) = 'LTF EDI 850 MULTIPLE LOCATION'
                                        AND (ADDR.ADDRESS_LOCATION_NUMBER || REF.REFERENCE_ID) = FLV.ATTRIBUTE1
                                        AND ADDR.ADDRESS_TYPECODE IN ('ST', 'BS', 'BY')
                                        AND ADDR.TRADING_PARTNER_ID(+) = HEADER.TRADING_PARTNER_ID
                                        AND ADDR.PURCHASE_ORDER_NUMBER(+) = HEADER.PURCHASE_ORDER_NUMBER
                                        AND ADDR.PROCESSED_FLAG(+) = 'N'
                                        --AND REF.REFERENCE_QUAL = 'ZZ'
                                        AND REF.DESCRIPTION = 'BUYER VENDOR SUFFIX'
                                        AND REF.TRADING_PARTNER_ID(+) = HEADER.TRADING_PARTNER_ID
                                        AND REF.PURCHASE_ORDER_NUMBER(+) = HEADER.PURCHASE_ORDER_NUMBER
                                        AND REF.PROCESSED_FLAG(+) = 'N'
                                        AND ROWNUM = 1
                                ),
                                (
                                    SELECT
                                        HPS.PARTY_SITE_ID
                                    FROM
                                        HZ_PARTY_SITE_PVO_INTF HPS,
                                        LOOKUP_VALUES_PVO_INTF FLV,
                                        XXEDI_SCM_INB_850_ADDRESSES_STG ADDR
                                    WHERE
                                        1 = 1
                                        AND HPS.PARTY_SITE_NUMBER = FLV.ATTRIBUTE2
                                        AND UPPER(FLV.LOOKUP_TYPE) = 'LTF EDI 850 MULTIPLE LOCATION'
                                        AND (ADDR.ADDRESS_LOCATION_NUMBER || SUBSTR(HEADER.VENDOR, -3)) = FLV.ATTRIBUTE1
                                        AND ADDR.ADDRESS_TYPECODE IN ('ST', 'BS', 'BY')
                                        AND ADDR.TRADING_PARTNER_ID(+) = HEADER.TRADING_PARTNER_ID
                                        AND ADDR.PURCHASE_ORDER_NUMBER(+) = HEADER.PURCHASE_ORDER_NUMBER
                                        AND ADDR.PROCESSED_FLAG(+) = 'N'
                                        AND ROWNUM = 1
                                ),
                                (
                                    SELECT
                                        HPS.PARTY_SITE_ID
                                    FROM
                                        HZ_PARTY_SITE_PVO_INTF HPS,
                                        LOOKUP_VALUES_PVO_INTF FLV,
                                        XXEDI_SCM_INB_850_ADDRESSES_STG ADDR
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
                                        HZ_CUSTOMER_ACCOUNT_PVO_INTF       hca,
                                        HZ_CUST_ACCT_SITE_USE_LOC_PVO_INTF hcasa,
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
                                        XXEDI_SCM_INB_850_ADDRESSES_STG    addressesBS
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
                                        HZ_CUSTOMER_ACCOUNT_PVO_INTF       hca,
                                        HZ_CUST_ACCT_SITE_USE_LOC_PVO_INTF hcasa,
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
                                            --AND hcasadff.ltf_edi_location_code_ = addresses.address_location_number
                                            AND hcasa.cust_account_id = hca.cust_account_id
                                            --AND hcasa.ship_to_flag IN ( 'P', 'Y' )
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
                                /*(
                                    SELECT
                                        contacts.contac_tname
                                    FROM
                                        XXEDI_SCM_INB_850_CONTACTS_STG contacts
                                    WHERE
                                            1 = 1
                                        AND contacts.trading_partner_id (+) = header.trading_partner_id
                                        AND contacts.purchase_order_number (+) = header.purchase_order_number
                                        AND contacts.contact_type_code (+) IN ( 'RE', 'OC' )
                                        AND ROWNUM = 1
                                )                                                         contact_name,
                                (
                                    SELECT
                                        contacts.primary_phone
                                    FROM
                                        XXEDI_SCM_INB_850_CONTACTS_STG contacts
                                    WHERE
                                            1 = 1
                                        AND contacts.trading_partner_id (+) = header.trading_partner_id
                                        AND contacts.purchase_order_number (+) = header.purchase_order_number
                                        AND contacts.contact_type_code (+) IN ( 'RE', 'OC' )
                                        AND contacts.primary_phone IS NOT NULL
                                        AND ROWNUM = 1
                                )                                                         contact_phone,
                                (
                                    SELECT
                                        contacts.primary_email
                                    FROM
                                        XXEDI_SCM_INB_850_CONTACTS_STG contacts
                                    WHERE
                                            1 = 1
                                        AND contacts.trading_partner_id (+) = header.trading_partner_id
                                        AND contacts.purchase_order_number (+) = header.purchase_order_number
                                        AND contacts.contact_type_code (+) IN ( 'RE', 'OC' )
                                        AND contacts.primary_email IS NOT NULL
                                        AND ROWNUM = 1
                                )                                                         contact_email,*/
                                sysdate                                                   creation_date,
                                'INTEGRATION'                                             created_by_name,
                                'P'                                                       processed_flag,
                                header.target_system_document_number                      target_system_document_number,
                                ROW_NUMBER()
                                OVER(PARTITION BY header.trading_partner_id, header.purchase_order_number
                                    ORDER BY
                                        header.last_update_date DESC
                                )                                                         AS rnk
                            -- fields
                        FROM 
                                      XXEDI_SCM_INB_850_HEADERS_STG        HEADER
                            LEFT JOIN XXEDI_SCM_INB_850_PAYMENT_TERMS_STG  TERMS            ON  TERMS.trading_partner_id            = HEADER.trading_partner_id   AND TERMS.purchase_order_number            =  HEADER.purchase_order_number
                            LEFT JOIN XXEDI_SCM_INB_850_CARRIER_INFO_STG   CARRIER          ON  CARRIER.trading_partner_id          = HEADER.trading_partner_id   AND CARRIER.purchase_order_number          =  HEADER.purchase_order_number
                            LEFT JOIN XXEDI_SCM_INB_850_ADDRESSES_STG      ADDRESSES        ON  ADDRESSES.trading_partner_id        = HEADER.trading_partner_id   AND ADDRESSES.purchase_order_number        =  HEADER.purchase_order_number  AND  ADDRESSES.address_typecode        = 'ST'
                            LEFT JOIN XXEDI_SCM_INB_850_ADDRESSES_STG      ADDRESSESBILLTO  ON  ADDRESSESBILLTO.trading_partner_id  = HEADER.trading_partner_id   AND ADDRESSESBILLTO.purchase_order_number  =  HEADER.purchase_order_number  AND  ADDRESSESBILLTO.address_typecode  = 'BT'
                            LEFT JOIN XXEDI_SCM_INB_850_REFERENCES_STG     REFERENCE        ON  REFERENCE.trading_partner_id        = HEADER.trading_partner_id   AND REFERENCE.purchase_order_number        =  HEADER.purchase_order_number  AND  REFERENCE.reference_qual          = 'GEN'
                        WHERE HEADER.processed_flag IN  ('N' , 'R')
                    ) subquery
                WHERE
                    subquery.rnk = 1;
            COMMIT;


            /* -- removed by Joao @2025-05-07. logic added to the insert above
                DECLARE
                    CURSOR c_data_header IS
                        SELECT  DISTINCT  PURCHASE_ORDER_NUMBER, TRADING_PARTNER_ID  FROM  XXEDI_SCM_INB_850_HEADERS_STG  WHERE  processed_flag (+)  =  'N';

                    CURSOR c_references(pc_TRADING_PARTNER_ID in varchar2, pc_PURCHASE_ORDER_NUMBER in varchar2) IS
                        SELECT
                            reference_qual, reference_id
                        FROM
                            XXEDI_SCM_INB_850_REFERENCES_STG
                        WHERE
                                TRADING_PARTNER_ID     =  pc_TRADING_PARTNER_ID
                            AND PURCHASE_ORDER_NUMBER  =  pc_PURCHASE_ORDER_NUMBER
                            AND reference_id           IS NOT NULL
                            AND reference_qual         IS NOT NULL;
                    v_actual_references VARCHAR2(5000);	
                    v_count_reference NUMBER := 0;
                BEGIN
                    FOR r_data_header in c_data_header LOOP
                        v_actual_references := NULL;
                        v_count_reference := 0;
                        FOR r_references IN c_references(r_data_header.TRADING_PARTNER_ID, r_data_header.PURCHASE_ORDER_NUMBER ) LOOP
                            IF v_count_reference = 0 THEN  v_actual_references := r_references.reference_qual  ||   '_'   ||  r_references.reference_id;
                            ELSE                           v_actual_references := v_actual_references          ||  ' / '  ||  r_references.reference_qual  ||  '_'  ||  r_references.reference_id;
                            END IF;
                            v_count_reference := v_count_reference + 1;
                        END LOOP;
                        UPDATE XXEDI_SCM_INB_850_HEADERS_INT  SET  orig_system_document_reference = v_actual_references  WHERE  transaction_id =  r_data_header.TRADING_PARTNER_ID  ||  r_data_header.PURCHASE_ORDER_NUMBER;
                        COMMIT;
                    END LOOP;
                END;
            */

            INSERT INTO XXEDI_SCM_INB_850_LINES_INT (
                transaction_id,
                source_transaction_line_id,
                source_transaction_line_number,
                source_transaction_schedule_id,
                source_schedule_number,
                transaction_category_code,
                product_number,
                ordered_quantity,
                ordered_uom_code,
                customer_po_number,
                customer_po_line_number,
                ATTRIBUTE_DATE1,
                ATTRIBUTE_DATE2,
                ATTRIBUTE_CHAR3,
                ATTRIBUTE_CHAR1,
                ATTRIBUTE_CHAR2,
                ATTRIBUTE_NUMBER1,
                ATTRIBUTE_NUMBER2,
                ATTRIBUTE_CHAR4,			
                --original_cases,
                --original_pounds,
                --edi_price,
                allow_chrg_amt,
                allow_chrg_percent,
                creation_date,
                created_by_name,
                processed_flag,
                oic_instance_id
            )
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
                    linesubquery.ATTRIBUTE_NUMBER2,
                    linesubquery.ATTRIBUTE_CHAR4,
                    linesubquery.allow_chrg_amt,
                    linesubquery.allow_chrg_percent,				
                    linesubquery.creation_date,
                    linesubquery.created_by_name,
                    linesubquery.processed_flag,
                    oic_id oic_instance_id
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
                                    XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                    XXEDI_SCM_INB_850_DATES_STG so_dates
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
                                FROM XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG ca
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
                            XXEDI_SCM_INB_850_LINES_STG lines
                        WHERE
                            lines.processed_flag IN ('N' , 'R')
                    ) linesubquery
                WHERE
                    linesubquery.rnk = 1
                ORDER BY
                    linesubquery.transaction_id,
                    linesubquery.source_transaction_line_id;

            COMMIT;

            /* -- removed by Joao @2025-05-07. logic added to the insert above
                DECLARE
                    cursor c_data_header is
                        SELECT DISTINCT  line_sequence_number, trading_partner_id, purchase_order_number  FROM  XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG   where  processed_flag (+) = 'N';

                    cursor c_references(pc_line_sequence_number in varchar2, pc_TRADING_PARTNER_ID in varchar2, pc_PURCHASE_ORDER_NUMBER in varchar2) is
                        SELECT
                                decode(CHARGESALLOWANCE.allow_chrg_amt    ,'','',CHARGESALLOWANCE.allow_chrg_amt)
                            ||  decode(CHARGESALLOWANCE.allow_chrg_percent,'','',decode(CHARGESALLOWANCE.allow_chrg_amt    ,'',CHARGESALLOWANCE.allow_chrg_percent ,' / '  ||  CHARGESALLOWANCE.allow_chrg_percent))
                            ||  decode(CHARGESALLOWANCE.allow_chrg_rate   ,'','',decode(CHARGESALLOWANCE.allow_chrg_percent,'',CHARGESALLOWANCE.allow_chrg_rate    ,' / '  ||  CHARGESALLOWANCE.allow_chrg_rate   )) 
                            as chargeValue
                        FROM
                            XXEDI_SCM_INB_850_LINE_CHARGESALLOWANCE_STG CHARGESALLOWANCE
                        WHERE
                                1 = 1
                            AND CHARGESALLOWANCE.line_sequence_number   =  pc_line_sequence_number
                            AND CHARGESALLOWANCE.trading_partner_id     =  pc_TRADING_PARTNER_ID
                            AND CHARGESALLOWANCE.purchase_order_number  =  pc_PURCHASE_ORDER_NUMBER
                            AND CHARGESALLOWANCE.processed_flag         =  'N';
                    v_actual_references VARCHAR2(5000);	
                    v_count_reference NUMBER := 0;

                BEGIN
                    FOR r_data_header in c_data_header LOOP
                        v_actual_references  :=  NULL;
                        v_count_reference    :=  0;
                        FOR r_references IN c_references(r_data_header.line_sequence_number ,r_data_header.TRADING_PARTNER_ID, r_data_header.PURCHASE_ORDER_NUMBER) LOOP
                            IF v_count_reference = 0 THEN  v_actual_references := r_references.chargeValue;
                            ELSE                           v_actual_references := v_actual_references  ||  ' / '  ||  r_references.chargeValue;
                            END IF;
                            v_count_reference  :=  v_count_reference + 1;
                        END LOOP;
                        UPDATE  XXEDI_SCM_INB_850_LINES_INT  SET  ATTRIBUTE_NUMBER2 = v_actual_references
                            WHERE source_transaction_line_number =  r_data_header.line_sequence_number  AND  transaction_id = r_data_header.TRADING_PARTNER_ID  ||  r_data_header.PURCHASE_ORDER_NUMBER;
                            COMMIT;
                    END LOOP;
                END;
            */

            UPDATE XXEDI_SCM_INB_850_HEADERS_INT  SET  processed_flag = 'P'  WHERE  processed_flag = 'N'  AND transaction_id IS NOT NULL;
            COMMIT;

            /*UPDATE XXEDI_SCM_INB_850_HEADERS_INT h
                SET
                    h.processed_flag = 'P',
                    h.error_code = 'E',
                    h.error_message = 'PO due date is before the Order Date',
                    h.last_update_date = sysdate,
                    h.last_update_by_name = 'INTEGRATION'
                WHERE
                    EXISTS (
                        SELECT
                            1
                        FROM
                            XXEDI_SCM_INB_850_VALIDATION_STG v
                        WHERE
                                v.po_number = h.customer_po_number
                            AND v.date_check = 'PO due date is before the Order Date'
                            AND v.oic_instance_id = oic_id
                    )
                AND h.oic_instance_id = oic_id
            ;*/

            UPDATE XXEDI_SCM_INB_850_HEADERS_INT h
                SET
                    h.processed_flag = 'P',
                    h.error_code = 'E',
                    h.error_message = 'RequestedShipDate, RequestedArrivalDate, LatestAcceptableShipDate and EarliestAcceptableShipDate can be not null',
                    h.last_update_date = sysdate,
                    h.last_update_by_name = 'INTEGRATION'
                WHERE
                    EXISTS (
                        SELECT
                            1
                        FROM
                            XXEDI_SCM_INB_850_HEADERS_INT v
                        WHERE
                                v.TRANSACTION_ID = h.TRANSACTION_ID
                            --AND v.po_number = h.customer_po_number
                            AND v.requested_ship_date is null
                            AND v.requested_arrival_date is null
                            AND v.latest_acceptable_ship_date is null
                            AND v.earliest_acceptable_ship_date is null
                            AND v.oic_instance_id = oic_id
                    )
                AND h.oic_instance_id = oic_id
            ;

            UPDATE XXEDI_SCM_INB_850_HEADERS_INT h
                SET
                    h.processed_flag = 'P',
                    h.error_code = 'E',
                    h.error_message = 'TradindPartnerId and PurchaseOrderNumber are required fields in the file',
                    h.last_update_date = sysdate,
                    h.last_update_by_name = 'INTEGRATION'
                WHERE
                    EXISTS ( SELECT 1
                        FROM  XXEDI_SCM_INB_850_HEADERS_INT v
                        WHERE v.transaction_id is null  AND  h.oic_instance_id = oic_id
                    )
                    AND h.oic_instance_id = oic_id
                    AND h.transaction_id is null
            ;

            UPDATE  XXEDI_SCM_INB_850_LINES_INT          SET  processed_flag  =  'P'                                                                            WHERE   processed_flag  =  'N'; 
            UPDATE  XXEDI_SCM_INB_850_HEADERS_STG        SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_ADDRESSES_STG      SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_CARRIER_INFO_STG   SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N';
                UPDATE  XXEDI_SCM_INB_850_DATES_STG          SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_FOB_STG            SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_PAYMENT_TERMS_STG  SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_REFERENCES_STG     SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_SUMMARY_STG        SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_LINES_STG          SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_LINE_DATES_STG     SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
                UPDATE  XXEDI_SCM_INB_850_LINE_ITEMS_STG     SET  processed_flag  =  'Y' ,  last_update_date  =  sysdate ,  last_update_by_name  =  'INTEGRATION'   WHERE   processed_flag   = 'N'; 
            COMMIT;
        --
    END process_data;


    PROCEDURE update_flag_processed (
            oic_id IN VARCHAR2,
            key_id IN VARCHAR2
        ) IS
        BEGIN
            UPDATE XXEDI_SCM_INB_850_HEADERS_INT
            SET
                processed_flag = 'Y',
                error_code = NULL,
                oic_instance_id = oic_id,
                error_message = NULL,
                last_update_date = sysdate,
                last_update_by_name = 'INTEGRATION'
            WHERE
                    processed_flag = 'P'
                AND transaction_id = key_id;

            UPDATE XXEDI_SCM_INB_850_LINES_INT
            SET
                processed_flag = 'Y',
                error_code = NULL,
                oic_instance_id = oic_id,
                error_message = NULL,
                last_update_date = sysdate,
                last_update_by_name = 'INTEGRATION'
            WHERE
                    processed_flag = 'P'
                AND transaction_id = key_id;

            COMMIT;
        --
    END update_flag_processed;


    PROCEDURE update_flag_error (
            oic_id     IN VARCHAR2,
            key_id     IN VARCHAR2,
            error_text IN VARCHAR2
        ) IS
        BEGIN
            UPDATE XXEDI_SCM_INB_850_HEADERS_INT
            SET
                processed_flag = 'E',
                error_code = 'E',
                oic_instance_id = oic_id,
                error_message = substr(error_text,1,8000),
                last_update_date = sysdate,
                last_update_by_name = 'INTEGRATION'
            WHERE
                    processed_flag = 'P'
                AND transaction_id = key_id;

            UPDATE XXEDI_SCM_INB_850_LINES_INT
            SET
                processed_flag = 'E',
                error_code = 'E',
                oic_instance_id = oic_id,
                error_message = substr(error_text,1,8000),
                last_update_date = sysdate,
                last_update_by_name = 'INTEGRATION'
            WHERE
                    processed_flag = 'P'
                AND transaction_id = key_id;

            COMMIT;
        --
    END update_flag_error;


    PROCEDURE validation_process (
            oic_id            IN VARCHAR2,
            current_file_name IN VARCHAR2
        ) IS
        BEGIN
            INSERT INTO XXEDI_SCM_INB_850_VALIDATION_STG (
                file_name,
                trading_partner_id,
                po_number,
                vendor_name,
                creation_date,
                last_update_date,
                oic_instance_id
            )
                SELECT
                    current_file_name                                                        AS file_name,
                    header.trading_partner_id                                                AS trading_partner_id,
                    header.purchase_order_number                                             AS po_number,
                    (
                        SELECT
                            hz.party_name
                        FROM
                            hz_customer_account_pvo_intf hca,
                            hz_customer_pvo_intf hz,
                            HZ_CUST_ACCT_DFF_PVO_INTF hcadff
                        WHERE
                            hcadff.LTF_BUYING_PARTY_ID_ = header.trading_partner_id
                            AND hz.party_id = hca.party_id
                            AND hcadff.CUST_ACCOUNT_ID = hca.CUST_ACCOUNT_ID
                            AND ROWNUM = 1
                     )                                                                       AS vendor_name,
                    sysdate                                                                  AS creation_date,
                    sysdate                                                                  AS last_update_date,
                    oic_id                                                                   AS oic_instance_id
                FROM
                    XXEDI_SCM_INB_850_HEADERS_STG   header
                    --XXEDI_SCM_INB_850_LINES_STG     lines
                WHERE
                        1 = 1
                    and header.target_system_document_number         =  current_file_name
                    --AND lines.trading_partner_id              (+)  =  header.trading_partner_id
                    --AND lines.purchase_order_number           (+)  =  header.purchase_order_number
                    AND header.processed_flag                   (+)  =  'N'
                    --AND lines.processed_flag                  (+)  =  'N'
                    AND header.oic_instance_id                       =  oic_id;
                    --AND lines.oic_instance_id                      =  oic_id;

            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                raise;
    END validation_process;



END XXEDI_SCM_INB_850_PKG;