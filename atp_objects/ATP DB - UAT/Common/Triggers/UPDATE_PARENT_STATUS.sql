create or replace TRIGGER "UPDATE_PARENT_STATUS" 
FOR UPDATE ON OIC_AUDIT_HIST
COMPOUND TRIGGER
    TYPE audit_record_type IS RECORD (
        parent_id   VARCHAR2(100 BYTE),
        general_status VARCHAR2(100 BYTE)
    );

    TYPE audit_table_type IS TABLE OF audit_record_type INDEX BY PLS_INTEGER;

    -- Declare a collection to store records
    audit_records audit_table_type;

BEFORE STATEMENT IS
BEGIN
    -- Initialize the collection before each statement
    audit_records := audit_table_type();
END BEFORE STATEMENT;

AFTER EACH ROW IS
BEGIN
    -- Store relevant data in the collection
    IF :NEW.PARENT_ID IS NOT NULL AND :NEW.GENERAL_STATUS IS NOT NULL AND :OLD.GENERAL_STATUS <> :NEW.GENERAL_STATUS THEN
        audit_records(audit_records.COUNT + 1).parent_id := :NEW.PARENT_ID;
        audit_records(audit_records.COUNT).general_status := :NEW.GENERAL_STATUS;
    END IF;
END AFTER EACH ROW;

AFTER STATEMENT IS
BEGIN
    -- Process the collection and update the parent rows
    FOR i IN 1..audit_records.COUNT LOOP
        UPDATE OIC_AUDIT_HIST
        SET GENERAL_STATUS = audit_records(i).general_status
        WHERE INSTANCE_ID = audit_records(i).parent_id;
    END LOOP;


END AFTER STATEMENT;

END update_parent_status;