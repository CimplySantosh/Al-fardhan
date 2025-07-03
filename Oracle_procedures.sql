CREATE OR REPLACE PROCEDURE proc_customer_master (
    p_operation IN VARCHAR2,
    p_customer_id IN NUMBER,
    p_customer_name IN VARCHAR2,
    p_customer_type IN VARCHAR2
)
AS
BEGIN
  IF p_operation = 'I' THEN
        INSERT INTO Customer_master (Customer_id, Customer_name, Customer_type)
        VALUES (p_customer_id, p_customer_name, p_customer_type);
    ELSIF p_operation = 'U' THEN
        UPDATE Customer_master
        SET Customer_name = p_customer_name,
            Customer_type = p_customer_type
        WHERE Customer_id = p_customer_id;
    ELSIF p_operation = 'D' THEN
        DELETE FROM Customer_master
        WHERE Customer_id = p_customer_id;
    END IF;
END;




CREATE OR REPLACE PROCEDURE proc_customer_detail (
    p_operation     IN VARCHAR2,
    p_customer_id   IN NUMBER,
    p_detail_id     IN NUMBER,
    p_phone_number  IN VARCHAR2
)
AS
BEGIN
    IF p_operation = 'I' THEN
        INSERT INTO Customer_detail (Customer_id, Detail_id, Phone_number)
        VALUES (p_customer_id, p_detail_id, p_phone_number);

    ELSIF p_operation = 'U' THEN
        UPDATE Customer_detail
        SET Phone_number = p_phone_number
        WHERE Customer_id = p_customer_id AND Detail_id = p_detail_id;

    ELSIF p_operation = 'D' THEN
        DELETE FROM Customer_detail
        WHERE Customer_id = p_customer_id AND Detail_id = p_detail_id;

    END IF;
END;