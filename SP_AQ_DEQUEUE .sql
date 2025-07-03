CREATE OR REPLACE PROCEDURE TEST_AQ_DEQUEUE AS
    -- Variables to hold the dequeue output
    dequeue_options   DBMS_AQ.DEQUEUE_OPTIONS_T;
    message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
    message_handle    RAW(16);
    -- For JMS Text Message payload
    jms_message      SYS.AQ$_JMS_TEXT_MESSAGE;
BEGIN
    -- Set dequeue options
    dequeue_options.wait := DBMS_AQ.NO_WAIT; -- Change to 5 for 5 seconds wait
    dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;

    -- Dequeue the message
    DBMS_AQ.DEQUEUE(
        queue_name          => 'CDC_ROW_AUDIT_1_EVENT_QUEUE',
        dequeue_options     => dequeue_options,
        message_properties  => message_properties,
        payload             => jms_message,
        msgid               => message_handle
    );

    -- Print the dequeued message (payload as text)
    DBMS_OUTPUT.put_line('Dequeued Message ID: ' || RAWTOHEX(message_handle));
    IF jms_message.TEXT_VC IS NOT NULL THEN
        DBMS_OUTPUT.put_line('Message Text: ' || jms_message.TEXT_VC);
    ELSE
        DBMS_OUTPUT.put_line('Message Text LOB (first 4000): ' || DBMS_LOB.SUBSTR(jms_message.TEXT_LOB, 4000, 1));
    END IF;
   
    COMMIT;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.put_line('No message available in the queue.');
    WHEN OTHERS THEN
        DBMS_OUTPUT.put_line('Error during dequeue: ' || SQLERRM);
	COMMIT;
END;
/