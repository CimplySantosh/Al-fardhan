import oracledb
import psycopg2
import pyodbc
import time
import os
import json
import datetime

FILES_DIR = '/home/sync_user/sync_code/files'

# ---- Oracle connection ----
ORACLE_CONN_PARAMS = {
    "user": "syncuser",
    "password": "Systech123",
    "dsn": "20.42.94.20/xe"
}

# ---- Postgres connection ----
POSTGRES_CONN = {
    "host": "20.42.94.20",
    "database": "alfardanpoc",
    "user": "admin",
    "password": "Systech@123",
    "port": 5432
}

# ---- SQL Server connection ----
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=tcp:al-fardan.database.windows.net,1433;"
    "DATABASE=al_fardan_poc;"
    "UID=sync_user;"
    "PWD=$y$t3ch@123;"
)

def get_dbms_output(cur):
    lines = []
    status_var = cur.var(int)
    line_var = cur.var(str)
    while True:
        cur.callproc("dbms_output.get_line", (line_var, status_var))
        if status_var.getvalue() != 0:
            break
        lines.append(line_var.getvalue())
    return lines

def dequeue_message_from_oracle():
    with oracledb.connect(**ORACLE_CONN_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.callproc("dbms_output.enable")
            cur.callproc("TEST_AQ_DEQUEUE")
            lines = get_dbms_output(cur)
            return lines

def parse_pipe_message(msg):
    fields = msg.split('|')
    if len(fields) < 20:
        raise ValueError(f"Not enough fields! Got {len(fields)}")
    return {
        'TableName': fields[0],
        'OperationType': fields[1],
        'OperationKey': fields[2],
        'OperationTimeKey': fields[3],
        'PK1': fields[4],
        'PK2': fields[5],
        'PK3': fields[6],
        'PK4': fields[7],
        'PK5': fields[8],
        'PK6': fields[9],
        'PK7': fields[10],
        'PK8': fields[11],
        'PK9': fields[12],
        'BeforeImage': fields[13],
        'AfterImage': fields[14],
        'valid_data': fields[15],
        'processed_data': fields[16],
        'applied_data': fields[17],
        'ChangeDate': fields[18],
        'ChangedBy': fields[19]
    }

def insert_postgres(data):
    with psycopg2.connect(**POSTGRES_CONN) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO CDC_Row_Audit_OB (
                    TableName, OperationType, OperationKey, OperationTimeKey,
                    PK1, PK2, PK3, PK4, PK5, PK6, PK7, PK8, PK9,
                    BeforeImage, AfterImage, valid_data, processed_data, applied_data,
                    ChangeDate, ChangedBy
                ) VALUES (
                    %(TableName)s, %(OperationType)s, %(OperationKey)s, %(OperationTimeKey)s,
                    %(PK1)s, %(PK2)s, %(PK3)s, %(PK4)s, %(PK5)s, %(PK6)s, %(PK7)s, %(PK8)s, %(PK9)s,
                    %(BeforeImage)s, %(AfterImage)s, %(valid_data)s, %(processed_data)s, %(applied_data)s,
                    %(ChangeDate)s, %(ChangedBy)s
                )
            """, data)
            pg_conn.commit()

def insert_sqlserver(data):
    with pyodbc.connect(SQLSERVER_CONN_STR) as sql_conn:
        with sql_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO CDC_Row_Audit_OB (
                    TableName, OperationType, OperationKey, OperationTimeKey,
                    PK1, PK2, PK3, PK4, PK5, PK6, PK7, PK8, PK9,
                    BeforeImage, AfterImage, valid_data, processed_data, applied_data,
                    ChangeDate, ChangedBy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['TableName'], data['OperationType'], data['OperationKey'], data['OperationTimeKey'],
                data['PK1'], data['PK2'], data['PK3'], data['PK4'], data['PK5'], data['PK6'],
                data['PK7'], data['PK8'], data['PK9'], data['BeforeImage'], data['AfterImage'],
                data['valid_data'], data['processed_data'], data['applied_data'],
                data['ChangeDate'], data['ChangedBy']
            ))
            sql_conn.commit()

def mark_processed_in_postgres(data):
    with psycopg2.connect(**POSTGRES_CONN) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("""
                UPDATE CDC_Row_Audit_OB
                SET processed_data = 'Y'
                WHERE OperationKey = %(OperationKey)s
                  AND OperationTimeKey = %(OperationTimeKey)s
                  AND TableName = %(TableName)s
                  AND PK1 = %(PK1)s
            """, data)
            pg_conn.commit()

def write_to_file(data):
    if not os.path.exists(FILES_DIR):
        os.makedirs(FILES_DIR)
    # Create a unique filename using timestamp and OperationKey
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    key = data['OperationKey']
    filename = f"cdc_audit_{key}_{timestamp}.json"
    filepath = os.path.join(FILES_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, default=str)
    print(f"Postgres unavailable. Message written to {filepath}")

def main():
    print("Starting AQ dequeue poller...")
    while True:
        print("Checking for new message in queue...")
        output_lines = dequeue_message_from_oracle()
        got_message = False
        for line in output_lines:
            if line.startswith("Message Text:"):
                msg = line.replace("Message Text: ", "")
                print("Dequeued message:", msg)
                data = parse_pipe_message(msg)
                # Try inserting into Postgres
                try:
                    print("Inserting into PostgreSQL...")
                    insert_postgres(data)
                    postgres_ok = True
                except Exception as e:
                    print("Postgres unavailable. Writing message to file.")
                    write_to_file(data)
                    postgres_ok = False
                # Only try SQL Server and mark as processed if Postgres insert (or file write) succeeded
                if postgres_ok:
                    print("Inserting into SQL Server...")
                    try:
                        insert_sqlserver(data)
                        print("Inserted into SQL Server successfully.")
                        print("Marking as processed in PostgreSQL...")
                        mark_processed_in_postgres(data)
                    except Exception as e:
                        print("Failed to insert into SQL Server. Not marking as processed in Postgres.")
                        print("Error:", e)
                print("Message processed.")
                got_message = True
        if not got_message:
            print("No message found. Sleeping for 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    main()
