import json
import oracledb  # Use oracledb for newer versions
import psycopg2
import time

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname='alfardanpoc',
    user='admin',
    password='Systech@123',
    host='20.42.94.20',
    port='5432'
)

# Oracle connection
oracle_conn = oracledb.connect(
    user='sync_user',
    password='Systech123',
    dsn='20.42.94.20/xe'
)
oracle_cursor = oracle_conn.cursor()


def fetch_cdc_rows():
    with pg_conn.cursor() as cursor:
        cursor.execute("""
            SELECT TableName, OperationType, AfterImage, PK1, PK2
            FROM CDC_Row_Audit
            WHERE processed_data = 'N' AND valid_data = 'Y'
        """)
        return cursor.fetchall()


def print_plsql_errors(object_name):
    try:
        error_cursor = oracle_conn.cursor()
        error_cursor.execute("""
            SELECT line, position, text
            FROM user_errors
            WHERE name = :1
            ORDER BY sequence
        """, [object_name.upper()])
        errors = error_cursor.fetchall()
        if errors:
            print(f"Compilation errors in {object_name}:")
            for line, pos, text in errors:
                print(f"Line {line}, Pos {pos}: {text}")
        else:
            print(f"No compilation errors found for {object_name}")
    except Exception as e:
        print(f"Could not fetch PL/SQL errors for {object_name}: {e}")


def process_customer_master(op_type, data):
    try:
        oracle_cursor.callproc("SYNC_USER.proc_customer_master", [
            op_type,
            int(data.get("Customer_id")),
            data.get("Customer_name"),
            data.get("Customer_type")
        ])
        print("Successfully called proc_customer_master.")
    except oracledb.DatabaseError as e:
        print(f"Error in proc_customer_master: {e}")
        print_plsql_errors("proc_customer_master")
        raise


def process_customer_detail(op_type, data):
    try:
        oracle_cursor.callproc("SYNC_USER.proc_customer_detail", [
            op_type,
            int(data.get("Customer_id")),
            int(data.get("Detail_id")),
            data.get("Phone_number")
        ])
        print("Successfully called proc_customer_detail.")
    except oracledb.DatabaseError as e:
        print(f"Error in proc_customer_detail: {e}")
        print_plsql_errors("proc_customer_detail")
        raise


def process_rows():
    rows = fetch_cdc_rows()
    for table, op_type, after_image, pk1, pk2 in rows:
        try:
            if op_type == 'D':
                data = {"Customer_id": int(pk1)}
                if table.lower() == "customer_detail":
                    data["Detail_id"] = int(pk2)
            else:
                data = json.loads(after_image)

            if table.lower() == "customer_master":
                process_customer_master(op_type, data)
            elif table.lower() == "customer_detail":
                process_customer_detail(op_type, data)

        except Exception as e:
            print(f"Error processing {table} ({op_type}): {e}")
            continue

    oracle_conn.commit()
    with pg_conn.cursor() as cursor:
        cursor.execute("UPDATE CDC_Row_Audit SET processed_data = 'Y' WHERE processed_data = 'N'")
        pg_conn.commit()

    print(f"Processed {len(rows)} records.")


if __name__ == "__main__":
    try:
        while True:
            print("Checking for new records...")
            process_rows()
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        oracle_cursor.close()
        oracle_conn.close()
        pg_conn.close()
