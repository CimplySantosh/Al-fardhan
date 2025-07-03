import pyodbc
import time

# SQL Server connection details (update for your environment)
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=al-fardan.database.windows.net,1433;"
    "DATABASE=al_fardan_poc;"
    "UID=sync_user;"
    "PWD=$y$t3ch@123;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

def parse_oracle_dict(s):
    """
    Parse string like {Customer_id:1111;Customer_name:QUEUE;Customer_type:T8}
    to a Python dict.
    """
    if not s or s == "NULL":
        return {}
    s = s.strip("{}")
    if not s:
        return {}
    items = [item.strip() for item in s.split(";") if item.strip()]
    d = {}
    for item in items:
        if ':' in item:
            k, v = item.split(':', 1)
            d[k.strip()] = v.strip()
    return d

def get_audit_rows(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM dbo.CDC_Row_Audit_OB WHERE applied_data = 'N' ORDER BY AuditID")
        columns = [col[0] for col in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

def call_stored_proc(conn, procname, params):
    with conn.cursor() as cur:
        placeholders = ", ".join(["?"] * len(params))
        sql = f"EXEC {procname} {placeholders}"
        print("Calling:", sql, params)
        cur.execute(sql, list(params.values()))

def build_where_clause(audit_row, data):
    where = []
    values = []
    # Use the primary key field names as they appear in the data dictionary if available,
    # fallback to PK1... if not found in the dict.
    for i in range(1, 10):
        pk_val = audit_row.get(f'PK{i}')
        if pk_val and pk_val != '':
            # Try to match actual column name from data dict
            # Default to 'PK1', 'PK2', etc if no better match
            col_name = None
            for key in data.keys():
                if str(data[key]) == str(pk_val):
                    col_name = key
                    break
            if not col_name:
                col_name = f'PK{i}'
            where.append(f"{col_name} = ?")
            values.append(pk_val)
    return " AND ".join(where), values

def apply_audit_row(conn, audit_row):
    tablename = audit_row['TableName']
    op = audit_row['OperationType']
    after_img = audit_row['AfterImage']
    before_img = audit_row['BeforeImage']
    data = parse_oracle_dict(after_img) if after_img else {}
    procbase = f"{tablename}".replace('.', '_')  # e.g., Customer_master or schema.Table

    if op == 'I':
        procname = f"sp_upsert_{procbase}"
        call_stored_proc(conn, procname, data)
    elif op == 'U':
        procname = f"sp_update_{procbase}"
        call_stored_proc(conn, procname, data)
    elif op == 'D':
        data_before = parse_oracle_dict(before_img) if before_img else {}
        procname = f"sp_delete_{procbase}"
        # Pass only PK(s) for delete; adjust as needed
        call_stored_proc(conn, procname, {'Customer_id': audit_row['PK1']})
    else:
        print(f"Unknown operation {op} for AuditID {audit_row['AuditID']}")
        return False
    return True

def mark_applied(conn, audit_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE dbo.CDC_Row_Audit_OB SET applied_data = 'Y' WHERE AuditID = ?", (audit_id,))

def main():
    print("Starting CDC applier poller...")
    while True:
        try:
            with pyodbc.connect(SQLSERVER_CONN_STR, autocommit=True) as conn:
                rows = get_audit_rows(conn)
                if not rows:
                    print("No new audit rows to process. Sleeping 10 seconds...")
                    time.sleep(10)
                else:
                    for row in rows:
                        try:
                            print(f"Processing AuditID={row['AuditID']} Table={row['TableName']} Op={row['OperationType']}")
                            if apply_audit_row(conn, row):
                                mark_applied(conn, row['AuditID'])
                                print("Applied successfully.")
                            else:
                                print("Failed to apply.")
                        except Exception as e:
                            print(f"Error processing AuditID={row['AuditID']}: {e}")
        except Exception as main_e:
            print(f"Main loop error: {main_e}")
            time.sleep(10)

if __name__ == "__main__":
    main()