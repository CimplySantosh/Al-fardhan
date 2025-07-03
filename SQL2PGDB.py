import pyodbc
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sys
import time  # For sleep

# SQL Server Connection Parameters
sql_server = 'tcp:al-fardan.database.windows.net,1433'
sql_database = 'al_fardan_poc'
sql_username = 'sync_user'
sql_password = '$y$t3ch@123'

# PostgreSQL Connection Parameters
pg_host = '20.42.94.20'
pg_port = '5432'
pg_database = 'alfardanpoc'
pg_user = 'admin'
pg_password = quote_plus('Systech@123')

# Step 1: Connect to SQL Server
def get_sqlserver_connection():
    try:
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};' \
                   f'SERVER={sql_server};DATABASE={sql_database};' \
                   f'UID={sql_username};PWD={sql_password}'
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"SQL Server connection error: {e}")
        sys.exit(1)

# Step 2: Connect to Postgres
def get_postgres_engine():
    try:
        engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
        return engine
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        sys.exit(1)

# Step 3: Read data from SQL Server
def fetch_data_from_sqlserver():
    conn = get_sqlserver_connection()
    query = "SELECT * FROM dbo.CDC_Row_Audit WHERE processed_data = 'N'"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Step 4: Load data into PostgreSQL
def load_data_to_postgres(df):
    if df.empty:
        print("No new data to load.")
        return

    engine = get_postgres_engine()

    if 'AuditID' in df.columns:
        df = df.drop(columns=['AuditID'])

    df.columns = [col.lower() for col in df.columns]

    # You might want to use a staging table or upsert logic depending on your use case
    df.to_sql('cdc_row_audit', engine, if_exists='append', index=False)
    print(f"{len(df)} records loaded into PostgreSQL.")

# Step 5: Update SQL Server records as processed
def mark_records_processed(df):
    if df.empty:
        return

    conn = get_sqlserver_connection()
    cursor = conn.cursor()

    for op_key in df['OperationTimeKey']:
        cursor.execute("UPDATE dbo.CDC_Row_Audit SET processed_data = 'Y' WHERE OperationTimeKey = ?", op_key)

    conn.commit()
    cursor.close()
    conn.close()
    print("Source records marked as processed.")

# Main Workflow with Loop
if __name__ == "__main__":
    try:
        while True:
            print("Checking for new data...")
            df = fetch_data_from_sqlserver()
            load_data_to_postgres(df)
            mark_records_processed(df)
            print("Sleeping for 5 seconds...\n")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Script terminated by user.")