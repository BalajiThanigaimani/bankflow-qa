import sqlite3
from bankflow.framework.data.db_manager import DBManager

def setup_and_load_azure():
    # ✅ Connect to SQLite
    sqlite_conn = sqlite3.connect("bankflow.db")
    sqlite_cursor = sqlite_conn.cursor()

    # ✅ Connect to Azure SQL
    azure_db = DBManager(
        db_type="azure",
        server="balajiazure-sql-server.database.windows.net",
        database="azuresourceDB",
        username="balajiadmin",
        password="Balaji@123"
    )

    print("✅ Connected to Azure SQL")

    # ✅ Drop tables if already exist
    azure_db.execute_query("DROP TABLE IF EXISTS source_customers;")
    azure_db.execute_query("DROP TABLE IF EXISTS target_customers;")

    # ✅ Create tables in Azure
    azure_db.execute_query("""
        CREATE TABLE source_customers (
            customer_id INT,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            country VARCHAR(50)
        );
    """)

    azure_db.execute_query("""
        CREATE TABLE target_customers (
            customer_id INT,
            full_name VARCHAR(100),
            email VARCHAR(100),
            country VARCHAR(50)
        );
    """)

    print("✅ Tables created in Azure")

    # ✅ Load data from SQLite → Azure
    sqlite_cursor.execute("SELECT * FROM source_customers;")
    rows = sqlite_cursor.fetchall()
    for row in rows:
        azure_db.execute_query(
            "INSERT INTO source_customers (customer_id, first_name, last_name, email, country) VALUES (?, ?, ?, ?, ?);",
            row
        )

    sqlite_cursor.execute("SELECT * FROM target_customers;")
    rows = sqlite_cursor.fetchall()
    for row in rows:
        azure_db.execute_query(
            "INSERT INTO target_customers (customer_id, full_name, email, country) VALUES (?, ?, ? ,?);",
            row
        )

    print("✅ Data loaded from SQLite → Azure successfully!")

if __name__ == "__main__":
    setup_and_load_azure()
