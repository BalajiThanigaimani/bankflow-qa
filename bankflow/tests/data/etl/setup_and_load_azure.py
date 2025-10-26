import sqlite3
from bankflow.framework.data.db_manager import DBManager

def setup_and_load_azure():
    # ✅ Connect to SQLite
    sqlite_conn = sqlite3.connect("bankflow.db")
    sqlite_cursor = sqlite_conn.cursor()

    print("✅ Connected to SQLite")

    # ✅ Create SQLite tables if they don't exist
    sqlite_cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_customers (
            customer_id INTEGER,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            country TEXT
        );
    """)

    sqlite_cursor.execute("""
        CREATE TABLE IF NOT EXISTS target_customers (
            customer_id INTEGER,
            full_name TEXT,
            email TEXT,
            country TEXT
        );
    """)

    # ✅ Insert sample data if table is empty
    sqlite_cursor.execute("SELECT COUNT(*) FROM source_customers;")
    if sqlite_cursor.fetchone()[0] == 0:
        sqlite_cursor.executemany(
            "INSERT INTO source_customers VALUES (?, ?, ?, ?, ?);",
            [
                (1, 'John', 'Doe', 'john@example.com', 'USA'),
                (2, 'Sara', 'Khan', 'sara@xyz.com', 'Canada'),
                (3, 'Mike', 'Lee', None, 'UK'),
                (3, 'Mike', 'Lee', None, 'UK')  # Duplicate for ETL validation
            ]
        )

    sqlite_cursor.execute("SELECT COUNT(*) FROM target_customers;")
    if sqlite_cursor.fetchone()[0] == 0:
        sqlite_cursor.executemany(
            "INSERT INTO target_customers VALUES (?, ?, ?, ?);",
            [
                (1, 'John Doe', 'john@example.com', 'USA'),
                (2, 'Sara Khan', 'sara@xyz.com', 'Canada')
                # Missing customer 3 to simulate ETL issue
            ]
        )

    sqlite_conn.commit()
    print("✅ SQLite tables ready with sample data")

    # ✅ Connect to Azure SQL
    azure_db = DBManager(
        db_type="azure",
        server="balajiazure-sql-server.database.windows.net",
        database="azuresourceDB",
        username="balajiadmin",
        password="Balaji@123"
    )

    print("✅ Connected to Azure SQL")

    # ✅ Drop Azure tables if they exist
    azure_db.execute_query("DROP TABLE IF EXISTS source_customers;")
    azure_db.execute_query("DROP TABLE IF EXISTS target_customers;")

    # ✅ Create Azure tables
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

    print("✅ Azure tables created")

    # ✅ Load SQLite → Azure
    sqlite_cursor.execute("SELECT * FROM source_customers;")
    for row in sqlite_cursor.fetchall():
        azure_db.execute_query(
            "INSERT INTO source_customers VALUES (?, ?, ?, ?, ?);", row
        )

    sqlite_cursor.execute("SELECT * FROM target_customers;")
    for row in sqlite_cursor.fetchall():
        azure_db.execute_query(
            "INSERT INTO target_customers VALUES (?, ?, ?, ?);", row
        )

    print("✅ Data loaded from SQLite → Azure successfully ✅")

if __name__ == "__main__":
    setup_and_load_azure()
