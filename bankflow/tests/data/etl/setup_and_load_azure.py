import sqlite3
from bankflow.framework.data.db_manager import DBManager

def setup_and_load_azure():
    # ✅ Connect to SQLite
    sqlite_conn = sqlite3.connect("bankflow.db")
    sqlite_cursor = sqlite_conn.cursor()
    print("✅ Connected to SQLite")

    # ✅ Create clean SQLite source and target tables
    sqlite_cursor.execute("DROP TABLE IF EXISTS source_customers;")
    sqlite_cursor.execute("DROP TABLE IF EXISTS target_customers;")

    sqlite_cursor.execute("""
        CREATE TABLE source_customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            country TEXT
        );
    """)

    sqlite_cursor.execute("""
        CREATE TABLE target_customers (
            customer_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            country TEXT
        );
    """)

    sqlite_cursor.executemany(
        "INSERT INTO source_customers VALUES (?, ?, ?, ?, ?);",
        [
            (1, 'John', 'Doe', 'john@example.com', 'USA'),
            (2, 'Sara', 'Khan', 'sara@xyz.com', 'Canada'),
            (3, 'Mike', 'Lee', 'mike.lee@test.com', 'UK')
        ]
    )

    sqlite_cursor.executemany(
        "INSERT INTO target_customers VALUES (?, ?, ?, ?);",
        [
            (1, 'John Doe', 'john@example.com', 'USA'),
            (2, 'Sara Khan', 'sara@xyz.com', 'Canada'),
            (3, 'Mike Lee', 'mike.lee@test.com', 'UK')
        ]
    )

    sqlite_conn.commit()
    print("✅ Clean sample data loaded into SQLite")

    # ✅ Connect to Azure SQL
    azure_db = DBManager(
        db_type="azure",
        server="balajiazure-sql-server.database.windows.net",
        database="azuresourceDB",
        username="balajiadmin",
        password="Balaji@123"
    )
    print("✅ Connected to Azure SQL")

    # ✅ Recreate clean Azure tables
    azure_db.execute_query("DROP TABLE IF EXISTS source_customers;")
    azure_db.execute_query("DROP TABLE IF EXISTS target_customers;")

    azure_db.execute_query("""
        CREATE TABLE source_customers (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            country VARCHAR(50)
        );
    """)

    azure_db.execute_query("""
        CREATE TABLE target_customers (
            customer_id INT PRIMARY KEY,
            full_name VARCHAR(100),
            email VARCHAR(100),
            country VARCHAR(50)
        );
    """)

    # ✅ Load SQLite data → Azure SQL
    for row in sqlite_cursor.execute("SELECT * FROM source_customers;"):
        azure_db.execute_query(
            "INSERT INTO source_customers VALUES (?, ?, ?, ?, ?);", row
        )

    for row in sqlite_cursor.execute("SELECT * FROM target_customers;"):
        azure_db.execute_query(
            "INSERT INTO target_customers VALUES (?, ?, ?, ?);", row
        )

    print("✅ Clean data loaded from SQLite → Azure ✅")

if __name__ == "__main__":
    setup_and_load_azure()
