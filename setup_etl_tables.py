import sqlite3

connection = sqlite3.connect("bankflow.db")
cursor = connection.cursor()

# Create source table
cursor.execute("""
CREATE TABLE IF NOT EXISTS source_customers (
    customer_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT
);
""")

# Create target table
cursor.execute("""
CREATE TABLE IF NOT EXISTS target_customers (
    customer_id INTEGER,
    full_name TEXT,
    email TEXT,
    country TEXT
);
""")

# Insert sample data into source table
cursor.execute("DELETE FROM source_customers;")
cursor.executemany("""
INSERT INTO source_customers VALUES (?, ?, ?, ?, ?)
""", [
    (1, 'John', 'Doe', 'john@example.com', 'USA'),
    (2, 'Mary', 'Smith', 'mary@example.com', 'Canada'),
    (3, 'Alex', 'Ray', None, 'USA'),
    (3, 'Alex', 'Ray', None, 'USA'),  # duplicate
])

# Insert ETL transformed data into target table
cursor.execute("DELETE FROM target_customers;")
cursor.executemany("""
INSERT INTO target_customers VALUES (?, ?, ?, ?)
""", [
    (1, 'John Doe', 'john@example.com', 'USA'),
    (2, 'Mary Smith', 'mary@example.com', 'Canada'),
])

connection.commit()
connection.close()

print("✅ ETL test tables created successfully!")
