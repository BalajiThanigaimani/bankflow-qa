import os
import sqlite3
from bankflow.framework.data.db_manager import DBManager

# ✅ DB Selector
def get_db():
    db_type = os.getenv("DB_TYPE", "sqlite")  # Default to local SQLite
    if db_type == "azure":
        return DBManager(
            db_type="azure",
            server="balajiazure-sql-server.database.windows.net",
            database="azuresourceDB",
            username="balajiadmin",
            password="Balaji@123"
        )
    return DBManager(db_path="bankflow.db")

# ✅ 1. Data Completeness Test
def test_data_completeness():
    db = get_db()
    source_count = db.fetch_all("SELECT COUNT(*) FROM source_customers")
    target_count = db.fetch_all("SELECT COUNT(*) FROM target_customers")
    assert source_count == target_count, f"❌ Data completeness failed: source={source_count}, target={target_count}"

# ✅ 2. Duplicate Record Check
def test_duplicate_records_in_source():
    db = get_db()
    duplicates = db.fetch_all("""
        SELECT customer_id, COUNT(*)
        FROM source_customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1;
    """)
    assert len(duplicates) == 0, f"❌ Duplicate records found in source: {duplicates}"

# ✅ 3. Null Value Check
def test_null_mandatory_fields():
    db = get_db()
    null_records = db.fetch_all("""
        SELECT customer_id, email
        FROM source_customers
        WHERE email IS NULL;
    """)
    assert len(null_records) == 0, f"❌ Null values found in mandatory fields: {null_records}"

# ✅ 4. Transformation Rule Check
def test_name_transformation_rule():
    db = get_db()
    incorrect_transform = db.fetch_all("""
        SELECT s.customer_id,
               s.first_name || ' ' || s.last_name AS expected_full_name,
               t.full_name AS actual_full_name
        FROM source_customers s
        LEFT JOIN target_customers t
        ON s.customer_id = t.customer_id
        WHERE t.full_name IS NOT NULL
        AND TRIM(s.first_name || ' ' || s.last_name) != TRIM(t.full_name);
    """)
    assert len(incorrect_transform) == 0, f"❌ Transformation error: {incorrect_transform}"

# ✅ 5. Missing Records Check
def test_missing_records():
    db = get_db()
    missing_rows = db.fetch_all("""
        SELECT s.customer_id
        FROM source_customers s
        LEFT JOIN target_customers t
        ON s.customer_id = t.customer_id
        WHERE t.customer_id IS NULL;
    """)
    assert len(missing_rows) == 0, f"❌ Missing rows after ETL load: {missing_rows}"
