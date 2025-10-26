import os
import sqlite3
import allure
from allure_commons.types import Severity
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


@allure.severity(Severity.CRITICAL)
@allure.title("✅ Data Completeness Check")
@allure.description("Verify the total number of rows in source matches target after ETL.")
def test_data_completeness():
    db = get_db()
    source_count = db.fetch_all("SELECT COUNT(*) FROM source_customers")[0][0]
    target_count = db.fetch_all("SELECT COUNT(*) FROM target_customers")[0][0]
    allure.attach(str(source_count), "Source Row Count")
    allure.attach(str(target_count), "Target Row Count")
    assert source_count == target_count, f"❌ Data completeness failed: source={source_count}, target={target_count}"


@allure.severity(Severity.NORMAL)
@allure.title("🔁 Duplicate Records Validation")
@allure.description("Ensure no duplicate customer_id present in source table.")
def test_duplicate_records_in_source():
    db = get_db()
    duplicates = db.fetch_all("""
        SELECT customer_id, COUNT(*)
        FROM source_customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1;
    """)
    allure.attach(str(duplicates), "Duplicate Records Found")
    assert len(duplicates) == 0, f"❌ Duplicate records found: {duplicates}"


@allure.severity(Severity.CRITICAL)
@allure.title("⚠️ Mandatory Fields Check")
@allure.description("Check if mandatory fields like email are populated.")
def test_null_mandatory_fields():
    db = get_db()
    null_records = db.fetch_all("""
        SELECT customer_id, email
        FROM source_customers
        WHERE email IS NULL;
    """)
    allure.attach(str(null_records), "Null Mandatory Fields")
    assert len(null_records) == 0, f"❌ Null values in mandatory fields: {null_records}"


@allure.severity(Severity.NORMAL)
@allure.title("🔧 Transformation Logic Check")
@allure.description("Validate name transformation rule: first + last = full_name.")
def test_name_transformation_rule():
    db = get_db()
    incorrect_transform = db.fetch_all("""
        SELECT s.customer_id,
               s.first_name || ' ' || s.last_name AS expected_full_name,
               t.full_name AS actual_full_name
        FROM source_customers s
        LEFT JOIN target_customers t
        ON s.customer_id = t.customer_id
        WHERE TRIM(s.first_name || ' ' || s.last_name) != TRIM(t.full_name);
    """)
    allure.attach(str(incorrect_transform), "Transformation Errors")
    assert len(incorrect_transform) == 0, f"❌ Transformation mismatch: {incorrect_transform}"


@allure.severity(Severity.CRITICAL)
@allure.title("🔎 Missing Records After ETL")
@allure.description("Ensure no missing rows between source and target after ETL.")
def test_missing_records():
    db = get_db()
    missing_rows = db.fetch_all("""
        SELECT s.customer_id
        FROM source_customers s
        LEFT JOIN target_customers t
        ON s.customer_id = t.customer_id
        WHERE t.customer_id IS NULL;
    """)
    allure.attach(str(missing_rows), "Missing Rows")
    assert len(missing_rows) == 0, f"❌ Missing rows found: {missing_rows}"
