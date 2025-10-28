import os
import sqlite3
import allure
from allure_commons.types import Severity
from bankflow.framework.data.db_manager import DBManager

# ------------------------------
# Reusable HTML attachment helper
# ------------------------------
def attach_html(title: str, query: str, expected: str, actual: str):
    html = f"""
    <html>
      <body>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:13px;">
          <tr>
            <th align="left" style="background:#f5f5f5;">Query</th>
            <td><pre style="margin:0;white-space:pre-wrap;">{query}</pre></td>
          </tr>
          <tr>
            <th align="left" style="background:#f5f5f5;">Expected</th>
            <td>{expected}</td>
          </tr>
          <tr>
            <th align="left" style="background:#f5f5f5;">Actual</th>
            <td>{actual}</td>
          </tr>
        </table>
      </body>
    </html>
    """
    allure.attach(html, title, allure.attachment_type.HTML)

# ------------------------------
# DB selector helper
# ------------------------------
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

# ==========================================================
#               TESTS (standalone, improved names)
# ==========================================================

@allure.severity(Severity.CRITICAL)
@allure.title("✅ Row Count Consistency Between Source and Target")
@allure.description("Validate total row counts are identical after ETL.")
def test_row_count_consistency_between_source_and_target():
    db = get_db()

    query_source = "SELECT COUNT(*) FROM source_customers;"
    query_target = "SELECT COUNT(*) FROM target_customers;"

    source_count = db.fetch_all(query_source)[0][0]
    target_count = db.fetch_all(query_target)[0][0]

    expected = "Source row count must equal Target row count."
    actual = f"Source={source_count}, Target={target_count}"

    attach_html("Row Count Consistency — Details",
                f"{query_source}\n{query_target}",
                expected,
                actual)

    assert source_count == target_count, f"❌ Row count mismatch: {actual}"

@allure.severity(Severity.NORMAL)
@allure.title("🔁 Duplicate Check on source_customers.customer_id")
@allure.description("Ensure no duplicate customer_id values exist in source table.")
def test_duplicate_check_on_source_customer_id():
    db = get_db()

    query_dups = """
        SELECT customer_id, COUNT(*)
        FROM source_customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1;
    """

    duplicates = db.fetch_all(query_dups)
    dup_count = len(duplicates)

    expected = "No duplicate customer_id should exist in source_customers."
    actual = f"Duplicate groups found: {dup_count}"

    attach_html("Duplicate Check — Details",
                query_dups.strip(),
                expected,
                actual)

    assert dup_count == 0, f"❌ Duplicate records found (count={dup_count})."

@allure.severity(Severity.CRITICAL)
@allure.title("⚠️ Mandatory Fields Not Null Check (email)")
@allure.description("Validate mandatory field 'email' is populated in source.")
def test_mandatory_fields_not_null_email():
    db = get_db()

    query_nulls = """
        SELECT customer_id
        FROM source_customers
        WHERE email IS NULL;
    """

    null_records = db.fetch_all(query_nulls)
    null_count = len(null_records)

    expected = "All mandatory emails must be populated (no NULL)."
    actual = f"Null email rows: {null_count}"

    attach_html("Mandatory Fields — Details",
                query_nulls.strip(),
                expected,
                actual)

    assert null_count == 0, f"❌ Null values in mandatory field 'email' (count={null_count})."

@allure.severity(Severity.NORMAL)
@allure.title("🔧 Transformation Logic – full_name = first_name + ' ' + last_name")
@allure.description("Validate full_name transformation against source first_name/last_name.")
def test_full_name_transformation_rule():
    db = get_db()

    query_transform = """
        SELECT s.customer_id,
               s.first_name || ' ' || s.last_name AS expected_full_name,
               t.full_name AS actual_full_name
        FROM source_customers s
        LEFT JOIN target_customers t
          ON s.customer_id = t.customer_id
        WHERE TRIM(s.first_name || ' ' || s.last_name) != TRIM(t.full_name);
    """

    mismatches = db.fetch_all(query_transform)
    mismatch_count = len(mismatches)

    expected = "Transformed full_name must equal first_name + ' ' + last_name."
    actual = f"Transformation mismatches: {mismatch_count}"

    attach_html("Transformation Rule — Details",
                query_transform.strip(),
                expected,
                actual)

    assert mismatch_count == 0, f"❌ full_name transformation mismatch (count={mismatch_count})."

@allure.severity(Severity.CRITICAL)
@allure.title("🔎 Missing Records Validation Between Source and Target")
@allure.description("Ensure all source rows exist in target after ETL.")
def test_missing_records_between_source_and_target():
    db = get_db()

    query_missing = """
        SELECT s.customer_id
        FROM source_customers s
        LEFT JOIN target_customers t
          ON s.customer_id = t.customer_id
        WHERE t.customer_id IS NULL;
    """

    missing_rows = db.fetch_all(query_missing)
    missing_count = len(missing_rows)

    expected = "Every source customer_id must exist in target_customers."
    actual = f"Missing target rows: {missing_count}"

    attach_html("Missing Records — Details",
                query_missing.strip(),
                expected,
                actual)

    assert missing_count == 0, f"❌ Missing rows between source and target (count={missing_count})."
