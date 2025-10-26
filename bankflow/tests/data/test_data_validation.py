import json
import allure
from bankflow.framework.data.db_manager import DBManager
from bankflow.framework.data.data_validator import DataValidator
from bankflow.framework.data.file_manager import FileManager
from bankflow.framework.data.json_validator import JSONValidator
from bankflow.framework.data.schemas import transaction_schema

@allure.feature("Data Validation")
@allure.story("API → JSON → DB → CSV Reconciliation")
@allure.severity(allure.severity_level.CRITICAL)
def test_api_db_csv_json_validation():
    # STEP 1: Mock API Transaction
    api_transaction = {
        "id": 500,
        "user_id": 88,
        "amount": 2500.99,
        "currency": "USD",
        "status": "SUCCESS"
    }
    allure.attach(json.dumps(api_transaction, indent=2), "API JSON Payload", allure.attachment_type.JSON)

    # ✅ STEP 2: JSON Schema Validation
    is_valid, error = JSONValidator.validate_schema(api_transaction, transaction_schema)
    assert is_valid, f"JSON Schema validation failed: {error}"
    allure.attach("JSON schema validation passed ✅", "Schema Validation", allure.attachment_type.TEXT)

    # ✅ STEP 3: Write JSON to file
    json_file = "transaction_export.json"
    FileManager.write_json(json_file, api_transaction)
    allure.attach.file(json_file, "JSON File E" \
    "xport", allure.attachment_type.JSON)

    # ✅ STEP 4: Insert into DB
    db = DBManager("bankflow.db")
    db.connect()
    # Clean up before insert to avoid duplicate key issues
    db.execute_query("DELETE FROM transactions WHERE id = ?", (api_transaction["id"],))
    db.execute_query(
        "INSERT INTO transactions (id, user_id, amount, currency, status) VALUES (?, ?, ?, ?, ?)",
        (api_transaction["id"], api_transaction["user_id"], api_transaction["amount"],
         api_transaction["currency"], api_transaction["status"])
    )

    # ✅ STEP 5: Fetch DB record
    result = db.fetch_all("SELECT * FROM transactions WHERE id=?", (api_transaction["id"],))
    db.close()
    db_record = {
        "id": result[0][0],
        "user_id": result[0][1],
        "amount": result[0][2],
        "currency": result[0][3],
        "status": result[0][4]
    }
    allure.attach(json.dumps(db_record, indent=2), "DB Record", allure.attachment_type.JSON)

    # ✅ STEP 6: CSV Recon
    csv_file = "transaction_export.csv"
    FileManager.write_to_csv(csv_file, [db_record])
    allure.attach.file(csv_file, "CSV Export", allure.attachment_type.CSV)
    csv_data = FileManager.read_csv(csv_file)[0]
    csv_data["id"] = int(csv_data["id"])
    csv_data["user_id"] = int(csv_data["user_id"])
    csv_data["amount"] = float(csv_data["amount"])

    # ✅ STEP 7: Final Validation Summary
    assert DataValidator.compare_records(api_transaction, db_record)
    assert DataValidator.compare_records(api_transaction, csv_data)

    validation_summary = f"""
    ✅ JSON Schema Valid: {is_valid}
    ✅ API == DB: {api_transaction == db_record}
    ✅ API == CSV: {api_transaction == csv_data}
    """
    allure.attach(validation_summary, "Validation Summary", allure.attachment_type.TEXT)
