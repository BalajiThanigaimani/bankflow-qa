transaction_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "number"},
        "user_id": {"type": "number"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "status": {"type": "string"}
    },
    "required": ["id", "user_id", "amount", "currency", "status"]
}
