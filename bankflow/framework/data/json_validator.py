from jsonschema import validate, ValidationError

class JSONValidator:

    @staticmethod
    def validate_schema(data, schema):
        """
        Validates JSON against a schema definition
        """
        try:
            validate(instance=data, schema=schema)
            return True, None
        except ValidationError as e:
            return False, str(e)
