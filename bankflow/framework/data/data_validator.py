class DataValidator:
    @staticmethod
    def compare_records(api_data, db_data):
        """
        Compare API response vs DB row
        """
        return api_data == db_data

    @staticmethod
    def compare_csv_to_db(csv_rows, db_rows):
        """
        Compare CSV rows vs DB rows
        """
        return csv_rows == db_rows
