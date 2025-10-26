import csv
import json

class FileManager:

    @staticmethod
    def write_to_csv(filename, data_list):
        with open(filename, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=data_list[0].keys())
            writer.writeheader()
            for data in data_list:
                writer.writerow(data)

    @staticmethod
    def read_csv(filename):
        with open(filename, mode='r') as file:
            reader = csv.DictReader(file)
            return list(reader)

    # ✅ ADD THIS PART
    @staticmethod
    def write_json(filename, data):
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def read_json(filename):
        with open(filename, "r") as f:
            return json.load(f)
