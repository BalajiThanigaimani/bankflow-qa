import sqlite3
import pyodbc
from pathlib import Path

class DBManager:
    def __init__(self, db_type="sqlite", db_path="bankflow.db",
                 server=None, database=None, username=None, password=None):
        self.db_type = db_type
        self.db_path = Path(db_path)
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
        self.connect()

    def connect(self):
        if self.db_type == "sqlite":
            self.connection = sqlite3.connect(self.db_path)
        elif self.db_type == "azure":
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
                "Connection Timeout=30;"
            )
            self.connection = pyodbc.connect(conn_str, autocommit=True)
        else:
            raise ValueError(f"Unsupported DB type: {self.db_type}")

    def execute_query(self, query, params=None):
        cursor = self.connection.cursor()
        # Enable for Azure SQL bulk operations
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
        cursor.execute(query, params or [])
        return cursor

    def fetch_all(self, query, params=None):
        cursor = self.execute_query(query, params)
        return cursor.fetchall()

    def fetch_one(self, query, params=None):
        cursor = self.execute_query(query, params)
        return cursor.fetchone()

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
