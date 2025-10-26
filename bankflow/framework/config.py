import os
from dotenv import load_dotenv

load_dotenv()  # Load .env file into environment variables

class Config:
    BASE_URL_UI = os.getenv("BASE_URL_UI")
    BASE_URL_API = os.getenv("BASE_URL_API")
    DB_PATH = os.getenv("DB_PATH")
