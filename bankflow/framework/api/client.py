import requests 
from bankflow.framework.config import Config

class ApiClient:
    def __init__(self):
        self.base_url = Config.BASE_URL_API.rstrip("/")

    def get_users(self):
        # Correct endpoint for jsonplaceholder
        return requests.get(f"{self.base_url}/users/")

    def create_user(self, payload):
        # jsonplaceholder allows POST but doesn't persist data
        return requests.post(f"{self.base_url}/users/", json=payload)
