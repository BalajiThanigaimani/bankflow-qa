from bankflow.framework.config import Config

class LoginPage:
    def __init__(self, page):
        self.page = page

    def load(self):
        self.page.goto(f"{Config.BASE_URL_UI}/todomvc")
        return self

    def is_loaded(self):
        return self.page.get_by_role("heading").is_visible()
