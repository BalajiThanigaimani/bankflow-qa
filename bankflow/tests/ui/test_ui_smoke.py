from bankflow.framework.ui.pages.login_page import LoginPage
import time


def test_open_ui(page):
    page.goto("https://demo.playwright.dev/todomvc")
    print("✅ Browser opened. Keeping it open for demo...")
