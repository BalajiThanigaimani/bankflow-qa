import pytest
from playwright.sync_api import sync_playwright

@pytest.fixture(scope="session")
def browser():
    # Launch browser one time for all tests
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=500)
        yield browser
        browser.close()

@pytest.fixture()
def page(browser):
    # Create a fresh page for each test
    context = browser.new_context()
    page = context.new_page()
    yield page
    context.close()
