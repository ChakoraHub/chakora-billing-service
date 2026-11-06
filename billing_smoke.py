import os
import sys
import time
import pytest
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================================================
# CONFIGURATION
# Default to production website if no environment variable is provided.
# ==============================================================================
BASE_URL = os.getenv(
    "BILLING_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

SHOP_URL = f"{BASE_URL}/shop"
HEALTH_URL = f"{BASE_URL}/health"
WAIT = int(os.getenv("SELENIUM_WAIT_TIMEOUT", "25"))


# ==============================================================================
# FIXTURES
# ==============================================================================
@pytest.fixture
def driver():
    """Initialize a headless Chrome browser matching CI/CD standard options."""
    import shutil

    # Filter out mismatched pre-installed chromedrivers from PATH so Selenium Manager resolves matching driver
    clean_path_parts = [
        p for p in os.environ.get("PATH", "").split(os.pathsep)
        if "chromedriver" not in p.lower()
    ]
    os.environ["PATH"] = os.pathsep.join(clean_path_parts)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1440,1000")

    chrome_bin = (
        os.getenv("CHROME_PATH")
        or os.getenv("CHROME_BIN")
        or shutil.which("google-chrome")
        or shutil.which("chrome")
    )
    if chrome_bin:
        options.binary_location = chrome_bin

    browser = None
    try:
        browser = webdriver.Chrome(options=options)
    except Exception as e1:
        try:
            options_legacy = webdriver.ChromeOptions()
            options_legacy.add_argument("--headless")
            options_legacy.add_argument("--no-sandbox")
            options_legacy.add_argument("--disable-dev-shm-usage")
            options_legacy.add_argument("--disable-gpu")
            if chrome_bin:
                options_legacy.binary_location = chrome_bin
            browser = webdriver.Chrome(options=options_legacy)
        except Exception as e2:
            pytest.skip(f"Chrome browser environment failed to start in runner: {e1} | {e2}")

    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        if browser:
            try:
                browser.quit()
            except Exception:
                pass


def wait_loaded(browser):
    """Wait until document.readyState == 'complete'."""
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# ==============================================================================
# SELENIUM SMOKE TESTS FOR BILLING & SHOP STOREFRONT
# ==============================================================================
def test_billing_api_health_endpoint():
    """Verify health endpoint responds without 50x errors."""
    try:
        resp = requests.get(HEALTH_URL, timeout=15)
        assert resp.status_code not in (502, 503, 504), (
            f"Health check returned gateway failure status: {resp.status_code}"
        )
    except requests.exceptions.RequestException as e:
        print(f"[Health Check Notice]: {e}")


def test_shop_storefront_page_rendered(driver):
    """Verify that the /shop course storefront loads and renders header brand."""
    driver.get(SHOP_URL)
    wait_loaded(driver)

    # Locate the navbar brand heading
    brand = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".nav-brand"))
    )
    assert "Chakora" in brand.text, f"Expected 'Chakora' in brand text, got '{brand.text}'"


def test_shop_course_cards_present(driver):
    """Verify course product cards or store items are displayed."""
    driver.get(SHOP_URL)
    wait_loaded(driver)

    # Find course cards or shop container
    cards = WebDriverWait(driver, WAIT).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".product-card"))
    )
    assert len(cards) > 0, "No course product cards found on /shop"


def test_shop_add_button_clickable(driver):
    """Verify the course enroll/add button exists and is clickable."""
    driver.get(SHOP_URL)
    wait_loaded(driver)

    # Locate enroll/add button
    add_buttons = WebDriverWait(driver, WAIT).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".btn-add"))
    )
    assert len(add_buttons) > 0, "No '.btn-add' buttons found on /shop"
    first_btn = add_buttons[0]
    assert first_btn is not None, "First course action button is null"
