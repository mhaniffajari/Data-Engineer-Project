import sys
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# === ARGUMENT HANDLING ===
if len(sys.argv) < 3:
    print("Usage: python get_review_tokopedia.py 'MERCHANT_NAME' PAGE_COUNT")
    sys.exit(1)

MERCHANT_NAME = sys.argv[1].strip()
PAGE_COUNT = int(sys.argv[2])

# === CONFIG ===
TARGET_URL = f"https://www.tokopedia.com/{MERCHANT_NAME}/reviews"
WAIT_TIME = 30
SCROLL_PAUSE = 2

# === SETUP SELENIUM ===
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)
driver.get(TARGET_URL)

# === CREATE OUTPUT FOLDER ===
os.makedirs("tokopedia_review_html", exist_ok=True)

print(f"Target merchant: {MERCHANT_NAME}")
print(f"Pages to extract: {PAGE_COUNT}")
print("Waiting for reviews section to load...")
time.sleep(8)  # let page initialize

page = 1
while page <= PAGE_COUNT:
    print(f"\n Processing page {page}...")

    # === Scroll to ensure lazy content loads ===
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # === Wait for review-related elements to appear ===
    try:
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ",".join([
                "span[data-testid='lblNamaPembeli']",
                "svg[fill='#FFC400']",
                "span[data-testid='lblVarianProduk']",
                "span[data-testid='lblItemUlasan']"
            ])))
        )
    except TimeoutException:
        print(" Timeout: No reviews found on this page.")
        break

    # === SAVE HTML ===
    html_content = driver.page_source
    html_path = f"tokopedia_review_html/reviews_page_{page}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f" Saved HTML for page {page} → {html_path}")

    # === Try to click 'Next Page' ===
    if page == PAGE_COUNT:
        print(" Reached page limit — stopping.")
        break

    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(@aria-label,'Laman berikutnya')]"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", next_button)
        print(" Clicked next page")
        page += 1
        time.sleep(6)

    except NoSuchElementException:
        print(" next page button found — scraping complete.")
        break
    except Exception as e:
        print(f" Retry click due to: {e}")
        try:
            driver.execute_script("arguments[0].click();", next_button)
            page += 1
            time.sleep(6)
        except:
            print(" Failed to go to next page — stopping.")
            break

driver.quit()
print("\n Done! All pages saved in 'tokopedia_review_html' folder.")
