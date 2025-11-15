from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os
import time

# === CONFIG ===
MERCHANT_NAME = "moell-official"
TARGET_URL = f"https://www.tokopedia.com/{MERCHANT_NAME}/product"
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
os.makedirs("tokopedia_product_html", exist_ok=True)

page = 1
while True:
    print(f"\n Processing product page {page}...")

    # === Wait until product list loads ===
# === Wait until product list loads ===
    try:
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, 'span[class="+tnoqZhn89+NHUA43BpiJg=="]')
            )
        )
    except TimeoutException:
        print(" Timeout: Products did not load.")
        break


    # === Save HTML ===
    html_content = driver.page_source
    html_path = f"tokopedia_product_html/products_page_{page}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f" Saved HTML for page {page} → {html_path}")

    # === Try to click 'Next Page' ===
    try:
        next_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a[data-testid='btnShopProductPageNext']")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", next_button)
        page += 1
        time.sleep(6)  # wait for next page to load

    except TimeoutException:
        print(" No next page button found — processing is complete.")
        break
    except Exception as e:
        print(f" Failed to click next page due to: {e}")
        break

driver.quit()
print("\n All product pages saved")
