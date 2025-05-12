import time
import os
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from bson import Binary

# MongoDB Atlas connection URL
MONGO_ATLAS_URL = os.getenv("MONGO_URL")
if not MONGO_ATLAS_URL:
    raise ValueError("MONGO_URL not set in environment variables.")

# MongoDB config
DB_NAME = "images"
COLLECTION_NAME_IMAGE = "image_data"
COLLECTION_NAME_TABLE = "market_table_data"

# Screenshot file path
screenshot_path = "screenshot.png"

# Path to ChromeDriver
CHROMEDRIVER_PATH = "chromedriver"

def initialize_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    service = Service(executable_path=CHROMEDRIVER_PATH)
    driver = uc.Chrome(service=service, options=options, use_subprocess=False)
    return driver

driver = None
client = None

try:
    # Initialize driver
    driver = initialize_driver()
    url = 'https://www.moneycontrol.com/stocks/marketstats/indexcomp.php?optex=NSE&opttopic=indexcomp&index=9'
    driver.get(url)
    time.sleep(5)

    # Save screenshot to file
    driver.save_screenshot(screenshot_path)
    print("📸 Screenshot saved successfully.")

    # Read screenshot as binary
    with open(screenshot_path, "rb") as f:
        binary_data = Binary(f.read())

    # Connect to MongoDB
    client = MongoClient(MONGO_ATLAS_URL)
    db = client[DB_NAME]

    # Save image to image_data collection
    collection_img = db[COLLECTION_NAME_IMAGE]
    delete_result_img = collection_img.delete_many({})
    print(f"🗑️ Cleared {delete_result_img.deleted_count} image document(s).")

    img_doc = {
        "image_data": binary_data,
        "timestamp": time.time(),
        "description": "NSE screenshot"
    }
    collection_img.insert_one(img_doc)
    print("✅ Binary screenshot saved to MongoDB.")

    # Get 2nd table from page
    tables = driver.find_elements(By.TAG_NAME, "table")
    if len(tables) > 1:
        html_table = tables[1].get_attribute("outerHTML")
        soup = BeautifulSoup(html_table, "html.parser")
        rows = soup.find_all("tr")

        table_data = []
        headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]

        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) == len(headers):
                row_data = {headers[i]: cols[i].get_text(strip=True) for i in range(len(headers))}
                table_data.append(row_data)

        # Save table data
        collection_table = db[COLLECTION_NAME_TABLE]
        delete_result_table = collection_table.delete_many({})
        print(f"🗑️ Cleared {delete_result_table.deleted_count} table document(s).")

        table_doc = {
            "table_data": table_data,
            "timestamp": time.time(),
            "source_url": url,
            "description": "NSE 2nd table from Moneycontrol"
        }
        collection_table.insert_one(table_doc)
        print("📊 Table data saved to MongoDB.")
    else:
        print("⚠️ Less than 2 tables found on the page.")

except Exception as e:
    print("❌ Error:", e)

finally:
    if driver:
        driver.quit()
        print("🛑 Chrome driver closed.")
    if client:
        client.close()
        print("🔌 MongoDB connection closed.")
