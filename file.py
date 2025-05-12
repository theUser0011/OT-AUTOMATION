import time
import os
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium.webdriver.chrome.service import Service
from bson import Binary  # 👈 For storing binary data

# MongoDB Atlas connection URL
MONGO_ATLAS_URL = os.getenv("MONGO_URL")
if not MONGO_ATLAS_URL:
    raise ValueError("MONGO_URL not set in environment variables.")

DB_NAME = "images"
COLLECTION_NAME = "image_data"
screenshot_path = "screenshot.png"
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
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    driver.get(url)
    time.sleep(3)

    # Save screenshot to file
    driver.save_screenshot(screenshot_path)
    print("📸 Screenshot saved successfully.")

    # Read image as binary
    with open(screenshot_path, "rb") as f:
        binary_data = Binary(f.read())

    # Connect to MongoDB
    client = MongoClient(MONGO_ATLAS_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Clear existing data
    delete_result = collection.delete_many({})
    print(f"🗑️ Cleared {delete_result.deleted_count} existing document(s).")

    # Insert binary image data
    document = {
        "image_data": binary_data,
        "timestamp": time.time(),
        "description": "NSE screenshot"
    }
    collection.insert_one(document)
    print("✅ Binary screenshot saved to MongoDB.")

except Exception as e:
    print("❌ Error:", e)

finally:
    if driver:
        driver.quit()
        print("🛑 Chrome driver closed.")
    if client:
        client.close()
        print("🔌 MongoDB connection closed.")
