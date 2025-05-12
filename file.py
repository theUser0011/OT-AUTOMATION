import time
import os
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium.webdriver.chrome.service import Service

# MongoDB Atlas connection URL
MONGO_ATLAS_URL = os.getenv("MONGO_URL")  # 👈 Make sure this is set in your environment
DB_NAME = "images"
COLLECTION_NAME = "image_data"
screenshot_path = "screenshot.png"

# Path to your locally downloaded ChromeDriver (Linux binary)
CHROMEDRIVER_PATH = "chromedriver"  # 👈 Make sure this path is correct and executable

def initialize_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    # options.add_argument("--headless=new")

    # Use ChromeDriver from specified path
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

    # Save screenshot
    driver.save_screenshot(screenshot_path)
    print("📸 Screenshot saved successfully.")

    # Connect to MongoDB
    client = MongoClient(MONGO_ATLAS_URL)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Clear existing data
    delete_result = collection.delete_many({})
    print(f"🗑️ Cleared {delete_result.deleted_count} existing document(s).")

    # Insert new image data
    document = {"screenshot_path": screenshot_path, "timestamp": time.time()}
    collection.insert_one(document)
    print("✅ Screenshot path saved to MongoDB.")

except Exception as e:
    print("❌ Error:", e)

finally:
    if driver:
        driver.quit()
        print("🛑 Chrome driver closed.")
    if client:
        client.close()
        print("🔌 MongoDB connection closed.")
