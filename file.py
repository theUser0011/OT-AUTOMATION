import time
import requests
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

# Set the stock code here
stock_code = "TCS"  # Replace this with any stock symbol you want to search

def parse_stock_text(text):
    return {"raw_text": text}

def initialize_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    # options.add_argument("--headless=new")

    driver = uc.Chrome(options=options, use_subprocess=False)
    return driver

def upload_to_fileio(filepath):
    with open(filepath, "rb") as f:
        response = requests.post("https://file.io", files={"file": f})
        if response.ok:
            data = response.json()
            if data.get("success"):
                return data["link"]
            else:
                print("Upload failed:", data)
        else:
            print("Failed to connect to file.io")
    return None

# Initialize Chrome driver
driver = initialize_driver()

try:
    query = f"{stock_code} stock price"
    url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
    driver.get(url)
    time.sleep(5)

    screenshot_path = "screenshot.png"
    driver.save_screenshot(screenshot_path)
    print("Page loaded successfully, screenshot taken.")

    # Upload to file.io
    link = upload_to_fileio(screenshot_path)
    if link:
        print("🔗 Screenshot URL:", link)
    else:
        print("❌ Failed to upload screenshot.")

    soup = BeautifulSoup(driver.page_source, "html.parser")
    stock_data = {"SYMBOL": stock_code}
    price_div = soup.find("div", attrs={"data-attrid": "Price"})

    if price_div:
        stock_data['code'] = stock_code
        stock_data['text'] = price_div.text.strip()
        parsed = parse_stock_text(price_div.text.strip())
        print("Parsed Stock Data:", parsed)
    else:
        print("⚠️ Warning: Stock price div not found.")
except Exception as e:
    print("❌ Error:", e)
finally:
    driver.quit()
