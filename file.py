import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os

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
    options.add_argument("--headless=new")

    driver = uc.Chrome(options=options, use_subprocess=False)
    return driver

def upload_screenshot(driver, screenshot_path):
    try:
        # Open file.io upload page or any other website with file upload
        driver.get("https://file.io/")
        
        # Wait for the file input element to be clickable and then send the file path
        file_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.NAME, "file"))
        )
        
        # Upload the file (path of screenshot)
        file_input.send_keys(screenshot_path)
        
        # Wait for the response link to appear after upload
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "file-link"))
        )
        
        # Get the URL from the response
        link = driver.find_element(By.CLASS_NAME, "file-link").text
        return link
    
    except Exception as e:
        print("Error during upload:", e)
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

    # Upload the screenshot using the WebDriver
    link = upload_screenshot(driver, screenshot_path)
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
