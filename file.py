import time
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

# Define your stock code here
stock_code = "TCS"  # Replace with the desired stock symbol

def parse_stock_text(text):
    # Basic example to parse text like '₹3,600.25 +30.50 (0.85%) today'
    # You can expand this based on real format
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

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    driver = uc.Chrome(options=options, use_subprocess=False)
    return driver

driver = initialize_driver()

try:
    query = f"{stock_code} stock price"
    url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
    driver.get(url)
    time.sleep(5)  # Allow time for dynamic content to load
    driver.save_screenshot("screenshot.png")
    print("Page loaded successfully.")

    soup = BeautifulSoup(driver.page_source, "html.parser")
    stock_data = {"SYMBOL": stock_code}

    # Try to find the main price block from Google's result
    price_div = soup.find("div", attrs={"data-attrid": "Price"})
    if price_div:
        stock_data['code'] = stock_code
        stock_data['text'] = price_div.text.strip()
        parsed = parse_stock_text(price_div.text.strip())
        print("Parsed Stock Data:", parsed)
    else:
        print("Warning: Stock price div not found.")

finally:
    driver.quit()
