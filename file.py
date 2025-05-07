import time
import undetected_chromedriver as uc

def initialize_driver():
    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    options.add_argument(f"user-agent={user_agent}")

    driver = uc.Chrome(options=options, use_subprocess=False)
    return driver

driver = initialize_driver()

try:
    url = "https://www.nseindia.com/get-quotes/equity?symbol=TCS"
    driver.get(url)
    time.sleep(5)  # Allow time for dynamic content to load
    driver.save_screenshot("screenshot.png")
finally:
    driver.quit()
