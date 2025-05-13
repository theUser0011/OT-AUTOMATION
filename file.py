import time, json,pytz,os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient


MONGO_URL = os.getenv("MONGO_URL")
client = None


os.makedirs("json",exist_ok=True)

# Path to ChromeDriver
chromedriver_path = r"chromedriver"

# Configure Chrome
options = Options()

options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920x1080")
options.add_argument("--disable-notifications")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)



def driver_initialize():
    try:
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        # Bypass navigator.webdriver detection
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        print("✅ WebDriver initialized")
        return driver
    except Exception as e:
        print("❌ Failed to initialize driver:", e)
        return None

def get_current_time(default_value=0):
   # Define IST timezone
    ist = pytz.timezone('Asia/Kolkata')
    if default_value==1:
        return datetime.now(ist)
    
    current_time = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
    return current_time    

def is_market_hours():
    now = get_current_time(1)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=40, second=0, microsecond=0)
    is_weekday = now.weekday() < 5  # 0-4 = Monday to Friday
    return is_weekday and market_open <= now <= market_close

# MongoDB Save Function
def save_to_mongodb(index_name, index_json_data):
    global client
    try:
        
        if client is None:
            client = MongoClient(MONGO_URL)
            
            
        db = client["OT_TRADING"]
        collection = db[index_name]

        # Optional: clear old data if you want fresh data every time
        collection.delete_many({})

        # Insert new data
        if index_json_data:
            collection.insert_many(index_json_data)
            print(f"✅ Data inserted into MongoDB collection '{index_name}'")
        else:
            print("⚠️ No data to insert.")
    except Exception as e:
        print(f"❌ MongoDB insertion failed for '{index_name}': {e}")
    finally:
        client.close()

def extract_and_save_data(driver, tab_index):
    driver.switch_to.window(driver.window_handles[tab_index])
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    tables = soup.find_all("table")
    index_name = soup.find(class_='listWeb_active__It_ic').text
    if len(tables) < 2:
        print(f"❌ Table not found in tab {tab_index+1} ({index_name})")
        return

    table = tables[1]
    rows = table.find_all("tr")[1:]  # Skip header
    
    current_time = get_current_time()
    
    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 6:
            item = {
                "Company Name": cols[0].get_text(strip=True),
                "Industry": cols[1].get_text(strip=True),
                "Last Price": cols[2].get_text(strip=True),
                "Chg": cols[3].get_text(strip=True),
                "%Chg": cols[4].get_text(strip=True),
                "Mkt Cap(Rs cr)": cols[5].get_text(strip=True)
            }
            stock_data = {
                "index_name":index_name,
                "live_data":item,
                "time_stamp":current_time
            }
            data.append(stock_data)

    # filename = f"./json/{index_name}.json"
    # with open(filename, "w", encoding="utf-8") as f:
    #     json.dump(data, f, indent=2)
    # print(f"✅ Data saved to {filename}")
        # Instead of saving to JSON file
        
    save_to_mongodb(index_name, data)


def open_tabs_and_extract_loop(url_lst, num_of_tab):
    
    
    # Check if current time is within market hours
    if not is_market_hours():
        print("⏳ Market is closed. Script will not run outside 9:15 AM to 3:40 PM IST (Mon–Fri).")
        return    
    
    driver = driver_initialize()
    if not driver:
        return

    try:
        print("🚀 Opening tabs...")
        # Open all URLs in separate tabs
        for i in range(num_of_tab):
            if i == 0:
                driver.get(url_lst[0])
            else:
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[i])
                driver.get(url_lst[i])
            time.sleep(2)

        print("🌀 Starting data extraction loop (Press Ctrl+C to stop)...")
        
        while True:
            for i in range(num_of_tab):
                extract_and_save_data(driver,  i)
            print("⏳ Waiting before next round...\n")
            time.sleep(3)  # Adjust frequency here

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    except Exception as e:
        print("❌ Error during processing:", e)
    finally:
        driver.quit()
        print("🚪 Browser closed.")

if __name__ == "__main__":
    with open("values.json", encoding='utf-8') as f:
        url_data = json.load(f)
    url_data = [obj['href'] for obj in url_data]
    num_of_tab = 2  # ✅ Change this number to open more or fewer tabs
    url_data = url_data[:num_of_tab]
    
    open_tabs_and_extract_loop(url_lst=url_data, num_of_tab=num_of_tab)

