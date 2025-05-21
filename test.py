import requests
import json
import os
import time
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pytz

# Get stock news from dhan.co
def get_news(obj):
    stock_part = obj.get('Seosym')
    if not stock_part:
        return None

    url = f'https://dhan.co/stocks/{stock_part}-share-price/'
    try:
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)
            if "Today" in text:
                obj['stock_news'] = text
                return obj
    except Exception as e:
        print(f"[Error] {stock_part} → {e}")
    return None

# Save the data to MongoDB
def save_data_to_mongodb(data):
    MONGO_URL = os.getenv("MONGO_URL")
    client = MongoClient(MONGO_URL)
    db = client['OT_TRADING']
    coll = db['stock_news']
    try:
        coll.insert_one(data)
        print("✅ Data saved to MongoDB")
    except Exception as e:
        print("❌ Error saving to MongoDB:", e)

# Main task: scrape news and save
def main():
    with open("values.json") as f:
        values = json.load(f)

    results = []

    # Concurrent requests
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_stock = {executor.submit(get_news, obj): obj for obj in values}
        for future in as_completed(future_to_stock):
            result = future.result()
            if result and 'stock_news' in result:
                results.append(result)

    # Timestamp
    india_time = datetime.now(pytz.timezone("Asia/Kolkata"))
    timestamp = india_time.strftime("%Y-%m-%d %H:%M:%S")

    output = {
        "time-stamp": timestamp,
        "data": results
    }

    save_data_to_mongodb(output)

# Wait until a specific IST time (e.g., 9:18 or 9:35)
def wait_until_ist(hour, minute):
    india = pytz.timezone("Asia/Kolkata")
    now = datetime.now(india)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if now >= target:
        print(f"⏱️ Skipping wait — already past {hour}:{minute:02d} IST")
        return

    wait_seconds = (target - now).total_seconds()
    print(f"⏳ Waiting {int(wait_seconds)} seconds until {hour}:{minute:02d} AM IST...")
    time.sleep(wait_seconds)

# Scheduler entry
if __name__ == "__main__":
    # First run at 9:18 AM IST
    wait_until_ist(9, 18)
    main()

    # Second run at 9:35 AM IST
    wait_until_ist(9, 35)
    main()
