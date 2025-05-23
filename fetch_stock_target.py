import requests
import json
import os
import time
from bs4 import BeautifulSoup
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pytz, re

# MONGO_URL = os.getenv("MONGO_URL")
MONGO_URL = 'mongodb+srv://afg154005:gnLhPlgHpuQaFjvh@cluster0.0yvn2uk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'

client = MongoClient(MONGO_URL)
    
db = client['OT_TRADING']
coll = db['stock_news']

def extract_prices(message):
    
    # Use regex to find prices in the message

    message = str(message)
    open_price = re.search(r"opened at ([\d,]+\.\d+)", message)
    previous_close = re.search(r"previous close was at ([\d,]+\.\d+)", message)
    high_price = re.search(r"reached a high of ([\d,]+\.\d+)", message)
    low_price = re.search(r"low of ([\d,]+\.\d+)", message)

    return {
        "open_price": float(open_price.group(1).replace(',', '')) if open_price else None,
        "previous_close": float(previous_close.group(1).replace(',', '')) if previous_close else None,
        "high_price": float(high_price.group(1).replace(',', '')) if high_price else None,
        "low_price": float(low_price.group(1).replace(',', '')) if low_price else None,
    }


def get_timestamp():
    # Timestamp
    india_time = datetime.now(pytz.timezone("Asia/Kolkata"))
    timestamp = india_time.strftime("%Y-%m-%d %H:%M:%S")
    return timestamp 

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
            text = p.text
            if text and "Today" in text:
                obj['stock_news'] = str(text)
                obj['fetched_data'] = extract_prices(text)
                obj['time'] = get_timestamp()
                return obj
    except Exception as e:
        print(f"[Error] {stock_part} → {e}")
    return None


# Save the data to MongoDB
def save_data_to_mongodb(data):
    global client, db, coll  # let Python use the global variables
    if client is None:
        client = MongoClient(MONGO_URL)
        db = client['OT_TRADING']
        coll = db['stock_news']
    try:
        coll.insert_one(data)
        print("✅ Data saved to MongoDB")
    except Exception as e:
        print("❌ Error saving to MongoDB:", e)

        
def get_time(date_only=False):
    india = pytz.timezone("Asia/Kolkata")
    now = datetime.now(india)
    if date_only:
        return now.date()  # returns a date object: YYYY-MM-DD
    return now

# Wait until a specific IST time (e.g., 9:18 or 9:35)
def wait_until_ist(hour, minute):
    now = get_time()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if now >= target:
        print(f"⏱️ Skipping wait — already past {hour}:{minute:02d} IST")
        return

    wait_seconds = (target - now).total_seconds()
    print(f"⏳ Waiting {int(wait_seconds)} seconds until {hour}:{minute:02d} AM IST...")
    time.sleep(wait_seconds)
    
def fetch_document_with_today_date(coll):
    today = get_time(date_only=True)
    date_str = today.strftime("%Y-%m-%d")  # e.g., '2025-05-23'

    query = {
        "time-stamp": {
            "$regex": f"^{date_str}"
        }
    }
    document = coll.find_one(query)
    return document


def start_main():
    
    doc = fetch_document_with_today_date(coll)
    if doc:
        # Remove _id field if present
        doc.pop('_id', None)
        
        return doc['data']
    else:
        print("No document found for today.")
        doc = None

    exit()


    if doc == None:
        
        wait_until_ist(9, 32)        
        start_time = time.time()  # Start time
        
        with open("values.json") as f:
            values = json.load(f)

        results = []

        # Concurrent requests
        with ThreadPoolExecutor(max_workers=40) as executor:
            future_to_stock = {executor.submit(get_news, obj): obj for obj in values}
            for future in as_completed(future_to_stock):
                result = future.result()
                if result and 'stock_news' in result:
                    results.append(result)

            
        end_time = time.time()  # End time
        total_time = round(end_time - start_time, 2)

        output = {
            "time-stamp": get_timestamp(),
            "total_time": f"{total_time//60} ",
            "data": results
        }

        save_data_to_mongodb(output)
        return output['data']

