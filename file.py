import time,json,pytz,os,requests
import zstandard as zstd
from bson.binary import Binary
from datetime import datetime, time as dtime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from mega import Mega




# ENV CONFIG
MONGO_URL = os.getenv("MONGO_URL")
M_TOKEN = os.getenv("M_TOKEN")

# Validation
if not MONGO_URL or not M_TOKEN:
    print("❌ [INIT] MONGO_URL or M_TOKEN not set. Exiting.")
    exit(1)

# GLOBALS
client = None
os.makedirs("json", exist_ok=True)
error_occured_count = 0


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(30*"-")
    print(f"[{ts}] [{level}] {msg}")
    print(30*"-")
    
def compress_json_to_binary(data: dict, level: int = 10) -> Binary:
    data = sort_data(data['data'])
    json_bytes = json.dumps(data).encode('utf-8')
    compressor = zstd.ZstdCompressor(level=level)
    compressed = compressor.compress(json_bytes)
    return Binary(compressed)

def sort_data(response_data):
    sorted_data = []
    for obj in response_data:
        item = {
            "id": obj.get("Sid"),
            "name": obj.get("DispSym"),
            "symbol": obj.get("Sym"),
            "price": obj.get("Ltp"),
            "volume": obj.get("Volume"),
            "isin": obj.get("Isin"),
            "exchange": obj.get("Exch"),
            "seo_symbol": obj.get("Seosym")
        }
        sorted_data.append(item)
    return sorted_data


def report_error_to_server(error_message):
    global error_occured_count
    error_occured_count += 1
    formatted_error = f"FROM REPO - 1\n{'*' * 30}\n{str(error_message)}"

    try:
        response = requests.post(
            'https://pass-actions-status.vercel.app/report-error',
            headers={'Content-Type': 'application/json'},
            json={'error': formatted_error, 'count': error_occured_count}
        )
        log(f"📡 Reported error #{error_occured_count}", "ERROR")
    except Exception as report_ex:
        log(f"⚠️ Failed to report error: {report_ex}", "ERROR")


def get_current_time(default_value=0):
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    return now if default_value == 1 else now.strftime('%Y-%m-%d %H:%M:%S')


def is_after_3_35_pm():
    now = get_current_time(1)
    return now.time() > dtime(15, 35)


def is_market_hours():
    now = get_current_time(1)
    open_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, microsecond=0)
    return now.weekday() < 5 and open_time <= now <= close_time


def save_file_to_mega(m, file_name):
    try:
        m.upload(file_name)
        log(f"✅ Uploaded {file_name} to MEGA.")
    except Exception as e:
        report_error_to_server(e)
        log(f"❌ Failed to upload {file_name} to MEGA: {e}", "ERROR")


def save_collection_as_json():
    global client
    try:
        if client is None:
            client = MongoClient(MONGO_URL)

        db = client["OT_TRADING"]
        collections = db.list_collection_names()

        mega = Mega()
        username, password = M_TOKEN.split("_")
        m = mega.login(username, password)

        collection_files = []
        timestamp = get_current_time()

        for name in collections:
            collection = db[name]
            data = list(collection.find({}, {'_id': False}))
            file_name = f"{name}_{timestamp}.json"

            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            collection_files.append((file_name, collection))

        for file_name, collection in collection_files:
            save_file_to_mega(m, file_name)
            os.remove(file_name)
            collection.delete_many({})
            log(f"🧹 Cleared collection '{collection.name}' after upload.")

    except Exception as e:
        report_error_to_server(e)
        log(f"❌ Error in saving collection as JSON: {e}", "ERROR")


def save_to_mongodb(index_name, index_json_data):
    global client
    try:
        if client is None:
            client = MongoClient(MONGO_URL)
        db = client["OT_TRADING"]
        collection = db[index_name]
        if index_json_data:
            collection.insert_one(index_json_data)
            log(f"✅ Inserted data into MongoDB '{index_name}'")
        else:
            log("⚠️ No data to insert.")
    except Exception as e:
        report_error_to_server(e)
        log(f"❌ MongoDB insertion failed for '{index_name}': {e}", "ERROR")


def fetch_page(page, url, headers):
    payload = {
        "data": {
            "sort": "Mcap", "sorder": "desc", "count": 50,
            "params": [{"field": "OgInst", "op": "", "val": "ES"},
                       {"field": "Exch", "op": "", "val": "BSE"}],
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue",
                "Year1RevenueGrowth", "NetProfitMargin", "YoYLastQtrlyProfitGrowth",
                "EBIDTAMargin", "volume", "PricePerchng1year", "PricePerchng3year",
                "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild", "Eps",
                "DaySMA50CurrentCandle", "DaySMA200CurrentCandle", "DayRSI14CurrentCandle",
                "ROCE", "Roe", "Sym", "PricePerchng1mon", "PricePerchng3mon"
            ],
            "pgno": page
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get('data')
    except requests.RequestException as e:
        log(f"❌ Error fetching page {page}: {e}", "ERROR")
    except json.JSONDecodeError as e:
        log(f"❌ JSON decode error on page {page}: {e}", "ERROR")
    return None

def get_bse_stocks():
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    headers = {
        "accept": "*/*",
        "content-type": "application/json; charset=UTF-8",
        "Referer": "https://dhan.co/"
    }

    total_pages = 87
    final_data = []

    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {
            executor.submit(fetch_page, page, url, headers): page
            for page in range(1, total_pages + 1)
        }

        for future in as_completed(futures):
            page = futures[future]
            result = future.result()
            if result:
                final_data.extend(result)
            if (page + 1) % 100 == 0:
                time.sleep(0.1)

    try:
        compressed_binary = compress_json_to_binary({"data": final_data})
        mongo_data = {
            "timestamp": get_current_time(),
            "binary_data": compressed_binary,
            "compression": "zstd",
            "compression_level": 15
        }
        save_to_mongodb('bse-stocks-data', mongo_data)
    except Exception as e:
        log("❌ Failed to save to MongoDB", "ERROR")

    if is_after_3_35_pm():
        save_collection_as_json()

def runner(max_attempts=3):
    attempt = 0
    while attempt < max_attempts:
        if not is_market_hours():
            log("📴 Market is closed. Exiting runner.")
            break
        try:
            log(f"🔁 Attempt {attempt + 1}/{max_attempts} started.")
            get_bse_stocks()
            attempt = 0
            time.sleep(15)
        except Exception as e:
            report_error_to_server(e)
            attempt += 1
            if attempt < max_attempts:
                log(f"⏳ Retrying in 5s after error: {e}")
                time.sleep(5)
            else:
                log("❌ All retry attempts failed. Exiting.", "ERROR")


if __name__ == "__main__":
    try:
        runner()
    except Exception as e:
        report_error_to_server(e)
        log(f"❌ Fatal error in main block: {e}", "ERROR")
