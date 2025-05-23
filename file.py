import time,json,pytz,os,requests,traceback
from bson.binary import Binary
from datetime import datetime, time as dtime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from mega import Mega
from fetch_stock_target import start_main


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

def send_trigger_alert(stock_obj):
    try:
        response = requests.post(
            "https://pass-actions-status.vercel.app/report-error",
            headers={'Content-Type': 'application/json'},
            json=stock_obj
        )
    except Exception as ex:
        log(f"e1 - ❌ Failed to send trigger alert: {ex}", "ERROR")


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


def report_msg_to_server(error_message):
    global error_occured_count
    error_occured_count += 1
    formatted_error = f"FROM REPO - 1\n{'*' * 30}\n{str(error_message)}"

    try:
        response = requests.post(
            'https://pass-actions-status.vercel.app/report-error',
            headers={'Content-Type': 'application/json'},
            json={'error': formatted_error, 'count': error_occured_count}
        )
        log(f"e-2 📡 Reported error #{error_occured_count}", "ERROR")
    except Exception as report_ex:
        log(f"e-3 ⚠️ Failed to report error: {report_ex}", "ERROR")


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
        log(f"e-4 ❌ Error fetching page {page}: {e}", "ERROR")
    except json.JSONDecodeError as e:
        log(f"e-5 ❌ JSON decode error on page {page}: {e}", "ERROR")
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
                
    return final_data


def set_values_to_each_stock(all_stock_values_lst, stocks_target_data):
    # Create a quick lookup dict using ISIN as key from target data
    target_data_map = {stock['Isin']: stock for stock in stocks_target_data}

    final_data = []
    for live_stock in all_stock_values_lst:
        isin = live_stock.get("isin")
        target_data = target_data_map.get(isin)

        if target_data:
            # Merge both dictionaries
            merged_stock = {**live_stock, **target_data}
            final_data.append(merged_stock)
        else:
            # Optionally log or skip unmatched
            pass

    return final_data


def runner(max_attempts=3):
    attempt = 0
    merged_data = None
    stocks_target_data = None
    target_map = {}

    while attempt < max_attempts:
        if not is_market_hours():
            log("📴 Market is closed. Exiting runner.")
            break

        try:
            log(f"🔁 Attempt {attempt + 1}/{max_attempts} started.")
            all_stocks_ltd = get_bse_stocks()
            live_data = sort_data(all_stocks_ltd)

            # Fetch target data only once
            if merged_data is None:
                stocks_target_data = start_main()
                
                msg = f"Today Stock News fetched successfully: {len(stocks_target_data)}"
                report_msg_to_server(msg)
                
                target_map = {item['Isin']: item for item in stocks_target_data}
                merged_data = set_values_to_each_stock(live_data, stocks_target_data)

            # Now do the comparison logic unconditionally after data is ready
            for live_stock in merged_data:
                isin = live_stock.get('isin')
                ltd = live_stock.get('price')

                if not ltd or not isin:
                    continue

                target = target_map.get(isin)
                if not target:
                    continue

                put_target = target.get('PutTarget')
                call_target = target.get('CallTarget')

                try:
                    put_target_val = float(put_target) if put_target is not None else None
                    call_target_val = float(call_target) if call_target is not None else None

                    if (put_target_val is not None and ltd > put_target_val) or \
                    (call_target_val is not None and ltd > call_target_val):
                        live_stock['single_target_flag'] = True
                        send_trigger_alert(live_stock)
                except Exception as cmp_err:
                    log(f"e-6 Comparison error: {cmp_err}", "WARN")

            attempt = 0

        except Exception as e:
            error_message = str(e) + "\n" + traceback.format_exc()
            report_msg_to_server(error_message)
            attempt += 1
            if attempt < max_attempts:
                log(f"⏳ Retrying in 5s after error: {e}")
                time.sleep(5)
            else:
                log("❌ All retry attempts failed. Exiting.", "ERROR")


try:
    runner()
except Exception as e:
    error_message = str(e) + "\n" + traceback.format_exc()
    report_msg_to_server(error_message)
    log(f"❌ Fatal error in main block: {e}", "ERROR")
