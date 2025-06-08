# etl_scripts/pull_exchange_rates.py
import os
import requests
import psycopg2
from datetime import datetime
from config.settings import DB_CONFIG
from dotenv import load_dotenv


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# ─── 1) Configure your API Key & endpoint ────────────────────────────────────
# 1.1) Replace "YOUR_API_KEY_HERE" with the key you obtain from exchangerate-api.com
API_KEY = os.getenv("API_KEY")
BASE_CURRENCY = "USD"  # or any ISO code you want as your “base”

# 1.2) This URL will return JSON like:
#      {
#        "result": "success",
#        "base_code": "USD",
#        "conversion_rates": {
#           "AED": 3.6725,
#           "AFN": 83.5000,
#           …,
#           "EUR": 0.8457,
#         }
#      }
API_URL = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{BASE_CURRENCY}"

def pull_and_stage_rates():
    """Fetch exchange‐rate JSON once, then insert all base→target rates into staging.exchange_rates_raw."""
    print("→ Hitting ExchangeRate-API.com…")
    resp = requests.get(API_URL)
    data = resp.json()

    # 1) If the API did not return success, print the error and exit
    if data.get("result") != "success":
        print("ERROR: ExchangeRate-API did not return success. Full JSON:")
        print(data)
        return

    fetched_at = datetime.utcnow()
    base       = data["base_code"]  # should match BASE_CURRENCY
    rates      = data["conversion_rates"]  # dict: { "EUR":0.8457, "GBP":0.7201, … }

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 2) For each (target_currency, rate_value) pair, insert one row
        for tcur, rvalue in rates.items():
            cur.execute(
                """
                INSERT INTO staging.exchange_rates_raw
                  (fetched_at, base_currency, target_currency, rate)
                VALUES (%s, %s, %s, %s)
                """,
                (fetched_at, base, tcur, rvalue)
            )

        conn.commit()
        print(f"[{fetched_at}] Inserted {len(rates)} rows into staging.exchange_rates_raw.")
        cur.close()
    except Exception as e:
        print("ERROR while inserting into staging.exchange_rates_raw:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    pull_and_stage_rates()
