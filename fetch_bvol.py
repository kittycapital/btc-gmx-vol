"""
Bitcoin Historical Volatility (BVOL) + BTC Price Data Fetcher
- Reads historical BTC price from BTC_USD.csv
- Fetches .BVOL data from BitMEX public API
- Fetches recent BTC price from BitMEX to fill gaps after CSV end date
- Outputs data/bvol_data.json
"""

import json
import csv
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

BASE_URL = "https://www.bitmex.com/api/v1"
DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = Path(__file__).parent / "BTC_USD.csv"


def fetch_bitmex(endpoint, params, max_retries=3):
    """Fetch data from BitMEX public API with retry logic."""
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/{endpoint}?{query}"

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", "HerdVibe-BVOL-Dashboard/1.0")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                remaining = resp.headers.get("x-ratelimit-remaining", "?")
                print(f"  Fetched {len(data)} rows (rate limit remaining: {remaining})")
                return data
        except Exception as e:
            print(f"  Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
    return []


def fetch_all_bvol():
    """Fetch all available .BVOL daily data from BitMEX (paginated)."""
    all_data = []
    start = 0
    count = 1000

    print("Fetching .BVOL data from BitMEX...")
    while True:
        params = {
            "binSize": "1d",
            "partial": "false",
            "symbol": ".BVOL",
            "count": count,
            "start": start,
            "reverse": "false",
        }
        batch = fetch_bitmex("trade/bucketed", params)
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < count:
            break
        start += count
        time.sleep(1.2)  # respect rate limits

    print(f"Total .BVOL rows: {len(all_data)}")
    return all_data


def fetch_recent_btc(start_date):
    """Fetch BTC price from BitMEX from start_date to now (paginated)."""
    all_data = []
    start = 0
    count = 1000

    print(f"Fetching BTC price from BitMEX (from {start_date})...")
    while True:
        params = {
            "binSize": "1d",
            "partial": "false",
            "symbol": "XBTUSD",
            "count": count,
            "start": start,
            "startTime": start_date,
            "reverse": "false",
        }
        batch = fetch_bitmex("trade/bucketed", params)
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < count:
            break
        start += count
        time.sleep(1.2)

    print(f"Total recent BTC rows: {len(all_data)}")
    return all_data


def load_csv():
    """Load BTC_USD.csv into a dict keyed by date string."""
    btc_data = {}
    if not CSV_PATH.exists():
        print("WARNING: BTC_USD.csv not found, will rely on BitMEX data only")
        return btc_data

    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row["Date"][:10]
            try:
                btc_data[date_str] = {
                    "close": float(row["Close"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "open": float(row["Open"]),
                }
            except (ValueError, KeyError):
                continue

    dates = sorted(btc_data.keys())
    print(f"CSV loaded: {len(btc_data)} rows ({dates[0]} ~ {dates[-1]})")
    return btc_data


def load_existing_json():
    """Load existing JSON to enable incremental updates."""
    json_path = DATA_DIR / "bvol_data.json"
    if json_path.exists():
        with open(json_path, "r") as f:
            return json.load(f)
    return None


def build_output(btc_csv, bvol_raw, btc_api_raw):
    """Merge all data sources into final JSON structure."""

    # Parse BVOL data
    bvol_by_date = {}
    for row in bvol_raw:
        date_str = row["timestamp"][:10]
        bvol_by_date[date_str] = round(row.get("close") or row.get("open") or 0, 2)

    # Parse BitMEX BTC data (to fill gaps after CSV)
    btc_api_by_date = {}
    for row in btc_api_raw:
        date_str = row["timestamp"][:10]
        btc_api_by_date[date_str] = {
            "close": row.get("close"),
            "high": row.get("high"),
            "low": row.get("low"),
            "open": row.get("open"),
        }

    # Merge BTC: CSV first, then API for missing dates
    btc_merged = {**btc_csv}
    for date_str, vals in btc_api_by_date.items():
        if date_str not in btc_merged and vals["close"] is not None:
            btc_merged[date_str] = {
                "close": vals["close"],
                "high": vals["high"],
                "low": vals["low"],
                "open": vals["open"],
            }

    # Find overlapping date range (where both BTC and BVOL exist)
    bvol_dates = set(bvol_by_date.keys())
    btc_dates = set(btc_merged.keys())
    common_dates = sorted(bvol_dates & btc_dates)

    if not common_dates:
        # Even if no overlap, output all BTC with null BVOL
        all_dates = sorted(btc_dates | bvol_dates)
    else:
        # Use BVOL date range as primary (BTC usually has more history)
        bvol_start = min(bvol_dates)
        all_dates = sorted(d for d in (btc_dates | bvol_dates) if d >= bvol_start)

    print(f"Output date range: {all_dates[0]} ~ {all_dates[-1]} ({len(all_dates)} days)")

    # Build arrays
    dates = []
    btc_prices = []
    bvol_values = []

    for d in all_dates:
        dates.append(d)
        btc_prices.append(round(btc_merged[d]["close"], 2) if d in btc_merged else None)
        bvol_values.append(bvol_by_date.get(d))

    output = {
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "meta": {
            "btc_source": "BTC_USD.csv + BitMEX XBTUSD",
            "bvol_source": "BitMEX .BVOL (30-day annualized historical volatility)",
            "bvol_formula": "Stdev(Ln(P1/P0),...,Ln(P30/P29)) × √365",
        },
        "dates": dates,
        "btc_close": btc_prices,
        "bvol": bvol_values,
    }

    return output


def main():
    DATA_DIR.mkdir(exist_ok=True)

    # 1. Load CSV
    btc_csv = load_csv()

    # 2. Determine what to fetch
    csv_dates = sorted(btc_csv.keys()) if btc_csv else []
    csv_end = csv_dates[-1] if csv_dates else "2014-01-01"

    # 3. Fetch BVOL from BitMEX
    bvol_raw = fetch_all_bvol()

    # 4. Fetch recent BTC price from BitMEX (from day after CSV ends)
    next_day = (datetime.strptime(csv_end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    btc_api_raw = fetch_recent_btc(next_day)

    # 5. Build and save output
    output = build_output(btc_csv, bvol_raw, btc_api_raw)

    output_path = DATA_DIR / "bvol_data.json"
    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    print(f"\nSaved to {output_path}")
    print(f"  Dates: {len(output['dates'])}")
    print(f"  BTC non-null: {sum(1 for v in output['btc_close'] if v is not None)}")
    print(f"  BVOL non-null: {sum(1 for v in output['bvol'] if v is not None)}")


if __name__ == "__main__":
    main()
