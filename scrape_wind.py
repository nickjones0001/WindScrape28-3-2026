import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys

# Diagnostic Boot Sequence: Intercept Authentication Drops
try:
    DEFAULT_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    raw_creds = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not raw_creds:
        print("FATAL ERROR: The GOOGLE_CREDENTIALS secret is completely missing from your GitHub Actions settings.")
        sys.exit(1)
        
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)

    # Diagnostic Database Linkage
    sheet_name = "Wind+WaveScrapeLLM 28-3-2026"
    try:
        sh = gc.open(sheet_name)
        worksheet = sh.worksheet("Wind")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"FATAL ERROR: The server is blocked. You must open your Google Sheet and click 'Share' to give Editor access to this exact email: {creds_json.get('client_email')}")
        sys.exit(1)
    except gspread.exceptions.WorksheetNotFound:
        print("FATAL ERROR: The database was found, but the tab at the bottom is not named exactly 'Wind'.")
        sys.exit(1)

except Exception as boot_error:
    print(f"FATAL ERROR during server boot sequence: {boot_error}")
    sys.exit(1)

# Verified Localized Geographic Node Endpoints (BoM JSON) - Internal IDs Applied
STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94853.json",
    "Frankston Beach": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94871.json"
}

headers = {'User-Agent': 'Mozilla/5.0'}

def kmh_to_knots(kmh):
    return round(kmh * 0.539957, 2) if kmh is not None else None

# Melbourne Time Offset for Server Extraction Timestamp
melb_tz = timezone(timedelta(hours=11))

# Execution Loop: Extract temporal data and standardized wind metrics
rows_added = 0
for name, url in STATIONS.items():
    # Capture exact localized server extraction time (Columns 10 & 11)
    ext_dt = datetime.now(melb_tz)
    ext_date = ext_dt.strftime('%d/%m/%Y')
    ext_time = ext_dt.strftime('%H:%M')
    
    try:
        response = requests.get(url, headers=headers)
        
        # CRITICAL FIX: Extract the most recent temporal observation
        data = response.json()['observations']['data'][ 0 ]
        
        # Isolate native BoM observation string (Columns 1 & 2)
        raw_time = data['local_date_time_full']
        obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
        obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"

        # Standardize metrics to knots
        wind_spd_kts = kmh_to_knots(data['wind_spd_kmh'])
        wind_gust_kts = kmh_to_knots(data['gust_kmh'])
        wind_dir = data['wind_dir']
        
        # Unconditional Database Append: 11-column array perfectly aligned to your headers
        worksheet.append_row([obs_date, obs_time, name, "N/A", "N/A", "N/A", wind_spd_kts, wind_gust_kts, wind_dir, ext_date, ext_time])
        rows_added += 1
    except Exception as e:
        # Diagnostic logger will record the drop without crashing the pipeline
        print(f"Extraction failure for {name}: {e}")

print(f"Success: {rows_added} rows added at {datetime.utcnow()}")
