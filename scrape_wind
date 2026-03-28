import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime

# Setup Google Sheets Auth
DEFAULT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
gc = gspread.authorize(creds)

# Open Sheet and target specific tab
sh = gc.open("Wind+WaveScrapeLLM 28-3-2026")
worksheet = sh.worksheet("Wind")

# Localized Geographic Node Endpoints (BoM JSON) - South Channel Removed
STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.086376.json",
    "Frankston Beach": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.95872.json"
}

headers = {'User-Agent': 'Mozilla/5.0'}

def kmh_to_knots(kmh):
    return round(kmh * 0.539957, 2) if kmh is not None else None

# Execution Loop: Extract temporal data and standardized wind metrics
rows_added = 0
for name, url in STATIONS.items():
    try:
        response = requests.get(url, headers=headers)
        
        # CRITICAL FIX: Spaces added inside [ 0 ] to extract the most recent temporal observation
        data = response.json()['observations']['data'][ 0 ]
        
        # Isolate temporal observation string
        raw_time = data['local_date_time_full']
        obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
        obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"

        # Standardize metrics to knots
        wind_spd_kts = kmh_to_knots(data['wind_spd_kmh'])
        wind_gust_kts = kmh_to_knots(data['gust_kmh'])
        wind_dir = data['wind_dir']
        
        # Append structured metrics to longitudinal tracking spreadsheet
        worksheet.append_row([obs_date, obs_time, name, "N/A", "N/A", "N/A", wind_spd_kts, wind_gust_kts, wind_dir])
        rows_added += 1
    except Exception as e:
        # Diagnostic logger will record the drop without crashing the pipeline
        print(f"Extraction failure for {name}: {e}")

print(f"Success: {rows_added} rows added at {datetime.utcnow()}")
