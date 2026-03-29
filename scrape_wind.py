import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys

# --- 1. AUTHENTICATION & INITIALIZATION ---
try:
    DEFAULT_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    raw_creds = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not raw_creds:
        print("FATAL ERROR: The GOOGLE_CREDENTIALS secret is missing from GitHub settings.")
        sys.exit(1)
        
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)

    # Database Linkage
    sheet_name = "Wind+WaveScrapeLLM 28-3-2026"
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet("Wind")

except Exception as boot_error:
    print(f"FATAL ERROR during server boot sequence: {boot_error}")
    sys.exit(1)

# --- 2. CONFIGURATION & STATION ENDPOINTS ---
# Using 94857 for both South Channel and Frankston to ensure Marine/Beach wind speeds
STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94857.json",
    "Frankston Beach": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94857.json"
}

headers = {'User-Agent': 'Mozilla/5.0'}

def kmh_to_knots(kmh):
    try:
        return round(float(kmh) * 0.539957, 1) if kmh is not None else None
    except (ValueError, TypeError):
        return None

# Melbourne Time Offset (AEDT is UTC+11)
melb_tz = timezone(timedelta(hours=11))

# --- 3. EXECUTION LOOP ---
rows_added = 0

for name, url in STATIONS.items():
    ext_dt = datetime.now(melb_tz)
    ext_date = ext_dt.strftime('%d/%m/%Y')
    ext_time = ext_dt.strftime('%H:%M')
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        
        # Accessing the most recent observation in the data list
        observations = json_data.get('observations', {}).get('data', [])
        
        if not observations:
            print(f"No data found in JSON for {name}")
            continue
            
        data = observations[0] # The first item is always the latest
        
        # Format the BoM observation timestamp
        raw_time = str(data.get('local_date_time_full', ''))
        if len(raw_time) >= 12:
            obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
            obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"
        else:
            obs_date, obs_time = "N/A", "N/A"

        # Extract and convert metrics
        wind_spd_kts = kmh_to_knots(data.get('wind_spd_kmh'))
        wind_gust_kts = kmh_to_knots(data.get('gust_kmh'))
        wind_dir = data.get('wind_dir', 'N/A')
        
        # Append 11-column row to Google Sheet
        row = [
            obs_date, 
            obs_time, 
            name, 
            "N/A", "N/A", "N/A", # Placeholders for Wave data if needed
            wind_spd_kts, 
            wind_gust_kts, 
            wind_dir, 
            ext_date, 
            ext_time
        ]
        
        worksheet.append_row(row)
        rows_added += 1
        print(f"Successfully logged {name}: {wind_spd_kts} kts {wind_dir}")

    except Exception as e:
        print(f"Extraction failure for {name}: {e}")

print(f"Process Complete: {rows_added} rows added to '{sheet_name}'.")
