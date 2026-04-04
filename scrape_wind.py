import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys

# --- 1. AUTHENTICATION ---
try:
    DEFAULT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_env = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_env:
        print("Error: GOOGLE_CREDENTIALS secret not found.")
        sys.exit(1)
        
    creds_json = json.loads(creds_env)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)
    
    sh = gc.open("Wind+WaveScrapeLLM 28-3-2026")
    worksheet = sh.worksheet("Wind")
except Exception as e:
    print(f"Auth/Init Error: {e}")
    sys.exit(1)

# --- 2. CONFIGURATION & HELPERS ---
STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94853.json",
    "Frankston Beach": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94871.json"
}

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        if kmh is None or str(kmh).strip().upper() in ["CALM", "-", ""]:
            return 0.0
        val = float(kmh) * 0.539957
        return round(val, 1)
    except:
        return 0.0

def get_wind_arrow(text_dir):
    """Maps BOM text direction to visual arrows."""
    clean_dir = str(text_dir).strip().upper()
    mapping = {
        "N": "↓ N", "NNE": "↙ NNE", "NE": "↙ NE", "ENE": "← ENE",
        "E": "← E", "ESE": "↖ ESE", "SE": "↖ SE", "SSE": "↑ SSE",
        "S": "↑ S", "SSW": "↗ SSW", "SW": "↗ SW", "WSW": "→ WSW",
        "W": "→ W", "WNW": "↘ WNW", "NW": "↘ NW", "NNW": "↓ NNW",
        "CALM": "○ Calm"
    }
    # Adding a single quote prefix forces Google Sheets to treat this as text
    arrow = mapping.get(clean_dir, f"{clean_dir}")
    return f"'{arrow}"

# --- 3. EXECUTION ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

for name, url in STATIONS.items():
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data_packet = resp.json()['observations']['data'][0]
        
        raw_t = str(data_packet['local_date_time_full'])
        obs_date = f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}"
        obs_time = f"{raw_t[8:10]}:{raw_t[10:12]}"

        bom_direction = data_packet.get('wind_dir', 'N/A')
        visual_arrow = get_wind_arrow(bom_direction)

        row = [
            obs_date,
            obs_time,
            name,
            "N/A",
            "N/A",
            "N/A",
            kmh_to_knots(data_packet.get('wind_spd_kmh')),
            kmh_to_knots(data_packet.get('gust_kmh')),
            bom_direction,
            ext_date,
            ext_time,
            visual_arrow  # Now prefixed with ' to stop date conversion
        ]
        
        # Using RAW to ensure our ' prefix is respected
        worksheet.append_row(row, value_input_option='RAW')
        rows_added += 1
        print(f"Logged {name} successfully.")

    except Exception as e:
        print(f"Error for {name}: {e}")

print(f"Process Complete: {rows_added}/3 rows added.")
