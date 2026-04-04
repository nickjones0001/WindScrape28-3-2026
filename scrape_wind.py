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
    
    # Opening the specific spreadsheet and tab
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
        if not kmh or str(kmh).strip().upper() in ["CALM", "-", ""]: return 0.0
        return round(float(kmh) * 0.539957, 1)
    except: return "N/A"

def get_wind_arrow(text_dir):
    """Maps BOM text direction to visual arrows (Wind blowing TOWARD)."""
    # Standardizing the input to uppercase and removing spaces
    clean_dir = str(text_dir).strip().upper()
    
    mapping = {
        "N": "↓ N", "NNE": "↙ NNE", "NE": "↙ NE", "ENE": "← ENE",
        "E": "← E", "ESE": "↖ ESE", "SE": "↖ SE", "SSE": "↑ SSE",
        "S": "↑ S", "SSW": "↗ SSW", "SW": "↗ SW", "WSW": "→ WSW",
        "W": "→ W", "WNW": "↘ WNW", "NW": "↘ NW", "NNW": "↓ NNW",
        "CALM": "○ Calm"
    }
    # If the direction isn't in the map, return the raw text or "N/A"
    return mapping.get(clean_dir, f"? {clean_dir}")

# --- 3. EXECUTION ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

for name, url in STATIONS.items():
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data_packet = resp.json()['observations']['data'][0]
        
        # 1. Parse Observation Time
        raw_t = str(data_packet['local_date_time_full'])
        obs_date = f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}"
        obs_time = f"{raw_t[8:10]}:{raw_t[10:12]}"

        # 2. Extract direction and generate arrow
        bom_direction = data_packet.get('wind_dir', 'N/A')
        visual_arrow = get_wind_arrow(bom_direction)

        # 3. Build the row (12 columns total)
        row = [
            obs_date,             # A: Observation_Date
            obs_time,             # B: Observation_Time
            name,                 # C: Geographic_Node
            "N/A",                # D: Significant_Wave_Height_m
            "N/A",                # E: Peak_Wave_Period_s
            "N/A",                # F: Peak_Wave_Direction_degrees
            kmh_to_knots(data_packet.get('wind_spd_kmh')), # G: Wind_Speed_knots
            kmh_to_knots(data_packet.get('gust_kmh')),     # H: Wind_Gust_knots
            bom_direction,        # I: Wind_Direction_BOM
            ext_date,             # J: Extracted_Date
            ext_time,             # K: Extracted_Time
            visual_arrow          # L: Wind_Visual (THE ARROW)
        ]
        
        # 4. Append to Google Sheets
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged {name}: Direction {bom_direction} mapped to {visual_arrow}")

    except Exception as e:
        print(f"Error for {name}: {e}")

print(f"Process Complete: {rows_added}/3 rows added.")
