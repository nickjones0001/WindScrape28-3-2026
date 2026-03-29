import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys
from bs4 import BeautifulSoup

# --- 1. AUTHENTICATION & INITIALIZATION ---
try:
    DEFAULT_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    raw_creds = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not raw_creds:
        print("FATAL ERROR: GOOGLE_CREDENTIALS secret is missing.")
        sys.exit(1)
        
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)

    sheet_name = "Wind+WaveScrapeLLM 28-3-2026"
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet("Wind")

except Exception as boot_error:
    print(f"FATAL ERROR during initialization: {boot_error}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
# JSON stations as per your registry 
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.086376.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.086344.json"
}

# HTML station for direct scraping 
HTML_STATIONS = {
    "Frankston Beach": "http://www.bom.gov.au/products/IDV60801/IDV60801.95872.shtml"
}

headers = {'User-Agent': 'Mozilla/5.0'}
melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        return round(float(kmh) * 0.539957, 1) if kmh is not None else "N/A"
    except (ValueError, TypeError):
        return "N/A"

# --- 3. EXECUTION LOOP ---
rows_added = 0
ext_dt = datetime.now(melb_tz)
ext_date = ext_dt.strftime('%d/%m/%Y')
ext_time = ext_dt.strftime('%H:%M')

# PART A: Process JSON Stations (Fawkner & South Channel)
for name, url in JSON_STATIONS.items():
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()['observations']['data'][0]
        
        raw_time = str(data['local_date_time_full'])
        obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
        obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"

        row = [
            obs_date, obs_time, name, "N/A", "N/A", "N/A", 
            kmh_to_knots(data.get('wind_spd_kmh')), 
            kmh_to_knots(data.get('gust_kmh')), 
            data.get('wind_dir'), ext_date, ext_time
        ]
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged JSON station: {name}")
    except Exception as e:
        print(f"Failure for JSON station {name}: {e}")

# PART B: Process HTML Station (Frankston Beach Scrape)
for name, url in HTML_STATIONS.items():
    try:
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # BOM observation tables usually have the latest data in the first body row
        table = soup.find('table')
        rows = table.find_all('tr')
        # Row 0 is header, Row 1 is sub-header, Row 2 is latest data
        latest_row = rows[2]
        cols = latest_row.find_all('td')

        # Mapping for IDV60801 table structure:
        # Col 1: Day/Time, Col 3: Wind Dir, Col 4: Wind Speed, Col 5: Wind Gust
        obs_time_raw = cols[1].text.strip() # e.g., "29/10:30am"
        wind_dir = cols[3].text.strip()
        wind_spd_kmh = cols[4].text.strip()
        wind_gust_kmh = cols[5].text.strip()

        # Clean 'obs_time' to match your format
        obs_time = obs_time_raw.split('/')[1] if '/' in obs_time_raw else obs_time_raw

        row = [
            ext_date, obs_time, name, "N/A", "N/A", "N/A", 
            kmh_to_knots(wind_spd_kmh), 
            kmh_to_knots(wind_gust_kmh), 
            wind_dir, ext_date, ext_time
        ]
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged Scraped station: {name}")
    except Exception as e:
        print(f"Failure for Scraped station {name}: {e}")

print(f"Process Complete: {rows_added} rows added.")
