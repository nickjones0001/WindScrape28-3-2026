import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys
from bs4 import BeautifulSoup

# --- 1. AUTHENTICATION & BOOT SEQUENCE ---
try:
    DEFAULT_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    raw_creds = os.environ.get('GOOGLE_CREDENTIALS')
    
    if not raw_creds:
        print("FATAL ERROR: The GOOGLE_CREDENTIALS secret is missing.")
        sys.exit(1)
        
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)

    sheet_name = "Wind+WaveScrapeLLM 28-3-2026"
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet("Wind")

except Exception as boot_error:
    print(f"FATAL ERROR during server boot sequence: {boot_error}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
# Reverted Fawkner and South Channel to the JSON IDs that worked yesterday
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94853.json"
}

# Targeted HTML page for REAL Frankston Beach coastal data
FRANKSTON_HTML_URL = "http://www.bom.gov.au/products/IDV60801/IDV60801.94871.shtml"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        if kmh is None or str(kmh).strip().upper() == "CALM" or str(kmh).strip() == "-":
            return 0.0
        return round(float(kmh) * 0.539957, 1)
    except:
        return None

# --- 3. EXECUTION LOOP ---
rows_added = 0
ext_dt = datetime.now(melb_tz)
ext_date = ext_dt.strftime('%d/%m/%Y')
ext_time = ext_dt.strftime('%H:%M')

# PART A: REVERTED JSON LOGIC (For Fawkner & South Channel)
for name, url in JSON_STATIONS.items():
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()['observations']['data'][0]
        
        raw_time = str(data['local_date_time_full'])
        obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
        obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"

        wind_spd_kts = kmh_to_knots(data.get('wind_spd_kmh'))
        wind_gust_kts = kmh_to_knots(data.get('gust_kmh'))
        wind_dir = data.get('wind_dir')

        worksheet.append_row([obs_date, obs_time, name, "N/A", "N/A", "N/A", wind_spd_kts, wind_gust_kts, wind_dir, ext_date, ext_time])
        rows_added += 1
        print(f"Logged JSON: {name}")
    except Exception as e:
        print(f"Extraction failure for {name}: {e}")

# PART B: BEAUTIFUL SOUP LOGIC (For Frankston Beach only)
try:
    response = requests.get(FRANKSTON_HTML_URL, headers=headers, timeout=15)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Target the specific data table row
    rows = soup.find('table').find_all('tr')
    for r in rows:
        tds = r.find_all('td')
        # We look for the first row that has enough columns and a numeric wind speed at index 6
        if len(tds) > 7:
            val_check = tds[6].text.strip()
            if val_check.replace('.','').isdigit() or val_check == "CALM":
                obs_time_scraped = tds[1].text.strip().split('/')[-1] # Extracts time from "29/10:30am"
                wind_dir_scraped = tds[5].text.strip()
                wind_spd_scraped = tds[6].text.strip()
                wind_gust_scraped = tds[7].text.strip()

                worksheet.append_row([ext_date, obs_time_scraped, "Frankston Beach", "N/A", "N/A", "N/A", 
                                     kmh_to_knots(wind_spd_scraped), kmh_to_knots(wind_gust_scraped), 
                                     wind_dir_scraped, ext_date, ext_time])
                rows_added += 1
                print("Logged Scraped: Frankston Beach")
                break
except Exception as e:
    print(f"Extraction failure for Frankston Beach: {e}")

print(f"Success: {rows_added} rows added.")
