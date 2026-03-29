import requests
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys
from bs4 import BeautifulSoup

# --- 1. AUTHENTICATION ---
try:
    DEFAULT_SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    raw_creds = os.environ.get('GOOGLE_CREDENTIALS')
    if not raw_creds:
        print("FATAL ERROR: GOOGLE_CREDENTIALS secret is missing.")
        sys.exit(1)
        
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)

    sh = gc.open("Wind+WaveScrapeLLM 28-3-2026")
    worksheet = sh.worksheet("Wind")
except Exception as e:
    print(f"FATAL ERROR during Init: {e}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
# Using the JSON endpoints that worked yesterday for these two
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94853.json"
}

# The specific HTML page for Frankston Beach Coastal Obs
FRANKSTON_HTML_URL = "http://www.bom.gov.au/products/IDV60801/IDV60801.94871.shtml"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        if kmh is None or str(kmh).strip().upper() in ["CALM", "-", ""]:
            return 0.0
        return round(float(kmh) * 0.539957, 1)
    except:
        return None

# --- 3. EXECUTION ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

# PART A: JSON STATIONS (Fawkner & South Channel)
for name, url in JSON_STATIONS.items():
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        data = resp.json()['observations']['data'][0]
        
        raw_t = str(data['local_date_time_full'])
        obs_date = f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}"
        obs_time = f"{raw_t[8:10]}:{raw_t[10:12]}"

        worksheet.append_row([
            obs_date, obs_time, name, 
            "N/A", "N/A", "N/A", 
            kmh_to_knots(data.get('wind_spd_kmh')), 
            kmh_to_knots(data.get('gust_kmh')), 
            data.get('wind_dir', 'N/A'), 
            ext_date, ext_time
        ])
        rows_added += 1
        print(f"Successfully logged JSON: {name}")
    except Exception as e:
        print(f"CRITICAL ERROR for {name}: {e}")

# PART B: FRANKSTON BEACH (Scraped)
frankston_success = False
try:
    resp = requests.get(FRANKSTON_HTML_URL, headers=headers, timeout=15)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    table = soup.find('table')
    if not table:
        raise ValueError("Could not find any table on the Frankston BOM page.")

    rows = table.find_all('tr')
    for r in rows:
        tds = r.find_all('td')
        # In this specific table: Col 1=Time, Col 5=Dir, Col 6=Speed, Col 7=Gust
        if len(tds) > 7:
            spd_raw = tds[6].text.strip()
            # Ensure we are on a data row by checking if wind speed is numeric or 'CALM'
            if spd_raw.replace('.','').isdigit() or spd_raw.upper() == "CALM":
                obs_t = tds[1].text.strip().split('/')[-1]
                w_dir = tds[5].text.strip()
                w_spd = spd_raw
                w_gst = tds[7].text.strip()

                worksheet.append_row([
                    ext_date, obs_t, "Frankston Beach", 
                    "N/A", "N/A", "N/A", 
                    kmh_to_knots(w_spd), 
                    kmh_to_knots(w_gst), 
                    w_dir, ext_date, ext_time
                ])
                rows_added += 1
                frankston_success = True
                print("Successfully logged Scraped: Frankston Beach")
                break

    if not frankston_success:
        print("CRITICAL FAILURE: Frankston Beach HTML was loaded but no valid data rows were found.")

except Exception as e:
    print(f"CRITICAL FAILURE for Frankston Beach Scrape: {e}")

print(f"Process Complete: {rows_added}/3 rows added.")
