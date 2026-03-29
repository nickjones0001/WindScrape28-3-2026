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
    creds_json = json.loads(os.environ.get('GOOGLE_CREDENTIALS'))
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)
    
    sh = gc.open("Wind+WaveScrapeLLM 28-3-2026")
    worksheet = sh.worksheet("Wind")
except Exception as e:
    print(f"Init Error: {e}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94871.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94857.json"
}
FRANKSTON_URL = "http://www.bom.gov.au/products/IDV60801/IDV60801.94871.shtml"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'http://www.bom.gov.au/vic/observations/vicall.shtml'
}

melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        if not kmh or str(kmh).strip().upper() == "CALM" or str(kmh).strip() == "-":
            return 0.0
        return round(float(kmh) * 0.539957, 1)
    except:
        return "N/A"

# --- 3. EXECUTION ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

# PART A: JSON (Fawkner & South Channel)
for name, url in JSON_STATIONS.items():
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        data = resp.json()['observations']['data'][0]
        
        raw_t = str(data['local_date_time_full'])
        obs_date = f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}"
        obs_time = f"{raw_t[8:10]}:{raw_t[10:12]}"

        # Fixed Column Alignment
        row = [
            obs_date, obs_time, name, 
            "N/A", "N/A", "N/A",      # Wave Columns
            kmh_to_knots(data.get('wind_spd_kmh')), 
            kmh_to_knots(data.get('gust_kmh')), 
            data.get('wind_dir', 'N/A'), 
            ext_date, ext_time
        ]
        worksheet.append_row(row)
        rows_added += 1
    except Exception as e:
        print(f"JSON Error {name}: {e}")

# PART B: FRANKSTON (Scraped)
try:
    resp = requests.get(FRANKSTON_URL, headers=headers, timeout=15)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Target the row specifically. In IDV60801, the columns are:
    # 0:Date, 1:Time, 2:Temp, 3:Dew, 4:RelHum, 5:Dir, 6:Spd, 7:Gust...
    rows = soup.find('table').find_all('tr')
    for r in rows:
        tds = r.find_all('td')
        if len(tds) > 7:
            # Check if index 6 (Wind Speed) actually contains a number or "CALM"
            val_check = tds[6].text.strip()
            if val_check.replace('.','').isdigit() or val_check == "CALM":
                obs_time = tds[1].text.strip().split('/')[-1]
                wind_dir = tds[5].text.strip()
                wind_spd = tds[6].text.strip()
                wind_gst = tds[7].text.strip()

                row = [
                    ext_date, obs_time, "Frankston Beach", 
                    "N/A", "N/A", "N/A", 
                    kmh_to_knots(wind_spd), 
                    kmh_to_knots(wind_gst), 
                    wind_dir, ext_date, ext_time
                ]
                worksheet.append_row(row)
                rows_added += 1
                break
except Exception as e:
    print(f"Scrape Error Frankston: {e}")

print(f"Done. {rows_added} rows added.")
