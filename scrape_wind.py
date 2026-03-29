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
# Corrected Victorian Marine IDs for 2026
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94871.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94857.json"
}

# Corrected Frankston Beach HTML page
FRANKSTON_URL = "http://www.bom.gov.au/products/IDV60801/IDV60801.94871.shtml"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'http://www.bom.gov.au/vic/observations/vicall.shtml'
}

def kmh_to_knots(kmh):
    try:
        return round(float(kmh) * 0.539957, 1) if kmh else "N/A"
    except:
        return "N/A"

melb_tz = timezone(timedelta(hours=11))

# --- 3. EXECUTION LOOP ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

# PART A: JSON STATIONS
for name, url in JSON_STATIONS.items():
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()['observations']['data'][0]
        
        raw_t = str(data['local_date_time_full'])
        obs_date = f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}"
        obs_time = f"{raw_t[8:10]}:{raw_t[10:12]}"

        row = [obs_date, obs_time, name, "N/A", "N/A", "N/A", 
               kmh_to_knots(data.get('wind_spd_kmh')), 
               kmh_to_knots(data.get('gust_kmh')), 
               data.get('wind_dir'), ext_date, ext_time]
        
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged {name}")
    except Exception as e:
        print(f"JSON Error for {name}: {e}")

# PART B: FRANKSTON BEACH (Scraped)
try:
    resp = requests.get(FRANKSTON_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Locate the table row containing the latest data
    table = soup.find('table')
    rows = table.find_all('tr')
    
    # Skip headers to find the first data row
    data_row = None
    for r in rows:
        tds = r.find_all('td')
        if len(tds) > 5:
            data_row = tds
            break

    if data_row:
        # Format: [Day/Time, Temp, AppTemp, DewPt, RelHum, DeltaT, WindDir, WindSpd, WindGust, ...]
        obs_time = data_row[1].text.strip().split('/')[-1]
        wind_dir = data_row[3].text.strip()
        wind_spd = data_row[4].text.strip()
        wind_gst = data_row[5].text.strip()

        row = [ext_date, obs_time, "Frankston Beach", "N/A", "N/A", "N/A", 
               kmh_to_knots(wind_spd), kmh_to_knots(wind_gst), wind_dir, ext_date, ext_time]
        
        worksheet.append_row(row)
        rows_added += 1
        print("Logged Frankston Beach (Scraped)")
except Exception as e:
    print(f"Scrape Error for Frankston: {e}")

print(f"Process Complete. {rows_added} rows added.")
