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
    print(f"Auth/Init Error: {e}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
# Fawkner and South Channel work perfectly with JSON
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94853.json"
}
# This URL is the specific 'Coastal' view for Frankston Beach
FRANKSTON_URL = "https://www.bom.gov.au/places/vic/st-andrews-beach/observations/frankston-beach/"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.bom.gov.au/vic/observations/melbourne.shtml'
}

melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        if not kmh or str(kmh).strip().upper() in ["CALM", "-", ""]: return 0.0
        return round(float(kmh) * 0.539957, 1)
    except: return "N/A"

# --- 3. EXECUTION ---
rows_added = 0
now = datetime.now(melb_tz)
ext_date, ext_time = now.strftime('%d/%m/%Y'), now.strftime('%H:%M')

# PART A: JSON STATIONS
for name, url in JSON_STATIONS.items():
    try:
        data = requests.get(url, headers=headers, timeout=15).json()['observations']['data'][0]
        raw_t = str(data['local_date_time_full'])
        row = [f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}", f"{raw_t[8:10]}:{raw_t[10:12]}", name, 
               "N/A", "N/A", "N/A", kmh_to_knots(data.get('wind_spd_kmh')), 
               kmh_to_knots(data.get('gust_kmh')), data.get('wind_dir'), ext_date, ext_time]
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged {name}")
    except Exception as e:
        print(f"JSON Error for {name}: {e}")

# PART B: FRANKSTON BEACH HTML SCRAPE
try:
    resp = requests.get(FRANKSTON_URL, headers=headers, timeout=15)
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # In the 'Places' observation table:
    # Row 0: Header, Row 1: Subheader, Row 2: Latest Observation
    table = soup.find('table')
    rows = table.find_all('tr')
    
    for r in rows:
        tds = r.find_all('td')
        # We need a row with at least 8 columns (Time, Temp, Feels, Hum, Dir, Spd, Gust, Press)
        if len(tds) >= 7:
            time_str = tds[0].text.strip()
            # If the first column looks like a time (e.g., '4:30 pm'), this is our row
            if "pm" in time_str.lower() or "am" in time_str.lower():
                w_dir = tds[4].text.strip()
                w_spd_kmh = tds[5].text.strip()
                w_gst_kmh = tds[6].text.strip()
                
                row = [ext_date, time_str, "Frankston Beach", "N/A", "N/A", "N/A", 
                       kmh_to_knots(w_spd_kmh), kmh_to_knots(w_gst_kmh), w_dir, ext_date, ext_time]
                worksheet.append_row(row)
                rows_added += 1
                print("Logged Frankston Beach via HTML Scrape")
                break
except Exception as e:
    print(f"Scrape Error for Frankston: {e}")

print(f"Done. {rows_added}/3 rows added.")
