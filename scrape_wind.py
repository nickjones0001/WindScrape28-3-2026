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
        print("ERROR: GOOGLE_CREDENTIALS secret missing.")
        sys.exit(1)
    creds_json = json.loads(raw_creds)
    creds = Credentials.from_service_account_info(creds_json, scopes=DEFAULT_SCOPES)
    gc = gspread.authorize(creds)
    
    sh = gc.open("Wind+WaveScrapeLLM 28-3-2026")
    worksheet = sh.worksheet("Wind")
except Exception as e:
    print(f"Init Error: {e}")
    sys.exit(1)

# --- 2. CONFIGURATION ---
# Using specific Marine IDs from your verified registry
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.086376.json",
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.086344.json"
}

# Frankston Beach HTML page
FRANKSTON_URL = "http://www.bom.gov.au/products/IDV60801/IDV60801.95872.shtml"

# Enhanced headers to bypass bot detection
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Referer': 'http://www.bom.gov.au/vic/observations/melbourne.shtml'
}

melb_tz = timezone(timedelta(hours=11))

def kmh_to_knots(kmh):
    try:
        return round(float(kmh) * 0.539957, 1) if kmh else "N/A"
    except:
        return "N/A"

# --- 3. EXECUTION ---
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
        row = [f"{raw_t[6:8]}/{raw_t[4:6]}/{raw_t[0:4]}", f"{raw_t[8:10]}:{raw_t[10:12]}", 
               name, "N/A", "N/A", "N/A", kmh_to_knots(data.get('wind_spd_kmh')), 
               kmh_to_knots(data.get('gust_kmh')), data.get('wind_dir'), ext_date, ext_time]
        
        worksheet.append_row(row)
        rows_added += 1
        print(f"Logged {name}")
    except Exception as e:
        print(f"JSON Error for {name}: {e}")

# PART B: FRANKSTON BEACH (BeautifulSoup)
try:
    resp = requests.get(FRANKSTON_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Find the observations table
    table = soup.find('table', {'class': 'stats'})
    if not table:
        table = soup.find('table') # Fallback to any table if class 'stats' is missing
        
    rows = table.find_all('tr')
    # Finding the first row with numerical data (usually index 2 or 3)
    data_row = None
    for r in rows:
        tds = r.find_all('td')
        if len(tds) > 5 and tds[4].text.strip().replace('.','').isdigit():
            data_row = tds
            break

    if data_row:
        obs_time = data_row[1].text.strip().split('/')[-1] # Grabs "10:30am" from "29/10:30am"
        wind_dir = data_row[3].text.strip()
        wind_spd = data_row[4].text.strip()
        wind_gst = data_row[5].text.strip()

        row = [ext_date, obs_time, "Frankston Beach", "N/A", "N/A", "N/A", 
               kmh_to_knots(wind_spd), kmh_to_knots(wind_gst), wind_dir, ext_date, ext_time]
        worksheet.append_row(row)
        rows_added += 1
        print("Logged Frankston Beach (Scraped)")
    else:
        print("Error: Could not find data row in Frankston HTML.")
except Exception as e:
    print(f"Scrape Error for Frankston: {e}")

print(f"Process Complete. {rows_added} rows added.")
