import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, timezone, timedelta
import sys

# Diagnostic Boot Sequence: Intercept Authentication Drops
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

    # Diagnostic Database Linkage
    sheet_name = "Wind+WaveScrapeLLM 28-3-2026"
    sh = gc.open(sheet_name)
    worksheet = sh.worksheet("Wind")

except Exception as boot_error:
    print(f"FATAL ERROR during server boot sequence: {boot_error}")
    sys.exit(1)

headers_req = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

def kmh_to_knots(kmh):
    try:
        return round(float(kmh) * 0.539957, 2)
    except (TypeError, ValueError):
        return "N/A"

rows_added = 0
melb_tz = timezone(timedelta(hours=11))

# 1. JSON Array (Fawkner Beacon & Verified South Channel Island)
JSON_STATIONS = {
    "Fawkner Beacon": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.95872.json",
    # CRITICAL FIX: Routed to the active Bay/Marine sensor ID for South Channel Island
    "South Channel Island": "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94857.json"
}

for name, url in JSON_STATIONS.items():
    ext_dt = datetime.now(melb_tz)
    ext_date = ext_dt.strftime('%d/%m/%Y')
    ext_time = ext_dt.strftime('%H:%M')
    
    try:
        response = requests.get(url, headers=headers_req, timeout=15)
        data = response.json()['observations']['data'][ 0 ]
        
        raw_time = data['local_date_time_full']
        obs_date = f"{raw_time[6:8]}/{raw_time[4:6]}/{raw_time[0:4]}"
        obs_time = f"{raw_time[8:10]}:{raw_time[10:12]}"

        wind_spd_kts = kmh_to_knots(data.get('wind_spd_kmh'))
        wind_gust_kts = kmh_to_knots(data.get('gust_kmh'))
        wind_dir = data.get('wind_dir', "N/A")
        
        worksheet.append_row([obs_date, obs_time, name, "N/A", "N/A", "N/A", wind_spd_kts, wind_gust_kts, wind_dir, ext_date, ext_time])
        rows_added += 1
    except Exception as e:
        print(f"Extraction failure for JSON {name}: {e}")

# 2. HTML Array (True Frankston Beach Coastal Node - 086371)
try:
    ext_dt = datetime.now(melb_tz)
    ext_date = ext_dt.strftime('%d/%m/%Y')
    ext_time = ext_dt.strftime('%H:%M')
    
    # Target the true Frankston Beach HTML endpoint
    frankston_url = "http://www.bom.gov.au/products/IDV60901/IDV60901.086371.shtml"
    html_response = requests.get(frankston_url, headers=headers_req, timeout=15)
    soup = BeautifulSoup(html_response.content, 'html.parser')
    
    table = soup.find('table')
    if table:
        # Isolate the most recent temporal observation
        latest_row = table.find('tbody').find('tr')
        raw_obs_time = latest_row.find('th').text.strip()
        
        # Safely split temporal string (e.g., "28/04:00pm") into Date and Time
        if '/' in raw_obs_time:
            day, time_str = raw_obs_time.split('/', 1)
            obs_date = f"{day}/{ext_dt.strftime('%m/%Y')}"
            obs_time = time_str
        else:
            obs_date = ext_date
            obs_time = raw_obs_time
        
        tds = latest_row.find_all('td')
        
        # CRITICAL FIX: Structural override bypassing BoM's 2-tier HTML header. 
        # Standardized maritime indices applied directly to the <td> array.
        wind_dir = tds[3].text.strip()
        wind_spd_kts = tds[4].text.strip()
        wind_gust_kts = tds[5].text.strip()
        
        # Filter empty HTML table dashes into structural 'N/A'
        wind_spd_kts = wind_spd_kts if wind_spd_kts != '-' else "N/A"
        wind_gust_kts = wind_gust_kts if wind_gust_kts != '-' else "N/A"
        wind_dir = wind_dir if wind_dir != '-' else "N/A"
        
        worksheet.append_row([obs_date, obs_time, "Frankston Beach", "N/A", "N/A", "N/A", wind_spd_kts, wind_gust_kts, wind_dir, ext_date, ext_time])
        rows_added += 1
except Exception as e:
    print(f"Extraction failure for HTML Frankston Beach: {e}")

print(f"Process Complete: {rows_added} rows added at {datetime.utcnow()}")
