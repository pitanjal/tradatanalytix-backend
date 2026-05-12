# backend/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from supabase import create_client
import google.generativeai as genai
import os
from dotenv import load_dotenv
from daily_sync import run_daily_upload
import urllib.parse
import datetime
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import numpy as np

# --- NEW: IMPORT YOUR SYNC FUNCTION ---
try:
    from daily_sync import run_daily_upload
except ImportError:
    def run_daily_upload():
        print("Error: daily_sync.py not found in the directory.")

# Load keys from the .env file
load_dotenv()

app = FastAPI()

# Allow the React frontend to talk to this Python backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize External Services
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))

# Note: Gemini 1.5 Flash is the standard naming, ensured compatibility here
model = genai.GenerativeModel("gemini-1.5-flash")

class ChatRequest(BaseModel):
    prompt: str
    context_data: list

# --- SCHEDULER LOGIC ---
def scheduled_daily_sync():
    print(f"[{datetime.datetime.now()}] Starting scheduled daily data upload...")
    try:
        run_daily_upload()
        print(f"[{datetime.datetime.now()}] Scheduled sync completed successfully.")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR in scheduled task: {e}")

scheduler = BackgroundScheduler()
# Set to 1:30 AM IST to ensure market close data is fully processed by providers
scheduler.add_job(scheduled_daily_sync, 'cron', hour=18, minute=30, timezone="Asia/Kolkata")

@app.on_event("startup")
def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        print("Background Scheduler started: Daily sync active for 01:30 AM IST.")

@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown()
    print("Background Scheduler shut down.")

# --- UPSTOX MASTER LOOKUP ---
# --- UPSTOX MASTER LOOKUP (Pre-loaded for efficiency) ---
print("Downloading Upstox Master CSV...")
try:
    fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(fileUrl)
    # Filter for BSE Equity stocks specifically for the mapping
    bse_eq = symboldf[symboldf['exchange'].str.contains('BSE', case=False, na=False) & 
                     symboldf['instrument_type'].str.contains('EQ', case=False, na=False)]
    
    # Mapping for joins: BSE Code is the exchange_token
    upstox_mapping = bse_eq[['instrument_key', 'name', 'exchange_token']].rename(
        columns={'exchange_token': 'BSE Code'}
    )
    # Ensure BSE Code is string for merging
    upstox_mapping['BSE Code'] = upstox_mapping['BSE Code'].astype(str)
    print("Upstox Master loaded successfully!")
except Exception as e:
    print(f"Failed to load Upstox Master: {e}")
    upstox_mapping = pd.DataFrame()


def fetch_all_supabase_data(table_name, batch_size=1000):
    all_rows = []
    start_index = 0
    while True:
        response = (
            supabase.table(table_name)
            .select("*")
            .range(start_index, start_index + batch_size - 1)
            .execute()
        )
        chunk = response.data
        all_rows.extend(chunk)
        if len(chunk) < batch_size:
            break
        start_index += batch_size
    return pd.DataFrame(all_rows)

@app.get("/api/swing-momentum")
def get_swing_data(date: str):
    try:
        # Convert input date
        target_date = pd.to_datetime(date).date()
        
        # 1. Fetch and Filter Scans
        df_scans = fetch_all_supabase_data("daily_scans_test")
        df_scans['created_at'] = pd.to_datetime(df_scans['created_at'])
        
        # Log row count before/after date filter
        print(f"Total scans fetched: {len(df_scans)}")
        df_scans = df_scans[df_scans['created_at'].dt.date == target_date]
        print(f"Scans for {target_date}: {len(df_scans)}")

        if df_scans.empty:
            return {"status": "success", "data": []}

        # 2. Fetch Fundamentals & Technicals
        df_funda = fetch_all_supabase_data("company_fundamentals")
        df_funda = df_funda.drop_duplicates(subset=['BSE Code'])
        df_tech = fetch_all_supabase_data("all_stocks_technicals")
        df_tech = df_tech.rename(columns={'Symbol': 'instrument_key'})

        # 3. Multi-Stage Merge
        # Step A: Get BSE Code and Name mapping
        df_merged_1 = pd.merge(df_scans, upstox_mapping[['instrument_key', 'BSE Code', 'name']], on='name', how='left')
        df_merged_1['ISIN'] = df_merged_1['instrument_key'].str.split('|').str[1]
        
        # Step B: Fundamentals merge
        # Force string types to ensure matching
        df_merged_1['ISIN'] = df_merged_1['ISIN'].astype(str)
        df_funda['ISIN'] = df_funda['ISIN'].astype(str)
        
        df_merged_2 = pd.merge(df_merged_1, df_funda, on='ISIN', how='inner')
        print(f"Rows after Fundamentals merge: {len(df_merged_2)}")

        # Step C: Technicals merge
        df_final = pd.merge(df_merged_2, df_tech, on='instrument_key', how='left')

        # 4. Filter and Clean
        # Set to 500 as per your latest local logic
        df_final = df_final[df_final['Market Capitalization'] > 500]
        print(f"Final rows after Market Cap filter: {len(df_final)}")

        # Replace placeholders
        cols_to_fix = ['Dist_EMA_200 %', 'RS (21)', 'RS (123)', 'dist_ema_200', 'rs_21', 'rs_123']
        for col in cols_to_fix:
            if col in df_final.columns:
                df_final[col] = df_final[col].replace({-99: None, np.nan: None})

        # 2. Replace all NaN, Inf, and -Inf values with None (null in JSON)
        # This prevents the "Out of range float values" error
        df_final = df_final.replace([np.nan, np.inf, -np.inf], None)
        
        # 3. Final safety conversion to ensure all records are serializable
        data_records = df_final.to_dict(orient="records")
        
        return {"status": "success", "data": data_records}
        
    except Exception as e:
        print(f"Error in swing-momentum: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/ai-chat")
def analyze_stocks(request: ChatRequest):
    system_prompt = f"Analyze this data: {request.context_data}. User asks: {request.prompt}"
    response = model.generate_content(system_prompt)
    return {"reply": response.text}

@app.get("/api/stock-history/{stock_name}")
def get_stock_history(stock_name: str):
    try:
        match = upstox_mapping[upstox_mapping['name'] == stock_name]
        if match.empty:
            raise HTTPException(status_code=404, detail=f"Symbol '{stock_name}' not found.")
        
        instrument_key = match['instrument_key'].iloc[0]
        instrument_encoded = urllib.parse.quote(instrument_key)
        
        today = datetime.date.today()
        to_date = today.strftime('%Y-%m-%d')
        from_date = (today - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        url = f'https://api.upstox.com/v2/historical-candle/{instrument_encoded}/day/{to_date}/{from_date}'
        candleRes = requests.get(url, headers={'accept': 'application/json'}).json()
        
        if 'data' not in candleRes or 'candles' not in candleRes['data']:
            raise HTTPException(status_code=404, detail="No candle data found")

        candleData = pd.DataFrame(candleRes['data']['candles'])
        candleData.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
        
        # Convert and sort for Light Charts
        candleData['time'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d')
        candleData = candleData[['time', 'open', 'high', 'low', 'close']]
        candleData.sort_values(by='time', inplace=True)
        
        return candleData.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def health_check():
    return {
        "status": "TraDatAnalytix Systems Online", 
        "scheduler_running": scheduler.running,
        "server_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }




























































