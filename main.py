# backend/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from supabase import create_client
import google.generativeai as genai
import os
from dotenv import load_dotenv
import urllib.parse
import datetime
import requests
from apscheduler.schedulers.background import BackgroundScheduler

# --- IMPORT YOUR SYNC FUNCTION ---
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

# Gemini 1.5 Flash configuration
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
# Updated to 18:30 IST as per your script requirement
scheduler.add_job(scheduled_daily_sync, 'cron', hour=18, minute=30, timezone="Asia/Kolkata")

@app.on_event("startup")
def start_scheduler():
    if not scheduler.running:
        scheduler.start()
        print("Background Scheduler started: Daily sync active for 06:30 PM IST.")

@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown()
    print("Background Scheduler shut down.")

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

@app.get("/api/swing-momentum")
def get_swing_data(date: str):
    try:
        # 1. Fetch Today's Scans
        start_of_day = f"{date}T00:00:00"
        end_of_day = f"{date}T23:59:59"
        
        scan_res = supabase.table("daily_scans_test").select("*").gte("created_at", start_of_day).lte("created_at", end_of_day).execute()
        df_scans = pd.DataFrame(scan_res.data)
        
        if df_scans.empty:
            return {"status": "success", "data": []}

        # 2. Fetch Fundamentals & Full Technicals from Supabase
        funda_res = supabase.table("company_fundamentals").select("*").execute()
        df_funda = pd.DataFrame(funda_res.data).drop_duplicates(subset=['BSE Code'])
        df_funda['BSE Code'] = df_funda['BSE Code'].astype(str)

        tech_res = supabase.table("all_stocks_technicals").select("*").execute()
        df_tech = pd.DataFrame(tech_res.data).rename(columns={'Symbol': 'instrument_key'})

        # 3. Multi-Stage Merge
        # Attach BSE Code to scans via Upstox Mapping
        df_merged = pd.merge(df_scans, upstox_mapping[['instrument_key', 'BSE Code']], on='instrument_key', how='left')
        
        # Merge with Fundamentals
        df_merged = pd.merge(df_merged, df_funda, on='BSE Code', how='left')
        
        # Merge with Technicals
        df_final = pd.merge(df_merged, df_tech, on='instrument_key', how='left')

        # 4. Filter and Clean
        # Remove Non-tradable instruments (MFs/ETFs) by requiring Market Cap
        df_final = df_final[df_final['Market Capitalization'].notna()]
        
        # Handle placeholder -99 for cleaner JSON response
        cols_to_null = ['Dist_EMA_200 %', 'RS (21)', 'RS (123)', 'dist_ema_200', 'rs_21', 'rs_123']
        for col in cols_to_null:
            if col in df_final.columns:
                df_final[col] = df_final[col].replace(-99, None)
        
        return {"status": "success", "data": df_final.to_dict(orient="records")}
        
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