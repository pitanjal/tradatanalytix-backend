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
import pandas as pd
from fastapi import HTTPException

# Load keys from the .env file
load_dotenv()

app = FastAPI()

# Allow the React frontend to talk to this Python backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Changed from "http://localhost:3000"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Initialize External Services
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")

class ChatRequest(BaseModel):
    prompt: str
    context_data: list


# --- UPSTOX MASTER LOOKUP ---
# Load this ONCE when the server boots so it is lightning fast on clicks
print("Downloading Upstox Master CSV...")
try:
    fileUrl = 'https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
    symboldf = pd.read_csv(fileUrl)
    
    # Filtering for Equity stocks (kept broad to catch both BSE/NSE based on your scans)
    eq_stocks = symboldf[symboldf['instrument_type'].str.contains('EQ', case=False, na=False) & (symboldf['last_price'] > 0)]
    upstox_mapping = eq_stocks[['instrument_key', 'name', 'exchange_token']]
    print("Upstox Master loaded successfully!")
except Exception as e:
    print(f"Failed to load Upstox Master: {e}")
    upstox_mapping = pd.DataFrame()



@app.get("/api/swing-momentum")
def get_swing_data(date: str):
    try:
        # Create a time range for the entire day
        start_of_day = f"{date}T00:00:00"
        end_of_day = f"{date}T23:59:59"
        
        # Use .gte (Greater Than or Equal) and .lte (Less Than or Equal)
        response = (
            supabase.table("daily_scans_test")
            .select("*")
            .gte("created_at", start_of_day)
            .lte("created_at", end_of_day)
            .execute()
        )
        
        df = pd.DataFrame(response.data)
        
        if df.empty:
            return {"status": "success", "data": []}
            
        return {"status": "success", "data": df.to_dict(orient="records")}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ai-chat")
def analyze_stocks(request: ChatRequest):
    system_prompt = f"Analyze this data: {request.context_data}. User asks: {request.prompt}"
    response = model.generate_content(system_prompt)
    return {"reply": response.text}



@app.get("/api/stock-history/{stock_name}")
def get_stock_history(stock_name: str):
    try:
        # 1. Use the Streamlit logic: Find the match in the Master CSV
        match = upstox_mapping[upstox_mapping['name'] == stock_name]
        
        if match.empty:
            raise HTTPException(status_code=404, detail=f"Symbol '{stock_name}' not found in Upstox list.")
        
        # 2. Extract the key safely
        instrument_key = match['instrument_key'].iloc[0]
        print(f"Translated {stock_name} -> {instrument_key}")
        
        # 3. Fetch from Upstox using the real key
        instrument_encoded = urllib.parse.quote(instrument_key)
        today = datetime.date.today()
        to_date = today.strftime('%Y-%m-%d')
        from_date = (today - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        url = f'https://api.upstox.com/v2/historical-candle/{instrument_encoded}/day/{to_date}/{from_date}'
        candleRes = requests.get(url, headers={'accept': 'application/json'}).json()
        
        if 'data' not in candleRes or 'candles' not in candleRes['data']:
            raise HTTPException(status_code=404, detail="No candle data found for this instrument")

        # 4. Format for TradingView Light Charts
        candleData = pd.DataFrame(candleRes['data']['candles'])
        candleData.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
        
        candleData['time'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d')
        candleData = candleData[['time', 'open', 'high', 'low', 'close']]
        candleData.sort_values(by='time', inplace=True)
        
        return candleData.to_dict(orient='records')
        
    except Exception as e:
        print(f"Exception when calling HistoryApi: {e}")
        raise HTTPException(status_code=500, detail=str(e))