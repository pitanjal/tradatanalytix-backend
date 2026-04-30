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



@app.get("/api/stock-history/{instrument}")
def get_stock_history(instrument: str):
    try:
        # Upstox requires the instrument to be URL encoded (e.g. NSE_EQ|INE123...)
        instrument_encoded = urllib.parse.quote(instrument)
        today = datetime.date.today()
        to_date = today.strftime('%Y-%m-%d')
        from_date = (today - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
        
        url = f'https://api.upstox.com/v2/historical-candle/{instrument_encoded}/day/{to_date}/{from_date}'
        candleRes = requests.get(url, headers={'accept': 'application/json'}).json()
        
        if 'data' not in candleRes or 'candles' not in candleRes['data']:
            raise HTTPException(status_code=404, detail="No candle data found for this instrument")

        # Convert to DataFrame
        candleData = pd.DataFrame(candleRes['data']['candles'])
        candleData.columns = ['date', 'open', 'high', 'low', 'close', 'vol', 'oi']
        
        # TradingView requires time in 'YYYY-MM-DD' string format, sorted oldest to newest
        candleData['time'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d')
        candleData = candleData[['time', 'open', 'high', 'low', 'close']]
        candleData.sort_values(by='time', inplace=True)
        
        # Convert DataFrame to a list of dictionaries for the JSON response
        return candleData.to_dict(orient='records')
        
    except Exception as e:
        print(f"Exception when calling HistoryApi: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Test