def run_daily_upload():

        import pandas as pd
        import numpy as np
        import traceback,requests
        import datetime
        from time import sleep
        import warnings
        import urllib.parse
        warnings.filterwarnings('ignore')
        import webbrowser
        from breeze_connect import BreezeConnect
        import pandas as pd
        #from datetime import date, time, datetime, timedelta

        import numpy as np
        # import plotly.express as px
        import time
        import pandas_ta as ta
        import numpy as np

        def getHistData(instrument):
            try:
                instrument = urllib.parse.quote(instrument)
                today = datetime.date.today()
                to_date =datetime.date.strftime(today,'%Y-%m-%d')
                from_date = datetime.date.strftime(today- datetime.timedelta(days=365),'%Y-%m-%d')
                url = f'https://api.upstox.com/v2/historical-candle/{instrument}/day/{to_date}/{from_date}'
                candleRes = requests.get(url, headers={'accept': 'application/json'}).json()
                candleData = pd.DataFrame(candleRes['data']['candles'])
                candleData.columns = ['date','open','high','low', 'close','vol','oi']
                candleData =  candleData[['date','open','high','low', 'close']]
                candleData['date'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata')
                candleData['date'] = candleData.apply(lambda x: x.date.date(), axis=1)
                candleData.sort_values(by = 'date', inplace=True)
                return candleData
            except Exception as e:
                print(f"Exception when calling HistoryApi->get_intra_day_candle_data {e}")



        fileUrl ='https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
        symboldf = pd.read_csv(fileUrl)

        print("worked fine till here")

        indexdf = symboldf[symboldf.instrument_type == 'INDEX']
        nse_indices = indexdf[indexdf['name'].str.contains('NSE', case=False, na=False) |
                                indexdf['exchange'].str.contains('NSE', case=False, na=False)]
        nse_indices_list = nse_indices['instrument_key'].tolist()


        bse_full_stocks = symboldf[symboldf['exchange'].str.contains('BSE', case=False, na=False) & symboldf['instrument_type'].str.contains('EQ', case=False, na=False) &
                                symboldf['last_price'] > 0]
        bse_full_stocks_list = bse_full_stocks['instrument_key'].tolist()
        len(bse_full_stocks_list)

        print("got bse stocks")

        global counter
        counter = 0
        df_result = pd.DataFrame()

        sym12 = 'NSE_INDEX|Nifty 50'
        df = getHistData(sym12)

        data = df

        # Convert with a specific format
        data['date_column'] = pd.to_datetime(data['date']).dt.date
        data['dt'] = pd.to_datetime(data['date']).dt.date
        data.set_index('date_column', inplace=True)
        data3 = data[['open', 'high', 'low', 'close', 'dt']]
        data4 = pd.DataFrame()
        data4['close_n'] = data3["close"].astype(float)
        data4['high_n'] = data3["high"].astype(float)
        data4['low_n'] = data3["low"].astype(float)
        data4['open_n'] = data3["open"].astype(float)
        #data4['dt'] = data3['dt']
        global df_nifty
        df_nifty = pd.DataFrame(data4)

        print("Got Nifty data")

        for sym in bse_full_stocks_list:
            #print(sym)
            df = getHistData(sym)

            if df is None:
                #print('No data for ', sym)
                continue

            data = df
            if data.empty or len(data) < 60:
                #print('No data for ', sym)
                continue

            # Convert with a specific format
            data['date_column'] = pd.to_datetime(data['date']).dt.date
            data['dt'] = pd.to_datetime(data['date']).dt.date
            data.set_index('date_column', inplace=True)
            data3 = data[['open', 'high', 'low', 'close', 'dt']]
            data4 = pd.DataFrame()
            data4['close'] = data3["close"].astype(float)
            data4['high'] = data3["high"].astype(float)
            data4['low'] = data3["low"].astype(float)
            data4['open'] = data3["open"].astype(float)
            data4['dt'] = data3['dt']

            df = pd.DataFrame(data4)


            df_final = pd.merge(df, df_nifty, left_index=True, right_index=True, how='left')

            df = df_final


            df['rsi'] = ta.rsi(df['close'], timeperiod = 14)
            # Calculate EMAs
            df['ema_20'] = ta.ema(df['close'], length=20)
            df['ema_50'] = ta.ema(df['close'], length=50)
            df['ema_100'] = ta.ema(df['close'], length=100)
            df['ema_200'] = ta.ema(df['close'], length=200)

            # Calculate Percentage Distance: ((Price / EMA) - 1) * 100
            df['dist_ema_20'] = ((df['close'] / df['ema_20']) - 1) * 100
            df['dist_ema_50'] = ((df['close'] / df['ema_50']) - 1) * 100
            df['dist_ema_100'] = ((df['close'] / df['ema_100']) - 1) * 100
            df['dist_ema_200'] = ((df['close'] / df['ema_200']) - 1) * 100


            # Calculate rolling returns of 55 days
            df['ret_55'] = (df['close'].rolling(window=55).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_55_n'] = (df['close_n'].rolling(window=55).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_55'] = (df['ret_55'] / df['ret_55_n']) - 1


            df['ret_21'] = (df['close'].rolling(window=21).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_21_n'] = (df['close_n'].rolling(window=21).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_21'] = (df['ret_21'] / df['ret_21_n']) - 1


            df['ret_123'] = (df['close'].rolling(window=123).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_123_n'] = (df['close_n'].rolling(window=123).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_123'] = (df['ret_123'] / df['ret_123_n']) - 1


            # 1. Prepare the Relative Strength Ratio
            # 'close' is the stock, 'close_n' is Nifty 50
            df['rs_ratio'] = df['close'] / df['close_n']

            # 2. Calculate SRS (123 Days - Static)
            df['srs'] = (df['rs_ratio'] / df['rs_ratio'].shift(123) - 1)

            # 3. Calculate ARS (Adaptive - often 40 or 60 days)
            # Premal Parekh often adapts this to a recent market low,
            # but 40 days is a solid 'Adaptive' default for momentum.
            df['ars'] = (df['rs_ratio'] / df['rs_ratio'].shift(40) - 1)


            highest_value = np.max(df['close'])
            highest_index = df['close'].idxmax()
            date_of_highest_value = df.loc[df['close'].idxmax(), 'dt']
            now = datetime.date.today()
            days_difference = (now - date_of_highest_value).days


            bc = df.iloc[-1]
            ic = df.iloc[-3]
            ba_c = df.iloc[-4]

            uptrend = bc['rsi'] >= 60
            #downtrend = bc['rsi'] < 40

            u_breakout = (bc['high'] > (0.97 * highest_value))
            #d_breakout = bc['low'] < ba_c['low']

            u_rs = bc['rs_55'] > 0

            #df['rs_55'].plot()

            #inside_candle_formed = (ba_c['high'] > ic['high']) and (ba_c['low'] < ic['low'])

            if u_breakout and uptrend and u_rs:
            #if u_rs:
                new_row = pd.DataFrame([{'Symbol': sym, 'Breakout_price' : bc['close'], 'Relative Strength (vs Nifty 50)' : round((bc['rs_55']),2) ,'Days since consolidation' : days_difference,
                'dist_ema_200' : bc['dist_ema_200'], 'rsi' : bc['rsi'], 'rs_21' : bc['rs_21'], 'rs_123' : bc['rs_123']}])
                df_result = pd.concat([df_result, new_row], ignore_index=True)
                print(sym, bc['close'] ,days_difference, date_of_highest_value, round((bc['rs_55']),2) )



        print("scan ran")

        df_result.sort_values(by='Relative Strength (vs Nifty 50)', ascending=False, inplace=True)
        #df_result = df_result[~df_result['Symbol'].str.contains('PR|Div|GS|VIX|TR|EW|EQL|LIQ', case=False, na=False)]
        df_result

        stocks = df_result['Symbol'].tolist()
        df_result['instrument_key'] = df_result['Symbol']

        df_result_with_names = df_result.merge(bse_full_stocks[['instrument_key', 'name']], on='instrument_key', how='left')
        df_result_with_names = df_result_with_names[~df_result_with_names['name'].str.contains('MUTUAL FUND|%|NCD', case=False, na=False)]
        df_result_with_names

        df_result_with_names.to_csv('watchlist.csv',index=False)


        print("exported")


        import pandas as pd
        from supabase import create_client


        # 1. Setup Connection
        url = "https://vgicevfkzjdfwziwoubo.supabase.co"
        key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnaWNldmZrempkZnd6aXdvdWJvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MjYzMDgsImV4cCI6MjA4MzAwMjMwOH0.GOyGvHTyYBNqsamPt6_N9OJN4Yv_HXZP6tnTTkOeGdk"
        supabase = create_client(url, key)


        df2 = df_result_with_names[["name", "Breakout_price", "Relative Strength (vs Nifty 50)", "Days since consolidation", "dist_ema_200", "rsi", "rs_21", "rs_123"]].fillna(-99)

        # 3. Convert DataFrame to List of Dicts (Supabase format)
        records = df2.to_dict('records')

        # 4. Upload to Supabase
        # .upsert() is better than .insert() because it updates existing rows if you run it twice
        response = supabase.table("daily_scans_test").upsert(records).execute()

        print("Data successfully uploaded to Supabase!")




        ##### All stock technicals




        import pandas as pd
        import numpy as np
        import traceback,requests
        import datetime
        from time import sleep
        import warnings
        import urllib.parse
        warnings.filterwarnings('ignore')
        import webbrowser
        from breeze_connect import BreezeConnect
        import pandas as pd
        #from datetime import date, time, datetime, timedelta

        import numpy as np
        import plotly.express as px
        import time
        import pandas_ta as ta
        import numpy as np



        def getHistData(instrument):
            try:
                instrument = urllib.parse.quote(instrument)
                today = datetime.date.today()
                to_date =datetime.date.strftime(today,'%Y-%m-%d')
                from_date = datetime.date.strftime(today- datetime.timedelta(days=365),'%Y-%m-%d')
                url = f'https://api.upstox.com/v2/historical-candle/{instrument}/day/{to_date}/{from_date}'
                candleRes = requests.get(url, headers={'accept': 'application/json'}).json()
                candleData = pd.DataFrame(candleRes['data']['candles'])
                candleData.columns = ['date','open','high','low', 'close','vol','oi']
                candleData =  candleData[['date','open','high','low', 'close', 'vol']]
                candleData['date'] = pd.to_datetime(candleData['date']).dt.tz_convert('Asia/Kolkata')
                candleData['date'] = candleData.apply(lambda x: x.date.date(), axis=1)
                candleData.sort_values(by = 'date', inplace=True)
                return candleData
            except Exception as e:
                print(f"Exception when calling HistoryApi->get_intra_day_candle_data {e}")




        fileUrl ='https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz'
        symboldf = pd.read_csv(fileUrl)


        indexdf = symboldf[symboldf.instrument_type == 'INDEX']
        nse_indices = indexdf[indexdf['name'].str.contains('NSE', case=False, na=False) |
                                indexdf['exchange'].str.contains('NSE', case=False, na=False)]
        nse_indices_list = nse_indices['instrument_key'].tolist()


        bse_full_stocks = symboldf[symboldf['exchange'].str.contains('BSE', case=False, na=False) & symboldf['instrument_type'].str.contains('EQ', case=False, na=False) &
                                symboldf['last_price'] > 0]
        bse_full_stocks_list = bse_full_stocks['instrument_key'].tolist()
        len(bse_full_stocks_list)



        bse_full_stocks_name = bse_full_stocks[['instrument_key', 'name']]


        sym12 = 'NSE_INDEX|Nifty 50'
        df = getHistData(sym12)

        data = df

        # Convert with a specific format
        data['date_column'] = pd.to_datetime(data['date']).dt.date
        data['dt'] = pd.to_datetime(data['date']).dt.date
        data.set_index('date_column', inplace=True)
        data3 = data[['open', 'high', 'low', 'close', 'dt']]
        data4 = pd.DataFrame()
        data4['close_n'] = data3["close"].astype(float)
        data4['high_n'] = data3["high"].astype(float)
        data4['low_n'] = data3["low"].astype(float)
        data4['open_n'] = data3["open"].astype(float)

        global df_nifty
        df_nifty = pd.DataFrame(data4)



        global counter
        counter = 0
        df_result = pd.DataFrame()

        for sym in bse_full_stocks_list:

            df = getHistData(sym)

            if df is None:
                continue

            data = df
            if data.empty or len(data) < 60:
                continue

            # Convert with a specific format
            data['date_column'] = pd.to_datetime(data['date']).dt.date
            data['dt'] = pd.to_datetime(data['date']).dt.date
            data.set_index('date_column', inplace=True)
            data3 = data[['open', 'high', 'low', 'close', 'dt', 'vol']]
            data4 = pd.DataFrame()
            data4['close'] = data3["close"].astype(float)
            data4['high'] = data3["high"].astype(float)
            data4['low'] = data3["low"].astype(float)
            data4['open'] = data3["open"].astype(float)
            data4['vol'] = data3["vol"].astype(float)
            data4['dt'] = data3['dt']

            df = pd.DataFrame(data4)

            df_final = pd.merge(df, df_nifty, left_index=True, right_index=True, how='left')

            df = df_final

            df['rsi'] = ta.rsi(df['close'], timeperiod = 14)
            # Calculate EMAs
            df['ema_20'] = ta.ema(df['close'], length=20)
            df['ema_50'] = ta.ema(df['close'], length=50)
            df['ema_100'] = ta.ema(df['close'], length=100)
            df['ema_200'] = ta.ema(df['close'], length=200)

            # Calculate rolling returns of 55 days
            
            df['ret_21'] = (df['close'].rolling(window=21).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_21_n'] = (df['close_n'].rolling(window=21).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_21'] = (df['ret_21'] / df['ret_21_n']) - 1
            
            
            df['ret_55'] = (df['close'].rolling(window=55).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_55_n'] = (df['close_n'].rolling(window=55).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_55'] = (df['ret_55'] / df['ret_55_n']) - 1
            
            df['ret_123'] = (df['close'].rolling(window=123).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['ret_123_n'] = (df['close_n'].rolling(window=123).apply(lambda x: (x[-1] / x[0]), raw=True))
            df['rs_123'] = (df['ret_123'] / df['ret_123_n']) - 1
            
            
            # 1. Prepare the Relative Strength Ratio
            # 'close' is the stock, 'close_n' is Nifty 50
            df['rs_ratio'] = df['close'] / df['close_n']

            # 2. Calculate SRS (123 Days - Static)
            df['srs'] = (df['rs_ratio'] / df['rs_ratio'].shift(123) - 1) * 100

            # 3. Calculate ARS (Adaptive - often 40 or 60 days)
            # Premal Parekh often adapts this to a recent market low, 
            # but 40 days is a solid 'Adaptive' default for momentum.
            df['ars'] = (df['rs_ratio'] / df['rs_ratio'].shift(40) - 1) * 100

            # 4. Define the "Emerging Tendulkar" Logic
            # Condition: ARS has just crossed above 0, but SRS is still below 0
            df['is_emerging'] = (df['ars'] > 0) & (df['srs'] < 0)

            # 5. Define the "RS Warrior" (Full Breakout)
            # Condition: Both are above 0
            df['is_rs_warrior'] = (df['ars'] > 0) & (df['srs'] > 0)
            
            
            # Calculate Percentage Distance: ((Price / EMA) - 1) * 100
            df['dist_ema_20'] = ((df['close'] / df['ema_20']) - 1) * 100
            df['dist_ema_50'] = ((df['close'] / df['ema_50']) - 1) * 100
            df['dist_ema_100'] = ((df['close'] / df['ema_100']) - 1) * 100
            df['dist_ema_200'] = ((df['close'] / df['ema_200']) - 1) * 100


            highest_value = np.max(df['close'])
            highest_index = df['close'].idxmax()
            date_of_highest_value = df.loc[df['close'].idxmax(), 'dt']
            now = datetime.date.today()
            days_difference = (now - date_of_highest_value).days


            bc = df.iloc[-1]

            uptrend = bc['rsi'] > 60

            u_breakout = (bc['high'] >= (0.95 * highest_value))

            u_rs = bc['rs_55'] > 0
            
            
            status = "Neutral"
            if bc['ars'] > 0 and bc['srs'] < 0:
                status = "Emerging Tendulkar"
            elif bc['ars'] > 0 and bc['srs'] > 0:
                status = "RS Warrior"
            elif bc['ars'] < 0 and bc['srs'] > 0:
                status = "Fading"
                
                
            

            
            # Create the new_row with ALL columns
            new_row = pd.DataFrame([{
                'Symbol': sym, 
                'Price': bc['close'],
                'RSI': bc['rsi'],
                'RS (21)' : bc['rs_21'] ,
                'RS (55)' : bc['rs_55'] ,
                'RS (123)' : bc['rs_123'] ,
                'SRS (123)': bc['srs'],
                'ARS (40)': bc['ars'],
                'EMA_20': bc['ema_20'],
                'EMA_50': bc['ema_50'],
                'EMA_100': bc['ema_100'],
                'EMA_200': bc['ema_200'],
                'RS Status': status,
                'RSI Uptrend': uptrend, 
                'Breakout Zone': u_breakout,
                'Days since consolidation': days_difference,
                # Distance from EMAs (Percentage)
                'Dist_EMA_20 %': bc['dist_ema_20'],
                'Dist_EMA_50 %': bc['dist_ema_50'],
                'Dist_EMA_100 %': bc['dist_ema_100'],
                'Dist_EMA_200 %': bc['dist_ema_200']
            }])

            
            df_result = pd.concat([df_result, new_row], ignore_index=True)
            print(sym)



        import pandas as pd
        from supabase import create_client

        # 1. Setup Connection
        url = "https://vgicevfkzjdfwziwoubo.supabase.co"
        key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnaWNldmZrempkZnd6aXdvdWJvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MjYzMDgsImV4cCI6MjA4MzAwMjMwOH0.GOyGvHTyYBNqsamPt6_N9OJN4Yv_HXZP6tnTTkOeGdk"
        supabase = create_client(url, key)

        # 3. Convert to dict (Index is ignored by default in 'records' format)
        records = df_result.fillna(-99).to_dict('records')

        # 4. OVERWRITE LOGIC: Delete all existing records first
        # We filter by 'neq' (not equal) to a value that doesn't exist to select all rows
        supabase.table("all_stocks_technicals").delete().neq("Symbol", "0").execute()

        # 5. Insert New Data
        response = supabase.table("all_stocks_technicals").upsert(records).execute()

        # df_result.to_csv('all_technicals.csv')



# This part allows the script to still work if you run it manually
if __name__ == "__main__":
    run_daily_upload()