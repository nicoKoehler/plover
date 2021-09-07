import os
from binance.client import Client
from binance.streams import ThreadedWebsocketManager as tsm
from binance.streams import BinanceSocketManager as bsm
import pandas as pd
import time
import inspect as i
import json
import pandas as pd
import datetime as dt
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import numpy as np
import pytz
from plotly.subplots import make_subplots
from finta import TA as ta
import threading as th
import sys

# ++++++++++++++++++++++ SETUP ++++++++++++++++++++++
tz_local = pytz.timezone("Europe/Zurich")

apiKey = "LuGhVqrq1cT27Dj9Lod24DxnvyJUy9FL11MfMZFulk2AUBjCIYAuz5xGBHs14YW2"
apiKey_secret = "viQI5xPonsALtTUYlBLFm66jMyIFjAZSXPPQcyMOHAKhpgMOnlcuoIRn0xbTM0Z5"
symbol = "BTCUSDT"

client = Client(apiKey, apiKey_secret)

cr_prices = {"error":False}

r_int = 2   #resample intervall
flag_start = 0
flag_lookback = 0
iLookBack = 20      # max period lookback


# ++++++++++++++++++++++ HELPER FUNCTION ++++++++++++++++++++++

def udf_df_con(df, dtCol="timestamp"):
    df[dtCol] = pd.to_datetime(df[dtCol]/1000, unit="s")
    df[dtCol]  = df[dtCol].dt.floor("S")

    df.reset_index(inplace=True, drop=True)
    df.set_index(dtCol, inplace=True)
    df.index = df.index.tz_localize("UTC").tz_convert("CET")

    cols = df.select_dtypes(exclude=["datetime64"]).columns
    df[cols] = df[cols].apply(pd.to_numeric, downcast="float",errors="coerce")


def udf_progressBar(step, total, steps, t0):
    incr = int(total/steps)
    prgr = (i-i%incr)/incr
    rem = steps-prgr
    print(f"Progress: {(i/total):.0%}  [{'█' * int(prgr)}{'-' * int(rem)}] @{(time.time() - t0):.1f}sec ", end="\r")

# ++++++++++++++++++++++ REPEAT FUNCTION ++++++++++++++++++++++
def udf_repeat():
    t = th.Timer(5.0, udf_repeat)
    t.daemon = True #exits timer when no non-daemon threads are alive anymore
    t.start()
    print(f"Min. Data Collection: {len(df_d10)} entries {(len(df_d10)/(iLookBack)):.0%} in {(time.time() - t0):.2f} seconds", end="\r")
    

# ++++++++++++++++++++++ WS ++++++++++++++++++++++
bsm = tsm(api_key=apiKey, api_secret=apiKey_secret)
bsm.start()

df_d10 = pd.DataFrame(columns=["open","high","low","close","bid", "ask"], index=pd.to_datetime([]))
df_data = pd.DataFrame(columns=["timestamp","open","high","low","close","bid", "ask"])
df_d_csv = df_d10   # csv write df

def bin_ws(msg):
    global df_data
    global df_d10
    global flag_start
    global df_d_csv

    lstTimeStamp = dt.datetime.fromtimestamp(msg["E"]/1000)
    
    # ensure that the first entry is a multiple of the interval, otherwise resampling becomes issue
    if flag_start == 0 and lstTimeStamp.second % r_int != 0:
        print(f"Not time yet: {lstTimeStamp} > {lstTimeStamp.second}")
        return 0
    elif flag_start == 0 and lstTimeStamp.second % r_int == 0:
        print("Its time...")
        flag_start = 1


    l_ws = [msg["E"],msg["o"], msg["h"] , msg["l"], msg["c"], msg["b"], msg["a"]]
    df_data.loc[len(df_data)] = l_ws

    #print(f"Took me {(time.time()-t0):.2f} seconds to get here...")

    # once enough data in DF, copy, delete and analyze
    if len(df_data) == r_int:
        df_d_copy = df_data.copy() # reset immediately to allow WS to continue writing in background
        df_data = df_data.iloc[0:0]

        udf_df_con(df_d_copy)
        df_d_csv = pd.concat([df_d_csv,df_d_copy])

        df_rs = df_d_copy.resample(f"{r_int}S", closed="left").mean()
        #print(df_d_copy)
        #print(df_d10)

        df_d10 = pd.concat([df_d10, df_rs])
        df_d_copy = df_d_copy.iloc[0:0]
        df_rs = df_rs.iloc[0:0]




    

print(f"Starting....{dt.datetime.now()}")
t0 = time.time()

bsm.start_symbol_ticker_socket(callback=bin_ws, symbol=symbol)
print(f"WS Start Time: {(time.time()-t0)}")

time.sleep(r_int) # crude approach, wait max r_int to get WS going

# ++++++++++++++++++++++ ANALYSIS BLOCK ++++++++++++++++++++++


# wait until sufficient data for rolling analysis 
while len(df_d10) < iLookBack:
    print(f"Min. Data Collection: {len(df_d10)} entries {(len(df_d10)/(iLookBack)):.0%} in {(time.time() - t0):.2f} seconds", end="\r")
    time.sleep(1)
print("\n Suffcient Data, lets START!!!")
t_a1 = time.time()

reqObs = 20
print("")
for i in range(0,r_int*reqObs):

    t_a2 = time.time()
    
    dfa = df_d10[["close"]].copy()  # analysis DF
    

    # ++++> EMA
    wd_short = 5
    wd_medium = 20
    wd_medium_ema = 13
    dfa["mav_short"] = dfa["close"].rolling(window=wd_short).mean()
    dfa["mav_medium"] = dfa["close"].rolling(window=wd_medium).mean()

    dfa["ema_short"] = dfa["close"].ewm(span=wd_short,  ignore_na=True).mean()
    dfa["ema_med"] = dfa["close"].ewm(span=wd_medium_ema,  ignore_na=True).mean()

        #... comp

    ema_comp_cond=[
        ((dfa["ema_short"].shift(1) < dfa["ema_med"].shift(1)) & (dfa["ema_short"] > dfa["ema_med"]))
        , ((dfa["ema_short"].shift(1) > dfa["ema_med"].shift(1)) & (dfa["ema_short"] < dfa["ema_med"]))
    ]

    ema_comp_choices =[1,-1]
    
    

    # ++++> stochastic Oscilator
    so_wd_k = 14
    so_wd_d = 3

    dfa["k_line"] = ((dfa["close"] - dfa["close"].rolling(window=so_wd_k).min()) * 100 / 
        (dfa["close"].rolling(window=so_wd_k).max() - dfa["close"].rolling(window=so_wd_k).min()))

    dfa["d_line"] = dfa["k_line"].rolling(window=so_wd_d).mean()

    dfa["so_20"] = 20
    dfa["so_80"] = 80

        #... comp
    so_comp_cond=[
    ((dfa["k_line"].shift(1) < 20) & (dfa["k_line"] > dfa["d_line"]))
    , ((dfa["k_line"].shift(1) > 80) & (dfa["k_line"] < 80))
    ]

    comp_choices =[1,-1]
    


    # ++++> MACD
    macd_fast = 26
    macd_slow = 12
    macd_smooth = 9

    dfa["macd_ema_fast"] = dfa["close"].ewm(span=macd_fast, adjust=False, ignore_na=True, min_periods=macd_fast).mean()
    dfa["macd_ema_slow"] = dfa["close"].ewm(span=macd_slow, adjust=False, ignore_na=True, min_periods=macd_fast).mean()
    dfa["macd_base"] = dfa["macd_ema_slow"] - dfa["macd_ema_fast"]
    dfa["macd_signal"] = dfa["macd_base"].ewm(span=macd_smooth, adjust=False, ignore_na=True).mean()
    dfa["macd_hist"] = dfa["macd_base"] - dfa["macd_signal"]
    dfa["macd_color"] = np.where(dfa["macd_hist"]<0, "red", "green")

        #... comp
    macd_comp_cond=[
        ((dfa["macd_base"].shift(1) < dfa["macd_signal"].shift(1)) & (dfa["macd_base"] > dfa["macd_signal"]))
        , ((dfa["macd_base"].shift(1) > dfa["macd_signal"].shift(1)) & (dfa["macd_base"] < dfa["macd_signal"]))
    ]

    macd_comp_choices =[1,-1]
    

    # ++++> BOHLINGER
    dfa["bohlinger_upper"] = dfa["mav_medium"] + (2*(dfa["mav_medium"].rolling(window=wd_medium).std()))
    dfa["bohlinger_lower"] = dfa["mav_medium"] - (2*(dfa["mav_medium"].rolling(window=wd_medium).std()))


    #... OVERLL comp

    dfa["ema_switch"] = np.select(ema_comp_cond, ema_comp_choices, default=0)
    dfa["so_switch"] = np.select(so_comp_cond, comp_choices, default=0)
    dfa["macd_switch"] = np.select(macd_comp_cond, macd_comp_choices, default=0)

    udf_progressBar(i+1,r_int*reqObs,reqObs, t_a1)
    
    time.sleep(1)

print("\n")
dfa.to_csv("./binance_dfa.csv", index=True)    

# ++++++++++++++++++++++ WS CLEAN UP ++++++++++++++++++++++
bsm.stop()
print("Stopping....")
time.sleep(5)

print("Stopped....")



# ++++++++++++++++++++++ FINAL DATA PROCESSING ++++++++++++++++++++++
udf_df_con(df_data)

cols = df_d10.select_dtypes(exclude=["datetime64"]).columns
df_d10[cols] = df_d10[cols].apply(pd.to_numeric, downcast="float",errors="coerce")

df_d_csv.to_csv("./binance_dfS.csv", index=True)
df_d10.to_csv("./binance_df10.csv", index=True)

print("Le Fin")