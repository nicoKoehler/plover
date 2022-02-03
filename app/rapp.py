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
import secrets
from tabulate import tabulate as tb
import random 

tPerf_start = time.perf_counter()

# ++++++++++++++++++++++ SETUP ++++++++++++++++++++++
tz_local = pytz.timezone("Europe/Zurich")

apiKey = "API KEY"
apiKey_secret = "API KEY"

client = Client(apiKey, apiKey_secret)

cr_prices = {"error":False}


flag_lookback = 0
wd_short = 5
wd_medium = 20
wd_long = 99
wd_medium_ema = 13
iLookBack = max([wd_short, wd_medium, wd_long, wd_medium_ema])    # max period lookback
dMaster = {}
lSymbols = [
    "BTCUSDT",
    "ETHUSDT",
    "BTCEUR",
    "ETHEUR",
    "LTCEUR",
    "DOGEEUR"
    ]
lMethods = [
    #"so",
    "rand"
    #, "ema"
    ]

if sys.argv:
    tRun_min = int(sys.argv[2])
else:
    tRun_min = 2

#resample intervall
if sys.argv:
    r_int = int(sys.argv[1])
else:
    r_int = 2

print(f"Starting for RESAMPLE INTERVAL >>> {r_int}...for {tRun_min} Minutes, with Lookback of {iLookBack} periods")


# ++++++++++++++++++++++ HELPER FUNCTION ++++++++++++++++++++++

def udf_df_con(df, dtCol="timestamp"):
    df[dtCol] = pd.to_datetime(df[dtCol]/1000, unit="s")
    df[dtCol]  = df[dtCol].dt.floor("S")

    df.reset_index(inplace=True, drop=True)
    df.set_index(dtCol, inplace=True)
    df.index = df.index.tz_localize("UTC").tz_convert("CET")

    cols = df.select_dtypes(exclude=["datetime64"]).columns
    df[cols] = df[cols].apply(pd.to_numeric, downcast="float",errors="coerce")


def udf_printDict(d, header=""):

    # iterate over dict and convert any instance of DF to json, otherwise ERROR
    dh = {}
    df=[]
    for k,v in d.items():
        if isinstance(v, pd.DataFrame):
            df.append(v)
            continue
        else:
            dh[k] = v
    print(f"\n ++++++++++++++++++ {header} ++++++++++++++++++")
    print(json.dumps(dh, indent=2))
    for x in df:
        print(tb(x, headers="keys", tablefmt="psql"))




def udf_trade(ttype, price, wallet, vSymbol="---", vMethod="---", vSellOption=""):
    global dMaster
    dTradeTime = dt.datetime.strftime(pd.to_datetime(time.time(), unit="s").tz_localize("UTC").tz_convert("CET"), "%y%m%d-%H%M%S")
    
    if ttype == 1:
        if dMaster[vSymbol]["funds"]== 0:
            print("ERROR: Not sufficient funds")
            return 0
        
        if wallet["openTrade"] == 0 and ttype == 1:
            wallet["trade_id"] = str(secrets.token_hex(4))

            while wallet["trade_id"] in dMaster['df_book'].index:
                wallet["trade_id"] = str(secrets.token_hex(4))

        else: 
            return print("invalid trade combination")

        print(f"Trade ID found, starting Trade: {str(wallet['trade_id'])}")

        
        # assumes investment of full funds
        print(f"----------> Funds: {dMaster[vSymbol]['funds']}, price: {price}")
        dMaster['df_book'].loc[str(wallet["trade_id"]), ["symbol","ind","trade", "buyPrice","qty", "buyTime", "status", "buy_dt"]] = [vSymbol,vMethod,"long",price, (dMaster[vSymbol]["funds"]/price), time.time(), "bought", dTradeTime]
        print("Purchase Complete")
        #print(f"===========> New Book: \n  {dMaster['df_book']}")
        dMaster[vSymbol]["funds"] = 0
        wallet['openTrade'] = 1
        return wallet

    if ttype < 0 and wallet["openTrade"] == 1:
        tradeid = wallet['trade_id']

        print(f"SELL: {tradeid}")
        fProfit = dMaster['df_book'].loc[str(tradeid)]["qty"] *  (price - dMaster['df_book'].loc[str(tradeid)]["buyPrice"])
        fSalesDuration_s = time.time() - dMaster['df_book'].loc[str(tradeid)]["buyTime"]
        fProfit_per_sec = fProfit / fSalesDuration_s
        dMaster['df_book'].loc[str(tradeid), ["sellPrice", "sellTime", "profit", "profit_ps","status", "sell_dt"]] = [price, time.time(), fProfit, round(fProfit_per_sec,4), f"closed-{str(ttype)}{vSellOption}", dTradeTime]


        print("Sell Completed")
        print("---------------------------------------")
        print(f"Bought { dMaster['df_book'].loc[str(tradeid)]['qty'] } \t @ { dMaster['df_book'].loc[str(tradeid)]['buyPrice']} USD")
        print(f"Sold { dMaster['df_book'].loc[str(tradeid)]['qty'] } \t @ { price} USD")
        print(f"==> Total Profit: {fProfit} in {fSalesDuration_s} seconds ||==> {(fProfit_per_sec):.3f} USD/sec")
        print("---------------------------------------\n")

        wallet["openTrade"] = 0
        dMaster[vSymbol]["funds"] = min((gFunds/len(lSymbols)), (dMaster['df_book'].loc[str(tradeid)]["qty"] * dMaster['df_book'].loc[str(tradeid)]["sellPrice"]))
        wallet["trade_id"] = ""
        print(f"New Funds for [{vSymbol}]: {dMaster[vSymbol]['funds']}\n")
        return wallet



def bin_ws(msg):

    global dMaster
    symbol = msg["s"]
    lstTimeStamp = dt.datetime.fromtimestamp(msg["E"]/1000)
    
    # ensure that the first entry is a multiple of the interval, otherwise resampling becomes issue
    if dMaster[symbol]["flag_start"] == 0 and lstTimeStamp.second % r_int != 0:
        #print(f"Not time yet: {lstTimeStamp} > {lstTimeStamp.second}")
        return 0
    elif dMaster[symbol]["flag_start"] == 0 and lstTimeStamp.second % r_int == 0:
        #print("Its time...")
        dMaster[symbol]["flag_start"] = 1


    l_ws = [msg["E"],msg["o"], msg["h"] , msg["l"], msg["c"], msg["b"], msg["a"]]
    dMaster[symbol]["lastPrice"] = float(msg["c"])
    dMaster[symbol]["dataframes"]["df_data"].loc[len(dMaster[symbol]["dataframes"]["df_data"])] = l_ws
    dMaster[symbol]["dataframes"]["df_data_record"].loc[len(dMaster[symbol]["dataframes"]["df_data_record"])] = l_ws

    # once enough data in DF, copy, delete and analyze
    if len(dMaster[symbol]["dataframes"]["df_data"]) == r_int:
        df_d_copy = dMaster[symbol]["dataframes"]["df_data"].copy() # reset immediately to allow WS to continue writing in background
        dMaster[symbol]["dataframes"]["df_data"] = dMaster[symbol]["dataframes"]["df_data"].iloc[0:0]

        udf_df_con(df_d_copy)
        dMaster[symbol]["dataframes"]["df_d_csv"] = pd.concat([dMaster[symbol]["dataframes"]["df_d_csv"],df_d_copy])

        df_rs = df_d_copy.resample(f"{r_int}S", closed="left").mean()

        dMaster[symbol]["dataframes"]["df_d10"] = pd.concat([dMaster[symbol]["dataframes"]["df_d10"], df_rs])
        df_d_copy = df_d_copy.iloc[0:0]
        df_rs = df_rs.iloc[0:0]


def udf_tradeMASTER(vSymbol, vMethod):
    """
    Purpose: Master trading function. Split out to be accessed by different threads
    Param: 
    - vOpenTrade    => Open trade flag indicating active trade for method
    - vSymbol       => Stock symbol to run trade for
    - vMethod       => method of comparison, [so, ema, macd, ...] 
    """

    # PARAMETERS
    global dMaster
    t0 = time.time()
    

    # DICTIONARIES
    dWallet = {
        "trade_id": ""
        , "openTrade": 0
        , "trade_streak": 0
        #, "funds": 100
        #, "book": pd.DataFrame(columns= ["tradeScope","tradeType","buyPrice","qty","buyTime","sellPrice", "sellTime", "profit", "profit_per_second", "status", "buyTime_dt", "sellTime_dt"])

    }

    # wait until sufficient data for rolling analysis 
    while len(dMaster[vSymbol]["dataframes"]["df_d10"]) < iLookBack:
        os.system("clear")
        print("*"*20, f" Current Data Lenght: {len(dMaster[vSymbol]['dataframes']['df_d10'])} ", "*"*20)
        time.sleep(1)
    print(f"{vSymbol}>{vMethod}: Suffcient Data...{len(dMaster[vSymbol]['dataframes']['df_d10'])}")
    t_a1 = time.time()


    # ++++++++++++++++++++++ ANALYSIS BLOCK ++++++++++++++++++++++


    ttrade_start = time.time()

    while (time.time() - ttrade_start) < (tRun_min*60) or dWallet['openTrade'] == 1:

        t_a2 = time.time()
        
        dfa = dMaster[vSymbol]['dataframes']['df_d10'][["close"]].copy()  # analysis DF
        dfa["tsv_so_k100"] = 0

        # ++++> EMA
        
        dfa["mav_short"] = dfa["close"].rolling(window=wd_short).mean()
        dfa["mav_medium"] = dfa["close"].rolling(window=wd_medium).mean()

        dfa["ema_short"] = dfa["close"].ewm(span=wd_short,  ignore_na=True).mean()
        dfa["ema_med"] = dfa["close"].ewm(span=wd_medium_ema,  ignore_na=True).mean()
        dfa["ema_long"] = dfa["close"].ewm(span=wd_long,  ignore_na=True).mean()

            #... comp

        ema_comp_cond_1=[
            ((dfa["ema_short"].shift(1) < dfa["ema_med"].shift(1)) & (dfa["ema_short"] > dfa["ema_med"]))
            , ((dfa["ema_short"].shift(1) > dfa["ema_med"].shift(1)) & (dfa["ema_short"] < dfa["ema_med"]))
        ]


        ema_comp_choices =[1,-1]

        #... OVERLL comp
        dfa["ema_switch"] = np.select(ema_comp_cond_1, ema_comp_choices, default=0)



        # ++++++++++++++++++++++ TRADING ++++++++++++++++++++++


        # ++++> STOP LIMIT
    
        fStopPerc_low = 0.1     #threshold percentage loss >> =1 means value can fall to 99% of previous value
        fStopPerc_high = 1.2     #threshold percentage loss >> =1 means value can fall to 99% of previous value
        if dWallet["trade_id"] != "":
            fLastPrice = dMaster["df_book"].loc[dWallet["trade_id"]]["buyPrice"]
            dWallet["trade_streak"] = dWallet["trade_streak"] + 1
        else:
            fLastPrice = 0

        # ++++> RAND TRADE
        fRand = random.uniform(0,1)
        if dfa["ema_long"].tail(1).item() > dfa["ema_med"].tail(1).item() and dWallet["openTrade"] == 0 and fRand > 0.5: 
            os.system("clear")
            print(".......")
            
            print("\nRAND **BUY** signal found")

            dWallet = udf_trade(1, dMaster[vSymbol]["lastPrice"], dWallet, vSymbol=vSymbol, vMethod=vMethod)
            print(tb(dMaster["df_book"], headers="keys", tablefmt="psql"))
            print("\n")
            print("*"*150)
            print("*"*150)
            #time.sleep(1)
            continue

        elif dfa["ema_long"].tail(1).item() > dfa["ema_med"].tail(1).item() and dWallet["openTrade"] == 1 and fRand > (1/(dWallet ["trade_streak"]+1)): 
            os.system("clear")
            print(".......")
            print("\nRAND **SELL** signal found")
            print(f"\n RAND SELL. RAND: {fRand}, RAND-THRESHOLD: {(1/(dWallet ['trade_streak']+1))} || Buy:{fLastPrice}, Now: {dMaster[vSymbol]['lastPrice']}")
            dWallet = udf_trade(-1, dMaster[vSymbol]["lastPrice"], dWallet, vSymbol=vSymbol, vMethod=vMethod, vSellOption="-n")
            print(tb(dMaster["df_book"], headers="keys", tablefmt="psql"))
            
            dWallet["trade_streak"] = 0
            print("\n")
            print("*"*150)
            print("*"*150)
            #time.sleep(1)
            continue

        else:
            if dMaster[vSymbol]["lastPrice"] >= (fLastPrice)*(fStopPerc_high) and dWallet["openTrade"] == 1:
                os.system("clear")
                print(".......")
                print(f"\n PRICE **ABOVE** STOP THRESHOLD. Buy:{fLastPrice}, Now: {dMaster[vSymbol]['lastPrice']}")
                dWallet = udf_trade(-1, dMaster[vSymbol]["lastPrice"], dWallet, vSymbol=vSymbol, vMethod=vMethod, vSellOption="-F")
                print(tb(dMaster["df_book"], headers="keys", tablefmt="psql"))
                
                print("\n")
                print("*"*150)
                print("*"*150)
                dWallet["trade_streak"] = 0
                #time.sleep(1)
                continue

            if dMaster[vSymbol]["lastPrice"] <= (fLastPrice)*(1-(fStopPerc_low/100)) and dWallet["openTrade"] == 1:
                os.system("clear")
                print(".......")
                print(f"\n PRICE **BELOW** STOP THRESHOLD. Buy:{fLastPrice}, Now: {dMaster[vSymbol]['lastPrice']}")
                dWallet = udf_trade(-1, dMaster[vSymbol]["lastPrice"], dWallet, vSymbol=vSymbol, vMethod=vMethod, vSellOption="-F")
                print(tb(dMaster["df_book"], headers="keys", tablefmt="psql"))
                
                print("\n")
                print("*"*150)
                print("*"*150)
                dWallet["trade_streak"] = 0
                #time.sleep(1)
                continue

        time.sleep(1)


    # print(f"hanging out here for: {vSymbol}")
    # for thread in th.enumerate(): 
    #     print(thread.name)
    # print("--"*50)

    dMaster[vSymbol][vMethod]['dataframes']['dfa'] = dfa


dWebSockets = {}
mprocesses = []
dMaster["df_book"] = pd.DataFrame(columns= ["symbol","ind","trade","buyPrice","qty","buyTime","sellPrice", "sellTime", "profit", "profit_ps", "status", "buy_dt", "sell_dt"])

gFunds = 300

for s in lSymbols:
    dMaster[s] = {}
    dMaster[s]["funds"] = gFunds / len(lSymbols)
    dMaster[s]["dataframes"] = {}
    dMaster[s]["flag_start"] = 0
    dMaster[s]["dataframes"]["df_d10"] = pd.DataFrame(columns=["open","high","low","close","bid", "ask"], index=pd.to_datetime([]))
    dMaster[s]["dataframes"]["df_data"] = pd.DataFrame(columns=["timestamp","open","high","low","close","bid", "ask"])
    dMaster[s]["dataframes"]["df_data_record"] = pd.DataFrame(columns=["timestamp","open","high","low","close","bid", "ask"])
    dMaster[s]["dataframes"]["df_d_csv"] = dMaster[s]["dataframes"]["df_d10"]   # csv write df
    

    # bin_ws
    # ++++++++++++++++++++++ WS ++++++++++++++++++++++
    # adding objects to global dictionary
    dMaster[s]["websocket"] = tsm(api_key=apiKey, api_secret=apiKey_secret)
    dMaster[s]["websocket"].start()
    
    #print(f"Starting....{dt.datetime.now()}")
    t0 = time.time()

    dMaster[s]["websocket"].start_symbol_ticker_socket(callback=bin_ws, symbol=s)
    #print(f"WS Start Time: {(time.time()-t0):.3f}")

    
    for m in lMethods:
        print(f"Created Websockets, starting MAIN for {s}>{m}")
        dMaster[s][m] = {}
        dMaster[s][m]["dataframes"] = {}
        mproc = th.Thread(target=udf_tradeMASTER, args=[s,m])
        mproc.name = f"THREADFOR_{s}_{m}"
        mproc.start()
        mprocesses.append(mproc)
        #udf_tradeMASTER(vSymbol=s, vMethod=m)

for p in mprocesses:
    p.join()

# ++++++++++++++++++++++ Waiting for Threads to finish ++++++++++++++++++++++
print("waiting for jobs to finish....")
for s in lSymbols:
    for m in lMethods:
        

        df_d10_cp = dMaster[s]["dataframes"]["df_d10"].copy()
        df_analysis_cp = dMaster[s][m]["dataframes"]["dfa"].copy()
        
        df_data_rec = dMaster[s]["dataframes"]["df_data_record"].copy()

        #cols = df_d10_cp.select_dtypes(exclude=["datetime64"]).columns
        #df_d10_cp[cols] = df_d10_cp[cols].apply(pd.to_numeric, downcast="float",errors="coerce")
        udf_df_con(df_data_rec)
        df_d10_cp.to_csv(f"./results/df_interval_{s}_{m}.csv", index=True)
        
        df_analysis_cp.to_csv(f"./results/df_analysis_{s}_{m}.csv", index=True)
        df_data_rec.to_csv(f"./results/df_record_{s}_{m}.csv", index=True)

    print(f"Stopping....{s}")
    dMaster[s]["websocket"].stop()

df_book_cp = dMaster["df_book"].copy()
df_book_cp.to_csv(f"./df_books.csv", index=True)
# ++++++++++++++++++++++ WS CLEAN UP ++++++++++++++++++++++

time.sleep(5)

print("Stopped....")
print("\n")
print("_"*150)
print("*"*150)
print("*"*150)
print("-"*150)
print("\n")
print("+"*70+" RESULTS "+"+"*70)
print("\n")
print(tb(dMaster["df_book"], headers="keys", tablefmt="psql"))
print("\n \n")
print("Profit By Currency")
print("-"*20)

print(tb(pd.DataFrame(dMaster["df_book"].groupby(["symbol"])["profit"].sum()), headers="keys", tablefmt="psql"))
print("\n\n")
print("Profit By Method")
print("-"*20)
print(tb(pd.DataFrame(dMaster["df_book"].groupby(["ind"])["profit"].sum()), headers="keys", tablefmt="psql"))
print("\n\n")
print("Profit By Currency and Method")
print("-"*20)
print(tb(pd.DataFrame(dMaster["df_book"].groupby(["symbol","ind"])["profit"].sum()), headers="keys", tablefmt="psql"))

# ++++++++++++++++++++++ FINAL DATA PROCESSING ++++++++++++++++++++++


print(f"\n\n ...Le Fin. Grand Total Time: >> {((time.perf_counter() - tPerf_start)/60):.2f} Minutes")