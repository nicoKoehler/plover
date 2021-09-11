import threading as th
from binance.streams import ThreadedWebsocketManager as tsm
import time

apiKey = "LuGhVqrq1cT27Dj9Lod24DxnvyJUy9FL11MfMZFulk2AUBjCIYAuz5xGBHs14YW2"
apiKey_secret = "viQI5xPonsALtTUYlBLFm66jMyIFjAZSXPPQcyMOHAKhpgMOnlcuoIRn0xbTM0Z5"
vSymbol = ["BTCUSDT","ETHUSDT"]

def bin_ws(msg):
    print(f"{msg['s']} >> {msg['E']} @ {(float(msg['c'])):.2f}")

dWS = {}

for i,s in enumerate(vSymbol):
    dWS[f"bsm_{i}"] = tsm(api_key=apiKey, api_secret=apiKey_secret)
    dWS[f"bsm_{i}"].start()
    dWS[f"bsm_{i}"].start_symbol_ticker_socket(callback=bin_ws, symbol=s)


time.sleep(3)

for thread in th.enumerate():
    print(thread.name)

for i,s in enumerate(vSymbol):
    dWS[f"bsm_{i}"].stop()
time.sleep(2)

for thread in th.enumerate():
    print(thread.name)