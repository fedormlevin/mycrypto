#!/usr/bin/env python

import websocket
import json
import asyncio
import sys
import pandas as pd

DF_LIST = []


# This function will handle incoming messages from the WebSocket.
def on_message(ws, message):
    n_records = 200

    data = json.loads(message)
    data_df = pd.DataFrame(data)
    DF_LIST.append(data_df)
    print(f'N records: {len(DF_LIST)}')

    if len(DF_LIST) > n_records:
        df = pd.concat(DF_LIST)
        df.to_csv(
            "/Users/fedorlevin/develop/data/binance/binance_traiding_pairs.csv",
            index=False,
        )
        print(f'loaded {n_records} records')
        sys.exit(0)


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    # Subscribe to the trade stream for the BTCUSDT pair.
    payload = {
        "method": "SUBSCRIBE",
        "params": [
            # "btcusdt@trade"
            "!ticker@arr"
        ],
        "id": 1,
    }
    ws.send(json.dumps(payload))


def run_websocket():
    # The Binance WebSocket endpoint for the streams
    endpoint = "wss://stream.binance.us:9443/ws"

    ws = websocket.WebSocketApp(
        endpoint, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()


def main():
    run_websocket()


if __name__ == "__main__":
    main()
