#!/usr/bin/env python

import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import os
import logging
import argparse
import hmac
import hashlib
import time

# Ensure the LOG directory exists
log_dir = os.path.expanduser("~/workspace/LOG")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the current date and time to format the log filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_dir}/coinbase_feed_ch_{current_time}.log"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def sign(str_to_sign, secret):
    return hmac.new(secret.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest()


def timestamp_and_sign(message, channel, products=[]):
    api_secret = os.environ["coinbase_api_secret"].strip()
    timestamp = str(int(time.time()))
    str_to_sign = f"{timestamp}{channel}{''.join(products)}"
    sig = sign(str_to_sign, api_secret)
    message.update({"signature": sig, "timestamp": timestamp})
    return message


def subscribe_to_products(products, channel_name, ws):
    message = {
        "type": "subscribe",
        "channel": channel_name,
        "api_key": os.environ["coinbase_api_key"],
        "product_ids": products,
    }
    subscribe_msg = timestamp_and_sign(message, channel_name, products)
    ws.send(json.dumps(subscribe_msg))


def on_message(ws, message):
    print(message)


def on_error(ws, error):
    print(f"Error: {error}")


def on_open(ws):
    ws.start_time = time.time()
    ws.sent_unsub = False
    products = ["BTC-USD"]
    subscribe_to_products(products, "market_trades", ws)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def run_websocket(endpoint):
    ws = websocket.WebSocketApp(
        endpoint,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # ws.on_open = partial(on_open, method="SUBSCRIBE", subscriptions=subscriptions)
    ws.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push coinbase data to Clickhouse")
    parser.add_argument("--table", type=str, default="binance_symbol_ticker_stream")

    run_websocket('wss://advanced-trade-ws.coinbase.com')