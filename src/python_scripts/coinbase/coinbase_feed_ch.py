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
from functools import partial

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
    api_secret = os.environ["coinbase_api_secret"]
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

DF_LIST = []
client = Client("localhost")
def on_message(ws, message):
    global DF_LIST
    df_ = parse_market_trades_msg(message)

    DF_LIST.append(df_)

    if len(DF_LIST)==1:
        print('Received 1st record')

    # If we've collected enough rows, insert the batch
    if len(DF_LIST) >= 100:
        logging.info(f"Dumping batch of {len(DF_LIST)}")

        df = pd.concat(DF_LIST)

        now_utc = datetime.utcnow()
        df["date"] = now_utc.date()

        epoch = datetime.utcfromtimestamp(0)
        microseconds_since_epoch = (now_utc - epoch).total_seconds() * 1_000_000
        df["insert_time"] = int(microseconds_since_epoch)

        client.execute(f"INSERT INTO coinbase_market_trades_stream VALUES", df.values.tolist())
        DF_LIST = []


def on_error(ws, error):
    print(f"Error: {error}")


def on_open(ws, product_ids):
    ws.sent_unsub = False
    subscribe_to_products(product_ids, "market_trades", ws)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def run_websocket(endpoint):
    ws = websocket.WebSocketApp(
        endpoint,
        on_open=partial(on_open, product_ids=product_ids),
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # ws.on_open = partial(on_open, method="SUBSCRIBE", subscriptions=subscriptions)
    ws.run_forever()


def parse_market_trades_msg(msg):
    message = json.loads(msg)
    if message['channel']=='market_trades':
        trades_df = pd.DataFrame(message["events"][0]["trades"])

        trades_df["channel"] = message["channel"]
        trades_df["client_id"] = message["client_id"]
        trades_df["timestamp"] = message["timestamp"]
        trades_df["sequence_num"] = message["sequence_num"]

        for col in ['time', 'timestamp']:
            trades_df[col] = pd.to_datetime(trades_df[col])
        trades_df = trades_df.apply(pd.to_numeric, errors='ignore')

        trades_df = trades_df.drop(columns='client_id')
        return trades_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push coinbase data to Clickhouse")
    parser.add_argument("--table", type=str, default="coinbase_market_trades_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://advanced-trade-ws.coinbase.com"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=5)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table

    endpoint = args.endpoint

    params_df = pd.read_csv("/Users/fedorlevin/workspace/mycrypto/coinbase_md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]
    channel = params_df["channel"].values[0]
    product_ids = params_df["product_ids"].values[0]
    product_ids_list = result = [s.strip() for s in product_ids.split(",")]

    run_websocket(endpoint=endpoint)