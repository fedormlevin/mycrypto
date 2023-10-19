#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
import logging
from datetime import datetime
import hmac
import hashlib
import time
import argparse
import pandas as pd
import json

# Ensure the LOG directory exists
log_dir = os.path.expanduser("~/workspace/LOG")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the current date and time to format the log filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_dir}/coinbase_feed_ch_{current_time}.log"

logging.basicConfig(
    # filename=log_filename,
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


class CoinbaseWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        df_ = parse_market_trades_msg(message)
        self.DF_LIST.append(df_)
        return super().on_message(ws, message)


def parse_market_trades_msg(msg):
    message = json.loads(msg)
    if message["channel"] == "market_trades":
        trades_df = pd.DataFrame(message["events"][0]["trades"])

        trades_df["channel"] = message["channel"]
        trades_df["client_id"] = message["client_id"]
        trades_df["timestamp"] = message["timestamp"]
        trades_df["sequence_num"] = message["sequence_num"]

        for col in ["time", "timestamp"]:
            trades_df[col] = pd.to_datetime(trades_df[col])

        trades_df = trades_df.drop(columns="client_id")
        return trades_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push coinbase data to Clickhouse")
    parser.add_argument("--table", type=str, default="coinbase_market_trades_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://advanced-trade-ws.coinbase.com"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=10)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv(
        "/Users/fedorlevin/workspace/mycrypto/coinbase_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]
    channel = params_df["channel"].values[0]
    product_ids = params_df["product_ids"].values[0]
    product_ids_list = result = [s.strip() for s in product_ids.split(",")]

    message = {
        "type": "subscribe",
        "channel": channel,
        "api_key": os.environ["coinbase_api_key"],
        "product_ids": product_ids_list,
    }
    payload = timestamp_and_sign(message, channel, product_ids_list)

    client = CoinbaseWebsocketClient(
        endpoint=endpoint, payload=payload, ch_table=tbl, batch_size=batch
    )
    client.run()
