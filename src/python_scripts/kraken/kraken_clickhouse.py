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
log_filename = f"{log_dir}/kraken_feed_ch_{current_time}.log"

logging.basicConfig(
    # filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

class KrakenWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        data = json.loads(message)

        if isinstance(data, dict):
            if data.get('status')=='online':
                logging.info('Status online')
                return
        if isinstance(data, list):
            df_ = parse_market_trades_msg(data)
            self.DF_LIST.append(df_)

        return super().on_message(ws, message)
    
def parse_market_trades_msg(lst):
  df = pd.DataFrame(lst[1], columns=['price', 'volume', 'time', 'side', 'orderType', 'misc'])
  df['channelName'] = lst[2]
  df['pair'] = lst[3]
  df = df.drop(columns='misc')
  return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push binance data to Clickhouse")
    parser.add_argument("--table", type=str, default="kraken_trade_data_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://ws.kraken.com"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=2)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv(
        "/Users/fedorlevin/workspace/mycrypto/kraken_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]

    pair = params_df["pair"].values[0]
    pair_list = result = [s.strip() for s in pair.split(",")]

    subscription = params_df["subscription"].values[0]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    payload = {
        "event": "subscribe",
        "pair": pair_list,
        "subscription": {
            "name": subscription
        }
    }

    client = KrakenWebsocketClient(
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
        ch_schema=orig_schema.keys(),
        batch_size=batch_size,
    )
    client.run()