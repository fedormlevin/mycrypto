#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
import logging
from datetime import datetime
import time
import ssl
import argparse
import pandas as pd
import websocket
import json

# Ensure the LOG directory exists
log_dir = os.path.expanduser("~/workspace/LOG")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the current date and time to format the log filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_dir}/gemini_feed_ch_{current_time}.log"

logging.basicConfig(
    # filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class GeminiWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        df_ = parse_market_trades_msg(message)
        self.DF_LIST.append(df_)
        return super().on_message(ws, message)
    
    def run(self):
        ws = websocket.WebSocketApp(
            self.endpoint,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        ws.run_forever()

def parse_market_trades_msg(msg):
    message = json.loads(msg)
    if message.get('events'):
      if message.get('events')[0].get('type')=='trade':
          trades_df = pd.DataFrame(message.get('events'))

          trades_df["eventId"] = message["eventId"]
          trades_df["socket_sequence"] = message["socket_sequence"]
          trades_df["timestamp"] = message["timestamp"]
          trades_df["timestampms"] = message["timestampms"]
          trades_df["type"] = message["type"]

          return trades_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push gemini data to Clickhouse")
    parser.add_argument("--table", type=str, default="gemini_market_trades_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://api.gemini.com/v1/multimarketdata/"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=10)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv(
        "/Users/fedorlevin/workspace/mycrypto/gemini_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]

    symbols = params_df['symbols'].values[0]
    heartbeat = params_df['heartbeat'].values[0]
    top_of_book = params_df['top_of_book'].values[0]
    bids = params_df['bids'].values[0]
    offers = params_df['offers'].values[0]
    trades = params_df['trades'].values[0]