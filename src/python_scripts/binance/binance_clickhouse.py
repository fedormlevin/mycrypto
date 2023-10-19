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
log_filename = f"{log_dir}/binance_feed_ch_{current_time}.log"

logging.basicConfig(
    # filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class BinanceWebsocketClient(WebSocketClient):
    def __init__(self, endpoint, payload, ch_table, batch_size, orig_schema):
        super().__init__(endpoint, payload, ch_table, batch_size)
        self.orig_schema = orig_schema
        
        

    def on_message(self, ws, message):
        data_list = json.loads(message)

        if "result" in data_list and "id" in data_list:
            if data_list["id"] == 1 and data_list["result"] is None:
                logging.info("Successfully subscribed to Binance!")
                return
      
        if isinstance(data_list, dict):
            data_list = [data_list]
    
        df_ = pd.DataFrame(data_list)
    
        df_ = df_[self.orig_schema]
  
        self.DF_LIST.append(df_)

        return super().on_message(ws, message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push binance data to Clickhouse")
    parser.add_argument("--table", type=str, default="binance_symbol_ticker_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://stream.binance.us:9443/ws"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=5)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv(
        "/Users/fedorlevin/workspace/mycrypto/binance_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]

    sub_id = params_df["subscription_id"].values[0]
    sub_id_list = result = [s.strip() for s in sub_id.split(",")]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    payload = {
        "method": "SUBSCRIBE",
        "params": sub_id_list,
        "id": 1,
    }

    client = BinanceWebsocketClient(
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
        batch_size=batch_size,
        orig_schema=orig_schema.keys(),
    )
    client.run()