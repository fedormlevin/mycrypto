#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
import logging
from datetime import datetime
import time
from packages import utils
import ssl
import argparse
import pandas as pd
import websocket
import json


class GeminiWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        df_ = parse_market_trades_msg(message)
        if not df_.empty:
            self.DF_LIST.append(df_)
        return super().on_message(ws, message)

    def on_open(self, ws):  # don't do anything
        return


def build_endpoint(
    endpoint,
    symbols,
    heartbeat="true",
    top_of_book="false",
    bids="false",
    offers="false",
    trades="true",
):
    base_url = (
        f"{endpoint}?symbols={symbols}?heartbeat={heartbeat}"
        f"&top_of_book={top_of_book}&bids={bids}&offers={offers}&trades={trades}"
    )

    return base_url


def parse_market_trades_msg(msg):
    message = json.loads(msg)
    if message.get("events"):
        if message.get("events")[0].get("type") == "trade":
            trades_df = pd.DataFrame(message.get("events"))

            trades_df["eventId"] = message["eventId"]
            trades_df["socket_sequence"] = message["socket_sequence"]
            trades_df["timestamp"] = message["timestamp"]
            trades_df["timestampms"] = message["timestampms"]

            return trades_df
    else:
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="Push gemini data to Clickhouse")
    parser.add_argument("--table", type=str, default="gemini_multimarketdata_trades")
    parser.add_argument(
        "--endpoint", type=str, default="wss://api.gemini.com/v1/multimarketdata"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=3)

    args = parser.parse_args()

    utils.setup_logging("gemini")
    logging.info("Starting script")

    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv("/Users/fedorlevin/workspace/mycrypto/gemini_md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]

    symbols = params_df["symbols"].values[0]
    # heartbeat = params_df['heartbeat'].values[0]
    # top_of_book = params_df['top_of_book'].values[0]
    # bids = params_df['bids'].values[0]
    # offers = params_df['offers'].values[0]
    # trades = params_df['trades'].values[0]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    endpoint = build_endpoint(endpoint, symbols)

    client = GeminiWebsocketClient(
        endpoint=endpoint,
        payload="",
        ch_table=tbl,
        ch_schema=orig_schema.keys(),
        batch_size=batch,
    )
    client.run()


if __name__ == "__main__":
    main()
