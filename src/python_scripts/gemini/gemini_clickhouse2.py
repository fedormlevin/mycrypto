#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
from packages import utils
import pandas as pd
import json
from queue import Queue


class GeminiWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        dict_ = parse_market_trades_msg(message)
        if dict_:
            return super().on_message(ws, dict_)

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
        f"{endpoint}?symbols={symbols}&heartbeat={heartbeat}"
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

            return trades_df.to_dict(orient='records')
    else:
        return


def main():
    # parser = argparse.ArgumentParser(description="Push gemini data to Clickhouse")
    # parser.add_argument("--table", type=str, default="gemini_multimarketdata_trades")
    # parser.add_argument(
    #     "--endpoint", type=str, default="wss://api.gemini.com/v1/multimarketdata"
    # )
    # parser.add_argument("-b", "--batch-size", type=int, default=3)

    # args = parser.parse_args()
    args = utils.setup_args()

    utils.setup_logging("gemini")
    logging.info("Starting script")

    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv("~/develop/mycrypto/gemini_md_config.csv")
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

    preprocessing_queue = Queue()
    db_queue = Queue()
    
    client = GeminiWebsocketClient(
        queue=preprocessing_queue,
        endpoint=endpoint,
        payload='',
    )
    
    utils.run_market_data_processor(
        client=client,
        preprocessing_queue=preprocessing_queue,
        db_queue=db_queue,
        batch_size=batch,
        tbl=tbl,
        orig_schema=orig_schema.keys(),
        stop_after=args.stop_after
    )


if __name__ == "__main__":
    main()
