#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
import pandas as pd
import json
from packages import utils
from queue import Queue


class KrakenWebsocketClient(WebSocketClient):
    def __init__(self, queue, endpoint, payload, ch_table):
        super().__init__(queue, endpoint, payload)
        self.ch_table = ch_table
    def on_message(self, ws, message):
        data = json.loads(message)

        if isinstance(data, dict):
            if data.get("status") == "online":
                logging.info("Status online")
                return
        if isinstance(data, list):
            df_ = parse_market_trades_msg(data, self.ch_table)

            return super().on_message(ws, df_)


def parse_market_trades_msg(lst, tbl):
    if tbl == "kraken_trade_data_stream":
        df = pd.DataFrame(
            lst[1], columns=["price", "volume", "time", "side", "orderType", "misc"]
        )
        # check if lst[1] is list or list of list
        df = df.drop(columns="misc")
    elif tbl == "kraken_ohlc_stream":
        df = pd.DataFrame(
            [lst[1]],
            columns=[
                "time",
                "etime",
                "open",
                "high",
                "low",
                "close",
                "vwap",
                "volume",
                "count",
            ],
        )

    df["channelName"] = lst[2]
    df["pair"] = lst[3]

    list_of_dicts = df.to_dict(orient='records')

    return list_of_dicts


def main():

    args = utils.setup_args()

    utils.setup_logging("kraken")
    logging.info("Starting script")

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv("~/develop/mycrypto/kraken_md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]

    pair = params_df["pair"].values[0]
    pair_list = [s.strip() for s in pair.split(",")]

    subscription = params_df["subscription"].values[0]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    payload = {
        "event": "subscribe",
        "pair": pair_list,
        "subscription": {"name": subscription},
    }

    preprocessing_queue = Queue()
    db_queue = Queue()
    
    client = KrakenWebsocketClient(
        queue=preprocessing_queue,
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
    )
    
    utils.run_market_data_processor(
        client=client,
        preprocessing_queue=preprocessing_queue,
        db_queue=db_queue,
        batch_size=batch_size,
        tbl=tbl,
        orig_schema=orig_schema.keys(),
        stop_after=args.stop_after
    )


if __name__ == "__main__":
    main()
