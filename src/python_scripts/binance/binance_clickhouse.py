#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
import logging
from datetime import datetime
import hmac
import hashlib
import time
import argparse
from packages import utils
import pandas as pd
import json


class BinanceWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        data_list = json.loads(message)

        if "result" in data_list and "id" in data_list:
            if data_list["id"] == 1 and data_list["result"] is None:
                logging.info("Successfully subscribed to Binance!")
                return

        if isinstance(data_list, dict):
            data_list = [data_list]

        df_ = pd.DataFrame(data_list)
        self.DF_LIST.append(df_)
        return super().on_message(ws, message)


def main():
    # python binance_clickhouse.py --table binance_ticker_order_book_stream --endpoint wss://stream.binance.us:9443/ws -b 2 --log-name binance
    args = utils.setup_args()

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = utils.load_params_df(
        "/Users/fedorlevin/workspace/mycrypto/binance_md_config.csv", tbl
    )

    sub_id = params_df["subscription_id"].values[0]
    sub_id_list = [s.strip() for s in sub_id.split(",")]

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
        ch_schema=orig_schema.keys(),
        batch_size=batch_size,
    )
    client.run()


if __name__ == "__main__":
    main()
