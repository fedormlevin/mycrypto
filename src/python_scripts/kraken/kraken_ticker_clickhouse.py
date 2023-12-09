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
from packages import utils
import json


class KrakenWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        data = json.loads(message)

        if isinstance(data, dict):
            if data.get("status") == "online":
                logging.info("Status online")
                return
        if isinstance(data, list):
            df_ = parse_market_trades_msg(data)
            self.DF_LIST.append(df_)

        return super().on_message(ws, message)


def parse_market_trades_msg(lst):
    cols_dict = {
        "a_0": "best_ask_price",
        "a_1": "ask_wholelot_vol",
        "a_2": "ask_lot_vol",
        "b_0": "best_bid_price",
        "b_1": "bid_wholelot_vol",
        "b_2": "bid_lot_vol",
        "c_0": "close_price",
        "c_1": "close_lot_vol",
        "v_0": "vol_today",
        "v_1": "vol_24hrs",
        "p_0": "vol_wavg_price_today",
        "p_1": "vol_wavg_price_24hrs",
        "t_0": "n_trades_today",
        "t_1": "n_trades_24hrs",
        "l_0": "low_price_today",
        "l_1": "low_price_24hrs",
        "h_0": "high_price_today",
        "h_1": "high_price_24hrs",
        "o_0": "open_price_today",
        "o_1": "open_price_24hrs",
    }
    data_dict = lst[1]
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame.from_dict(data_dict, orient="index").T
    # Flatten and rename the DataFrame
    df_flat = df.stack().to_frame().T
    df_flat.columns = [f"{col[1]}_{col[0]}" for col in df_flat.columns]
    df_flat = df_flat.rename(columns=cols_dict)
    df_flat["ticker"] = lst[3]
    return df_flat


def main():
    # parser = argparse.ArgumentParser(description="Push kraken data to Clickhouse")
    # parser.add_argument("--table", type=str, default="kraken_ticker_stream")
    # parser.add_argument("--endpoint", type=str, default="wss://ws.kraken.com")
    # parser.add_argument("-b", "--batch-size", type=int, default=2)

    # args = parser.parse_args()
    args = utils.setup_args()

    utils.setup_logging("kraken_trades")

    logging.info("Starting script")

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv("/Users/fedorlevin/develop/mycrypto/kraken_md_config.csv")
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

    client = KrakenWebsocketClient(
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
        ch_schema=orig_schema.keys(),
        batch_size=batch_size,
        stop_after=args.stop_after
    )
    client.run()


if __name__ == "__main__":
    main()
