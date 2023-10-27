#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
from packages import utils
import logging
from datetime import datetime
import argparse
import pandas as pd
import json


class CryptocomWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        data_dict = json.loads(message)
        if "method" in data_dict and data_dict["method"] == "public/heartbeat":
            # Respond to the heartbeat
            response = {
                "id": data_dict["id"],
                "method": "public/respond-heartbeat",
            }
            ws.send(json.dumps(response))
            logging.info("Responded to heartbeat")

        if data_dict.get("result"):
            if data_dict.get("result").get("channel") == "trade":
                df_ = pd.DataFrame(data_dict["result"]["data"])
                self.DF_LIST.append(df_)
                return super().on_message(ws, message)
        else:
            return


def main():
    parser = argparse.ArgumentParser(description="Push cryptocom data to Clickhouse")
    parser.add_argument("--table", type=str, default="cryptocom_trade_data_stream")
    parser.add_argument(
        "--endpoint", type=str, default="wss://stream.crypto.com/exchange/v1/market"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=7)

    args = parser.parse_args()

    utils.setup_logging("cryptocom_trades")
    logging.info("Starting script")

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv(
        "/Users/fedorlevin/workspace/mycrypto/cryptocom_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]

    pair = params_df["pair"].values[0]
    pair_list = result = [s.strip() for s in pair.split(",")]

    channel = params_df["channel"].values[0]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    payload = {
        "id": 1,
        "method": "subscribe",
        "params": {"channels": [f"{channel}.{pair}"]},  # this needs to be changed
    }

    client = CryptocomWebsocketClient(
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
        ch_schema=orig_schema.keys(),
        batch_size=batch_size,
    )
    client.run()


if __name__ == "__main__":
    main()
