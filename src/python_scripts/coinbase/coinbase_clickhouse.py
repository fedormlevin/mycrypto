#!/usr/bin/env python

import os
from packages.websocket_handler import WebSocketClient
import logging
from datetime import datetime
import hmac
import hashlib
from dotenv import load_dotenv
import time
from packages import utils
import pandas as pd
import json


def sign(str_to_sign, secret):
    return hmac.new(secret.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest()


def timestamp_and_sign(message, channel, products=[]):
    api_secret = os.environ.get("coinbase_api_secret")
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
    # logging.info(message)
    if message["channel"] == "market_trades":
        trades_df = pd.DataFrame(message["events"][0]["trades"])
        if not trades_df.empty:
            trades_df["channel"] = message["channel"]
            trades_df["client_id"] = message["client_id"]
            trades_df["timestamp"] = message["timestamp"]
            trades_df["sequence_num"] = message["sequence_num"]

            for col in ["time", "timestamp"]:
                trades_df[col] = pd.to_datetime(trades_df[col])

            trades_df = trades_df.drop(columns="client_id")
        return trades_df


def main():
    load_dotenv()  # only for coinbase to get api key secret
    
    args = utils.setup_args()

    utils.setup_logging("coinbase")
    logging.info("Starting script")

    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv(
        "~/develop/mycrypto/coinbase_md_config.csv"
    )
    params_df = params_df[params_df["table_name"] == tbl]
    channel = params_df["channel"].values[0]
    product_ids = params_df["product_ids"].values[0]
    product_ids_list = result = [s.strip() for s in product_ids.split(",")]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    message = {
        "type": "subscribe",
        "channel": channel,
        "api_key": os.environ.get("coinbase_api_key"),
        "product_ids": product_ids_list,
    }
    payload = timestamp_and_sign(message, channel, product_ids_list)

    client = CoinbaseWebsocketClient(
        endpoint=endpoint,
        payload=payload,
        ch_table=tbl,
        ch_schema=orig_schema.keys(),
        batch_size=batch,
        stop_after=args.stop_after
    )
    client.run()




if __name__ == "__main__":
    main()
