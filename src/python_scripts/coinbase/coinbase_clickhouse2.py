#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
import hmac
import hashlib
from dotenv import load_dotenv
import time
from packages import utils
import pandas as pd
import json
from queue import Queue


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
        message_dict = parse_market_trades_msg(message)
        return super().on_message(ws, message_dict)


def parse_market_trades_msg(msg):
    message = json.loads(msg)
    # print(message)
    # logging.info(message)
    if message["channel"] == "market_trades":
        # print('message', message)
        trades_dict = message["events"][0]["trades"]
        if trades_dict:
            trades_df = pd.DataFrame(trades_dict)
            # print('trades dict', trades_dict)
            trades_df["channel"] = message["channel"]
            trades_df["timestamp"] = message["timestamp"]
            trades_df["sequence_num"] = message["sequence_num"]
            

            for col in ["time", "timestamp"]:
                trades_df[col] = pd.to_datetime(trades_df[col])
                
        list_of_dicts = trades_df.to_dict(orient='records')
        return list_of_dicts


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

    preprocessing_queue = Queue()
    db_queue = Queue()
    
    client = CoinbaseWebsocketClient(
        queue=preprocessing_queue,
        endpoint=endpoint,
        payload=payload,
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
