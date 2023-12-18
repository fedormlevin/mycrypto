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
from packages.market_data_handler import MDProcessor


def sign(str_to_sign, secret):
    return hmac.new(secret.encode(), str_to_sign.encode(), hashlib.sha256).hexdigest()


def timestamp_and_sign(message, channel, products=[]):
    api_secret = os.environ.get("coinbase_api_secret")
    timestamp = str(int(time.time()))
    str_to_sign = f"{timestamp}{channel}{''.join(products)}"
    sig = sign(str_to_sign, api_secret)
    message.update({"signature": sig, "timestamp": timestamp})
    return message


class CoinbaseWebsocketClientTopOfBook(WebSocketClient):
    def on_message(self, ws, message):
        message_dict = parse_top_of_book_msg(message, self.first_message)
        return super().on_message(ws, message_dict)


class CoinbaseWebsocketClientTrades(WebSocketClient):
    def on_message(self, ws, message):
        message_dict = parse_market_trades_msg(message)
        return super().on_message(ws, message_dict)


def get_top_of_book(df):
    df['price_level'] = df['price_level'].astype(float)
    smallest_bid = df[(df['side'] == 'bid')&(df['new_quantity'] != 0)].nsmallest(1, 'price_level')
    highest_ask = df[(df['side'] == 'ask')&(df['new_quantity'] != 0)].nlargest(1, 'price_level')
    return pd.concat([smallest_bid, highest_ask])


class MDProcessorCoinbaseTopOfBook(MDProcessor):
    def __init__(self):
        self.batches_processed = 0
        self.n_inserts = 0
        self.topofbook_state = pd.DataFrame()

    def prepare_data(self, processing_queue, db_queue, batch_size):
        dict_list = []

        while True:
            data = processing_queue.get()
            processing_queue.task_done()
            
            if self.topofbook_state.empty:
                df = get_top_of_book(data)
                self.topofbook_state = df
            else:
                break  # just for now
                

            if data == "POISON_PILL":
                logging.info("Stop flag in processing_queue")
                
                
                if dict_list:
                    db_queue.put(dict_list)
                    
                db_queue.put("POISON_PILL")
                break

            if data:
                dict_list.extend(data)

            if len(dict_list) > batch_size:
                db_queue.put(dict_list)

                dict_list = []
    


# think how to reduce ammount of code for the 2 functions below
def parse_top_of_book_msg(msg):
    message = json.loads(msg)
    # print(message)
    # logging.info(message)
    if message["channel"] == "l2_data":
        # print('message', message)
        book_dict = message["events"][0]["updates"]
        if book_dict:
            book_df = pd.DataFrame(book_dict)
            # print('trades dict', trades_dict)
            book_df["channel"] = message["channel"]
            book_df["timestamp"] = message["timestamp"]
            book_df["sequence_num"] = message["sequence_num"]
            

            for col in ["event_time", "timestamp"]:
                book_df[col] = pd.to_datetime(book_df[col])
                
            return book_df
    
    
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
    
    if tbl=='coinbase_market_trades_stream':
        client = CoinbaseWebsocketClientTrades(
            queue=preprocessing_queue,
            endpoint=endpoint,
            payload=payload,
        )
    else:
        client = CoinbaseWebsocketClientTopOfBook(
            queue=preprocessing_queue,
            endpoint=endpoint,
            payload=payload,
        )
        
    md_handler = MDProcessorCoinbaseTopOfBook()
    
    utils.run_market_data_processor(
        client=client,
        md_handler=md_handler,
        preprocessing_queue=preprocessing_queue,
        db_queue=db_queue,
        batch_size=batch,
        tbl=tbl,
        orig_schema=orig_schema.keys(),
        stop_after=args.stop_after,
        test=args.test
    )




if __name__ == "__main__":
    main()
