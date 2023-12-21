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
pd.options.display.max_columns = None

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
        # print(message)
        message_dict = parse_top_of_book_msg(message)
        return super().on_message(ws, message_dict)



class CoinbaseWebsocketClientTrades(WebSocketClient):
    def on_message(self, ws, message):
        message_dict = parse_market_trades_msg(message)
        return super().on_message(ws, message_dict)


def get_top_of_book(df):
    # df['price_level'] = df['price_level'].astype(float)
    smallest_bid = df[(df['side'] == 'bid')&(df['new_quantity'] != 0)].nlargest(1, 'price_level')
    highest_ask = df[(df['side'] == 'offer')&(df['new_quantity'] != 0)].nsmallest(1, 'price_level')
    frame = pd.concat([smallest_bid, highest_ask])
    frame['sequence_num'] = frame['sequence_num'].astype(int)
    return frame


def transform_to_db_view(old_df, new_df):
    history_data = []
    # Extracting bid and ask rows from both DataFrames
    new_bid = new_df[new_df["side"] == "bid"].iloc[0]
    new_ask = new_df[new_df["side"] == "offer"].iloc[0]
    
    if old_df.empty:
        print('first trans book')
        history_data.append({
            "event_time": new_ask["event_time"],
            "BidPx": new_bid["price_level"],
            "BidSz": new_bid["new_quantity"],
            "OfferPx": new_ask["price_level"],
            "OfferSz": new_ask["new_quantity"],
            
        })
        return pd.DataFrame(history_data)
    old_bid = old_df[old_df["side"] == "bid"].iloc[0]
    old_ask = old_df[old_df["side"] == "offer"].iloc[0]

    # Determining which event (bid or ask) occurred first in the 'new' DataFrame
    first_update_is_bid = new_bid["event_time"] < new_ask["event_time"]

  
    
    


    # If the first update in 'new' is a bid
    if first_update_is_bid:
        print('first is bid')
        if new_bid["event_time"] != old_bid["event_time"]:
            history_data.append({
                "event_time": new_bid["event_time"],
                "BidPx": new_bid["price_level"],
                "BidSz": new_bid["new_quantity"],
                "OfferPx": old_ask["price_level"],
                "OfferSz": old_ask["new_quantity"],
                
            })
        history_data.append({
            "event_time": new_ask["event_time"],
            "BidPx": new_bid["price_level"],
            "BidSz": new_bid["new_quantity"],
            "OfferPx": new_ask["price_level"],
            "OfferSz": new_ask["new_quantity"],
            
        })
    else:
        print('first is ask')
        if new_ask["event_time"] != new_ask["event_time"]:
            history_data.append({
                "event_time": new_ask["event_time"],
                "BidPx": old_bid["price_level"],
                "BidSz": old_bid["new_quantity"],
                "OfferPx": new_ask["price_level"],
                "OfferSz": new_ask["new_quantity"],
                
            })
        history_data.append({
            "event_time": new_bid["event_time"],
            "BidPx": new_bid["price_level"],
            "BidSz": new_bid["new_quantity"],
            "OfferPx": new_ask["price_level"],
            "OfferSz": new_ask["new_quantity"],
            
        })
        
    res = pd.DataFrame(history_data)
    for col in ['channel', 'timestamp', 'sequence_num', 'product_id']:
        res[col] = new_bid[col]
    
    return res.drop_duplicates()
    


class MDProcessorCoinbaseTopOfBook(MDProcessor):
    def __init__(self):
        self.batches_processed = 0
        self.n_inserts = 0
        self.topofbook_state = pd.DataFrame()
        self.topofbook_prev_state = pd.DataFrame()

    def prepare_data(self, processing_queue, db_queue, batch_size):
        dict_list = []

        while True:
            data = processing_queue.get()
            processing_queue.task_done()
            
            if isinstance(data, str) and data == "POISON_PILL":
                logging.info("Stop flag in processing_queue")
                if dict_list:
                    db_queue.put(dict_list)
                db_queue.put("POISON_PILL")
                break
            
            if self.topofbook_state.empty:

                self.topofbook_state = get_top_of_book(data)
                self.topofbook_state['event_time'] = self.topofbook_state['timestamp']
                # self.topofbook_prev_state = self.topofbook_state
                print('first top of book')
                print(self.topofbook_state)
                
            else:
                print('second book')

                topofbook_upd = data
                if isinstance(data, pd.DataFrame):
                    if not topofbook_upd.empty:
                        status_upd = self.topofbook_state.merge(topofbook_upd, on=['side', 'price_level'], how='outer', suffixes=('', '_df2'))
                    
                        for col in ['new_quantity', 'event_time', 'channel', 'timestamp', 'sequence_num', 'product_id']:
                            status_upd[col] = status_upd.apply(
                                lambda row: row[f'{col}_df2'] if pd.notna(row[f'{col}_df2']) else row[col],
                                axis=1)
            
                        status_upd = status_upd.drop(columns=['event_time_df2', 'new_quantity_df2', 'channel_df2', 'timestamp_df2', 'sequence_num_df2', 'product_id_df2'])
                        status_upd = status_upd[status_upd['new_quantity'] != 0]
                        self.topofbook_state = get_top_of_book(status_upd)
                        print('topofbook:')
                        print(self.topofbook_state)
                    else:
                        continue
            
            topofbook_transformed = transform_to_db_view(self.topofbook_prev_state, self.topofbook_state)
            
            print('trans order book')
            print(topofbook_transformed)
            topofbook_transformed = topofbook_transformed.to_dict(orient='records')
            
        
            df1_sorted = self.topofbook_prev_state.sort_values(by=self.topofbook_prev_state.columns.tolist()).reset_index(drop=True)
            df2_sorted = self.topofbook_state.sort_values(by=self.topofbook_state.columns.tolist()).reset_index(drop=True)
            are_identical = df1_sorted.equals(df2_sorted)
            
            # making sure there was a change in top of book before putting to db queue
            if topofbook_transformed and not are_identical:
                dict_list.extend(topofbook_transformed)
                self.topofbook_prev_state = self.topofbook_state
            else:
                logging.info('No change in top of book')

            if len(dict_list) > batch_size:
                db_queue.put(dict_list)

                dict_list = []
    


# think how to reduce ammount of code for the 2 functions below
def parse_top_of_book_msg(msg):
    i=0
    message = json.loads(msg)

    if message["channel"] == "l2_data":

        book_dict = message["events"][0]["updates"]
        if book_dict:

            book_df = pd.DataFrame(book_dict)

            book_df["channel"] = message["channel"]
            book_df["timestamp"] = message["timestamp"]
            book_df["sequence_num"] = message["sequence_num"]
            book_df["product_id"] = message["events"][0]['product_id']
            

            for col in ["event_time", "timestamp"]:
                book_df[col] = pd.to_datetime(book_df[col])
                
            book_df['price_level'] = book_df['price_level'].astype(float).round(1)
            book_df['new_quantity'] = book_df['new_quantity'].astype(float)
            book_df['sequence_num'] = book_df['sequence_num'].astype(int)
            
            book_df = book_df[book_df['price_level'] > 0]

            return book_df
    
    
def parse_market_trades_msg(msg):
    message = json.loads(msg)

    if message["channel"] == "market_trades":
  
        trades_dict = message["events"][0]["trades"]
        if trades_dict:
            trades_df = pd.DataFrame(trades_dict)

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
