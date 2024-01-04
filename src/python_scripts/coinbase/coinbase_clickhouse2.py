#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
import hmac
import hashlib
from dotenv import load_dotenv
import time
from decimal import Decimal
import math
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
        message_dict = parse_top_of_book_msg(message)

        return super().on_message(ws, message_dict)


class CoinbaseWebsocketClientTrades(WebSocketClient):
    def on_message(self, ws, message):
        message_dict = parse_market_trades_msg(message)
        return super().on_message(ws, message_dict)
    
    
class CoinbaseWebsocketClientHB(WebSocketClient):

    def on_message(self, ws, message):
            
        if self.stop_event.is_set():
            logging.info("WS Stop Event is set")
            ws.close()


def get_top_of_book(df):
    if not df.empty:
        # df['price_level'] = df['price_level'].astype(float)
        highest_bid = df[(df["side"] == "bid") & (df["new_quantity"] != 0)].nlargest(
            1, "price_level"
        )
        smallest_ask = df[
            (df["side"] == "offer") & (df["new_quantity"] != 0)
        ].nsmallest(1, "price_level")
        frame = pd.concat([highest_bid, smallest_ask])
        frame["sequence_num"] = frame["sequence_num"].astype(int)
        return frame
    else:
        return pd.DataFrame()


def transform_to_db_view(old_df, new_df):
    history_data = []
    if new_df.empty:
        return pd.DataFrame()
    elif new_df[new_df["side"] == "bid"].empty:
        new_bid = old_df[old_df["side"] == "bid"].iloc[0]
        new_ask = new_df[new_df["side"] == "offer"].iloc[0]

    elif new_df[new_df["side"] == "offer"].empty:
        new_ask = old_df[old_df["side"] == "offer"].iloc[0]
        new_bid = new_df[new_df["side"] == "bid"].iloc[0]

    else:
        new_bid = new_df[new_df["side"] == "bid"].iloc[0]
        new_ask = new_df[new_df["side"] == "offer"].iloc[0]

    if old_df.empty:
        history_data.append(
            {
                "event_time": new_ask["event_time"],
                "BidPx": new_bid["price_level"],
                "BidSz": new_bid["new_quantity"],
                "OfferPx": new_ask["price_level"],
                "OfferSz": new_ask["new_quantity"],
            }
        )
        res = pd.DataFrame(history_data)
        for col in ["channel", "timestamp", "sequence_num", "product_id"]:
            res[col] = new_bid[col]
        return res

    old_bid = old_df[old_df["side"] == "bid"].iloc[0]
    old_ask = old_df[old_df["side"] == "offer"].iloc[0]

    # Determining which event (bid or ask) occurred first in the 'new' DataFrame
    first_update_is_bid = new_bid["event_time"] < new_ask["event_time"]

    # If the first update in 'new' is a bid
    if first_update_is_bid:
        if (
            new_bid["event_time"] != old_bid["event_time"]
            and new_bid["price_level"] != old_bid["price_level"]
        ):
            history_data.append(
                {
                    "event_time": new_bid["event_time"],
                    "BidPx": new_bid["price_level"],
                    "BidSz": new_bid["new_quantity"],
                    "OfferPx": old_ask["price_level"],
                    "OfferSz": old_ask["new_quantity"],
                    "sequence_num": new_bid["sequence_num"],
                }
            )
        history_data.append(
            {
                "event_time": new_ask["event_time"],
                "BidPx": new_bid["price_level"],
                "BidSz": new_bid["new_quantity"],
                "OfferPx": new_ask["price_level"],
                "OfferSz": new_ask["new_quantity"],
                "sequence_num": new_ask["sequence_num"],
            }
        )
    else:
        if (
            new_ask["event_time"] != new_ask["event_time"]
            and new_ask["price_level"] != new_ask["price_level"]
        ):
            history_data.append(
                {
                    "event_time": new_ask["event_time"],
                    "BidPx": old_bid["price_level"],
                    "BidSz": old_bid["new_quantity"],
                    "OfferPx": new_ask["price_level"],
                    "OfferSz": new_ask["new_quantity"],
                    "sequence_num": new_ask["sequence_num"],
                }
            )
        history_data.append(
            {
                "event_time": new_bid["event_time"],
                "BidPx": new_bid["price_level"],
                "BidSz": new_bid["new_quantity"],
                "OfferPx": new_ask["price_level"],
                "OfferSz": new_ask["new_quantity"],
                "sequence_num": new_bid["sequence_num"],
            }
        )

    res = pd.DataFrame(history_data)
    for col in ["channel", "timestamp", "product_id"]:
        res[col] = new_bid[col]

    return res.drop_duplicates()


class MDProcessorCoinbaseTopOfBook(MDProcessor):
    def __init__(self):
        self.batches_processed = 0
        self.n_inserts = 0
        # self.topofbook_state = pd.DataFrame()
        # self.topofbook_prev_state = pd.DataFrame()
        self.book_state = pd.DataFrame()
        self.book_state_prev = pd.DataFrame()

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

            if isinstance(data, pd.DataFrame):
                if not data.empty:
                    data = aggregate_quantity_decimal(data, agg_level=1)

            # if self.topofbook_state.empty:
            #     self.topofbook_state = get_top_of_book(data)
            #     self.topofbook_state["event_time"] = self.topofbook_state["timestamp"]
            if self.book_state.empty:
                self.book_state = data

            else:
                topofbook_upd = data
                if isinstance(data, pd.DataFrame):
                    if not topofbook_upd.empty:
                        # status_upd = self.topofbook_state.merge(
                        status_upd = self.book_state.merge(
                            topofbook_upd,
                            on=["side", "price_level"],
                            how="outer",
                            suffixes=("", "_df2"),
                        )

                        for col in [
                            "new_quantity",
                            "event_time",
                            "channel",
                            "timestamp",
                            "sequence_num",
                            "product_id",
                        ]:
                            status_upd[col] = status_upd.apply(
                                lambda row: row[f"{col}_df2"]
                                if pd.notna(row[f"{col}_df2"])
                                else row[col],
                                axis=1,
                            )

                        status_upd = status_upd.drop(
                            columns=[
                                "event_time_df2",
                                "new_quantity_df2",
                                "channel_df2",
                                "timestamp_df2",
                                "sequence_num_df2",
                                "product_id_df2",
                            ]
                        )

                        self.book_state = status_upd
                        # self.topofbook_state = get_top_of_book(status_upd)

                    else:
                        continue

            self.topofbook_state = get_top_of_book(self.book_state)
            self.topofbook_prev_state = get_top_of_book(self.book_state_prev)

            self.book_state_prev = self.book_state

            topofbook_transformed = transform_to_db_view(
                self.topofbook_prev_state, self.topofbook_state
            )
            print('topofbook')
            print(topofbook_transformed)
            
            
            
            if topofbook_transformed['BidPx'].values[0]>=topofbook_transformed['OfferPx'].values[0]:
                
                logging.error("Bad spread")
                if dict_list:
                    db_queue.put(dict_list)
                db_queue.put("POISON_PILL")
                break
            
            if not topofbook_transformed.empty:
                topofbook_transformed = topofbook_transformed.to_dict(orient="records")

                df1_sorted = self.topofbook_prev_state.sort_values(
                    by=self.topofbook_prev_state.columns.tolist()
                ).reset_index(drop=True)
                df2_sorted = self.topofbook_state.sort_values(
                    by=self.topofbook_state.columns.tolist()
                ).reset_index(drop=True)
                are_identical = df1_sorted.equals(df2_sorted)

                # making sure there was a change in top of book before putting to db queue
                if topofbook_transformed and not are_identical:
                    dict_list.extend(topofbook_transformed)
                    self.topofbook_prev_state = self.topofbook_state

                if len(dict_list) > batch_size:
                    db_queue.put(dict_list)

                    dict_list = []





def aggregate_quantity_decimal(df, agg_level=Decimal(1)):
    bid = df[df["side"] == "bid"]["price_level"].max() - 100
    offer = df[df["side"] == "offer"]["price_level"].min() + 100
    df = df[df["price_level"].between(bid, offer)]

    agg_lst = []
    for side in ["bid", "offer"]:
        levels_df = df[df["side"] == side].copy()
        if not levels_df.empty:
            if side == "bid":
                right = False

                def label_func(x):
                    return x.left

            else:
                right = True

                def label_func(x):
                    return x.right

            min_level = (
                math.floor(Decimal(min(levels_df["price_level"])) / agg_level - 1)
                * agg_level
            )
            max_level = (
                math.ceil(Decimal(max(levels_df["price_level"])) / agg_level + 1)
                * agg_level
            )

            level_bounds = [
                float(min_level + agg_level * x)
                for x in range(int((max_level - min_level) / agg_level) + 1)
            ]

            levels_df["bin"] = pd.cut(
                levels_df["price_level"], bins=level_bounds, precision=10, right=right
            )

            # levels_df = levels_df.groupby('bin').agg(qty=('new_quantity', 'sum')).reset_index()
            levels_df = (
                levels_df.groupby(["bin", "channel", "product_id"], observed=False)
                .agg(
                    {
                        "new_quantity": "sum",
                        "event_time": "max",
                        "timestamp": "max",
                        "sequence_num": "max",
                    }
                )
                .reset_index()
            )

            levels_df["price_level"] = levels_df.bin.apply(label_func).astype(float)

            levels_df["side"] = side
            levels_df = levels_df.drop(columns="bin")
            agg_lst.append(levels_df)
            # levels_df = levels_df[['px', 'qty']]
        else:
            continue
    if agg_lst:
        return pd.concat(agg_lst)
    else:
        return pd.DataFrame()


# think how to reduce ammount of code for the 2 functions below
def parse_top_of_book_msg(msg):
    message = json.loads(msg)

    if message.get("channel") == "l2_data":
        book_dict = message["events"][0]["updates"]
        if book_dict:
            book_df = pd.DataFrame(book_dict)

            book_df["channel"] = message["channel"]
            book_df["timestamp"] = message["timestamp"]
            book_df["sequence_num"] = message["sequence_num"]
            book_df["product_id"] = message["events"][0]["product_id"]

            for col in ["event_time", "timestamp"]:
                book_df[col] = pd.to_datetime(book_df[col])

            book_df["price_level"] = book_df["price_level"].astype(float)
            book_df["new_quantity"] = book_df["new_quantity"].astype(float)
            book_df["sequence_num"] = book_df["sequence_num"].astype(int)
            book_df["product_id"] = book_df["product_id"].astype(str)
            book_df["channel"] = book_df["channel"].astype(str)

            return book_df
    else:
        if not message.get("channel"):
            logging.error(f"channel in message is None")
            logging.error(message)
            return ("ERROR", message)


def parse_market_trades_msg(msg):
    message = json.loads(msg)

    if message.get("channel") == "market_trades":
        trades_dict = message["events"][0]["trades"]
        if trades_dict:
            trades_df = pd.DataFrame(trades_dict)

            trades_df["channel"] = message["channel"]
            trades_df["timestamp"] = message["timestamp"]
            trades_df["sequence_num"] = message["sequence_num"]

            for col in ["time", "timestamp"]:
                trades_df[col] = pd.to_datetime(trades_df[col])

            list_of_dicts = trades_df.to_dict(orient="records")
            return list_of_dicts
    else:
        if not message.get("channel"):
            logging.error(f"channel in message is None")
            logging.error(message)


def main():
    load_dotenv()  # only for coinbase to get api key secret

    args = utils.setup_args()

    utils.setup_logging("coinbase")
    logging.info("Starting script")

    tbl = args.table
    batch = args.batch_size

    endpoint = args.endpoint

    params_df = pd.read_csv("~/develop/mycrypto/coinbase_md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]
    channel = params_df["channel"].values[0]
    product_ids = params_df["product_ids"].values[0]
    product_ids_list = [s.strip() for s in product_ids.split(",")]

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

    if tbl != "coinbase_top_of_book_stream":
        client = CoinbaseWebsocketClientTrades(
            queue=preprocessing_queue,
            endpoint=endpoint,
            payload=payload,
        )
        md_handler = MDProcessor()

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
        test=args.test,
    )
    
    if args.heartbeats:
        message_hb = {
            "type": "subscribe",
            "channel": 'heartbeats',
            "api_key": os.environ.get("coinbase_api_key"),
            "product_ids": product_ids_list,
        }
        payload_hb = timestamp_and_sign(message_hb, channel, product_ids_list)
        client_hb = CoinbaseWebsocketClientHB(queue=None, endpoint=endpoint, payload=payload_hb)
        
        utils.run_heartbeats(client=client_hb, stop_after=args.stop_after)


if __name__ == "__main__":
    main()
