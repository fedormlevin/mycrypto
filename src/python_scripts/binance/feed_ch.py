#!/usr/bin/env python

import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging
from functools import partial
import argparse
import os

# Ensure the LOG directory exists
if not os.path.exists("LOG"):
    os.makedirs("LOG")

# Get the current date and time to format the log filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"LOG/feed_ch_{current_time}.log"

# Setting up the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


DF_LIST = []


def on_message(ws, message, client, tbl, orig_schema, batch_size):
    global DF_LIST

    data_list = json.loads(message)

    if "result" in data_list and "id" in data_list:
        if data_list["id"] == 1 and data_list["result"] is None:
            logging.info("Successfully subscribed to Binance!")
            return

    if isinstance(data_list, dict):
        data_list = [data_list]

    df_ = pd.DataFrame(data_list)
    DF_LIST.append(df_)

    # If we've collected enough rows, insert the batch
    if len(DF_LIST) >= batch_size:
        logging.info(f"Dumping batch of {len(DF_LIST)}")

        df = pd.concat(DF_LIST)
        df = df[orig_schema]

        df = df.apply(pd.to_numeric, errors="ignore")

        now_utc = datetime.utcnow()
        df["date"] = now_utc.date()

        epoch = datetime.utcfromtimestamp(0)
        microseconds_since_epoch = (now_utc - epoch).total_seconds() * 1_000_000
        df["insert_time"] = int(microseconds_since_epoch)

        client.execute(f"INSERT INTO {tbl} VALUES", df.values.tolist())

        # client.insert_dataframe(query=f"INSERT INTO {tbl} VALUES", dataframe=df)
        DF_LIST = []  # Clear the buffer


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws, method, subscriptions):
    # Subscribe to the trade stream for BTCUSDT pair.
    payload = {
        "method": method,
        "params": subscriptions,
        "id": 1,
    }
    ws.send(json.dumps(payload))


def run_websocket(client, tbl, orig_schema, subscriptions, endpoint, batch_size):
    ws = websocket.WebSocketApp(
        endpoint,
        on_message=partial(
            on_message,
            client=client,
            tbl=tbl,
            orig_schema=orig_schema,
            batch_size=batch_size,
        ),
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = partial(on_open, method="SUBSCRIBE", subscriptions=subscriptions)
    ws.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push binance data to Clickhouse")
    parser.add_argument("--table", type=str, default="binance_symbol_ticker_stream")

    parser.add_argument(
        "--endpoint", type=str, default="wss://stream.binance.us:9443/ws"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=5)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv("/Users/fedorlevin/workspace/mycrypto/md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]

    sub_id = params_df["subscription_id"].values[0]
    sub_id_list = result = [s.strip() for s in sub_id.split(",")]

    col_names_dir = params_df["colnames_json"].values[0]
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    client = Client("localhost")

    try:
        run_websocket(
            client=client,
            tbl=tbl,
            orig_schema=orig_schema.keys(),
            subscriptions=sub_id_list,
            endpoint=endpoint,
            batch_size=batch_size,
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
