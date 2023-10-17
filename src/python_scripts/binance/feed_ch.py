import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging
from functools import partial
import argparse

# Setting up the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


DF_LIST = []


with open(
    "/Users/fedorlevin/workspace/data/binance/refdata/binance_symbol_ticker_stream_colnames.json",
    "r",
) as f:
    COL_ = json.load(f)

client = Client("localhost")  # Assuming ClickHouse is running on localhost


def on_message(ws, message, tbl, tbl_cols, batch_size):
    global DF_LIST

    data_list = json.loads(message)

    if "result" in data_list and "id" in data_list:
        if data_list["id"] == 1 and data_list["result"] is None:
            logging.info("Successfully subscribed to Binance!")
            return

    df_ = pd.DataFrame(data_list)
    DF_LIST.append(df_)

    # If we've collected enough rows, insert the batch
    if len(DF_LIST) >= batch_size:
        logging.info(f"Dumping batch of {len(DF_LIST)}")

        df = pd.concat(DF_LIST)
        df = df[tbl_cols]

        df["date"] = datetime.now().date()

        df = df.apply(pd.to_numeric, errors="ignore")

        client.execute(f"INSERT INTO {tbl} VALUES", df.values.tolist())
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


def run_websocket(tbl, tbl_cols, subscriptions, endpoint, batch_size):
    ws = websocket.WebSocketApp(
        endpoint,
        on_message=partial(
            on_message,
            tbl=tbl,
            tbl_cols=tbl_cols,
            batch_size=batch_size,
        ),
        on_error=on_error,
        on_close=on_close,
    )
    ws.on_open = partial(
        on_open, method="SUBSCRIBE", subscriptions=[f"{subscriptions}"]
    )
    ws.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push binance data to Clickhouse")
    parser.add_argument("--table", type=str, default="binance_symbol_ticker_stream")
    parser.add_argument("-s", "--subscription", type=str, default="!ticker@arr")
    parser.add_argument(
        "--endpoint", type=str, default="wss://stream.binance.us:9443/ws"
    )
    parser.add_argument("-b", "--batch-size", type=int, default=20)

    logging.info("Starting script")
    args = parser.parse_args()
    tbl = args.table
    subscriptions = args.subscription
    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv("/Users/fedorlevin/workspace/mycrypto/md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]

    sub_id = params_df["subscription_id"].values[0]
    col_names_dir = params_df["colnames_json"].values[0]
    with open(col_names_dir, "r") as f:
        col_names = json.load(f)

    try:
        run_websocket(
            tbl=tbl,
            tbl_cols=col_names.keys(),
            subscriptions=subscriptions,
            endpoint=endpoint,
            batch_size=batch_size,
        )
    except Exception as e:
        logging.error(f"Error occurred: {e}")
