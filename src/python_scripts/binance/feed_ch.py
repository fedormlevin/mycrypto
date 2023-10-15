import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging

# Setting up the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BATCH_SIZE = 5
DF_LIST = []
MESSAGE_COUNT = 0

client = Client("localhost")  # Assuming ClickHouse is running on localhost


def on_message(ws, message):
    global DF_LIST
    global MESSAGE_COUNT

    data_list = json.loads(message)

    MESSAGE_COUNT += 1
    print(f"Received message #{MESSAGE_COUNT}")

    if "result" in data_list and "id" in data_list:
        if data_list["id"] == 1 and data_list["result"] is None:
            logging.info("Successfully subscribed to Binance!")
            return

    df_ = pd.DataFrame(data_list)
    DF_LIST.append(df_)

    # If we've collected enough rows, insert the batch
    if len(DF_LIST) >= BATCH_SIZE:
        logging.info(f"Dumping batch of {len(DF_LIST)}")

        df = pd.concat(DF_LIST)

        df["date"] = datetime.now().date()

        df = df.apply(pd.to_numeric, errors="ignore")

        client.execute(
            "INSERT INTO binance_symbol_ticker_stream VALUES", df.values.tolist()
        )
        DF_LIST = []  # Clear the buffer


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    # Subscribe to the trade stream for BTCUSDT pair.
    payload = {
        "method": "SUBSCRIBE",
        "params": ["!ticker@arr"],
        "id": 1,
    }
    ws.send(json.dumps(payload))


def run_websocket():
    endpoint = "wss://stream.binance.us:9443/ws"

    ws = websocket.WebSocketApp(
        endpoint, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()


if __name__ == "__main__":
    logging.info("Starting script")
    try:
        run_websocket()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
