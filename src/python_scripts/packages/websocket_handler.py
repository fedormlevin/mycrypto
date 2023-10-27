import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging
from functools import partial
import argparse
import os


class WebSocketClient:
    DF_LIST = []

    def __init__(self, endpoint, payload, ch_table, ch_schema, batch_size):
        self.endpoint = endpoint
        self.payload = payload
        self.ch_table = ch_table
        self.ch_schema = ch_schema
        self.batch_size = batch_size

    def on_message(self, ws, message):
        # print(message)

        if len(self.DF_LIST) >= self.batch_size:
            df = pd.concat(self.DF_LIST)
            self.flush_to_ch(df, self.ch_table, self.ch_schema)

    def flush_to_ch(self, df, ch_table, ch_schema):
        client = Client("localhost")

        df = df.apply(pd.to_numeric, errors="ignore")
        df = df[ch_schema]

        now_utc = datetime.utcnow()
        df["date"] = now_utc.date()

        epoch = datetime.utcfromtimestamp(0)
        microseconds_since_epoch = (now_utc - epoch).total_seconds() * 1_000_000
        df["insert_time"] = int(microseconds_since_epoch)

        logging.info(f"Dumping batch of {len(self.DF_LIST)}")
        client.execute(f"INSERT INTO {ch_table} VALUES", df.values.tolist())
        self.DF_LIST = []

    def on_error(self, ws, error):
        logging.error(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logging.info(f"### closed {close_status_code} {close_msg} ###")

    def on_open(self, ws):
        ws.send(json.dumps(self.payload))

    def run(self):
        ws = websocket.WebSocketApp(
            self.endpoint,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever()
