#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
from packages import utils
import logging
import pandas as pd
import json
from queue import Queue
from packages.market_data_handler import MDProcessor


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
                data_dict = data_dict["result"]["data"]
                return super().on_message(ws, data_dict)
        else:
            return


def main():
    args = utils.setup_args()

    utils.setup_logging("cryptocom_trades")
    logging.info("Starting script")

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = pd.read_csv("~/develop/mycrypto/cryptocom_md_config.csv")
    params_df = params_df[params_df["table_name"] == tbl]

    pair = params_df["pair"].values[0]
    pair_list = [s.strip() for s in pair.split(",")]

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
    
    preprocessing_queue = Queue()
    db_queue = Queue()
    
    client = CryptocomWebsocketClient(
        queue=preprocessing_queue,
        endpoint=endpoint,
        payload=payload,
    )
    
    md_handler = MDProcessor()
    
    utils.run_market_data_processor(
        client=client,
        md_handler=md_handler,
        preprocessing_queue=preprocessing_queue,
        db_queue=db_queue,
        batch_size=batch_size,
        tbl=tbl,
        orig_schema=orig_schema.keys(),
        stop_after=args.stop_after
    )


if __name__ == "__main__":
    main()
