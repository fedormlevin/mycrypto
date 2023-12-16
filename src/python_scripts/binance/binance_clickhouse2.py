#!/usr/bin/env python

import os
from packages.websocket_handler2 import WebSocketClient
import logging
from packages import utils
import json
from queue import Queue


class BinanceWebsocketClient(WebSocketClient):
    def on_message(self, ws, message):
        data_list = json.loads(message)

        if "result" in data_list and "id" in data_list:
            if data_list["id"] == 1 and data_list["result"] is None:
                logging.info("Successfully subscribed to Binance!")
                return

        if isinstance(data_list, dict):
            data_list = [data_list]

        return super().on_message(ws, data_list)


def main():
    
    args = utils.setup_args()

    tbl = args.table

    endpoint = args.endpoint
    batch_size = args.batch_size

    params_df = utils.load_params_df(
        "~/develop/mycrypto/binance_md_config.csv", tbl
    )

    sub_id = params_df["subscription_id"].values[0]
    sub_id_list = [s.strip() for s in sub_id.split(",")]

    col_names_dir = params_df["colnames_json"].values[0]
    col_names_dir = os.path.expanduser(col_names_dir)
    with open(col_names_dir, "r") as f:
        orig_schema = json.load(f)

    payload = {
        "method": "SUBSCRIBE",
        "params": sub_id_list,
        "id": 1,
    }

    preprocessing_queue = Queue()
    db_queue = Queue()
    
    client = BinanceWebsocketClient(
        queue=preprocessing_queue,
        endpoint=endpoint,
        payload=payload,
    )
    
    utils.run_market_data_processor(
        client=client,
        preprocessing_queue=preprocessing_queue,
        db_queue=db_queue,
        batch_size=batch_size,
        tbl=tbl,
        orig_schema=orig_schema.keys(),
        stop_after=args.stop_after
    )


if __name__ == "__main__":
    main()
