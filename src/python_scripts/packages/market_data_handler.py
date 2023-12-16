import logging
import pandas as pd
from clickhouse_driver import Client
from packages import utils
from datetime import datetime


class MDProcessor:
    def __init__(self):
        self.batches_processed = 0
        self.n_inserts = 0

    def prepare_data(self, processing_queue, db_queue, batch_size):
        dict_list = []

        while True:
            data = processing_queue.get()
            processing_queue.task_done()

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

    def flush_to_clickhouse(self, db_queue, ch_schema, ch_table):
        while True:
            data = db_queue.get()

            db_queue.task_done()

            if data == "POISON_PILL":
                logging.info("Stop flag in db_queue")
                break

            df = pd.DataFrame(data)

            df = df.apply(pd.to_numeric, errors="ignore")
            df = df[ch_schema]

            now_utc = datetime.utcnow()
            df["date"] = now_utc.date()

            epoch = datetime.utcfromtimestamp(0)
            microseconds_since_epoch = (now_utc - epoch).total_seconds() * 1_000_000
            df["insert_time"] = int(microseconds_since_epoch)

            if self.batches_processed < 1:
                logging.info(f"Dumping 1st batch of len {len(df)}")

            client = Client("localhost", user="default", password="myuser")
            client.execute(f"INSERT INTO mydb.{ch_table} VALUES", df.values.tolist())
            self.batches_processed += len(df)
            self.n_inserts += 1
