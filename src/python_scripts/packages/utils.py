import os
import logging
from datetime import datetime
import argparse
import pandas as pd
import sys

import threading
import time
from clickhouse_driver import Client as cl


def stop_script(signum, frame):
    logging.info("Scheduled stop of the script after 2 hours.")
    sys.exit(0)


def setup_logging(log_name="default"):
    """
    Set up logging with a default log directory and naming convention based on the current time.
    """

    # Ensure the LOG directory exists
    log_dir = os.path.expanduser("~/develop/LOG")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Get the current date and time to format the log filename
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{log_dir}/{log_name}_{current_time}.log"

    logging.basicConfig(
        # filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def setup_args():
    parser = argparse.ArgumentParser(description="Push exchange data to Clickhouse")
    parser.add_argument("--table", type=str, required=True)
    parser.add_argument("--endpoint", type=str, required=True)
    parser.add_argument("-b", "--batch-size", type=int, required=True)
    parser.add_argument("--log-name", type=str, required=True, default='log_name')
    parser.add_argument("--stop-after", type=int, required=True, default=86400)  # 24 hrs
    parser.add_argument("-t", "--test", required=False, action='store_true', default=False) 
    
    args = parser.parse_args()
    setup_logging(args.log_name)
    logging.info("Starting script")
    
    return args


def load_params_df(csv_path, table_name):
    params_df = pd.read_csv(csv_path)
    return params_df[params_df["table_name"] == table_name]


def run_market_data_processor(client, md_handler, preprocessing_queue, db_queue, batch_size, tbl,
                              orig_schema, stop_after, test=False):

    # md_handler = MDProcessor()

    ws_thread = threading.Thread(target=client.run)
    ws_thread.start()

    ps_thread = threading.Thread(
        target=md_handler.prepare_data, args=(preprocessing_queue, db_queue, batch_size)
    )
    ps_thread.start()

    db_thread = threading.Thread(
        target=md_handler.flush_to_clickhouse, args=(db_queue, orig_schema, tbl, test)
    )
    db_thread.start()

    time.sleep(stop_after)

    client.stop()
    preprocessing_queue.put("POISON_PILL")

    ws_thread.join()
    ps_thread.join()
    db_thread.join()

    logging.info(f"Records processed: {md_handler.batches_processed}")
    logging.info(f"N inserts: {md_handler.n_inserts}")
    
    
def run_clickhouse_query(host, user, psw, db, query):


    client = cl(host, user=user, password=psw, database=db)

    # Execute a query
    result = client.execute(query, with_column_types=True)

    # Split the results and column types
    rows, columns = result

    # Extract column names
    column_names = [column[0] for column in columns]

    # Combine column names and row data to get a list of dictionaries
    data_with_column_names = [dict(zip(column_names, row)) for row in rows]

    return pd.DataFrame(data_with_column_names)