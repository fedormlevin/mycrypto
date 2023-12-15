import os
import logging
from datetime import datetime
import argparse
import pandas as pd
import sys


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
    
    args = parser.parse_args()
    setup_logging(args.log_name)
    logging.info("Starting script")
    
    return args


def load_params_df(csv_path, table_name):
    params_df = pd.read_csv(csv_path)
    return params_df[params_df["table_name"] == table_name]


def flush_to_ch(df, ch_table, ch_schema):
    client = Client("localhost", user='default', password='myuser')

    df = df.apply(pd.to_numeric, errors="ignore")
    df = df[ch_schema]

    now_utc = datetime.utcnow()
    df["date"] = now_utc.date()

    epoch = datetime.utcfromtimestamp(0)
    microseconds_since_epoch = (now_utc - epoch).total_seconds() * 1_000_000
    df["insert_time"] = int(microseconds_since_epoch)

    if not first_batch_flushed:
        logging.info(f"Dumping batch of {len(df_list)}")
        first_batch_flushed = True
        
    client.execute(f"INSERT INTO mydb.{ch_table} VALUES", df.values.tolist())
    total_records_flushed += len(df_list)
    df_list = []
    return df_list