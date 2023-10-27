import os
import logging
from datetime import datetime
import argparse
import pandas as pd


def setup_logging(log_name="default"):
    """
    Set up logging with a default log directory and naming convention based on the current time.
    """

    # Ensure the LOG directory exists
    log_dir = os.path.expanduser("~/workspace/LOG")
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
    
    args = parser.parse_args()
    setup_logging(args.log_name)
    logging.info("Starting script")
    
    return args


def load_params_df(csv_path, table_name):
    params_df = pd.read_csv(csv_path)
    return params_df[params_df["table_name"] == table_name]
