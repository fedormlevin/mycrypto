#!/usr/bin/env python
import logging
import os
from datetime import datetime, timedelta
import argparse
from clickhouse_driver import Client

# Ensure the LOG directory exists
log_dir = os.path.expanduser("~/workspace/LOG")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Get the current date and time to format the log filename
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_dir}/cleanup_ch_{current_time}.log"

logging.basicConfig(filename=log_filename,
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push binance data to Clickhouse")
    parser.add_argument("-d", "--days", type=int, default=5)

    args = parser.parse_args()

    days_to_keep = args.days

    threshold_date = (datetime.utcnow().date() - timedelta(days=days_to_keep)).strftime(
        "%Y-%m-%d"
    )
    logging.info(f"Delete from CH older than {threshold_date} ({days_to_keep} days)")

    client = Client("localhost")
    tables = client.execute("SHOW TABLES FROM default")
    
    for tbl in tables:
        tbl_name = tbl[0]

        columns = client.execute(f'DESCRIBE TABLE default.{tbl_name}')
        date_columns = [col[0] for col in columns if col[0].lower() == 'date']

        if date_columns:
            print(f'Clearing {tbl_name}')
            delete_query = f"""
        ALTER TABLE default.{tbl_name} DELETE WHERE {date_columns[0]} < '{threshold_date}'
        """
        
        client.execute(delete_query)