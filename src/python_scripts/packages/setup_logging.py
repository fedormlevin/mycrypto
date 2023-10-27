import os
import logging
from datetime import datetime

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