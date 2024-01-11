import multiprocessing
import pandas as pd
import numpy as np
import time
import random
import string
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)1s - %(message)s',
                    #datefmt='%Y-%m-%dT%H:%M:%S',
                    level=logging.INFO)

def generate_dataframe():
    # Generate a DataFrame with random data
    data = {
        'col1': np.random.randint(0, 100, size=100000),
        'col2': np.random.randint(0, 100, size=100000),
        'col3': np.random.randint(0, 100, size=100000),
        'col4': np.random.randint(0, 100, size=100000),
        'col5': [''.join(random.choices(string.ascii_uppercase + string.digits, k=5)) for _ in range(100000)]
    }
    return pd.DataFrame(data)

def producer(queue):
    prod_id = 0
    while True:
        # Simulate delay with avg 498 ms and std 30 ms
        time.sleep(max(0, np.random.normal(0.498, 0.03)))
        df = generate_dataframe()
        queue.put(df)
        logging.info(f'putting to consumer queue {prod_id}')
        prod_id+=1

def consumer(input_queue, output_queue):
    df_list = []
    prod_id = 0
    while True:
        df = input_queue.get()
        # Simulate processing time
        time.sleep(max(0, np.random.normal(0.51288, 0.097)))
        df_list.append(df)
        logging.info(f'Finished processing {prod_id}')
        prod_id+=1
        if len(df_list) == 100:
            output_queue.put(df_list)
            df_list = []

if __name__ == "__main__":
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()

    # Start producer and consumer processes
    producer_process = multiprocessing.Process(target=producer, args=(input_queue,))
    consumer_process = multiprocessing.Process(target=consumer, args=(input_queue, output_queue,))

    producer_process.start()
    consumer_process.start()

    # Run for 30 minutes
    # time.sleep(1800)
    time.sleep(520)

    # Terminate processes
    producer_process.terminate()
    consumer_process.terminate()

    # Gather and print statistics (example: queue sizes)
    print(f'Input queue size: {input_queue.qsize()}')
    print(f'Output queue size: {output_queue.qsize()}')

    # Further statistical analysis can be performed here
