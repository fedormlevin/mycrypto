#!/usr/bin/env python

import zmq
import json
import time

# Prepare the context and the subscriber socket
context = zmq.Context()
#subscriber = context.socket(zmq.SUB)
subscriber = context.socket(zmq.PULL)

# Connect to the publisher
subscriber.connect("tcp://localhost:5555")

# Subscribe to all messages
#subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

try:
    while True:
        # Receive and print the message
        message = subscriber.recv_string()
        msg = json.loads(message)

        sent_time = msg['timestamp']
        current_time = time.time()
    
        latency = current_time - sent_time
        print(f"Latency: {latency*1000:.2f} ms")  # Print latency in milliseconds
        inner_data = json.loads(msg['data'])
        if inner_data['stream']=='btcusdt@trade':
            print(f"Received: {msg}")
        else:
            print(f'ignored: {msg}')
except KeyboardInterrupt:
    print("Subscriber shutting down...")
finally:
    subscriber.close()
    context.term()
