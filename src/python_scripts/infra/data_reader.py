import zmq
import json

# Prepare the context and the subscriber socket
context = zmq.Context()
subscriber = context.socket(zmq.SUB)

# Connect to the publisher
subscriber.connect("tcp://localhost:5555")

# Subscribe to all messages
subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

try:
    while True:
        # Receive and print the message
        message = subscriber.recv_string()
        msg = json.loads(message)
        if msg['stream']=='btcusdt@trade':
            print(f"Received: {msg}")
        else:
            print(f'ignored: {msg}')
except KeyboardInterrupt:
    print("Subscriber shutting down...")
finally:
    subscriber.close()
    context.term()
