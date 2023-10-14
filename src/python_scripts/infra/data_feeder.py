import websocket
import json
import zmq

# ZeroMQ setup
context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind("tcp://*:5555")  # Publisher listens on all interfaces at port 5555

# This function will handle incoming messages from the WebSocket.
def on_message(ws, message):
    data = json.loads(message)
    print(data)

    # Publish the received data via ZeroMQ
    #msg = f"Message {data}"  # Format or adapt the data as needed
    msg = json.dumps(data)
    publisher.send_string(msg)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    # Subscribe to the trade stream for the BTCUSDT pair.
    payload = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@trade",
            "!ticker@arr"
        ],
        "id": 1,
    }
    ws.send(json.dumps(payload))

def run_websocket():
    # The Binance WebSocket endpoint for the streams
    endpoint = "wss://stream.binance.us:9443/stream"

    ws = websocket.WebSocketApp(
        endpoint, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

def main():
    run_websocket()

if __name__ == "__main__":
    main()
