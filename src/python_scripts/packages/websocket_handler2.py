import json
import websocket
import logging
import time
import threading


class WebSocketClient:
    # DF_LIST = []

    def __init__(self, queue, endpoint, payload, stop_flag):
        self.queue = queue
        self.endpoint = endpoint
        self.payload = payload
        self.stop_event = threading.Event()
        self.start_time = time.time()
        self.df_list = []

    def on_message(self, ws, message):
        self.queue.put(message)
        if self.stop_event.is_set():
            logging.info("WS Stop Event is set")
            ws.close()

    def stop(self):
        self.stop_event.set()

    def on_error(self, ws, error):
        logging.error(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logging.info(f"### closed {close_status_code} {close_msg} ###")

    def on_open(self, ws):
        ws.send(json.dumps(self.payload))

    def run(self):
        reconn_after = 10
        while not self.stop_event.is_set():
            try:
                ws = websocket.WebSocketApp(
                    self.endpoint,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                ws.run_forever()
            except Exception as e:
                logging.error(f"Websocket error: {e}")
            if self.stop_event.is_set():
                break
            logging.info(f"Websocket conn lost. Reconn in {reconn_after} sec")
            time.sleep(10)
