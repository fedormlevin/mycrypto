import json
import websocket
import logging
import time
import threading
import sys

class WebSocketClient:
    def __init__(self, queue, endpoint, payload):
        self.queue = queue
        self.endpoint = endpoint
        self.payload = payload
        self.stop_event = threading.Event()
        self.start_time = time.time()
        self.df_list = []
        self.first_message = True
        self.recon_attempt = 0
        self.count_id = 0

    def on_message(self, ws, message):
        
        self.queue.put(message)
        logging.info(f'WS to processing {self.count_id}')
        self.count_id+=1
        
        if self.first_message:
            logging.info('1st message is in processing queue')
            
            self.first_message = False
            
        if self.stop_event.is_set():
            logging.info("WS Stop Event is set")
            ws.close()

    def stop(self):
        self.queue.put('POISON PILL')
        self.stop_event.set()

    def on_error(self, ws, error):
        logging.error(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logging.info(f"### closed {close_status_code} {close_msg} ###")

    def on_open(self, ws):
        ws.send(json.dumps(self.payload))
        logging.info('Payload:')
        logging.info(self.payload)

    def run(self):
        reconn_after = 20
        while not self.stop_event.is_set():
            try:
                if self.recon_attempt>2:
                    logging.info('No more reconn attempts')
                    self.stop()
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
            self.recon_attempt+=1