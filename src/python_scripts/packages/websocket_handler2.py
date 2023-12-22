import json
import websocket
import logging
import time
import threading


class WebSocketClient:
    def __init__(self, queue, endpoint, payload):
        self.queue = queue
        self.endpoint = endpoint
        self.payload = payload
        self.stop_event = threading.Event()
        self.start_time = time.time()
        self.df_list = []
        self.first_message = True

    def on_message(self, ws, message):
        # if message:  # will fail if message is df
        if isinstance(message, str):
            if message=='ERROR':
                logging.info('Sending ws.close')
                ws.close()
        self.queue.put(message)
        
        if self.first_message:
            logging.info('1st message is in processing queue')
            self.first_message = False
            
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
    
    # def run(self):
    #     reconn_after = 10  # Reconnection attempt after 10 seconds
    #     connection_loops = 0  # Initialize a counter for connection loops

    #     while not self.stop_event.is_set():
    #         try:
    #             ws = websocket.WebSocketApp(
    #                 self.endpoint,
    #                 on_open=self.on_open,
    #                 on_message=self.on_message,
    #                 on_error=self.on_error,
    #                 on_close=self.on_close,
    #             )
                
    #             # Start the websocket connection on a separate thread
    #             wst = threading.Thread(target=lambda: ws.run_forever())
    #             wst.start()
                
    #             # Wait for a short period before simulating a disconnect
    #             time.sleep(2)  # Wait for 2 seconds or however long you expect to be connected before testing the disconnect
    #             if connection_loops == 0:
    #                 # Simulate disconnect on the first loop
    #                 ws.close()
    #                 connection_loops += 1
    #                 logging.info("Forced a disconnect for testing reconnection.")
                
    #             # Wait for the reconnection attempt interval
    #             time.sleep(reconn_after)

    #         except Exception as e:
    #             logging.error(f"Websocket error: {e}")
    #         finally:
    #             # If an exception occurs or the ws closes, it logs the reconnect attempt and waits
    #             if not self.stop_event.is_set():
    #                 logging.info(f"Attempting to reconnect in {reconn_after} seconds.")
    #                 time.sleep(reconn_after)  # Wait for the specified reconnect time


