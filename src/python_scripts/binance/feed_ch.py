import json
import websocket
import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging

# Setting up the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BATCH_SIZE = 5
DF_LIST = []
MESSAGE_COUNT = 0

client = Client('localhost')  # Assuming ClickHouse is running on localhost

def on_message(ws, message):
    global DF_LIST
    global MESSAGE_COUNT

    data_list = json.loads(message)
    
    MESSAGE_COUNT += 1
    print(f"Received message #{MESSAGE_COUNT}")

    if 'result' in data_list and 'id' in data_list:
            
        if data_list['id'] == 1 and data_list['result'] is None:
            logging.info("Successfully subscribed to Binance!")
            return

    df_ = pd.DataFrame(data_list)
    DF_LIST.append(df_)
    
    # If we've collected enough rows, insert the batch
    if len(DF_LIST) >= BATCH_SIZE:
        logging.info(f'Dumping batch of {len(DF_LIST)}')

        #with open('/Users/fedorlevin/workspace/data/binance/refdata/binance_symbol_ticker_stream_colnames.json', 'r') as f:
            #col_names = json.load(f)

        df = pd.concat(DF_LIST)
        # df = df.rename(columns=col_names)

        df['date'] = datetime.now().date()
        
        df.columns = [
            'event_type', 'event_time', 'symbol', 'px_change',
            'px_change_percent', 'weighted_avg_px',
            'first_trade_f_1_px', 'last_px', 'last_qty',
            'best_bid_px', 'best_bid_qty', 'best_ask_px',
            'best_ask_qty', 'open_px', 'high_px', 'low_px',
            'total_traded_base_asset_volume', 'total_traded_quote_asset_volume',
            'statistics_open_time', 'statistics_close_time', 
            'first_trade_id', 'last_trade_id', 'total_number_of_trades', 'date'
        ]
        df = df.apply(pd.to_numeric, errors='ignore')
        #print(df[['first_trade_id', 'last_trade_id']])
        #df.to_clickhouse('binance_datas', index=False, connection=client)
        client.execute('INSERT INTO binance_symbol_ticker_stream VALUES', df.values.tolist())
        DF_LIST = []  # Clear the buffer


def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    # Subscribe to the trade stream for BTCUSDT pair.
    payload = {
        "method": "SUBSCRIBE",
        "params": ["!ticker@arr"],
        "id": 1,
    }
    ws.send(json.dumps(payload))

def run_websocket():
    endpoint = "wss://stream.binance.us:9443/ws"
    
    ws = websocket.WebSocketApp(
        endpoint, on_message=on_message, on_error=on_error, on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()

if __name__ == "__main__":
    logging.info("Starting script")
    try:
        run_websocket()
    except Exception as e:
        logging.error(f"Error occurred: {e}")