import streamlit as st
import os
from packages.utils import run_clickhouse_query
from dotenv import load_dotenv
import pandas as pd
load_dotenv()

st.set_page_config(page_title="Market Data", page_icon="📢")

tables = [
    'binance_ticker_order_book_stream',
    'binance_trade_data_stream',
    'coinbase_market_trades_stream',
    'coinbase_top_of_book_stream',
    'cryptocom_trade_data_stream',
    'gemini_multimarketdata_trades',
    'kraken_ohlc_stream',
    'kraken_trade_data_stream',
]


sz_query = f"""
SELECT 
    name as table,
    size_readable,
    total_bytes*1e-9 as total_gb
FROM 
(
	SELECT 
        name,
        formatReadableSize(total_bytes) AS size_readable,
        total_bytes
	FROM system.tables
	WHERE database = 'mydb' AND name IN ({tables})
) AS sizes

"""
sizes = run_clickhouse_query('localhost', os.environ.get('ch_user'), 
                             os.environ.get('ch_psw'), 'mydb', sz_query)

df_lst = []
for t in tables:
    symbol = 'symbol'
    sym_query = f"""
SELECT DISTINCT {symbol}, COUNT(DISTINCT date) AS date_count, MAX(date) as last_date
from {t}
group by {symbol}
"""
    if 'kraken' in t:
        symbol='pair'
        
        sym_query = f"""
SELECT DISTINCT {symbol}, COUNT(DISTINCT date) AS date_count, MAX(date) as last_date
from {t}
group by {symbol}
"""
        
    df = run_clickhouse_query('localhost', 'default', 'myuser', 'mydb', sym_query)
    df = df.rename(columns={'pair': 'symbol'})
    df['table'] = t

    df_lst.append(df)

res = pd.concat(df_lst)
res = res.set_index('table').join(sizes.set_index('table'))

st.write("Market Data in ClickHouse")
st.dataframe(res, height=425)