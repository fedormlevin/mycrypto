CREATE TABLE coinbase_market_trades_stream
(
    symbol String,
    trade_id UInt64,
    price Float64,
    size Float64,
    time UInt64,
    side String,
    channel String,
    timestamp UInt64,
    sequence_num UInt64,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (time, symbol);