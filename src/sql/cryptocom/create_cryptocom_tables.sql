CREATE TABLE cryptocom_trade_data_stream
(
    side String,
    trade_price Float64,
    trade_qty Float64,
    trade_timestmp UInt64,
    trade_id UInt64,
    symbol String,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (trade_timestmp, symbol);