CREATE TABLE cryptocom_market_trades_stream
(
    trade_id UInt64,
    trade_timestmp UInt64,
    trade_price Float64,
    trade_qty Float64,
    side String,
    symbol String,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (trade_timestmp, symbol);