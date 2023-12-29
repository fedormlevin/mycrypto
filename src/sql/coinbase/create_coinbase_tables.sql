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


CREATE TABLE coinbase_top_of_book_stream
(
    symbol String,
    event_time UInt64,
    bid_px Float64,
    bid_sz Float64,
    offer_px Float64,
    offer_sz Float64,
    sequence_num UInt64,
    channel String,
    timestamp UInt64,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (event_time, symbol);

TRUNCATE TABLE coinbase_top_of_book_stream