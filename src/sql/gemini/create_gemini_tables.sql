CREATE TABLE gemini_multimarketdata_trades
(
    type String,
    tid UInt64,
    price Float64,
    amount Float64,
    makerSide String,
    symbol String,
    eventId UInt64,
    socket_sequence UInt64,
    timestamp UInt64,
    timestampms UInt64,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (timestamp, symbol);