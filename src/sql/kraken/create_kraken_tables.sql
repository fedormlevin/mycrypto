CREATE TABLE kraken_trade_data_stream
(
    price Float64,
    volume Float64,
    time Float64,
    side String,
    orderType String,
    channelName String,
    pair String,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY (time, pair);

CREATE TABLE kraken_ticker_stream
(
    best_ask_price Float64,
    best_bid_price Float64,
    close_price Float64,
    vol_today Float64,
    vol_wavg_price_today Float64,
    n_trades_today Int64,
    low_price_today Float64,
    high_price_today Float64,
    open_price_today Float64,
    ask_wholelot_vol Int64,
    bid_wholelot_vol Int64,
    close_lot_vol Float64,
    vol_24hrs Float64,
    vol_wavg_price_24hrs Float64,
    n_trades_24hrs Int64,
    low_price_24hrs Float64,
    high_price_24hrs Float64,
    open_price_24hrs Float64,
    ask_lot_vol Float64,
    bid_lot_vol Float64,
    ticker String,
    date Date,
    insert_time UInt64
) ENGINE = MergeTree()
ORDER BY ticker;