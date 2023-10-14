CREATE TABLE binance_symbol_ticker_stream
(
    event_type String,
    event_time UInt64,
    symbol String,
    px_change Float64,
    px_change_percent Float64,
    weighted_average_px Float64,
    first_trade_f_1_px Float64,
    last_px Float64,
    last_quantity Float64,
    best_bid_px Float64,
    best_bid_quantity Float64,
    best_ask_px Float64,
    best_ask_quantity Float64,
    open_px Float64,
    high_px Float64,
    low_px Float64,
    total_traded_base_asset_volume Float64,
    total_traded_quote_asset_volume Float64,
    statistics_open_time UInt64,
    statistics_close_time UInt64,
    first_trade_id UInt64,
    last_trade_id UInt64,
    total_number_of_trades UInt64
) ENGINE = MergeTree()
ORDER BY (event_time, symbol);
