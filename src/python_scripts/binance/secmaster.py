#!/usr/bin/env python

import pandas as pd
import requests
from datetime import datetime


def get_exchange_info():
    url = "https://api.binance.us/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()
    return data


def process_symbol_data(data):
    # Convert filters list to DataFrame
    df = pd.DataFrame(data["filters"])

    # Broadcast top-level keys across the DataFrame
    for key, value in data.items():
        if key != "filters":
            if isinstance(value, list):
                df[key] = ", ".join(value)
            else:
                df[key] = value

    return df


def main():
    data = get_exchange_info()

    # Process each dictionary in the list
    dfs = [process_symbol_data(symbol) for symbol in data["symbols"]]

    # Concatenate all the individual DataFrames into one
    result_df = pd.concat(dfs, ignore_index=True)

    current_date = datetime.now().strftime("%Y%m%d")
    filename = f"binance_secmaster_{current_date}.csv"
    result_df.to_csv(f"~/develop/data/binance/refdata/{filename}", index=False)


if __name__ == "__main__":
    main()
