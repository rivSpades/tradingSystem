from datetime import datetime
import statsmodels.tsa.stattools as ts
from get_data import get_daily_price_data_from_db,get_symbols_from_db
import numpy as np
from hurst import compute_Hc
import pandas as pd


def analyze_symbol(symbol, start_date='2020-01-01', end_date='2023-11-17'):
    # Query daily price data from the database
    asset = get_daily_price_data_from_db(symbol, start_date, end_date)

    if asset is None or asset.empty:
        print(f"No data available for {symbol}.")
        return

    # ADF Test for 'close_price' column
    adf_test = ts.adfuller(asset['close_price'], 1)
    print("ADF Test results for 'close_price':")
    print("ADF Statistic:", adf_test[0])
    print("p-value:", adf_test[1])
    print("Critical Values:", adf_test[4])

    # Convert 'adj_close_price' to float
    asset['adj_close_price'] = asset['adj_close_price'].apply(lambda x: float(x))
    asset_prices = np.array(asset['adj_close_price'])

    # Calculate the Hurst exponent
    H, _, _ = compute_Hc(asset_prices, kind='price')
    print(f"Hurst({symbol}): {H}")



def filter_stocks_mean_reverting():
    # Get all symbols from your database
    symbols = get_symbols_from_db()

    result_data = []

    for symbol in symbols:
        # Query daily price data from the database
        start_date_to_query = '2020-01-01'
        end_date_to_query = '2023-11-17'
        asset = get_daily_price_data_from_db(symbol, start_date_to_query, end_date_to_query)

        if asset is None or asset.empty:
            print(f"No data available for {symbol}. Skipping...")
            continue  # Skip symbols with no data

        # Check for constant time series
        if asset['close_price'].nunique() == 1:
            print(f"Constant data for {symbol}. Skipping...")
            continue  # Skip symbols with constant data

        # Check if the time series has sufficient data for ADF test
        if len(asset['close_price']) <= 1:
            print(f"Insufficient data for {symbol}. Skipping...")
            continue  # Skip symbols with insufficient data

        # Calculate the dynamic maxlag
        maxlag = min(len(asset['close_price']) // 2 - 2, 40)  # You can adjust the 40 based on your specific needs

        # ADF Test for 'close_price' column with dynamically set maxlag
        try:
            adf_test = ts.adfuller(asset['close_price'], maxlag=maxlag)
        except Exception as e:
            print(f"Error in ADF test for {symbol}: {e}")
            continue  # Skip symbols with ADF test errors

 
        if all(adf_test[0] >= critical_value for critical_value in adf_test[4].values()):
            print(f"ADF Statistic greater than or equal to all critical values for {symbol}. Skipping...")
            continue  # Skip symbols failing the criterion

        # Hurst exponent calculation
        asset_prices = asset['adj_close_price'].apply(lambda x: float(x))
        asset_prices = asset_prices[~pd.isna(asset_prices) & (asset_prices != 0)]

        if asset_prices.empty:
            print(f"No valid price data for {symbol}. Skipping...")
            continue  # Skip symbols with no valid price data

        try:
            H, _, _ = compute_Hc(asset_prices, kind='price')

            # Append result to the data
            result_data.append({
                'Symbol': symbol,
                'ADF_Statistic': adf_test[0],
                'ADF_p-value': adf_test[1],
                'Critical_Values': adf_test[4],
                'Hurst_Exponent': H
            })
        except Exception as e:
            print(f"Error calculating Hurst exponent for {symbol}: {e}")
            continue  # Skip symbols with calculation errors

    # Convert the result to a pandas DataFrame
    result_df = pd.DataFrame(result_data)

    # Filter stocks based on your criteria (e.g., ADF p-value < 0.05 and Hurst Exponent < 0.5 for mean-reverting)
    filtered_stocks = result_df[(result_df['ADF_p-value'] < 0.05) & (result_df['Hurst_Exponent'] < 0.5)]
    csv_filename = 'mean_reverting_stocks_results.csv'
    filtered_stocks.to_csv(csv_filename, index=False)
    return filtered_stocks








# Example usage:
check_stocks = filter_stocks_mean_reverting()
print( pd.DataFrame(check_stocks))
#symbols = get_symbols_from_db()
#print(symbols)

#analyze_symbol("BCH")
