from datetime import datetime
import statsmodels.tsa.stattools as ts
from get_data import get_daily_price_data_from_db
import numpy as np
from hurst import compute_Hc



symbol=input("insert symbol ")

start_date_to_query = '2021-01-01'
end_date_to_query = '2023-11-16'
asset= get_daily_price_data_from_db(symbol, start_date_to_query, end_date_to_query)

test = ts.adfuller(asset['close_price'], 1)
print("ADF Test results for 'close_price':")
print("ADF Statistic:", test[0])
print("p-value:", test[1])
print("Critical Values:", test[4])
 
asset['adj_close_price'] = asset['adj_close_price'].apply(lambda x: float(x))
asset_prices = np.array(asset['adj_close_price'])


H, c, data = compute_Hc(asset_prices, kind='price')



print("Hurst("+symbol+"): %s" % H)



