import pandas as pd
import yfinance
import statsmodels.tsa.stattools as ts
from hurst import compute_Hc
from get_data import get_symbols_from_db,get_daily_price_from_db
import numpy as np



#Statistics/Testing
def _check_adf(symbol, start_date, end_date=None):
    
    df=get_daily_price_from_db(symbol,start_date,end_date)
    if df.empty:
        print(f"No data available for {symbol} in the specified date range.")
        return False

    if 'close_price' not in df.columns:
        print("DataFrame doesn't contain a column named 'close_price'.")
        return False

    df['close_price'] = df['close_price'].astype(float)
    if df['close_price'].std() == 0:
        print(f"The close_price for {symbol} is constant, skipping ADF test.")
        return False
    
    try:
        results = ts.adfuller(df['close_price'], 1)
    except:
        return False 
    critical_value = results[0]
    p_value = results[1]
    nr_datapoints = results[3]
    t_value_1 = results[4]['1%']
    t_value_2 = results[4]['5%']
    t_value_3 = results[4]['10%']

    print("\nADF Statistic:", critical_value)
    print("p-value:", p_value)
    print("Number of data points used:", nr_datapoints)
    print("Critical Values:")
    print(f"\t1%: {t_value_1}")
    print(f"\t5%: {t_value_2}")
    print(f"\t10%: {t_value_3}")

    if p_value < 0.05 and critical_value < t_value_1 and critical_value < t_value_2 and critical_value < t_value_3:
        
        return True
    else:
        
        return False

def _check_hurst(symbol, start_date, end_date=None):
    df=get_daily_price_from_db(symbol,start_date,end_date)
    df['close_price'] = df['close_price'].apply(lambda x: float(x))
    asset_prices = np.array(df['close_price'])
    if np.any(asset_prices == 0):
        print(f"Warning: Zero values found in {symbol} asset prices. Removing them.")
        asset_prices = asset_prices[asset_prices != 0]
    if len(asset_prices) < 100:
        print(f"Series length for {symbol} is less than 100.")
        return False    
    try:
        H, _, _ = compute_Hc(asset_prices, kind='price')
        print(f"Hurst(): {H}")
        return H < 0.5
    except:
        print(f"Error: Floating point error encountered in compute_Hc for {symbol}.")
        return False

def _check_stationary(symbol, start_date, end_date=None):
    adf_result = _check_adf(symbol,start_date,end_date)
    
     
   
    if(adf_result):
      hurst_result = _check_hurst(symbol,start_date,end_date)
    else:
      print(symbol + " series is not stationary , adf fail")
      return False  
    if not hurst_result:
       print(symbol + " series is not stationary , hurst fail")
    if(adf_result and hurst_result):    
        print(symbol + " series is stationary!")
    return adf_result and hurst_result



def output_results(start_date):
    stationary_symbols = []

    symbols = get_symbols_from_db()
    for symbol in symbols:
        if check_stationary(symbol, start_date):
            stationary_symbols.append(symbol)

    if stationary_symbols:
        # Create a DataFrame with the stationary symbols
        stationary_df = pd.DataFrame({'Symbol': stationary_symbols})

        # Export the DataFrame to a CSV file
        stationary_df.to_csv('stationary_symbols.csv', index=False)

        #print("Stationary symbols have been saved to stationary_symbols.csv")
    #else:
        #print("No stationary symbols found.")


#print(check_stationary("IRWD",start_date="2013-01-01"))
#output_results("2013-01-01")



#Real Strategy

def check_adf(df):
    
   
    if df.empty:
        
        return False

    if 'close_price' not in df.columns:
        #print("DataFrame doesn't contain a column named 'close_price'.")
        return False

    df['close_price'] = df['close_price'].astype(float)
    if df['close_price'].std() == 0:
      
        return False
    
    try:
        results = ts.adfuller(df['close_price'], 1)
    except:
        return False 
    critical_value = results[0]
    p_value = results[1]
    nr_datapoints = results[3]
    t_value_1 = results[4]['1%']
    t_value_2 = results[4]['5%']
    t_value_3 = results[4]['10%']

    #print("\nADF Statistic:", critical_value)
    #print("p-value:", p_value)
    #print("Number of data points used:", nr_datapoints)
    #print("Critical Values:")
    #print(f"\t1%: {t_value_1}")
    #print(f"\t5%: {t_value_2}")
    #print(f"\t10%: {t_value_3}")

    if p_value < 0.05 and critical_value < t_value_1 and critical_value < t_value_2 and critical_value < t_value_3:
        
        return True
    else:
        
        return False
    


def check_adf_p_value(df):
    if df.empty:
        return False

    if 'close_price' not in df.columns:
        return False

    df['close_price'] = df['close_price'].astype(float)
    if df['close_price'].std() == 0:
        return False
    
    try:
        results = ts.adfuller(df['close_price'], 1)
    except:
        return False 

    p_value = results[1]

    # Check if the p-value is greater than 0.5
    if p_value > 0.5:
        return False
    else:
        return True


def check_hurst(df):
   
    df['close_price'] = df['close_price'].apply(lambda x: float(x))
    asset_prices = np.array(df['close_price'])
    if np.any(asset_prices == 0):
        
        asset_prices = asset_prices[asset_prices != 0]
    if len(asset_prices) < 100:
        
        return False    
    try:
        H, _, _ = compute_Hc(asset_prices, kind='price')
        #print(f"Hurst(): {H}")
        return H < 0.5
    except:
        
        return False    
    

def check_stationary(df):
    adf_result = check_adf(df)
    
     
   
    if(adf_result):
      hurst_result = check_hurst(df)
    else:
      #print("series is not stationary , adf fail")
      return False  
    #if not hurst_result:
       #print("series is not stationary , hurst fail")
    #if(adf_result and hurst_result):    
        #print("series is stationary!")
    return adf_result and hurst_result    

def moving_average(df, n):
    """Calculate the moving average for the given data.

    :param df: pandas.DataFrame
    :param n:
    :return: pandas.DataFrame
    """
    MA = pd.Series(df['close_price'].rolling(n, min_periods=n).mean(), name='MA_' + str(n))
    df = df.join(MA)
    return df

def calculate_ratio(df):
    ma = '100'
    df = moving_average(df, int(ma))
    
    # Ensure 'close_price' and 'MA_'+ma are numeric types
    df['close_price'] = pd.to_numeric(df['close_price'], errors='coerce')
    df['MA_'+ma] = pd.to_numeric(df['MA_'+ma], errors='coerce')

    # Check for NaN values after conversion

    
    df['Ratio'] = df['close_price'] / df['MA_'+ma]
    
    return df

def execute(df,buy=False,last_action=''):
  df= calculate_ratio(df)
  if df['Ratio'].dropna().empty:
      return 'None'
      
  #print(df)
  percentiles=[5,10,50,90,95]
  p=np.percentile(df['Ratio'].dropna(),percentiles)

  #print(df)
  
  if buy==False and df['Ratio'].iloc[-1]<=p[0]  and check_stationary(df):
      print("Buying Long")
      #print( df.tail(10))
      #print(p)
      
      return 'Long'

  elif buy==False and df['Ratio'].iloc[-1]>=p[-1]  and check_stationary(df):
      print("Buying Short")
      #print( df.tail(10))
      #print(p)      
      return 'Short'

  elif buy==True and  df['Ratio'].iloc[-1]>=p[2]  and last_action=="Long" : 
      print("Exiting")
      #print( df.tail(10)) 
      #print(p)
      
      return 'Exit'

  elif buy==True and  df['Ratio'].iloc[-1]<=p[2] and last_action=="Short":
      print("Exiting")
      #print( df.tail(10))
      #print(p)

      return 'Exit'      
  else:
      return 'None'
        