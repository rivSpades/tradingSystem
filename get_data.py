import requests
import pandas as pd
from io import StringIO
import mysql.connector as mdb
import datetime
import yfinance as yf
from datetime import timedelta

api_key = 'HOG8WJ5U6FDNIBQD'
endpoint = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo'




api_key_crypto='6b515a38920034e96cf7f221695cc4e16a17d7b57a6358d3a5749c2a1ed1c50e' 
url_crypto = f'https://min-api.cryptocompare.com/data/v4/all/exchanges?e=Binance&api_key={api_key_crypto}'
# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'admin_securities'
db_pass = '#Benfica4ever'
db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

def get_symbol_id(ticker):
    cursor = con.cursor()
    sql = "SELECT id FROM symbol WHERE ticker ='"+ticker+"'"
    cursor.execute(sql)
    result = cursor.fetchone()
    
    # Fetch all remaining rows (if any)
    cursor.fetchall()

    cursor.close()  # Close the cursor after fetching result

    return list(result)[0] if result else None


def get_data_vendor_id(vendor):
    with con.cursor() as cursor:
        sql = "SELECT id FROM data_vendor WHERE name = '"+vendor+"'"
        cursor.execute(sql)
        result = cursor.fetchone()

    return list(result)[0] if result else None



def get_symbols_from_vendor():
    # Send a GET request to the API
    response = requests.get(endpoint)

    # Check if the request was successful
    if response.status_code == 200:
        # Read the CSV data from the response content
        csv_data = StringIO(response.text)

        # Create a DataFrame from the CSV data
        df = pd.read_csv(csv_data)
  
        # Filter for rows with 'status' as 'Active' and 'assetType' as 'stock'
        active_stocks = df[(df['status'] == 'Active') & (pd.isna(df['delistingDate']))]

        # Replace NaN values with None
        active_stocks = active_stocks.where(pd.notnull(active_stocks), None)
        return active_stocks
        
       # cursor = con.cursor()                
        #fetch_stock_data_from_vendor(ticker,'Yahoo Finance',True)     
            
            
            
 
            
        #con.commit()
        #cursor.close()

def get_crypto_symbols_from_vendor():
    
    response = requests.get(url_crypto)
    
    if response.status_code == 200:
        data = response.json()
        binance_pairs = []

        # Filter for BTC pairs in Binance
        binance_data = data['Data']['exchanges']['Binance']['pairs']
        for fsym, pair_data in binance_data.items():
            for tsym in pair_data['tsyms']:
                if tsym == 'BTC':
                    binance_pairs.append(f"{fsym}-{tsym}")

        return binance_pairs
    else:
        print("Failed to fetch data from CryptoCompare")
        return []


def insert_symbols_in_db():
    cursor = con.cursor()    
    symbols=get_symbols_from_vendor()
    for index, row in symbols.iterrows():
        
        ticker = row['symbol']
        print(ticker)
        if ticker is None:
            continue  # Skip rows with a null ticker

        cursor.execute("SELECT ticker FROM symbol WHERE ticker = %s", (ticker,))
        existing_symbol = cursor.fetchone()
        cursor.fetchall()    
       
        if existing_symbol:
            continue  # Skip insertion if symbol already exists

        instrument = row['assetType']
        name = row['name']
        sector = None  # Replace this with the actual sector value if available in your DataFrame
        currency = None  # Replace this with the actual currency value if available in your DataFrame
        created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert data into the table
        sql = "INSERT INTO symbol (ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (ticker, instrument, name, sector, currency, created_date, last_updated_date)
        
        cursor.execute(sql, val)
        
    con.commit()
    cursor.close()


def insert_crypto_symbols_in_db():
    symbols = get_crypto_symbols_from_vendor()
    with con.cursor() as cursor:
        for pair in symbols:
            ticker, tsym = pair.split('-')
            cursor.execute("SELECT ticker FROM symbol WHERE ticker = %s", (ticker,))
            existing_symbol = cursor.fetchone()
            
            if existing_symbol:
                continue  # Skip insertion if symbol already exists

            instrument = 'cryptocurrency'
            name = ticker  # You might want to get the full name from another API
            sector = 'Cryptocurrency'
            currency = tsym
            created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            sql = """
                INSERT INTO symbol (ticker, instrument, name, sector, currency, created_date, last_updated_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            val = (pair, instrument, name, sector, currency, created_date, last_updated_date)
            cursor.execute(sql, val)
            
        con.commit()

import datetime

def get_daily_price_from_vendor(symbol, data_vendor, start_date, end_date=None):
    data_vendor_id = get_data_vendor_id(data_vendor)
    symbol_id = get_symbol_id(symbol)

    if data_vendor_id is None or symbol_id is None:
        print("Data vendor or symbol not found.")
        return False

    if end_date is None:
        end_date = datetime.date.today().strftime('%Y-%m-%d')

    if data_vendor == 'Yahoo Finance':
        stock_data = yf.download(symbol, start=start_date, end=end_date)
        stock_data.reset_index(inplace=True)
        stock_data['Date'] = pd.to_datetime(stock_data['Date']).dt.strftime('%Y-%m-%d')

    elif data_vendor == 'Alphavantage':
        endpoint = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
        response = requests.get(endpoint)

        if response.status_code == 200:
            try:
                data = response.json()['Time Series (Daily)']
                stock_data = pd.DataFrame([(datetime.datetime.strptime(date, '%Y-%m-%d'), float(values['1. open']), float(values['2. high']),
                                             float(values['3. low']), float(values['4. close']), 0, float(values['5. volume']))
                                            for date, values in data.items()], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

                # Filter data based on the specified date range
                stock_data = stock_data[(stock_data['Date'] >= start_date) & (stock_data['Date'] <= end_date)]

            except KeyError:
                print(f"No daily price data found for {symbol} on Alpha Vantage.")
                return pd.DataFrame()
        else:
            print(f"Failed to fetch data from Alpha Vantage for {symbol}")
            return pd.DataFrame()
        
        
    if stock_data.empty:
        print(f"No data available for {symbol} on {data_vendor}.")
        return pd.DataFrame()
    
    return stock_data

def get_crypto_daily_price_from_vendor(symbol, start_date, end_date=None):
    fsym, tsym = symbol.split('-')
    url = f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={fsym}&tsym={tsym}&limit=2000&api_key={api_key_crypto}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()['Data']['Data']
        prices = pd.DataFrame(data)
        prices['time'] = pd.to_datetime(prices['time'], unit='s')
        prices.rename(columns={'time': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volumeto': 'Volume'}, inplace=True)
        prices['Adj Close'] = prices['Close']  # For cryptocurrencies, adjusted close is typically the same as close
        return prices[['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
    else:
        print(f"Failed to fetch daily prices for {symbol} from CryptoCompare")
        return pd.DataFrame()

def insert_daily_price_data_in_db(symbol, start_date, end_date=None):
    print(symbol)
    #data_vendor = 'Alphavantage'
    data_vendor = 'Yahoo Finance'
    last_date = get_last_date_from_db(symbol)
    #first_date = get_first_date_from_db(symbol)
    if last_date:
        # Increment last_date by one day to set as the start_date
        start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    #stock_data = get_daily_price_from_vendor(symbol, 'Alphavantage', start_date, end_date)
    stock_data = get_daily_price_from_vendor(symbol, 'Yahoo Finance', start_date, end_date)

    if stock_data.empty:
        data_vendor = 'Yahoo Finance'
        stock_data = get_daily_price_from_vendor(symbol, 'Yahoo Finance', start_date, end_date)

        if stock_data.empty:
            print(f"No data available for {symbol} on {data_vendor}.")
            return False

    cursor = con.cursor()
    data_vendor_id = get_data_vendor_id(data_vendor)
    symbol_id = get_symbol_id(symbol)

    for index, row in stock_data.iterrows():
        price_date = row['Date']
        open_price = row['Open']
        high_price = row['High']
        low_price = row['Low']
        close_price = row['Close']
        adj_close_price = row['Adj Close']
        volume = row['Volume']

        created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = "INSERT INTO daily_price (data_vendor_id, exchange_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (data_vendor_id, 1, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
        cursor.execute(sql, val)

    con.commit()
    cursor.close()

def insert_crypto_daily_price_data_in_db(symbol, start_date, end_date=None):
    print(symbol)
    
    last_date = get_last_date_from_db(symbol)
    if last_date:
        start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    crypto_data = get_crypto_daily_price_from_vendor(symbol, start_date, end_date)

    if crypto_data.empty:
        print(f"No data available for {symbol}.")
        return False

    cursor = con.cursor()
    data_vendor_id = get_data_vendor_id('CryptoCompare')  # Assuming CryptoCompare as data vendor
    symbol_id = get_symbol_id(symbol)

    for index, row in crypto_data.iterrows():
        price_date = row['Date']
        open_price = row['Open']
        high_price = row['High']
        low_price = row['Low']
        close_price = row['Close']
        adj_close_price = row['Adj Close']
        volume = row['Volume']

        created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = """INSERT INTO daily_price (data_vendor_id, exchange_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        val = (data_vendor_id, 1, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
        cursor.execute(sql, val)

    con.commit()
    cursor.close()    


def insert_daily_price_data_for_all_symbols(start_date, end_date=None):
    symbols = get_symbols_from_db()
    for symbol, instrument in symbols:
        if instrument == 'cryptocurrency':
            insert_crypto_daily_price_data_in_db(symbol, start_date, end_date)
        else:
            insert_daily_price_data_in_db(symbol, start_date, end_date)
        

def get_last_date_from_db(symbol):
    symbol_id=get_symbol_id(symbol)
    with con.cursor() as cursor:
        sql = "SELECT MAX(price_date) FROM daily_price WHERE symbol_id = %s"
        cursor.execute(sql, (symbol_id,))
        result = cursor.fetchone()
     
    return result[0] if result[0] else None

def get_first_date_from_db(symbol):
    symbol_id=get_symbol_id(symbol)
    with con.cursor() as cursor:
        sql = "SELECT MIN(price_date) FROM daily_price WHERE symbol_id = %s"
        cursor.execute(sql, (symbol_id,))
        result = cursor.fetchone()
     
    return result[0] if result[0] else None



def get_daily_price_from_db(symbol, start_date, end_date=None):
    with con.cursor() as cursor:
        # Convert start_date to datetime if it's a string
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')

        # Set end_date to today's date if not specified
        if end_date is None:
            end_date = get_last_date_from_db(symbol)
        
        first_date = get_first_date_from_db(symbol)
        if first_date is None:
            print(f"No data available for {symbol} in the database.")
            return pd.DataFrame()  # Return an empty DataFrame

        if start_date < first_date:
            start_date = first_date

        sql = """
        SELECT price_date, open_price, high_price, low_price, close_price, adj_close_price, volume 
        FROM daily_price 
        WHERE symbol_id = %s AND price_date BETWEEN %s AND %s
        ORDER BY price_date ASC
        """
        cursor.execute(sql, (get_symbol_id(symbol), start_date, end_date))
        result = cursor.fetchall()

    if result:
        columns = ['price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']
        df = pd.DataFrame(result, columns=columns)
       
        return df
    else:
        print(f"No data available for {symbol} in the specified date range.")
        return pd.DataFrame()  # Return an empty DataFrame when there's no data



def get_symbols_from_db():
    con = mdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
    symbols = []

    with con.cursor() as cursor:
        sql = "SELECT ticker, instrument FROM symbol"
        cursor.execute(sql)
        symbols = [(row[0], row[1]) for row in cursor.fetchall()]

    con.close()

    return symbols



#insert_daily_price_data_for_all_symbols("2013-01-01")
def main():

    insert_daily_price_data_for_all_symbols("2013-01-01")
    #insert_daily_price_data_in_db("IRWD", "2013-01-01")
    #crypto=get_crypto_symbols_from_vendor()
    #print(crypto)
    #insert_crypto_symbols_in_db()
    #print(get_crypto_daily_price_from_vendor("BNB-BTC","2013-01-01"))
    #insert_crypto_daily_price_data_in_db("BNB-BTC","2013-01-01")


#main()