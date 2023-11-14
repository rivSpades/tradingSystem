import requests
import pandas as pd
from io import StringIO
import mysql.connector as mdb
import datetime
import yfinance as yf

api_key = 'HOG8WJ5U6FDNIBQD'
endpoint = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo'

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

    cursor.close()  # Close the cursor after fetching results
    print(result)
    print("passa aqui")
    return list(result)[0] if result else None


def get_data_vendor_id(vendor):
    with con.cursor() as cursor:
        sql = "SELECT id FROM data_vendor WHERE name = '"+vendor+"'"
        cursor.execute(sql)
        result = cursor.fetchone()

    return list(result)[0] if result else None



def get_symbols():
    # Send a GET request to the API
    response = requests.get(endpoint)

    # Check if the request was successful
    if response.status_code == 200:
        # Read the CSV data from the response content
        csv_data = StringIO(response.text)

        # Create a DataFrame from the CSV data
        df = pd.read_csv(csv_data)
        print(df)
        # Filter for rows with 'status' as 'Active' and 'assetType' as 'stock'
        active_stocks = df[(df['status'] == 'Active') & (pd.isna(df['delistingDate']))]

        # Replace NaN values with None
        active_stocks = active_stocks.where(pd.notnull(active_stocks), None)

        cursor = con.cursor()
        for index, row in active_stocks.iterrows():
            ticker = row['symbol']
            if ticker is None:
                continue  # Skip rows with a null ticker

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
            print(ticker)
            try:
                fetch_data_alphavantage(ticker,True)
            except: 
                fetch_data_yahoo(ticker,True)    
        con.commit()
        cursor.close()



def get_daily_price_data(symbol):
    
    data_vendor_id = get_data_vendor_id()
    symbol_id = get_symbol_id(symbol)
    print(symbol_id)
    if data_vendor_id is None or symbol_id is None:
        print("Data vendor or symbol not found.")
        return

    endpoint= f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
    response = requests.get(endpoint)

    data = fetch_data_alphavantage(symbol, api_key)

    cursor = con.cursor()
    for date, values in data.items():
        price_date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
        created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        open_price = values['1. open']
        high_price = values['2. high']
        low_price = values['3. low']
        close_price = values['4. close']
        adj_close_price =0
        volume = values['5. volume']

        # Insert data into the table
        sql = "INSERT INTO daily_price (data_vendor_id, exchange_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (data_vendor_id, 1, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
        cursor.execute(sql, val)

        con.commit()
        cursor.close()




    


def fetch_data_yahoo(symbol,insert=False):
# Retrieve historical daily price data from Yahoo Finance

    data_vendor_id = get_data_vendor_id('Yahoo Finance')
    symbol_id = get_symbol_id(symbol)
   
    if data_vendor_id is None or symbol_id is None:
        print("Data vendor or symbol not found.")
        return False

    #try:
        # Retrieve historical daily price data from Yahoo Finance
    stock_data = yf.download(symbol, start="2020-01-01", end=datetime.datetime.now().strftime('%Y-%m-%d'))    
   
    #print(stock_data)
    if stock_data.empty:
        print(f"No data available for {symbol} on Yahoo Finance.")
        return False
    if(insert):
        print(f"inserting with yahoo for {symbol} ")
        for index, row in stock_data.iterrows():
            price_date = index.strftime('%Y-%m-%d')
            open_price = row['Open']
            high_price = row['High']
            low_price = row['Low']
            close_price = row['Close']
            adj_close_price = row['Adj Close']
            volume = row['Volume']
            
            insert_daily_price_data(data_vendor_id, symbol_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume)

      
    #except Exception as e:
        #print(f"Error fetching data from Yahoo Finance for {symbol}: {e}")
       # return False


def fetch_data_alphavantage(symbol,insert=False):
    data_vendor_id = get_data_vendor_id('Alphavantage')
    symbol_id = get_symbol_id(symbol)
  
    if data_vendor_id is None or symbol_id is None:
        print("Data vendor or symbol not found.")
        return False

    endpoint = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
    response = requests.get(endpoint)

    if response.status_code == 200:
        #try:
        data = response.json()['Time Series (Daily)']
        
        if(insert):
            print(f"inserting with alpha for {symbol} ")    
            for date, values in data.items():
                price_date = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
                open_price = values['1. open']
                high_price = values['2. high']
                low_price = values['3. low']
                close_price = values['4. close']
                adj_close_price = 0
                volume = values['5. volume']
            
                insert_daily_price_data(data_vendor_id, symbol_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume)

        
        #except KeyError:
            #print(f"No daily price data found for {symbol} on Alpha Vantage.")
            #return False
    else:
        print(f"Failed to fetch data from Alpha Vantage for {symbol}")
        return False

def insert_daily_price_data(data_vendor_id, symbol_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume):
    cursor = con.cursor()
    created_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    last_updated_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Insert data into the table
    sql = "INSERT INTO daily_price (data_vendor_id, exchange_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (data_vendor_id, 1, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
    cursor.execute(sql, val)

    con.commit()
    cursor.close()

get_symbols()