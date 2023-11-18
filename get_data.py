import requests
import pandas as pd
from io import StringIO
import mysql.connector as mdb
import datetime
import yfinance as yf
from datetime import timedelta

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
            
 
            fetch_stock_data(ticker,'Yahoo Finance',True)     
        con.commit()
        cursor.close()






def fetch_stock_data(symbol, data_vendor, insert=False):
    data_vendor_id = get_data_vendor_id(data_vendor)
    symbol_id = get_symbol_id(symbol)
    endpoint = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
    if data_vendor_id is None or symbol_id is None:
        print("Data vendor or symbol not found.")
        return False

    # Check the last available date in the database for the given stock
    last_date_in_database = get_last_date_from_db(symbol_id)

    if data_vendor == 'Yahoo Finance':
        stock_data = yf.download(symbol, start=last_date_in_database + timedelta(days=1) if last_date_in_database and insert==True else "2020-01-01", end=datetime.datetime.now().strftime('%Y-%m-%d'))
        stock_data.reset_index(inplace=True)
       
        stock_data['Date'] = pd.to_datetime(stock_data['Date']).dt.strftime('%Y-%m-%d')

    elif data_vendor == 'Alphavantage':
        
        response = requests.get(endpoint)

  
     

        if response.status_code == 200:
            try:
                data = response.json()['Time Series (Daily)']

                stock_data = pd.DataFrame([(datetime.datetime.strptime(date, '%Y-%m-%d'), float(values['1. open']), float(values['2. high']),
                                             float(values['3. low']), float(values['4. close']), 0, float(values['5. volume']))
                                            for date, values in data.items()], columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

                # Filter data based on the last date in the database
                if last_date_in_database:
                    stock_data = stock_data[stock_data['Date'] > last_date_in_database]

            except KeyError:
                print(f"No daily price data found for {symbol} on Alpha Vantage.")
                return False
        else:
            print(f"Failed to fetch data from Alpha Vantage for {symbol}")
            return False



    if stock_data.empty:
        print(f"No data available for {symbol} on {data_vendor}.")
        return False


 
    if insert:
        print(f"Inserting data for {symbol} from {data_vendor}")
        for index, row in stock_data.iterrows():
            price_date = row['Date']
            open_price = row['Open']
            high_price = row['High']
            low_price = row['Low']
            close_price = row['Close']
            adj_close_price = row['Adj Close']
            volume = row['Volume']

            insert_daily_price_data(data_vendor_id, symbol_id, price_date, open_price, high_price, low_price, close_price, adj_close_price, volume)

    return stock_data


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




def get_last_date_from_db(symbol_id):
    with con.cursor() as cursor:
        sql = "SELECT MAX(price_date) FROM daily_price WHERE symbol_id = %s"
        cursor.execute(sql, (symbol_id,))
        result = cursor.fetchone()
     
    return result[0] if result[0] else None



def get_daily_price_data_from_db(symbol, start_date, end_date):
    with con.cursor() as cursor:
        sql = "SELECT price_date, open_price, high_price, low_price, close_price, adj_close_price, volume FROM daily_price WHERE symbol_id = %s AND price_date BETWEEN %s AND %s"
        cursor.execute(sql, (get_symbol_id(symbol), start_date, end_date))
        result = cursor.fetchall()

    if result:
        columns = ['price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']
        df = pd.DataFrame(result, columns=columns)
        print(df)
        return df
    else:
        print(f"No data available for {symbol} in the specified date range.")
        return None

#get_symbols()


#get_last_date_from_db(7628)

# Example usage:
#symbol_to_query = 'AAPL'
#start_date_to_query = '2023-01-01'
#end_date_to_query = '2023-11-16'

#get_daily_price_data_from_db(symbol_to_query, start_date_to_query, end_date_to_query)
