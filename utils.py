def insert_data_to_db(dataframe, connection):
    cursor = connection.cursor()
    for index, row in dataframe.iterrows():
        ticker = row['symbol']
        instrument = row['assetType']
        name = row['name']
        sector = ''  # Replace this with the actual sector value if available in your DataFrame
        currency = ''  # Replace this with the actual currency value if available in your DataFrame
        created_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_updated_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Insert data into the table
        sql = "INSERT INTO symbol (ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (ticker, instrument, name, sector, currency, created_date, last_updated_date)
        cursor.execute(sql, val)

    connection.commit()
    cursor.close()