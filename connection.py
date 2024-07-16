import mysql.connector as mdb

def connect_db():
    db_host = 'localhost'
    db_user = 'admin_securities'
    db_pass = '#Benfica4ever'
    db_name = 'securities_master'

    try:
        conn = mdb.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        print("Connected to the database successfully")
        return conn
    except mdb.Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None
