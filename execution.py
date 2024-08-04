import pandas as pd
import numpy as np
from datetime import datetime
from strategies.mean_reverting import long
from get_data import get_symbols_from_db, get_daily_price_from_db, get_symbol_id
from connection import connect_db
import mysql.connector as mdb
from portfolio import calc_betsize
from decimal import Decimal
total_capital = 100000
def get_active_symbols_and_strategies():
    # Connect to the database
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Query to get active symbols and strategies where slot is free
        query = """
            SELECT 
                a.symbol_id, 
                s.ticker, 
                a.strategy_id, 
                a.slot_free
            FROM 
                Assets_Strategies a
            JOIN 
                symbol s ON a.symbol_id = s.id
            WHERE 
                a.strategy_active = 1 OR a.slot_free = 0;
        """
        
        cursor.execute(query)
        result = cursor.fetchall()

        # Convert the result to a DataFrame for easier manipulation (optional)
        df = pd.DataFrame(result, columns=['symbol_id', 'symbol', 'strategy_id', 'slot_free'])

        return df

    except mdb.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        cursor.close()
        conn.close()

# Example usage:
def check_trades(df):
    conn = connect_db()
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        symbol_id = row['symbol_id']
        symbol_name = row['symbol']
        strategy_id = row['strategy_id']
        slot_free = row['slot_free'] == 1  # Convert slot_free to boolean

        print(f"Processing Symbol ID: {symbol_id}, Strategy ID: {strategy_id}, Slot Free: {slot_free}")

        # Get daily price data for the symbol
        df_prices = get_daily_price_from_db(symbol_name, "2013-01-01")
        signal = None

        if 'close_price' in df_prices.columns:
            signal = long(df_prices, not slot_free)
            print(f"Signal for Symbol {symbol_name}: {signal}")
        else:
            print(f"No 'close_price' column in data for {symbol_name}.")
            continue

        if signal == 'Long' and slot_free:
            # Update the slot_free in the database
            update_slot_free_query = """
                UPDATE Assets_Strategies
                SET slot_free = 0
                WHERE symbol_id = %s AND strategy_id = %s;
            """
            cursor.execute(update_slot_free_query, (symbol_id, strategy_id))
            
            # Calculate the quantity based on bet size
            buyprice = df_prices['close_price'].iloc[-1] if not df_prices.empty else None
            bet_size_percentage = calc_betsize(symbol_name,strategy_id)
            allocated_capital = total_capital * (bet_size_percentage / 100)
            quantity = allocated_capital / float(buyprice) if buyprice else 0
            if (quantity==0):
                print("error: quantity is zero")
                continue
            # Create a record in the execute_orders table
            insert_execute_order_query = """
                INSERT INTO execute_orders (symbol_id, action, buyprice, signal_date, created_date, strategy_id, quantity, betsize)
                VALUES (%s, 'Long', %s, %s, NOW(), %s, %s, %s);
            """
            cursor.execute(insert_execute_order_query, (symbol_id, buyprice, datetime.now().date(), strategy_id, quantity, bet_size_percentage))
            
            conn.commit()  # Commit changes
            print(f"Inserted Long order for Symbol {symbol_name}")

            # Create a dummy record in the cashout_orders table
            insert_cashout_order_query = """
                INSERT INTO cashout_orders (id_exec_order, symbol_id, sellprice, signal_date, created_date, id_strategy, quantity)
                VALUES (LAST_INSERT_ID(), %s, NULL, %s, NOW(), %s, %s);
            """
            cursor.execute(insert_cashout_order_query, (symbol_id, datetime.now().date(), strategy_id, quantity))
            
            conn.commit()  # Commit changes
            print(f"Inserted dummy Cashout order for Symbol {symbol_name}")
        
        elif signal == 'Exit' and not slot_free:
            # Update the slot_free in the database
            print("entra AQUI")
            update_slot_free_query = """
                UPDATE Assets_Strategies
                SET slot_free = 1
                WHERE symbol_id = %s AND strategy_id = %s;
            """
            cursor.execute(update_slot_free_query, (symbol_id, strategy_id))
            
            # Obtain the id_exec from the most recent Long order
            select_exec_order_query = """
                SELECT id FROM execute_orders
                WHERE symbol_id = %s AND action = 'Long' AND strategy_id = %s
                ORDER BY created_date DESC
                LIMIT 1;
            """
            cursor.execute(select_exec_order_query, (symbol_id, strategy_id))
            exec_order_id = cursor.fetchone()
            
            if exec_order_id:
                exec_order_id = exec_order_id[0]
                profit_loss = (sellprice - buyprice) * quantity if sellprice and buyprice else None
                # Update the cashout_orders table
                update_cashout_order_query = """
                    UPDATE cashout_orders
                    SET sellprice = %s, profit_loss = %s
                    WHERE id_exec_order = %s;
                """
                sellprice = df_prices['close_price'].iloc[-1] if not df_prices.empty else None
                cursor.execute(update_cashout_order_query, (sellprice, profit_loss, exec_order_id))
                
                conn.commit()  # Commit changes
                print(f"Updated Cashout order for Symbol {symbol_name}")
            else:
                print(f"No matching Long order found for Symbol {symbol_name}.")

    cursor.close()
    conn.close()

# Example usage:
if __name__ == "__main__":
    assets_strategies_df = get_active_symbols_and_strategies()
    if assets_strategies_df is not None and not assets_strategies_df.empty:
        print("Active Symbols and Strategies:")
        check_trades(assets_strategies_df)
    else:
        print("No active symbols and strategies found.")