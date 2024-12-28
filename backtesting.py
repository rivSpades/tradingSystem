import pandas as pd
import numpy as np
from datetime import datetime
from strategies.mean_reverting import execute as execute_mean_reverting
from strategies.cointegration import execute as execute_cointegration
from get_data import get_symbols_from_db, get_daily_price_from_db,get_symbol_id,insert_daily_price_data_in_db
from connection import connect_db
import mysql.connector as mdb

def train_test_split(df, split_ratio=0.3):
    split_index = int(len(df) * split_ratio)
    current_data = df.iloc[:split_index]
    future_data = df.iloc[split_index:]
    return current_data, future_data

def delete_existing_entries(conn, strategy_id, symbol_id, coint_id=None):
    cursor = conn.cursor()

    # Build base query
    query_results = "DELETE FROM backtesting_results WHERE strategy_id = %s AND symbol_id = %s"
    query_trades = "DELETE FROM backtesting_trades WHERE strategy_id = %s AND symbol_id = %s"
    
    # Check if coint_id is provided and modify the query accordingly
    if coint_id:
        query_results += " AND coint_id = %s"
        query_trades += " AND coint_id = %s"
        params = (strategy_id, symbol_id, coint_id)
    else:
        params = (strategy_id, symbol_id)
    
    # Execute the deletion queries
    cursor.execute(query_trades, params)
    cursor.execute(query_results, params)
    
    # Commit the transaction
    conn.commit()

    print("Entries deleted successfully.")



def calculate_statistics(trade_log, price_data):
    if not trade_log:
        return {}

    total_pnl = sum(trade['profit_loss'] for trade in trade_log)  # Total PnL
    period_pnls = [trade['profit_loss'] for trade in trade_log]  # List of PnLs for each period
    number_of_periods = len(period_pnls)  # Total number of periods
    average_period_pnl = np.mean(period_pnls) if period_pnls else 0  # Average Period PnL

    max_period_profit = max(period_pnls) if period_pnls else 0  # Maximum Period Profit
    max_period_loss = min(period_pnls) if period_pnls else 0  # Maximum Period Loss
    profitable_periods = [pnl for pnl in period_pnls if pnl > 0]
    unprofitable_periods = [pnl for pnl in period_pnls if pnl <= 0]
    average_period_profit = np.mean(profitable_periods) if profitable_periods else 0  # Average Period Profit
    average_period_loss = np.mean(unprofitable_periods) if unprofitable_periods else 0  # Average Period Loss
    winning_periods = len(profitable_periods)  # Winning Periods
    losing_periods = len(unprofitable_periods)  # Losing Periods
    percentage_win_periods = (winning_periods / len(period_pnls) * 100) if period_pnls else 0  # Percentage Win Periods
    percentage_loss_periods = (losing_periods / len(period_pnls) * 100) if period_pnls else 0  # Percentage Loss Periods

    # Calculate ROI %
    total_investment = sum(1 for trade in trade_log if trade['trade_type'] in ['Long', 'Short'])  # Each trade represents a $1 investment
    roi = (total_pnl / total_investment) * 100 if total_investment != 0 else 0

    # Calculate Average Period PnL in %
    period_pnl_percentages = [(trade['profit_loss'] / (trade['price'])) * 100 if (trade['price']) != 0 else 0 for trade in trade_log]
    average_period_percentage = np.mean(period_pnl_percentages) if period_pnl_percentages else 0

    # Calculate Average Holding Period
    holding_periods = []
    max_drawdown = 0
    for i in range(1, len(trade_log)):
        if trade_log[i]['trade_type'] == 'Exit' and trade_log[i-1]['trade_type'] in ['Long', 'Short']:
            entry_price = trade_log[i-1]['price']
            if entry_price == 0:  # Ensure no division by zero
                print("entry price is zero, error")
                continue

            holding_data = price_data[(price_data['price_date'] >= trade_log[i-1]['trade_date']) & (price_data['price_date'] <= trade_log[i]['trade_date'])]
            if trade_log[i-1]['trade_type'] == 'Long':
                lowest_price = holding_data['low_price'].min()
                drawdown = ((entry_price - lowest_price) / entry_price) * 100  # Drawdown in percentage for Long trades
            elif trade_log[i-1]['trade_type'] == 'Short':
                highest_price = holding_data['high_price'].max()
                drawdown = ((highest_price - entry_price) / entry_price) * 100  # Drawdown in percentage for Short trades

            if drawdown > max_drawdown:
                max_drawdown = drawdown

            holding_periods.append((trade_log[i]['trade_date'] - trade_log[i-1]['trade_date']).days)

    average_holding_period = np.mean(holding_periods) if holding_periods else 0

    return {
        "total_pnl": total_pnl,  # Total Profit/Loss (PnL)
        "average_period_pnl": average_period_pnl,  # Average Period PnL
        "average_period_percentage": average_period_percentage,  # Average Period PnL in %
        "max_period_profit": max_period_profit,  # Maximum Period Profit
        "max_period_loss": max_period_loss,  # Maximum Period Loss
        "average_period_profit": average_period_profit,  # Average Period Profit
        "average_period_loss": average_period_loss,  # Average Period Loss
        "winning_periods": winning_periods,  # Winning Periods during strategy execution
        "losing_periods": losing_periods,  # Losing Periods during strategy execution
        "percentage_win_periods": percentage_win_periods,  # Percentage Win Periods
        "percentage_loss_periods": percentage_loss_periods,  # Percentage Loss Periods
        "roi": roi,  # ROI %
        "max_drawdown": max_drawdown,  # Maximum Drawdown in %
        "average_holding_period": average_holding_period  # Average Holding Period
    }


def insert_backtesting_results(conn, strategy_id, symbol_id, start_date, end_date, statistics, coint_id=None):
    cursor = conn.cursor()
    
    query = '''
        INSERT INTO backtesting_results (
            strategy_id, symbol_id, start_date, end_date, profit, drawdown, number_of_trades, win_rate,
            total_pnl, average_period_pnl, max_period_profit, max_period_loss,
            average_period_profit, average_period_loss, winning_periods, losing_periods,
            percentage_win_periods, percentage_loss_periods, average_holding_period, average_period_percentage,
            coint_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    
    # Handle cases where statistics may not be available or are empty
    cursor.execute(query, (
        strategy_id, symbol_id, start_date, end_date, statistics.get('total_pnl'), statistics.get('max_drawdown'),
        len(statistics), statistics.get('percentage_win_periods'), statistics.get('total_pnl'),
        statistics.get('average_period_pnl'), statistics.get('max_period_profit'), statistics.get('max_period_loss'),
        statistics.get('average_period_profit'), statistics.get('average_period_loss'), statistics.get('winning_periods'),
        statistics.get('losing_periods'), statistics.get('percentage_win_periods'), statistics.get('percentage_loss_periods'),
        statistics.get('average_holding_period'), statistics.get('average_period_percentage'),
        coint_id  # Include coint_id in the parameters
    ))
    
    conn.commit()



def check_backtesting_results_exists(conn, strategy_id, symbol_id_1, symbol_id_2):
    cursor = conn.cursor()
    
    # SQL query to check for the specified conditions
    query = """
        SELECT COUNT(*)
        FROM backtesting_results
        WHERE strategy_id = %s
        AND (
            (symbol_id = %s AND symbol_id_2 = %s)
            OR
            (symbol_id = %s AND symbol_id_2 = %s)
        )
    """
    
    # Execute query with the appropriate parameters
    cursor.execute(query, (strategy_id, symbol_id_1, symbol_id_2, symbol_id_2, symbol_id_1))
    
    # Fetch the result
    result = cursor.fetchone()
    
    # Return True if a matching record exists, otherwise False
    return result[0] > 0

def insert_trade_log(conn, strategy_id, symbol_id, trade_log, coint_id=None):
    cursor = conn.cursor()
    query = '''
        INSERT INTO backtesting_trades (
            strategy_id, symbol_id, trade_date, trade_type, price, profit_loss, max_drawdown, market_type, coint_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    
    for trade in trade_log:
        cursor.execute(query, (
            strategy_id, symbol_id, trade['trade_date'], trade['trade_type'], trade['price'], trade['profit_loss'],
            trade['max_drawdown'], trade.get('market_type', None), coint_id  # Include coint_id
        ))
    
    conn.commit()

def get_coint_id(conn, symbol_id_1, symbol_id_2):
    cursor = conn.cursor()

    # Query to check both combinations of symbol_id_1 and symbol_id_2
    query = '''
        SELECT id FROM cointegration 
        WHERE (symbol_id_1 = %s AND symbol_id_2 = %s) 
        OR (symbol_id_1 = %s AND symbol_id_2 = %s)
    '''
    
    # Execute the query with both possible combinations of the symbols
    cursor.execute(query, (symbol_id_1, symbol_id_2, symbol_id_2, symbol_id_1))
    result = cursor.fetchone()

    if result:
        return result[0]  # Return the id (coint_id)
    else:
        return None  # No matching record found


def insert_cointegration(conn, symbol_1_id, symbol_2_id):
    cursor = conn.cursor()

    # Check if the cointegration record already exists
    query_check = '''
        SELECT COUNT(*) 
        FROM cointegration 
        WHERE (symbol_id_1 = %s AND symbol_id_2 = %s) 
           OR (symbol_id_1 = %s AND symbol_id_2 = %s)
    '''
    cursor.execute(query_check, (symbol_1_id, symbol_2_id, symbol_2_id, symbol_1_id))
    result = cursor.fetchone()

    if result[0] > 0:
        print("Cointegration record already exists.")
        return

    # Insert into cointegration table if record doesn't exist
    query_insert = '''
        INSERT INTO cointegration (symbol_id_1, symbol_id_2) 
        VALUES (%s, %s)
    '''
    cursor.execute(query_insert, (symbol_1_id, symbol_2_id))
    
    # Commit the transaction
    conn.commit()

    print("Cointegration record inserted successfully.")


# Replace with actual values
def mean_reverting_test():
    strategy_id=1
    symbols = get_symbols_from_db()
    for symbol, instrument in symbols:
        print(symbol)
        #print(instrument)
        if instrument == 'cryptocurrency':
            continue

      

        symbol_id = get_symbol_id(symbol)
        conn = connect_db()
        if  check_backtesting_results_exists(conn, strategy_id, symbol_id):
            conn.close()
            print("already exists, skipping")
            continue
        df = get_daily_price_from_db(symbol, "2013-01-01")
    
        if df.empty: 
            print("df is empty")
            conn.close()
            continue
        current_data, future_data = train_test_split(df)


        buy = False
        trade_log = []
        signal="None"
        last_action=""
        for i in range(len(future_data.index)):
            current_data = pd.concat([current_data, pd.DataFrame(future_data.iloc[i]).transpose()], axis=0)
            signal = execute_mean_reverting(current_data, buy,last_action)

            if signal == 'Long' and not buy:
                entry_price = future_data['close_price'].iloc[i]
                lowest_price = entry_price
                highest_price = entry_price
                quantity = 1 / entry_price
                last_action='Long'
                buy = True
                trade_log.append({
                    'trade_date': future_data["price_date"].iloc[i],
                    'trade_type': 'Long',
                    'price': future_data['close_price'].iloc[i],
                    'quantity': quantity,
                    'profit_loss': 0,
                    'max_drawdown': 0,  # Initialize drawdown
                    'market_type': 'Long'
                })

            elif signal == 'Short' and not buy:
                entry_price = future_data['close_price'].iloc[i]
                highest_price = entry_price
                lowest_price = entry_price
                quantity = 1 / entry_price
                last_action='Short'
                buy = True
                trade_log.append({
                    'trade_date': future_data["price_date"].iloc[i],
                    'trade_type': 'Short',
                    'price': future_data['close_price'].iloc[i],
                    'quantity': quantity,
                    'profit_loss': 0,
                    'max_drawdown': 0,
                    'market_type': 'Short'
                })

            elif signal == "Exit" and buy and last_action=='Long':
                exit_price = future_data['close_price'].iloc[i]
                quantity = trade_log[-1]['quantity']
                profit_loss = (exit_price - trade_log[-1]['price']) * quantity
                drawdown = (entry_price - lowest_price) / entry_price * 100
                last_action=''
                buy = False
                trade_log.append({
                    'trade_date': future_data["price_date"].iloc[i],
                    'trade_type': 'Exit',
                    'price': future_data['close_price'].iloc[i],
                    'quantity': quantity,
                    'profit_loss': profit_loss,
                    'max_drawdown': drawdown,
                    'market_type': 'Long'
                })

            elif signal == "Exit" and buy and last_action=='Short':
                exit_price = future_data['close_price'].iloc[i]
                quantity = trade_log[-1]['quantity']
                profit_loss = (trade_log[-1]['price'] - exit_price) * quantity
                drawdown = (highest_price -entry_price) / entry_price * 100
                last_action=''
                buy = False
                trade_log.append({
                    'trade_date': future_data["price_date"].iloc[i],
                    'trade_type': 'Exit',
                    'price': future_data['close_price'].iloc[i],
                    'quantity': quantity,
                    'profit_loss': profit_loss,
                    'max_drawdown': drawdown,
                    'market_type': 'Short'
                })


            if buy:
                # Update the lowest price while the trade is active
                current_low_price = future_data['low_price'].iloc[i]
                current_high_price = future_data['high_price'].iloc[i]

                if current_low_price < lowest_price:
                    lowest_price = current_low_price

                if current_high_price > highest_price:
                    highest_price = current_high_price                    

        # Calculate statistics
        statistics = calculate_statistics(trade_log, df)
        if not statistics:
            print("No statistics, error")
            
            insert_backtesting_results(conn, strategy_id, symbol_id, current_data["price_date"].iloc[0], current_data["price_date"].iloc[-1], {})
            continue
    # Connect to the database
        

        
        delete_existing_entries(conn, strategy_id, symbol_id)

    # Insert backtesting results
        
        insert_backtesting_results(conn, strategy_id, symbol_id, current_data["price_date"].iloc[0], current_data["price_date"].iloc[-1], statistics)

    # Insert trade log
        insert_trade_log(conn, strategy_id, symbol_id, trade_log)

    # Close the connection
        conn.close()
    
     
        #conn.close()
                 

    #print(trade_log)
 
        print(statistics)

#mean_reverting_test()        


def cointegration_test():

    strategy_id=2
    symbols = get_symbols_from_db()

    for symbol_1, instrument_1 in symbols:
        print(symbol_1)

        symbol_id_1 = get_symbol_id(symbol_1)
        conn = connect_db()
        insert_daily_price_data_in_db(symbol_1, "2013-01-01")
        df_1 = get_daily_price_from_db(symbol_1, "2013-01-01")
        print(df_1)
        if df_1.empty:
            continue

        current_data_1, future_data_1 = train_test_split(df_1)
        for symbol_2 , instrument_2 in symbols:
            print(symbol_1 +"-"+symbol_2)    

            if symbol_1 == symbol_2:
                continue

            

            symbol_id_2 = get_symbol_id(symbol_2)

            #if  check_backtesting_results_exists(conn, strategy_id, symbol_id_1,coint_id):
            #    conn.close()
            #    print("already exists, skipping")
            #    continue            
            coint_id= get_coint_id(conn, symbol_id_1, symbol_id_2)

            delete_existing_entries(conn, strategy_id, symbol_id_1, coint_id)
            delete_existing_entries(conn, strategy_id, symbol_id_2, coint_id)

            insert_daily_price_data_in_db(symbol_2, "2013-01-01")
            df_2 = get_daily_price_from_db(symbol_2, "2013-01-01")
            current_data_2, future_data_2 = train_test_split(df_2)

            if len(df_1) != len(df_2):
                print(len(df_1))
                print(len(df_2))
                print("error: datasets with different sizes")
                continue


            buy = False
            trade_log_1 = []
            trade_log_2 = []
            signal="None"
            last_action=""
            print("chega aqui")
            for i in range(len(future_data_1.index)):
                print("entra aqui")
                current_data_1 = pd.concat([current_data_1, pd.DataFrame(future_data_1.iloc[i]).transpose()], axis=0)
                current_data_2 = pd.concat([current_data_2, pd.DataFrame(future_data_2.iloc[i]).transpose()], axis=0)
                
                S1 = current_data_1['close_price']
                S2 = current_data_2['close_price']

                S1 = sm.add_constant(S1)

                results = sm.OLS(S2, S1).fit()

                S1 = S1['close_price']
                b = results.params['close_price']
                print(results)

                Z= S2 - b * S1



                signal = execute_cointegration(current_data_1,current_data_2,Z, buy,last_action)
                print("Signal: ",signal)
                if signal == 'Long' and not buy:

                    entry_price_1 = future_data_1['close_price'].iloc[i]
                    lowest_price_1 = entry_price_1
                    highest_price_1 = entry_price_1
                    quantity_1 = 1 / entry_price_1

                    entry_price_2 = future_data_2['close_price'].iloc[i]
                    lowest_price_2 = entry_price_2
                    highest_price_2 = entry_price_2
                    quantity_2 = 1 / entry_price_2

                    last_action='Long'
                    buy = True

                    insert_cointegration(conn, symbol_id_1, symbol_id_2)

                    trade_log_1.append({
                                        'trade_date': future_data_1["price_date"].iloc[i],
                                        'trade_type': 'Short',
                                        'price': future_data_1['close_price'].iloc[i],
                                        'quantity': quantity_1,
                                        'profit_loss': 0,
                                        'max_drawdown': 0,  # Initialize drawdown
                                        'market_type': 'Long'
                                    })
                    trade_log_2.append({
                                        'trade_date': future_data_2["price_date"].iloc[i],
                                        'trade_type': 'Long',
                                        'price': future_data_2['close_price'].iloc[i],
                                        'quantity': quantity_2,
                                        'profit_loss': 0,
                                        'max_drawdown': 0,  # Initialize drawdown
                                        'market_type': 'Long'
                                    })                                    

                elif signal == 'Short' and not buy:
                    entry_price_1 = future_data_1['close_price'].iloc[i]
                    lowest_price_1 = entry_price_1
                    highest_price_1 = entry_price_1
                    quantity_1 = 1 / entry_price_1

                    entry_price_2 = future_data_2['close_price'].iloc[i]
                    lowest_price_2 = entry_price_2
                    highest_price_2 = entry_price_2
                    quantity_2 = 1 / entry_price_2

                    last_action='Short'
                    buy = True

                    insert_cointegration(conn, symbol_id_1, symbol_id_2)

                    trade_log.append({
                        'trade_date': future_data_1["price_date"].iloc[i],
                        'trade_type': 'Long',
                        'price': future_data_1['close_price'].iloc[i],
                        'quantity': quantity_1,
                        'profit_loss': 0,
                        'max_drawdown': 0,
                        'market_type': 'Short'
                    })

                    trade_log_2.append({
                                        'trade_date': future_data_2["price_date"].iloc[i],
                                        'trade_type': 'Short',
                                        'price': future_data_2['close_price'].iloc[i],
                                        'quantity': quantity_2,
                                        'profit_loss': 0,
                                        'max_drawdown': 0,  # Initialize drawdown
                                        'market_type': 'Short'
                                    })      


                elif signal == "Exit" and buy and last_action=='Long':

                    exit_price_1 = future_data_1['close_price'].iloc[i]
                    exit_price_2 = future_data_2['close_price'].iloc[i]

                    quantity_1 = trade_log_1[-1]['quantity']
                    quantity_2 = trade_log_2[-1]['quantity']

                    profit_loss_1 = (trade_log_1[-1]['price'] - exit_price_1) * quantity_1
                    profit_loss_2 = (exit_price_2 - trade_log_2[-1]['price']) * quantity_2 

                    drawdown_1 = (highest_price_1 -entry_price_1) / entry_price_1 * 100
                    drawdown_2 = (entry_price_2 - lowest_price_2) / entry_price_2 * 100
                    
                    last_action=''
                    buy = False

                    trade_log_1.append({
                        'trade_date': future_data_1["price_date"].iloc[i],
                        'trade_type': 'Exit',
                        'price': future_data_1['close_price'].iloc[i],
                        'quantity': quantity_1,
                        'profit_loss': profit_loss_1,
                        'max_drawdown': drawdown_1,
                        'market_type': 'Long'
                    })                     

                    trade_log_2.append({
                        'trade_date': future_data_2["price_date"].iloc[i],
                        'trade_type': 'Exit',
                        'price': future_data_2['close_price'].iloc[i],
                        'quantity': quantity_2,
                        'profit_loss': profit_loss_2,
                        'max_drawdown': drawdown_2,
                        'market_type': 'Long'
                    })                     

                elif signal == "Exit" and buy and last_action=='Short':

                    exit_price_1 = future_data_1['close_price'].iloc[i]
                    exit_price_2 = future_data_2['close_price'].iloc[i]

                    quantity_1 = trade_log_1[-1]['quantity']
                    quantity_2 = trade_log_2[-1]['quantity']

                    profit_loss_2 = (trade_log_2[-1]['price'] - exit_price_2) * quantity_2
                    profit_loss_1 = (exit_price_1 - trade_log_1[-1]['price']) * quantity_1 

                    drawdown_2 = (highest_price_2 -entry_price_2) / entry_price_2 * 100
                    drawdown_1 = (entry_price_1 - lowest_price_1) / entry_price_1 * 100
                    
                    last_action=''
                    buy = False

                    trade_log_1.append({
                        'trade_date': future_data_1["price_date"].iloc[i],
                        'trade_type': 'Exit',
                        'price': future_data_1['close_price'].iloc[i],
                        'quantity': quantity_1,
                        'profit_loss': profit_loss_1,
                        'max_drawdown': drawdown_1,
                        'market_type': 'Short'
                    })                     

                    trade_log_2.append({
                        'trade_date': future_data_2["price_date"].iloc[i],
                        'trade_type': 'Exit',
                        'price': future_data_2['close_price'].iloc[i],
                        'quantity': quantity_2,
                        'profit_loss': profit_loss_2,
                        'max_drawdown': drawdown_2,
                        'market_type': 'Short'
                    })                       
                                                                      

                if buy:
                    # Update the lowest price while the trade is active
                    current_low_price_1 = future_data_1['low_price'].iloc[i]
                    current_high_price_1 = future_data_1['high_price'].iloc[i]

                    if current_low_price_1 < lowest_price_1:
                        lowest_price_1 = current_low_price_1

                    if current_high_price_1 > highest_price_1:
                        highest_price_1 = current_high_price_1    

                    current_low_price_2 = future_data_2['low_price'].iloc[i]
                    current_high_price_2 = future_data_2['high_price'].iloc[i]

                    if current_low_price_2 < lowest_price_2:
                        lowest_price_2 = current_low_price_2

                    if current_high_price_2 > highest_price_2:
                        highest_price_2 = current_high_price_2                          

            statistics_1 = calculate_statistics(trade_log_1, df_1)
            statistics_2 = calculate_statistics(trade_log_2, df_2)
            if not statistics_1:
                print("No statistics, error")
                
                insert_backtesting_results(conn, strategy_id, symbol_id_1, current_data_1["price_date"].iloc[0], current_data_1["price_date"].iloc[-1], {},coint_id)
            if not statistics_2:
                print("No statistics, error")
                
                insert_backtesting_results(conn, strategy_id, symbol_id_2, current_data_2["price_date"].iloc[0], current_data_2["price_date"].iloc[-1], {},coint_id)                
                
            if not statistics_1 and not statistics_2:
                continue
            
            if statistics_1:
                insert_backtesting_results(conn, strategy_id, symbol_id_1, current_data_1["price_date"].iloc[0], current_data_1["price_date"].iloc[-1], statistics_1,coint_id)

            if statistics_2:
                insert_backtesting_results(conn, strategy_id, symbol_id_2, current_data_2["price_date"].iloc[0], current_data_2["price_date"].iloc[-1], statistics_2,coint_id)

        # Insert trade log
            insert_trade_log(conn, strategy_id_1, symbol_id_1, trade_log_1,coint_id)
            insert_trade_log(conn, strategy_id_2, symbol_id_2, trade_log_2,coint_id)

        # Close the connection
            conn.close()
    
            print(statistics_1)                    
            print(statistics_2)                    


cointegration_test()            