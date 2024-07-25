import pandas as pd
import numpy as np
from datetime import datetime
from strategies.mean_reverting import long
from get_data import get_symbols_from_db, get_daily_price_from_db,get_symbol_id
from connection import connect_db
import mysql.connector as mdb

def train_test_split(df, split_ratio=0.3):
    split_index = int(len(df) * split_ratio)
    current_data = df.iloc[:split_index]
    future_data = df.iloc[split_index:]
    return current_data, future_data

def delete_existing_entries(conn, strategy_id, symbol_id):
    cursor = conn.cursor()
    query_results = "DELETE FROM backtesting_results WHERE strategy_id = %s AND symbol_id = %s"
    query_trades = "DELETE FROM backtesting_trades WHERE strategy_id = %s AND symbol_id = %s"
    
    cursor.execute(query_trades, (strategy_id, symbol_id))
    cursor.execute(query_results, (strategy_id, symbol_id))
    conn.commit()


def calculate_statistics(trade_log,price_data):
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
    initial_investment = 1
    roi = (total_pnl- initial_investment) * 100 if initial_investment != 0 else 0

    # Calculate max drawdown
    cumulative_pnl = np.cumsum(period_pnls)
    
    

    # Calculate Average Period PnL in %
    period_pnl_percentages = [(trade['profit_loss'] / (trade['price'] )) * 100 if (trade['price'] ) != 0 else 0 for trade in trade_log]
    average_period_percentage = np.mean(period_pnl_percentages) if period_pnl_percentages else 0


     # Calculate Average Holding Period
    holding_periods = []
    max_drawdown = 0
    for i in range(1, len(trade_log)):
        if trade_log[i]['trade_type'] == 'Exit' and trade_log[i-1]['trade_type'] == 'Long':
            entry_price = trade_log[i-1]['price']
            if entry_price == 0:  # Ensure no division by zero
                print("entry price is zero , error")
                continue
            holding_data = price_data[(price_data['price_date'] >= trade_log[i-1]['trade_date']) & (price_data['price_date'] <= trade_log[i]['trade_date'])]
           
            lowest_price = holding_data['low_price'].min()
         
            drawdown = ((entry_price - lowest_price) / entry_price) * 100  # Drawdown in percentage
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

def insert_backtesting_results(conn, strategy_id, symbol_id, start_date, end_date, statistics):
    cursor = conn.cursor()
    query = '''
        INSERT INTO backtesting_results (
            strategy_id, symbol_id, start_date, end_date, profit, drawdown, number_of_trades, win_rate,
            total_pnl, average_period_pnl, max_period_profit, max_period_loss,
            average_period_profit, average_period_loss, winning_periods, losing_periods,
            percentage_win_periods, percentage_loss_periods, average_holding_period, average_period_percentage
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    cursor.execute(query, (
        strategy_id, symbol_id, start_date, end_date, statistics['total_pnl'], statistics['max_drawdown'], len(statistics),
        statistics['percentage_win_periods'], statistics['total_pnl'], statistics['average_period_pnl'],
        statistics['max_period_profit'], statistics['max_period_loss'], statistics['average_period_profit'],
        statistics['average_period_loss'], statistics['winning_periods'], statistics['losing_periods'],
        statistics['percentage_win_periods'], statistics['percentage_loss_periods'], statistics['average_holding_period'],
        statistics['average_period_percentage']
    ))
    conn.commit()


def check_backtesting_results_exists(conn, strategy_id, symbol_id):
    cursor = conn.cursor()
    query = "SELECT COUNT(*) FROM backtesting_results WHERE strategy_id = %s AND symbol_id = %s"
    cursor.execute(query, (strategy_id, symbol_id))
    result = cursor.fetchone()
    return result[0] > 0    

def insert_trade_log(conn, strategy_id, symbol_id, trade_log):
    cursor = conn.cursor()
    query = '''
        INSERT INTO backtesting_trades (
            strategy_id, symbol_id, trade_date, trade_type, price, profit_loss
        ) VALUES (%s, %s, %s, %s, %s, %s)
    '''
    for trade in trade_log:
        cursor.execute(query, (
            strategy_id, symbol_id, trade['trade_date'], trade['trade_type'], trade['price'], trade['profit_loss']
        ))
    conn.commit()

# Replace with actual values
def main():
    strategy_id = 1
    symbols = get_symbols_from_db()
    for symbol, instrument in symbols:
        print(symbol)
        #print(instrument)
        if instrument != 'cryptocurrency':
            continue

      

        symbol_id = get_symbol_id(symbol)
        conn = connect_db()
        if  check_backtesting_results_exists(conn, strategy_id, symbol_id):
            conn.close()
            continue
        df = get_daily_price_from_db(symbol, "2013-01-01")
    

        current_data, future_data = train_test_split(df)


        buy = False
        trade_log = []
        signal="None"
        for i in range(len(future_data.index)):
            current_data = pd.concat([current_data, pd.DataFrame(future_data.iloc[i]).transpose()], axis=0)
            signal = long(current_data, buy)
            #print(signal)
            #print(buy)
            if signal=='Long' and not buy:
                buy = True
                trade_log.append({'trade_date': future_data["price_date"].iloc[i], 'trade_type': 'Long', 'price': future_data['close_price'].iloc[i],  'profit_loss': 0})
            elif signal=="Exit" and buy:
                buy = False
                trade_log.append({'trade_date': future_data["price_date"].iloc[i], 'trade_type': 'Exit', 'price': future_data['close_price'].iloc[i],  'profit_loss': future_data['close_price'].iloc[i] - trade_log[-1]['price']})

    # Calculate statistics
        statistics = calculate_statistics(trade_log,df)
        if not statistics:
            print("no statistics , error")
            continue

    # Connect to the database
        

        
        #delete_existing_entries(conn, strategy_id, symbol_id)

    # Insert backtesting results
        
        insert_backtesting_results(conn, strategy_id, symbol_id, current_data["price_date"].iloc[0], current_data["price_date"].iloc[-1], statistics)

    # Insert trade log
        insert_trade_log(conn, strategy_id, symbol_id, trade_log)

    # Close the connection
        conn.close()
    
     
        #conn.close()
                 

    #print(trade_log)
 
        print(statistics)

main()        
