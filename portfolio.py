import pandas as pd
import numpy as np
from datetime import datetime
from strategies.mean_reverting import long
from get_data import get_symbols_from_db, get_daily_price_from_db, get_symbol_id
from connection import connect_db
import mysql.connector as mdb
from decimal import Decimal

def populate_assets_strategies_table():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Insert query using correct table and column names
        populate_table_query = """
        INSERT IGNORE INTO Assets_Strategies (symbol_id, strategy_id, slot_free, strategy_active)
        SELECT s.id, t.id, TRUE, FALSE
        FROM symbol s
        CROSS JOIN strategies t;
        """
        cursor.execute(populate_table_query)
        conn.commit()
        print("Assets_Strategies table populated successfully.")
    except mdb.Error as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def update_assets_strategies():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        select_query = """
            SELECT
                sub.symbol_id,
                t.id AS strategy_id
            FROM
                (SELECT 
                    br.symbol_id,
                    ROUND(
                        (SUM(CASE WHEN bt.trade_type = 'Exit' THEN bt.profit_loss ELSE 0 END) / 
                        NULLIF(SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END), 0)) * 100, 
                        2
                    ) AS avg_roi_per_trade_percentage,
                    ROUND(
                        AVG(br.average_holding_period),
                        2
                    ) AS avg_holding_period,
                    SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END) AS total_exit_trades,
                    ROUND(
                        SUM(CASE WHEN bt.trade_type = 'Exit' AND bt.profit_loss > 0 THEN 1 ELSE 0 END) / 
                        NULLIF(SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END), 0) * 100, 
                        2
                    ) AS win_rate,
                    ROUND(
                        SUM(CASE WHEN bt.trade_type = 'Exit' THEN bt.profit_loss ELSE 0 END),
                        2
                    ) AS total_profit_loss,
                    ROUND(
                        AVG(br.drawdown),
                        2
                    ) AS drawdown
                FROM
                    backtesting_results br
                JOIN
                    symbol s ON br.symbol_id = s.id
                JOIN
                    backtesting_trades bt ON br.symbol_id = bt.symbol_id
                WHERE
                    br.profit > 0
                GROUP BY
                    br.symbol_id
                HAVING
                    avg_roi_per_trade_percentage < 1000
                    AND avg_holding_period < 70
                    AND total_exit_trades >= 2
                    AND win_rate >= 80
                    AND total_profit_loss > 0
                    AND avg_roi_per_trade_percentage >= 3
                    
                ) AS sub
            CROSS JOIN
                strategies t;
        """
        cursor.execute(select_query)
        records = cursor.fetchall()

        update_query = """
            UPDATE Assets_Strategies
            SET strategy_active = TRUE
            WHERE symbol_id = %s AND strategy_id = %s
        """

        for symbol_id, strategy_id in records:
            cursor.execute(update_query, (symbol_id, strategy_id))

        conn.commit()
        print("Assets_Strategies table updated successfully.")
    except mdb.Error as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def delete_invalid_data():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Find symbols with avg ROI per trade > 1000 or any trade with profit_loss * 100 < -200
        find_invalid_symbols_query = """
            SELECT symbol_id
            FROM (
                SELECT 
                    br.symbol_id
                FROM backtesting_results br
                JOIN backtesting_trades bt ON br.symbol_id = bt.symbol_id
                GROUP BY br.symbol_id
                HAVING 
                    ROUND(
                        (SUM(CASE WHEN bt.trade_type = 'Exit' THEN bt.profit_loss ELSE 0 END) / 
                        NULLIF(SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END), 0)) * 100, 
                        2
                    ) > 1000
                UNION
                SELECT DISTINCT bt.symbol_id
                FROM backtesting_trades bt
                WHERE bt.profit_loss * 100 < -200
            ) AS invalid_symbols;
        """

        cursor.execute(find_invalid_symbols_query)
        invalid_symbols = cursor.fetchall()

        if invalid_symbols:
            symbol_ids = [str(row[0]) for row in invalid_symbols]
            symbol_ids_str = ', '.join(symbol_ids)

            # Delete from backtesting_trades and backtesting_results
            delete_backtesting_trades_query = f"""
                DELETE FROM backtesting_trades
                WHERE symbol_id IN ({symbol_ids_str});
            """

            delete_backtesting_results_query = f"""
                DELETE FROM backtesting_results
                WHERE symbol_id IN ({symbol_ids_str});
            """

            cursor.execute(delete_backtesting_trades_query)
            cursor.execute(delete_backtesting_results_query)
            
            conn.commit()
            print(f"Deleted data for symbols with avg ROI per trade > 1000% or profit_loss * 100 < -200: {symbol_ids_str}")
        else:
            print("No invalid symbols found.")
    except mdb.Error as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def get_overall_statistics(strategy_id):
    # Connect to the database
    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Query to calculate overall win rate for the specified strategy
        win_rate_query = """
            SELECT 
                ROUND(
                    SUM(CASE WHEN bt.trade_type = 'Exit' AND bt.profit_loss > 0 THEN 1 ELSE 0 END) / 
                    NULLIF(SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END), 0) * 100, 
                    2
                ) AS overall_win_rate
            FROM backtesting_trades bt
            JOIN Assets_Strategies as_strat ON bt.symbol_id = as_strat.symbol_id
            WHERE as_strat.strategy_id = %s;
        """

        # Query to calculate average drawdown for the specified strategy
        avg_drawdown_query = """
            SELECT 
                ROUND(AVG(br.drawdown), 2) AS avg_drawdown
            FROM backtesting_results br
            JOIN Assets_Strategies as_strat ON br.symbol_id = as_strat.symbol_id
            WHERE as_strat.strategy_id = %s;
        """

        # Query to calculate average ROI per trade for the specified strategy
        avg_roi_per_trade_query = """
            SELECT 
                ROUND(
                    (SUM(bt.profit_loss) / NULLIF(COUNT(bt.id), 0)) * 100, 
                    2
                ) AS avg_roi_per_trade
            FROM backtesting_trades bt
            JOIN Assets_Strategies as_strat ON bt.symbol_id = as_strat.symbol_id
            WHERE bt.trade_type = 'Exit'
              AND as_strat.strategy_id = %s;
        """

        # Execute queries and fetch results
        cursor.execute(win_rate_query, (strategy_id,))
        win_rate_result = cursor.fetchone()[0]

        cursor.execute(avg_drawdown_query, (strategy_id,))
        avg_drawdown_result = cursor.fetchone()[0]

        cursor.execute(avg_roi_per_trade_query, (strategy_id,))
        avg_roi_per_trade_result = cursor.fetchone()[0]

        # Return the results as a dictionary
        overall_stats = {
            'overall_win_rate': win_rate_result,
            'avg_max_drawdown': avg_drawdown_result,
            'avg_roi_per_trade': avg_roi_per_trade_result
        }

        return overall_stats

    except mdb.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()



def get_symbol_statistics(symbol):
    symbol_id = get_symbol_id(symbol)
    # Connect to the database
    conn = connect_db()
    cursor = conn.cursor()
    
    try:
        # Query to get the max drawdown and average ROI for the specified symbol from backtesting_trades
        query = """
            SELECT 
                ROUND(AVG(bt.max_drawdown), 2) AS avg_max_drawdown,
                ROUND(
                    SUM(CASE WHEN bt.trade_type = 'Exit' THEN bt.profit_loss ELSE 0 END) / 
                    NULLIF(SUM(CASE WHEN bt.trade_type = 'Exit' THEN 1 ELSE 0 END), 0) * 100, 
                    2
                ) AS avg_roi_per_trade_percentage
            FROM backtesting_trades bt
            WHERE bt.symbol_id = %s
            GROUP BY bt.symbol_id;
        """

        # Execute the query
        cursor.execute(query, (symbol_id,))
        result = cursor.fetchone()

        # Fetch the results
        avg_max_drawdown = result[0] if result[0] is not None else Decimal('0')
        avg_roi_per_trade = result[1] if result[1] is not None else Decimal('0')

        # Return the results as a dictionary
        symbol_stats = {
            'avg_max_drawdown': avg_max_drawdown,
            'avg_roi': avg_roi_per_trade
        }

        return symbol_stats

    except mdb.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()



def calc_betsize(symbol, id_strat):
    # Kelly fraction constant
    k_f = Decimal('0.50')  # Half-Kelly

    # Get overall statistics for the strategy
    overall_stats = get_overall_statistics(id_strat)
    # Get symbol-specific statistics
    symbol_stats = get_symbol_statistics(symbol)

    if not overall_stats or not symbol_stats:
        print("Error: Could not retrieve statistics.")
        return 0

    # Extract and convert values from statistics
    avg_roi_overall = Decimal(overall_stats['avg_roi_per_trade']) / Decimal('100')
    avg_drawdown_overall = Decimal(overall_stats['avg_max_drawdown']) / Decimal('100')
    win_rate_overall = Decimal(overall_stats['overall_win_rate']) / Decimal('100')  # Convert to decimal
    
    avg_roi_symbol = Decimal(symbol_stats['avg_roi']) / Decimal('100')
    avg_drawdown_symbol = Decimal(symbol_stats['avg_max_drawdown']) / Decimal('100')

    # Determine maximum drawdown and minimum ROI
    drawdown = avg_drawdown_symbol
    avg_roi = min(avg_roi_overall, avg_roi_symbol)

    print(win_rate_overall)
    print(avg_roi)
    print(drawdown)
    # Calculate the ratio for the Kelly Criterion
    if drawdown == 0:
        drawdown=Decimal(0.00000001)
        print("Error: Drawdown is zero. Putting 0.000001")
        #return 0
    
    ratio = avg_roi / drawdown
    print(ratio)
    # Calculate the Kelly Fraction
    f = win_rate_overall - ((Decimal('1') - win_rate_overall) / ratio)
    f = f * k_f

    # Output the bet size
    bet_size = f * Decimal('100')
    print("The bet size is:", bet_size)
    return float(bet_size)


def main():
    delete_invalid_data()
    populate_assets_strategies_table()
    update_assets_strategies()
    #print(get_symbol_statistics('BIL'))
    calc_betsize('LEED.L',1)

main()
#if __name__ == "__main__":
    #main()
