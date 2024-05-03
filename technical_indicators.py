def moving_average(df, n):
    """Calculate the moving average for the given data.
    
    :param df: pandas.DataFrame
    :param n: The number of periods over which to calculate the moving average
    :return: pandas.Series
    """
    MA = df['close_price'].rolling(window=n, min_periods=n).mean()
    return MA
