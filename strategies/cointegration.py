from statsmodels.tsa.vector_ar.vecm import coint_johansen
import pandas as pd
import yfinance
import statsmodels.api as get_symbols_from_vendor


def zscore(series):

    return (series - series.mean()) / np.std(series)  

def check_johansen_test(df1, df2):
    # Merge the two dataframes on the date index
    df_combined = pd.merge(df1[['Close']], df2[['Close']], left_index=True, right_index=True, suffixes=('_1', '_2'))
    df_combined = df_combined.dropna()

    # Perform Johansen test
    johansen_test_result = coint_johansen(df_combined, det_order=0, k_ar_diff=1)
    
    # Extract the Trace Statistic and Critical Values
    trace_stat = johansen_test_result.lr1
    crit_values = johansen_test_result.cvt

    # Check if the Trace Statistic exceeds the Critical Value at the 5% significance level
    # The Johansen test is considered to have found cointegration if the Trace Statistic is greater than the Critical Value
    num_criterias = len(crit_values[:, 1])
    cointegrated = any(trace_stat[i] > crit_values[i, 1] for i in range(num_criterias))

    #print("Johansen Test Results:")
    #print("Trace Statistic:", trace_stat)
    #print("Critical Values (5% level):", crit_values[:, 1])
    
    return cointegrated



def execute(df_1,df_2,Z,buy=False,last_action=''):

  #LONG means spread low - long asset 2 short asset 1
  #SHORT means spread high - long asset 1 short asset 2
  Z_df = pd.DataFrame({'Close': Z})
  Z_df.index.name = 'Date'

  if buy==False and zscore(Z.values)[-1]<=-2.0   and check_johansen_test(df_1,df_2) :
      print("Long Spread - long asset 2 short asset 1 ")
    

      return 'Long'

  elif buy==False and   zscore(Z.values)[-1]>=2  and check_johansen_test(df_1,df_2):
      print("Short Spread - long asset 1 short asset 2 ")
     

      return 'Short'      

  elif buy==True and  last_action=="Long" and (zscore(Z.values)[-1]>0):
      print("Exiting from Long Spread")
      

      return 'Exit'

  elif buy==True and  last_action=="Short" and (zscore(Z.values)[-1]<=0):
      print("Exiting from Short Spread")
      

      return 'Exit'

  else:
      return 'None'