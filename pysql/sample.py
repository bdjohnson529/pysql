import pandas as pd
import numpy as np

def calculate_mean(input_df, stratum_cols, weight_col, value_col):
  """
  Single random sample
  """

  cols = stratum_cols.copy()
  cols.append(weight_col)
  cols.append(value_col)

  df = input_df[cols].copy()

  # will be used to calculate weighted mean
  df['Weight x value'] = df[weight_col] * df[value_col]


  calc_df = df.groupby(stratum_cols) \
        .agg({value_col: ['count', 'sum', np.var],
            weight_col: ['sum'],
            'Weight x value': ['sum']}) \
        .reset_index(drop=False)
        

  # rename columns
  new_cols = ['n', 'Sum', 'Variance', 'N', 'Weighted Sum']
  col_names  = stratum_cols + new_cols

  calc_df.columns = col_names

  # calculate weighted mean
  calc_df['Weighted Mean'] = calc_df['Weighted Sum'] / calc_df['N']

  # calculate standard error
  calc_df['factor'] = (1 / (calc_df['n'] - 1)) * (1 - calc_df['n'] / calc_df['N'])
  calc_df['Estimated Var'] = calc_df['Variance'] * calc_df['factor']
  calc_df['Std Err of Mean'] = np.sqrt(calc_df['Estimated Var'])
  calc_df['Std Err of Sum'] = calc_df['N'] * calc_df['Std Err of Mean']

  return calc_df


def estimate_population_mean(input_df, stratum_cols, weight_col, value_col):
  """
  Stratified random sample
  """
  cols = stratum_cols.copy()
  cols.append(weight_col)
  cols.append(value_col)
  
  df = input_df[cols].copy()

  ##########################
  # calculate strata statistics
  ##########################

  # will be used to calculate weighted mean
  df['Weight x value'] = df[weight_col] * df[value_col]


  calc_df = df.groupby(stratum_cols) \
        .agg({value_col: ['count', 'sum', lambda x : np.var(x, ddof=1)],
            weight_col: ['sum'],
            'Weight x value': ['sum']}) \
        .reset_index(drop=False)

  # rename columns
  new_cols = ['n', 'Sum', 'Variance', 'N', 'Weighted Sum']
  col_names  = stratum_cols + new_cols
  calc_df.columns = col_names

  # calculate population fractions
  N_total = calc_df['N'].sum()
  calc_df['W'] = calc_df['N'] / N_total

  # calculate weighted mean
  calc_df['Weighted Mean'] = calc_df['Weighted Sum'] / calc_df['N']
  calc_df['W x Weighted Mean'] = calc_df['W'] * calc_df['Weighted Mean']

  # factor to use for standard error
  calc_df['factor'] = (1 / calc_df['n']) * (calc_df['N'] - calc_df['n'])/(calc_df['N'] - 1)
  calc_df['Estimated Mean Variance'] = np.square(calc_df['W']) * calc_df['factor'] * calc_df['Variance'] 
  calc_df['Estimated Sum Variance'] = np.square(calc_df['N']) * calc_df['factor'] * calc_df['Variance'] 

  ######################
  # sum into population totals
  ######################
  selected_df = calc_df[['N',
              'Sum',
              'Weighted Sum',
              'W x Weighted Mean',
              'Estimated Mean Variance',
              'Estimated Sum Variance']]

  # rename columns
  new_cols = ['Population Total',
        'Unwgt Sum',
        'Wgt Sum',
        'Wgt Mean',
        'Wgt Mean Variance',
        'Wgt Sum Variance']

  selected_df.columns = new_cols

  sum_stats = selected_df.sum()

  # calculate standard errors
  sum_stats['Wgt Mean Std Err'] = np.sqrt(sum_stats['Wgt Mean Variance'])
  sum_stats['Wgt Sum Std Err'] = np.sqrt(sum_stats['Wgt Sum Variance'])

  sum_df = pd.DataFrame(sum_stats).transpose()

  return sum_df
