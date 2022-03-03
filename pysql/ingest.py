#####
# Ingest Library
# For data validation and QC in Pandas
#####
import hashlib
import pandas as pd


# Count nulls in each column
def CountNullFrequency(df):
  
  # use a dictionary to load data into pandas
  freq_dict = {}

  for column in df.columns:
    # count frequencies
    total = df[column].shape[0]
    notnull = df[column].notnull().sum()
    null = df[column].isna().sum()

    freq_dict.update( {column: [total, null, notnull]} )


  # convert dictionary to dataframe
  freq_df = pd.DataFrame(freq_dict)
  
  return freq_df




def locateDuplicates(df, column):
  count_df = df.groupby(column).count().reset_index()

  count_df = count_df.rename(columns={count_df.columns[1] : 'Count'}) \
        [[column, 'Count']]

  count_df['Count'] = count_df['Count'].astype('Int64')
  
  dup_df   = count_df.loc[count_df[count_df.columns[1]] > 1]

  return dup_df


def getTableDisposition(df):
  count_arr = []
  for column in df.columns:
    blank_rows = df.loc[(df[column] == '') | (df[column].isna())].shape[0]
    count_arr.append(blank_rows)

  frame = { 'Column Name': df.columns, 'Total Rows': df.shape[0], 'Blank Rows': count_arr }

  return pd.DataFrame(frame) \
        .style.format({"Total Rows": "{:,.0f}",
                 "Blank Rows": "{:,.0f}"})



def getFrequencyTable(df, column):
  df = df.groupby(column).count()
  df = df.reset_index().rename(columns={df.columns[1]: "Frequency"})
  df = df[[column, 'Frequency']]

  return df \
    .sort_values(by=column, ascending=False).reset_index(drop=True) 


def getSliceByBlanks(df, sliceColumn):
  
  out_df = df[sliceColumn].drop_duplicates()
  
  for column in df.columns:
    if column == sliceColumn:
      continue
    
    filter_df = df.loc[(df[column] == '') | df[column].isna()]
    freq_df = getFrequencyTable(filter_df, sliceColumn)
    freq_df = freq_df.rename(columns={"Frequency": column})
  
    out_df = pd.merge(out_df, freq_df, on=sliceColumn, how='outer')
    out_df = out_df.fillna('')
    
  return out_df


def getFrequencyAcrossSlices(df, calcColumn, sliceColumn):
  df_list = []

  slices = set(df[sliceColumn])
  for sliceName in slices:

    slice_df = df.loc[df[sliceColumn] == sliceName]
    freq_df = slice_df[calcColumn].value_counts().reset_index()
    freq_df = freq_df.rename(columns={calcColumn: "Frequency", "index": calcColumn})
    freq_df[sliceColumn] = sliceName

    df_list.append(freq_df)


  freq_disposition = pd.concat(df_list, sort=True).sort_values(by=[sliceColumn, calcColumn])
  
  return freq_disposition



def identifyConflictingFields(df, unique_ids, field):
  count_df = df[unique_ids + [field]] \
          .drop_duplicates() \
          .groupby(unique_ids).count() \
          .rename(columns={field : "Count"}) \
          .reset_index()

  duplicates = count_df.loc[count_df['Count'] > 1]
  duplicate_rows = duplicates.shape[0]
  print(f"Number of records with multiple values : {duplicate_rows:,d}")


def getAverageValues(df, id_column, columns):
  avg_df = df.groupby(id_column).mean().reset_index()

  return avg_df \
    .sort_values(by=id_column, ascending=False).reset_index(drop=True) 