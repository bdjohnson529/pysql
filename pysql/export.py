import pandas as pd
import re
import BambooLib.ingest as bamboo


#######################
# Check zip code length
#######################
def checkFieldLength(df, column):
    colLength = column + ' Length'

    df[colLength] = df[column].apply(bamboo.getStrLen)
    length_df = df[[df.columns[0], 'Utility', colLength]] \
                    .groupby(['Utility', colLength]).count().reset_index() \
                    .rename(columns={df.columns[0] : 'Count'})

    return length_df

def locateRecordsByLength(df, column, length):
    df['colLength'] = df[column].apply(bamboo.getStrLen)
    select_df = df.loc[df['colLength'] == length]

    return select_df



#####################
# Check whitespace in dataframe
#####################
def checkWhitespace(df):
    blankColumns = []

    for col in df.columns:
        check = df[col].apply(isWhitespace)
        val   = check.sum()

        if(val > 0):
            blankColumns.append(col)

    return blankColumns

def isWhitespace(inputStr):
    if type(inputStr) != str:
        return False

    pattern = r'^\s*$'
    reMatch = re.search(pattern, inputStr)
    
    if(reMatch):
        return True
    else:
        return False

#####################
# Verify column
#####################
def checkNulls(df, col):
    null_df = df.loc[df[col].isna()]
    null_count = null_df.shape[0]
    return null_count

def checkDuplicates(df, column):
    count_df = df.groupby(column).count().reset_index()

    count_df = count_df.rename(columns={count_df.columns[1] : 'Count'}) \
                [[column, 'Count']]

    count_df['Count'] = count_df['Count'].astype('Int64')
    
    dup_df   = count_df.loc[count_df[count_df.columns[1]] > 1]
    dup_count = dup_df.shape[0]

    return dup_count

def percentageFilled(df, col):
    match_df = df.loc[~(df[col].isna())]

    match_count = match_df.shape[0]
    total_count = df.shape[0]
    pct = round(match_count / total_count, 2)

    return pct
