"""
unittest.py
====================================
Unit test functions
"""

import pandas as pd
from pysql.io import executeQuery

def expectEmptySet(server, database, query):
    """
    Expect empty result set
    """
    df = executeQuery(server, database, query)

    if(df.shape[0] == 0):
        return 'PASS'
    else:
        return 'FAIL'


def expectFullSet(server, database, query):
    """
    Expect empty result set
    """
    df = executeQuery(server, database, query)

    if(df.shape[0] > 0):
        return 'PASS'
    else:
        return 'FAIL'


def expectColumnValues(server, database, table, column, expected_values):
    """
    Expect column values.
    """
    query = f"""SELECT DISTINCT {column}
    			FROM {table}
    		"""

    df = executeQuery(server, database, query)
    values = df[df.columns[0]].tolist()

    if(values == expected_values):
    	return 'PASS'
    else:
    	return 'FAIL'


def assertTablesExist(server, database, table_list):
    """
    List of tables exist in database.
    """
    query = f"""SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG='{database}'
            ORDER BY table_name
        """

    df = executeQuery(server, database, query)
    db_tables = df['table_name'].tolist()
    
    db_tables = [x.lower().strip() for x in db_tables]
    input_tables = [x.lower().strip() for x in table_list]
    missing_tables = [x for x in input_tables if x not in db_tables]


    return missing_tables


def assertRecordsExist(server, database, table):
    """
    Table exists.
    """
    query = f"""SELECT TOP 10 *
                FROM {table}
            """ 

    df = executeQuery(server, database, query)

    if(df.shape[0] > 0):
        return 'PASS'
    else:
        return 'FAIL'