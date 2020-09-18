"""
io.py
====================================
SQL IO module
"""

import time
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy


def getTableSchema(server, database, table):
    query = f"""
                SELECT      COLUMN_NAME,
                            IS_NULLABLE,
                            DATA_TYPE,
                            CHARACTER_MAXIMUM_LENGTH
                FROM        INFORMATION_SCHEMA.COLUMNS
                WHERE       TABLE_NAME = N'{table}'
            """

    df = executeQuery(server, database, query)

    return df



def readTableFromQuery(server_name, db_name, query):
    """
    Read table from SQL query, using Pyodbc.

    :param server_name: Server name
    :type server_name: str
    :param db_name: Database name
    :type db_name: str
    :param query: SQL query
    :type query: str
    """

    # Create the connection
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server_name + 
                          ';DATABASE=' + db_name + ';Trusted_Connection=yes')

    # Read in query
    start = time.time()
    input_df = pd.io.sql.read_sql(query, conn)
    end = time.time()

    # Print descriptive statistics
    print("\nRead time :\t", int(end-start), "s")
    rows = input_df.shape[0]
    cols = input_df.shape[1]

    print(f"Rows : \t\t {rows:,d}")
    print(f"Columns : \t {cols:,d}")

    # Close connection
    conn.close()

    return input_df



def executeQueryFromFile(server, database, file_name):
    """
    Excutes a query from a file.

    :param server: Server name
    :type server: str
    :param database: Database name
    :type database: str
    :param file_name: SQL file
    :type query: str
    """

    # Create the connection
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')

    # read query into string
    with open(file_name) as file:
        query = file.read()

    # Read in query
    start = time.time()
    df = pd.io.sql.read_sql(query, conn)
    end = time.time()

    # Close connection
    conn.close()

    # Print descriptive statistics
    print("\nRead time :\t", int(end-start), "s")
    rows = df.shape[0]
    cols = df.shape[1]

    print(f"Rows : \t\t {rows:,d}")
    print(f"Columns : \t {cols:,d}")

    return df


def writeDfToSQL(df, server, database, table, method='Pandas', driver='SQL+Server', chunksize=200):
    """
    Writes pandas dataframe to SQL, using either BCP or Pandas.

    :param df: table
    :type df: pd.DataFrame
    :param server: SQL Server - Server name
    :type server: str
    :param database: SQL Server - Database name
    :type database: str
    :param table: SQL Server - Table name
    :type table: str
    :param method: Write method (either 'Pandas', or 'BCP')
    :type method: str
    """
    if(method == 'Pandas'):
        writeWithPandas(df, server, database, table, driver, chunksize)
    elif(method == 'BCP'):
        writeWithBCP(df, server, database, table)
    else:
        print("Invalid method specified.")

    return None


def writeWithPandas(df, server, database, table, driver='SQL+Server', chunksize=200):
    """
    Wrapper for pd.to_sql()

    :param df: table
    :type df: pd.DataFrame
    :param server: Server name
    :type server: str
    :param database: Database name
    :type database: str
    :param table_name: Database name
    :type table_name: str
    """

    # initialize SQL engine
    if(True):
        connection_str = 'mssql+pyodbc://' + server + '/' + database + '?driver=' + driver
        engine = sqlalchemy.create_engine(connection_str)
    else:
        params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db)
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    start = time.time()

    # table name needs to be lower case
    table_name = table_name.lower()

    df.to_sql(table_name, con=engine,
                      if_exists='replace', index=False,
                      method='multi', chunksize=chunksize)


    end = time.time()

    # diagnostic timing
    print("Write time :\t", int(end - start), " s")

    return None

def writeWithBCP(server, database, table, df):
    """
    Writes a pandas dataframe to a SQL server instance, using the BCP utility.
    :param server: Server name
    :type server: str
    :param database: Database name
    :type database: str
    :param df: dataframe
    :type df: pd.DataFrame
    """
    if server not in ('KSITEST', 'KSISTAGING'):
        print("Write permitted only on KISTEST, KSISTAGING")
        return False

    # write dataframe to temp file
    os.makedirs('tmp', exist_ok=True)
    tmp_file = os.path.abspath("tmp/" + randomWord(10) + ".txt")


    # truncate table on destination server
    query = f"TRUNCATE TABLE {database}.dbo.{table}"
    cmd = f"sqlcmd -S {server} -h -1 -Q \"{query}\""
    os.system(cmd)

    # write df to temp file
    df.to_csv(tmp_file, index=False, sep="|")

    # upload temp file to destination server
    cmd = f"bcp {database}.dbo.{table} in \"{tmp_file}\" -c -T -t\"|\" -S {server}"
    os.system(cmd)


    # delete temp file
    shutil.rmtree('tmp')

    return None