"""
checksum.py
====================================
Checksum functions
"""

import subprocess
import pyodbc
import pandas as pd


def getChecksum(server, database, table):
	"""
	Retrieves the checksum of a table.
	Uses SQLCMD to connect to a SQL Server instance.
	Query is written in T-SQL.

	:param server
	:type database: string
	:param database
	:type database: string
	:param table
	:type table: string
	:return: checksum (-1 if failure)
	:rtype: int
	"""

	# get column names
	query = f"""
	        SELECT      COLUMN_NAME
	        FROM        INFORMATION_SCHEMA.COLUMNS
	        WHERE       TABLE_NAME = N'{table}'
	    """



	conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
	names_df = pd.io.sql.read_sql(query, conn)
	conn.close()

	names = names_df['COLUMN_NAME'].tolist()

	# filter out refresh dates
	column_names = ["[" + x + "]" for x in names if x not in ('DateCreated', 'DateRefreshed')]

	# construct query
	query_part1 = "SELECT CHECKSUM_AGG(CHECKSUM("
	query_part2 = ",".join(column_names)
	query_part3 = f")) FROM {database}.dbo.{table}"

	query = query_part1 + query_part2 + query_part3


	cmd = f"sqlcmd -S " + server + " -h -1 -Q \"" + query + '\"'


	result = subprocess.check_output(cmd, shell=True)

	try:
		checksum =  int(result.split()[0])
	except:
		checksum = -1

	return checksum