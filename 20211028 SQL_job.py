# import lib
import pyodbc
import pandas as pd
import time

# connect to SQL server
server = 'DESKTOP-Q21C3LU\SQLEXPRESS'
database = 'stockdata_qdd'
username = 'sa'
password = 'abc@1234'
cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

# exec job
cursor.execute("exec Stockdata_qdd.dbo.Backup_stock_data;")

# Close cursor
cnxn.commit()
cursor.close()