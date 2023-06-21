# import
import requests
import pandas as pd
import itertools
from datetime import datetime
import pyodbc
import time

# insert data from csv file into dataframe.
server = 'DESKTOP-Q21C3LU\SQLEXPRESS'
database = 'stockdata_qdd'
username = 'sa'
password = 
cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

# update data
cursor = cnxn.cursor()
cursor.execute("exec [Stock_model].dbo.Update_price_stock")
cnxn.commit()
stock_price = pd.read_sql_query("select * from stock_model.dbo.price_update_latest",cnxn)
cursor.close()

# function send telegram message
def telegram_bot_sendtext(bot_message):
    bot_token = 
    bot_chatID =
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    return response.json()

# get list of Symbol
symbol_lst = stock_price['SYMBOL'].tolist()
symbol_lst

#symbol = list(itertools.chain(*symbol_lst))
for symbol in symbol_lst:
   if  stock_price['latest_update_time'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) != stock_price['DATE'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None):
        flag = float(stock_price['lag1'].loc[stock_price['SYMBOL'] == symbol]) #.to_string(header=None, index=None)
        price_to_max = (round(stock_price['OPEN'].loc[stock_price['SYMBOL']== symbol] / stock_price['max_open'].loc[stock_price[
            'SYMBOL']== symbol] - 1, 4)*100).to_string(header=None, index=None)
        detail= ('Latest: ' + stock_price['OPEN'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None)+'\nMAX: ' + stock_price['max_open'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) +  '\nPricetomax: ' + price_to_max +'\nLag1: ' + stock_price['lag1'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '\nLag2: '+ stock_price['lag2'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '\nLag3: ' + stock_price['lag3'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '\nprofit: ' + stock_price['profit'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '\nvalue: ' + stock_price['profit_value'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None))
        if flag > 0:
            telegram_bot_sendtext((stock_price['DATE'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None)) + ' - '+ symbol + ' : ' + 'TĂNG '+ stock_price['lag1'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '%'+ '\n-----------------' + '\n' + detail)
            #telegram_bot_sendtext(detail)
        elif flag < 0:
            telegram_bot_sendtext((stock_price['DATE'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None)) + ' - '+ symbol + ' : ' + 'GIẢM '+ stock_price['lag1'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None) + '%'+ '\n-----------------' + '\n' + detail)
            #telegram_bot_sendtext(detail)
        # elif flag == 0:
        #     telegram_bot_sendtext((stock_price['DATE'].loc[stock_price['SYMBOL'] == symbol].to_string(header=None, index=None)) + ' - ' + symbol + ' : ' + 'GIỮ NGUYÊN')
