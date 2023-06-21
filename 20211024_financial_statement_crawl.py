import pyodbc
import itertools
import vnquant.DataLoader as dl
import pandas as pd
import time
import numpy as np
import requests

# insert data from csv file into dataframe.
server = 'DESKTOP-Q21C3LU\SQLEXPRESS'
database = 'stockdata_qdd'
username = 'sa'
password = 'abc@1234'
cnxn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
symbol_lst = cursor.execute("select Symbol from Stockdata_qdd.dbo.VN_symbol")
symbol_lst = list(itertools.chain(*symbol_lst))
cursor.close()


# Def get financial info
def get_basic_index(starty, endy, symbol):
    start_year = int(starty[:4])
    end_year = int(endy[:4])
    years = np.arange(start_year, end_year + 1, 1)[::-1]
    years = ['{}-12-31'.format(year) for year in years]
    data_dates = {}
    for year in years:
        page = requests.get("https://finfo-api.vndirect.com.vn/v4/ratios?q=code:{}~reportDate:{}".format(symbol, year),
                            headers={'User-Agent': 'Mozilla/5.0'})
        data = page.json()

        for item in data['data']:
            date = item['reportDate']
            itemName = item['itemName']
            value = item['value']
            if date not in data_dates:
                data_dates[date] = [[], []]
            else:
                if itemName not in data_dates[date][0] and itemName != "":
                    data_dates[date][0].append(itemName)
                    data_dates[date][1].append(value)
    for i, (date, item) in enumerate(data_dates.items()):
        df_date = pd.DataFrame(data={'index': item[0], date[:7]: item[1]})
        if i == 0:
            df = df_date
        else:
            df = pd.merge(df, df_date, how='inner')
    df.set_index('index', inplace=True)
    return df


# get_basic_index(starty='2020-01-01',endy='2021-12-31', symbol='VCB')

###############################################################
####################### START #################################
###############################################################
# Declare time range
starty_var = '2022-01-01'
endy_var = '2022-09-30'

# %%
# Get financial statement
print("crawling financial statement data...")
start_time = time.time()

cursor = cnxn.cursor()
cursor.execute("truncate table Stockdata_qdd.dbo.financial_info_tmp")
count_ticket = 1
for ticket in symbol_lst:
    print("No 1.{}_ Ticket:{}".format(count_ticket, ticket))
    count_ticket = count_ticket + 1
    try:
        loader = dl.FinanceLoader(symbol=ticket,
                                  start=starty_var,
                                  end=endy_var)
        data = loader.get_finan_report()
        data = data.transpose()
        data = data.unstack().reset_index()
        data.rename(columns={'index': 'criteria', 'level_1': 'year', 0: 'value'}, inplace=True)
        data['Ticket'] = ticket
        for index, row in data.iterrows():
            cursor.execute(
                "insert into Stockdata_qdd.dbo.financial_info_tmp (criteria,year,value, ticket) values (?,?,?,?)",
                row.criteria, row.year, row.value, row.Ticket)
            cnxn.commit()
    except (Exception,):
        pass
cursor.execute("exec stockdata_qdd.dbo.insert_finance_data @table = 1")
cnxn.commit()
cursor.close()

# Get Income statement report
print("Crawling financial statement done! {0:.3f}s".format(time.time() - start_time))
print("crawling income statement data...")
start_time = time.time()

cursor = cnxn.cursor()
cursor.execute("truncate table Stockdata_qdd.dbo.financial_info_tmp")
count_ticket = 1
for ticket in symbol_lst:
    print("No 2.{}_ Ticket:{}".format(count_ticket, ticket))
    count_ticket = count_ticket + 1
    try:
        loader = dl.FinanceLoader(symbol=ticket,
                                  start=starty_var,
                                  end=endy_var)
        data = loader.get_business_report()
        data = data.transpose()
        data = data.unstack().reset_index()
        data.rename(columns={'index': 'criteria', 'level_1': 'year', 0: 'value'}, inplace=True)
        data['Ticket'] = ticket
        cursor = cnxn.cursor()
        for index, row in data.iterrows():
            cursor.execute(
                "insert into Stockdata_qdd.dbo.financial_info_tmp (criteria,year,value, ticket) values (?,?,?,?)",
                row.criteria, row.year, row.value, row.Ticket)
            cnxn.commit()
    except (Exception,):
        pass
cursor.execute("exec stockdata_qdd.dbo.insert_finance_data @table = 2")
cnxn.commit()
cursor.close()

# Get cash-flow report
print("Crawling income statement done! {0:.3f}s".format(time.time() - start_time))
print("crawling cash-flow statement data...")
start_time = time.time()

cursor = cnxn.cursor()
cursor.execute("truncate table Stockdata_qdd.dbo.financial_info_tmp")
count_ticket = 1
for ticket in symbol_lst:
    print("No 3.{}_ Ticket:{}".format(count_ticket, ticket))
    count_ticket = count_ticket + 1
    try:
        loader = dl.FinanceLoader(symbol=ticket,
                                  start=starty_var,
                                  end=endy_var)
        data = loader.get_cashflow_report()
        data = data.transpose()
        data = data.unstack().reset_index()
        data.rename(columns={'index': 'criteria', 'level_1': 'year', 0: 'value'}, inplace=True)
        data['Ticket'] = ticket
        cursor = cnxn.cursor()
        for index, row in data.iterrows():
            cursor.execute(
                "insert into Stockdata_qdd.dbo.financial_info_tmp(criteria,year,value, ticket) values (?,?,?,?)",
                row.criteria, row.year, row.value, row.Ticket)
            cnxn.commit()
    except (Exception,):
        pass
cursor.execute("exec stockdata_qdd.dbo.insert_finance_data @table = 3")
cnxn.commit()
cursor.close()

# Get financial basic index
print("Crawling cash-flow statement done! {0:.3f}s".format(time.time() - start_time))
print("crawling financial index data...")
start_time = time.time()

cursor = cnxn.cursor()
cursor.execute("truncate table Stockdata_qdd.dbo.financial_info_tmp")
count_ticket = 1
for ticket in symbol_lst:
    print("No 4.{}_ Ticket:{}".format(count_ticket, ticket))
    count_ticket = count_ticket + 1
    try:
        data = get_basic_index(starty=starty_var, endy=endy_var, symbol=ticket)
        data = data.transpose()
        data = data.unstack().reset_index()
        data.rename(columns={'index': 'criteria', 'level_1': 'year', 0: 'value'}, inplace=True)
        data['Ticket'] = ticket
        cursor = cnxn.cursor()
        for index, row in data.iterrows():
            cursor.execute(
                "insert into Stockdata_qdd.dbo.financial_info_tmp(criteria,year,value, ticket) values (?,?,?,?)",
                row.criteria, row.year, row.value, row.Ticket)
            cnxn.commit()
    except (Exception,):
        pass
cursor.execute("exec stockdata_qdd.dbo.insert_finance_data @table = 4")
cnxn.commit()
cursor.close()
print("Crawling finance index done! {0:.3f}s".format(time.time() - start_time))
