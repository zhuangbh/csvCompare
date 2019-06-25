import pandas as pd
import numpy as np
import time
from pymongo import MongoClient
from pandas import ExcelWriter
from datetime import datetime
from datetime import timedelta

desired_width=1000
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 25)

client = MongoClient('47.244.166.253', 27017)
#client = MongoClient('localhost', 27017)

db = client['dashboard']
collection_qupital_blotter = db['qupital_blotter']
collection_fundpark_blotter = db['fundpark_blotter']
collection_culum_blotter = db['culum_blotter']
collection_incomlend_blotter = db['incomlend_blotter']
collection_portfolio = db['portfolio']
search_portfolio = db['search_portfolio']

def read_qupital_local_blotter():
    dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Qupital", converters={'Advanced Date': str, 'Due date': str, 'Advanced Amount':str}, keep_default_na = False)
    dfExcel = pd.DataFrame(dataExcel)
    dfExcel.fillna("NA", inplace=True)

    return dfExcel

def read_fundpark_local_blotter():
    dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Fundpark", converters={'Trade Date': str, 'Actual Repayment Date':str}, keep_default_na = False)
    dfExcel = pd.DataFrame(dataExcel)
    dfExcel.fillna("NA", inplace=True)

    return dfExcel

def read_culum_local_blotter():
    dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Culum", converters={'Purchase date': str, 'Expected payment date':str}, keep_default_na = False)
    dfExcel = pd.DataFrame(dataExcel)
    dfExcel.fillna("NA", inplace=True)

    return dfExcel

def read_incomlend_local_blotter():
    dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Incomlend", converters={'Effective Date': str, 'Expected repayment date':str}, keep_default_na = False)
    dfExcel = pd.DataFrame(dataExcel)
    dfExcel.fillna("NA", inplace=True)

    return dfExcel

###################################

def read_qupital_db_blotter():
    dfData = pd.DataFrame(list(collection_qupital_blotter.find()),
                          columns=['Auction no', 'Obligor', 'Seller No', 'Currency', 'Advanced Amount', 'Gross gain',
                                   'Platform Fee', 'Net total to be received', 'Gross Return (% pa)',
                                   'Net Return (% pa)', 'Advanced Date', 'Due date', 'Remitted date',
                                   'Late day (day(s))', 'Expected Duration (day(s))', 'Aucutal Duration (day(s))',
                                   'Status', 'Insured invoice', 'Obligor notification', 'Rationale'])

    return dfData

def read_fundpark_db_blotter():

    dfData = pd.DataFrame(list(collection_fundpark_blotter.find()),
                          columns=['Trade ID',	'Requested Loan Amount',	'Expected Tenor (days)',	'Interest Rate (per month)',	'(Expected) Interest Income',	'Buyer',	'Trade Date',	'Actual Repayment Date',	'Actual Tenor (day(s))',	'Rationale'])

    return dfData

def read_culum_db_blotter():
    dfData = pd.DataFrame(list(collection_culum_blotter.find()),
                          columns=['Investment No',	'Obligor',	'Seller',	'Total investment',	'Return on investment (annualised)',	'Credit grade',	'Purchase date',	'Expected payment date',	'Unrealized gain',	'Acutal payment date',	'In recovery',	'Realized gain',	'Tenor',	'Rationale'])

    return dfData

def read_incomlend_db_blotter():
    dfData = pd.DataFrame(list(collection_incomlend_blotter.find()),
                          columns=['Effective Date',	'Transaction Type',	'Invoice',	'Supplier Invoice Ref Number',	'Amount',	'Allocation Amount',	'Allocation Funder Name',	'External Reference',	'Trustee Approved',	'Status',	'Discount rate',	'Expected repayment date',	'Financing period',	'Credit insured'])
    return dfData

###################################

def update_qupital_blotter():
    collection_qupital_blotter.delete_many({})
    data = read_qupital_local_blotter().to_dict(orient='records')  # Here's our added param..
    collection_qupital_blotter.insert_many(data)

def update_fundpark_blotter():
    collection_fundpark_blotter.delete_many({})
    data = read_fundpark_local_blotter().to_dict(orient='records')  # Here's our added param..
    collection_fundpark_blotter.insert_many(data)

def update_culum_blotter():
    collection_culum_blotter.delete_many({})
    data = read_culum_local_blotter().to_dict(orient='records')  # Here's our added param..
    collection_culum_blotter.insert_many(data)

def update_incomlend_blotter():
    collection_incomlend_blotter.delete_many({})
    data = read_incomlend_local_blotter().to_dict(orient='records')  # Here's our added param..
    collection_incomlend_blotter.insert_many(data)

###################################

def update_qupital_portfolio(df):

    indexNames = df[df['Status'] == 'Remitted'].index
    df.drop(indexNames, inplace=True)

    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
    df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (0.128*df['Advanced Amount']), df['Advanced Amount'])

    df['Account'] = 'Qupital'
    df['% total'] = "NA"

    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df['Per position limit'] = "NA"

    df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)","Advanced Date":"Start date","Due date":"End date","Net Return (% pa)":"Annualized return (%)","Seller No":"Seller code","Obligor":"Obligor code"},inplace=True)

    df = df[['Trade ID',	'Currency',	'Advanced Amount',	'Tenor (days)',	'Start date',	'End date',	'Annualized return (%)','Advance amount (USD)',	'Seller code',	'Obligor code',	'% total',	'Account',	'Per position limit']]

    return df

def update_fundpark_portfolio(df):

    indexNames = df[df['Actual Repayment Date'] != ''].index
    df.drop(indexNames, inplace=True)

    df['Currency'] = df['Requested Loan Amount'].str.slice(0,3)
    df['Advanced Amount'] = df['Requested Loan Amount'].str.slice(5, 30)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = (12 * df['Interest Rate (per month)'] - 0.01)*100
    df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (0.128 * df['Advanced Amount']),df['Advanced Amount'])

    df['End date'] = pd.to_datetime(df['Trade Date']) + pd.to_timedelta(df['Expected Tenor (days)'], unit='D')
    df['End date'] = df['End date'].astype(str)

    df['Seller code'] = df['Trade ID'].str.slice(2, 6)

    df['Account'] = 'Fundpark'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df.rename(columns={"Expected Tenor (days)": "Tenor (days)","Trade Date":"Start date", "Buyer":"Obligor code"}, inplace=True)

    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df

def update_culum_portfolio(df):

    indexNames = df[df['Acutal payment date'] != 'NA'].index
    df.drop(indexNames, inplace=True)

    df['Currency'] = df['Total investment'].str.slice(0,3)

    df['Advanced Amount'] = df['Total investment'].str.slice(3, 30)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = 100 * df['Return on investment (annualised)']

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                          df['Advanced Amount'], (0.735 * df['Advanced Amount']))

    df['Account'] = 'Culum'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')
    
    df.rename(columns={"Investment No": "Trade ID", "Tenor":"Tenor (days)", "Purchase date":"Start date","Expected payment date":"End date","Seller":"Seller code", "Obligor":"Obligor code"}, inplace=True)


    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df


def update_incomlend_portfolio(df):

    df['Trade ID'] = ('L' + df['Supplier Invoice Ref Number'])

    df['Currency'] = df['Amount'].str.slice(0, 3)

    df['Advanced Amount'] = df['Allocation Amount'].str.slice(4, 15)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = ((1 /(1-df['Discount rate']))**12-1)*100

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                          df['Advanced Amount'], (df['Advanced Amount']/1.36))

    df['Obligor code'] = "NA"
    df['Account'] = 'Incomlend'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df.rename(columns={"Financing period": "Tenor (days)", "Effective Date":"Start date","Expected repayment date":"End date","Invoice":"Seller code"}, inplace=True)

    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df

def update_portfolio(dfQ,dfF,dfC,dfI):

    dataQ = dfQ.to_dict(orient='records')  # Here's our added param..
    dataF = dfF.to_dict(orient='records')
    dataC = dfC.to_dict(orient='records')
    dataI = dfI.to_dict(orient='records')

    collection_portfolio.delete_many({})
    collection_portfolio.insert_many(dataQ)
    collection_portfolio.insert_many(dataF)
    collection_portfolio.insert_many(dataC)
    collection_portfolio.insert_many(dataI)

    dfPortfolio = pd.DataFrame(list(collection_portfolio.find()),
                               columns=['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date',
                                        'End date', 'Annualized return (%)', 'Advance amount (USD)', 'Seller code',
                                        'Obligor code', '% total', 'Account', 'Per position limit'])

    dfPortfolio.to_excel(r'C:\Users\Solomon\Desktop\Current Portfolio Test.xlsx', sheet_name='current',index=False)


def date_portfolio(dfQ, dfF, dfC, dfI):
    dataQ = dfQ.to_dict(orient='records')  # Here's our added param..
    dataF = dfF.to_dict(orient='records')
    dataC = dfC.to_dict(orient='records')
    dataI = dfI.to_dict(orient='records')

    search_portfolio.delete_many({})
    search_portfolio.insert_many(dataQ)
    search_portfolio.insert_many(dataF)
    search_portfolio.insert_many(dataC)
    search_portfolio.insert_many(dataI)

def date_qupital_portfolio(df,date):

    indexNames = df[df['Status'] == 'Remitted'].index
    df.drop(indexNames, inplace=True)

    df['Advanced Date'] = pd.to_datetime(df['Advanced Date'])
    df = df[(df['Advanced Date'] <= date)]
    df['Advanced Date'] = df['Advanced Date'].astype(str)

    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
    df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (0.128*df['Advanced Amount']), df['Advanced Amount'])

    df['Account'] = 'Qupital'
    df['% total'] = "NA"

    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df['Per position limit'] = "NA"

    df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)","Advanced Date":"Start date","Due date":"End date","Net Return (% pa)":"Annualized return (%)","Seller No":"Seller code","Obligor":"Obligor code"},inplace=True)

    df = df[['Trade ID',	'Currency',	'Advanced Amount',	'Tenor (days)',	'Start date',	'End date',	'Annualized return (%)','Advance amount (USD)',	'Seller code',	'Obligor code',	'% total',	'Account',	'Per position limit']]

    return df

def date_fundpark_portfolio(df,date):
    indexNames = df[df['Actual Repayment Date'] != ''].index
    df.drop(indexNames, inplace=True)

    df['Trade Date'] = pd.to_datetime(df['Trade Date'])
    df = df[(df['Trade Date'] <= date)]
    df['Trade Date'] = df['Trade Date'].astype(str)

    df['Currency'] = df['Requested Loan Amount'].str.slice(0, 3)
    df['Advanced Amount'] = df['Requested Loan Amount'].str.slice(5, 30)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = (12 * df['Interest Rate (per month)'] - 0.01) * 100
    df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (0.128 * df['Advanced Amount']),
                                          df['Advanced Amount'])

    df['End date'] = pd.to_datetime(df['Trade Date']) + pd.to_timedelta(df['Expected Tenor (days)'], unit='D')
    df['End date'] = df['End date'].astype(str)

    df['Seller code'] = df['Trade ID'].str.slice(2, 6)

    df['Account'] = 'Fundpark'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df.rename(columns={"Expected Tenor (days)": "Tenor (days)", "Trade Date": "Start date", "Buyer": "Obligor code"},
              inplace=True)

    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df

def date_culum_portfolio(df,date):
    indexNames = df[df['Acutal payment date'] != 'NA'].index
    df.drop(indexNames, inplace=True)

    df['Purchase date'] = pd.to_datetime(df['Purchase date'])
    df = df[(df['Purchase date'] <= date)]
    df['Purchase date'] = df['Purchase date'].astype(str)

    df['Currency'] = df['Total investment'].str.slice(0, 3)

    df['Advanced Amount'] = df['Total investment'].str.slice(3, 30)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = 100 * df['Return on investment (annualised)']

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                          df['Advanced Amount'], (0.735 * df['Advanced Amount']))

    df['Account'] = 'Culum'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df.rename(columns={"Investment No": "Trade ID", "Tenor": "Tenor (days)", "Purchase date": "Start date",
                       "Expected payment date": "End date", "Seller": "Seller code", "Obligor": "Obligor code"},
              inplace=True)

    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df


def date_incomlend_portfolio(df,date):

    df['Effective Date'] = pd.to_datetime(df['Effective Date'])
    df = df[(df['Effective Date'] <= date)]
    df['Effective Date'] = df['Effective Date'].astype(str)

    df['Trade ID'] = ('L' + df['Supplier Invoice Ref Number'])

    df['Currency'] = df['Amount'].str.slice(0, 3)

    df['Advanced Amount'] = df['Allocation Amount'].str.slice(4, 15)
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
    df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

    df['Annualized return (%)'] = ((1 / (1 - df['Discount rate'])) ** 12 - 1) * 100

    df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                          df['Advanced Amount'], (df['Advanced Amount'] / 1.36))

    df['Obligor code'] = "NA"
    df['Account'] = 'Incomlend'
    df['% total'] = "NA"
    df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

    df.rename(columns={"Financing period": "Tenor (days)", "Effective Date": "Start date",
                       "Expected repayment date": "End date", "Invoice": "Seller code"}, inplace=True)

    df = df[
        ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
         'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

    return df


def save_xls(date1, date2, xls_path):
    i = datetime.strptime(date1, '%Y-%m-%d')
    j = datetime.strptime(date2, '%Y-%m-%d')
    with ExcelWriter(xls_path) as writer:
        while i <= j:
            df = pd.concat([date_qupital_portfolio(read_qupital_local_blotter(),i), date_fundpark_portfolio(read_fundpark_local_blotter(), i),date_culum_portfolio(read_culum_local_blotter(), i),date_incomlend_portfolio(read_incomlend_db_blotter(), i)])
            df.to_excel(writer, sheet_name=i.strftime('%Y-%m-%d'), index=False)
            writer.save()
            print(i)
            i = i + timedelta(days=1)

a = r'C:\Users\Solomon\Desktop\Export Test June.xlsx'

save_xls('2019-06-01', '2019-06-30', a)


while True:
    start = time.time()

    if read_qupital_db_blotter().equals(read_qupital_local_blotter()):
        print('sameQ')
    else:
        print('diffQ')
        update_qupital_blotter()
        update_qupital_portfolio(read_qupital_local_blotter())

    if read_fundpark_db_blotter().equals(read_fundpark_local_blotter()):
        print('sameF')
    else:
        print('diffF')
        update_fundpark_blotter()
        update_fundpark_portfolio(read_fundpark_local_blotter())

    if read_culum_db_blotter().equals(read_culum_local_blotter()):
        print('sameC')
    else:
        print('diffC')
        update_culum_blotter()
        update_culum_portfolio(read_culum_local_blotter())

    if read_incomlend_db_blotter().equals(read_incomlend_local_blotter()):
        print('sameI')
    else:
        print('diffI')
        update_incomlend_blotter()
        update_incomlend_portfolio(read_incomlend_local_blotter())

    update_portfolio(update_qupital_portfolio(read_qupital_local_blotter()),
                     update_fundpark_portfolio(read_fundpark_local_blotter()),
                     update_culum_portfolio(read_culum_local_blotter()),update_incomlend_portfolio(read_incomlend_local_blotter()))

    date_portfolio(date_qupital_portfolio(read_qupital_local_blotter(), '2019-06-01'),
                   date_fundpark_portfolio(read_fundpark_local_blotter(), '2019-06-01'),
                   date_culum_portfolio(read_culum_local_blotter(), '2019-06-01'),
                   date_incomlend_portfolio(read_incomlend_db_blotter(), '2019-06-01'))


    end = time.time()
    print(end - start)

    time.sleep(1)







