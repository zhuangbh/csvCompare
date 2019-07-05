import pandas as pd
import numpy as np
import time
from pymongo import MongoClient
from pandas import ExcelWriter
from datetime import datetime
from datetime import timedelta
from src.database import Database


class Download():
    desired_width = 1000
    pd.set_option('display.width', desired_width)
    np.set_printoptions(linewidth=desired_width)
    pd.set_option('display.max_columns', 25)

    #client = MongoClient('47.244.166.253', 27017)
    client = MongoClient('47.74.231.215', 27018)
    #client = MongoClient('localhost',27017)

    db = client['dashboard']
    collection_qupital_blotter = db['qupital_blotter']
    collection_fundpark_blotter = db['fundpark_blotter']
    collection_culum_blotter = db['culum_blotter']
    collection_incomlend_blotter = db['incomlend_blotter']
    collection_portfolio = db['portfolio']
    collection_search_portfolio = db['search_portfolio']

    @staticmethod

    def read_total_advance_USD():
        df = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name="Subscription")

        #total = df.iloc[1:2,7:8]
        #total = df.loc[1:2,7:8].values[0]

        total = df.at[0, 'Total Subscription amount (USD)']

        return(total)

    def read_qupital_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name="Qupital",
                                  converters={'Advanced Date': str, 'Due date': str, 'Advanced Amount': str},
                                  keep_default_na=False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    @staticmethod
    def read_fundpark_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name="Fundpark",
                                  converters={'Trade Date': str, 'Actual Repayment Date': str}, keep_default_na=False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)


        return dfExcel

    @staticmethod
    def read_culum_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name="Culum",
                                  converters={'Purchase date': str, 'Expected payment date': str},
                                  keep_default_na=False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    @staticmethod
    def read_incomlend_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name="Incomlend",
                                  converters={'Effective Date': str, 'Expected repayment date': str},
                                  keep_default_na=False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    ###################################

    @staticmethod
    def read_qupital_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_qupital_blotter.find()),
                              columns=['Auction no', 'Obligor', 'Seller No', 'Currency', 'Advanced Amount',
                                       'Gross gain',
                                       'Platform Fee', 'Net total to be received', 'Gross Return (% pa)',
                                       'Net Return (% pa)', 'Advanced Date', 'Due date', 'Remitted date',
                                       'Late day (day(s))', 'Expected Duration (day(s))', 'Aucutal Duration (day(s))',
                                       'Status', 'Insured invoice', 'Obligor notification', 'Rationale'])

        return dfData

    @staticmethod
    def read_fundpark_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_fundpark_blotter.find()),
                              columns=['Trade ID', 'Requested Loan Amount', 'Expected Tenor (days)',
                                       'Interest Rate (per month)', '(Expected) Interest Income', 'Buyer', 'Trade Date',
                                       'Actual Repayment Date', 'Actual Tenor (day(s))', 'Rationale'])

        return dfData

    @staticmethod
    def read_culum_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_culum_blotter.find()),
                              columns=['Investment No', 'Obligor', 'Seller', 'Total investment',
                                       'Return on investment (annualised)', 'Credit grade', 'Purchase date',
                                       'Expected payment date', 'Unrealized gain', 'Acutal payment date', 'In recovery',
                                       'Realized gain', 'Tenor', 'Rationale'])

        return dfData

    @staticmethod
    def read_incomlend_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_incomlend_blotter.find()),
                              columns=['Effective Date', 'Transaction Type', 'Invoice', 'Supplier Invoice Ref Number',
                                       'Amount', 'Allocation Amount', 'Allocation Funder Name', 'External Reference',
                                       'Trustee Approved', 'Status', 'Discount rate', 'Expected repayment date',
                                       'Financing period', 'Credit insured', 'Supplier name'])
        return dfData

    ###################################

    @staticmethod
    def update_qupital_blotter():
        Download.collection_qupital_blotter.delete_many({})
        data = Download.read_qupital_local_blotter().to_dict(orient='records')  # Here's our added param..
        if data == True:
            Download.collection_qupital_blotter.insert_many(data)

    @staticmethod
    def update_fundpark_blotter():
        Download.collection_fundpark_blotter.delete_many({})
        data = Download.read_fundpark_local_blotter().to_dict(orient='records')  # Here's our added param..
        if data == True:
            Download.collection_fundpark_blotter.insert_many(data)

    @staticmethod
    def update_culum_blotter():
        Download.collection_culum_blotter.delete_many({})
        data = Download.read_culum_local_blotter().to_dict(orient='records')  # Here's our added param..
        if data == True:
            Download.collection_culum_blotter.insert_many(data)

    @staticmethod
    def update_incomlend_blotter():
        Download.collection_incomlend_blotter.delete_many({})
        data = Download.read_incomlend_local_blotter().to_dict(orient='records')  # Here's our added param..
        if data == True:
            Download.collection_incomlend_blotter.insert_many(data)

    ###################################
    '''
    @staticmethod
    def update_qupital_portfolio(df):
        indexNames = df[df['Status'] == 'Remitted'].index
        df.drop(indexNames, inplace=True)

        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
        df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount'] / 7.8),
                                              df['Advanced Amount'])

        df['Account'] = 'Qupital'
        df['% total'] = "NA"

        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)",
                           "Advanced Date": "Start date", "Due date": "End date",
                           "Net Return (% pa)": "Annualized return (%)", "Seller No": "Seller code",
                           "Obligor": "Obligor code"}, inplace=True)

        df = df[['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
                 'Annualized return (%)', 'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account',
                 'Per position limit']]

        return df

    @staticmethod
    def update_fundpark_portfolio(df):
        indexNames = df[df['Actual Repayment Date'] != ''].index
        df.drop(indexNames, inplace=True)

        df['Currency'] = df['Requested Loan Amount'].str.slice(0, 3)
        df['Advanced Amount'] = df['Requested Loan Amount'].str.slice(5, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = (12 * df['Interest Rate (per month)'] - 0.01) * 100
        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount'] / 7.8),
                                              df['Advanced Amount'])

        df['End date'] = pd.to_datetime(df['Trade Date']) + pd.to_timedelta(df['Expected Tenor (days)'], unit='D')
        df['End date'] = df['End date'].astype(str)

        df['Seller code'] = df['Trade ID'].str.slice(2, 6)

        df['Account'] = 'Fundpark'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(
            columns={"Expected Tenor (days)": "Tenor (days)", "Trade Date": "Start date", "Buyer": "Obligor code"},
            inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def update_culum_portfolio(df):
        indexNames = df[df['Acutal payment date'] != 'NA'].index
        df.drop(indexNames, inplace=True)

        df['Currency'] = df['Total investment'].str.slice(0, 3)

        df['Advanced Amount'] = df['Total investment'].str.slice(3, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = 100 * df['Return on investment (annualised)']

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                              df['Advanced Amount'], (df['Advanced Amount'] / 1.36))

        df['Account'] = 'Culum'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Investment No": "Trade ID", "Tenor": "Tenor (days)", "Purchase date": "Start date",
                           "Expected payment date": "End date", "Seller": "Seller code", "Obligor": "Obligor code"},
                  inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def update_incomlend_portfolio(df):
        df['Trade ID'] = ('L' + df['Supplier Invoice Ref Number'])

        df['Currency'] = df['Amount'].str.slice(0, 3)

        df['Advanced Amount'] = df['Allocation Amount'].str.slice(4, 15)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = ((1 / (1 - df['Discount rate'])) ** 12 - 1) * 100

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                              df['Advanced Amount'], (df['Advanced Amount'] / 1.36))

        df['Account'] = 'Incomlend'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Financing period": "Tenor (days)", "Effective Date": "Start date",
                           "Expected repayment date": "End date", "Invoice": "Seller code",
                           "Supplier name": "Obligor code"}, inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df
        '''

    @staticmethod
    def search_portfolio(dfQ, dfF, dfC, dfI,i):

        df = pd.concat([dfQ, dfF, dfC, dfI])

        if type(i) == str:
            i = pd.to_datetime(i)

        df['Late day'] = np.where(i > pd.to_datetime(df['End date']),
                 (i - pd.to_datetime(df['End date'])).dt.days, pd.to_timedelta("0"))

        df['Late day'] = df['Late day'].astype(str)


        '''
        df['Late day'] = np.where(i > pd.to_datetime(df['End date']),
                                  df['Late day'], pd.to_timedelta("0"))

        df['Late day'] = df['Late day'].astype(str)
        '''

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
             'Per position limit']]


        #total_advance_USD = ((7000000 + 9850000) / 7.8 + 10100000)
        #total_advance_USD = 13000000
        total_advance_USD = Download.read_total_advance_USD()

        cash_advance_USD = total_advance_USD - df['Advance amount (USD)'].sum()
        invested_advance_USD = df['Advance amount (USD)'].sum()

        df['% total'] = (df['Advance amount (USD)'] / total_advance_USD)
        #df['% total'] = (df['Advance amount (USD)'] / 10)

        df['Annualized return (%)'] = df['Annualized return (%)'] / 100

        cash_ratio = (cash_advance_USD / total_advance_USD)

        df['temp'] = (df['Annualized return (%)'] * df['% total'])
        portfolio_gross_return = df['temp'].sum()
        weighted_average_return = portfolio_gross_return / (1 - (cash_ratio))

        df2 = pd.DataFrame(
            np.array([["Cash", "", "", "", "", "", "", cash_advance_USD, "Cash", "Cash", cash_ratio, "Cash", "", ""],
                      ["Total", "", "", "", "", "Weighted average return (%)", weighted_average_return,
                       total_advance_USD,
                       "", "", 1, "", "", ""],
                      ["Invested", "", "", "", "", "Portfolio gross return (%)", portfolio_gross_return,
                       invested_advance_USD, "", "", "", "", "", ""]]),
            columns=['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date',
                     'End date',
                     'Annualized return (%)',
                     'Advance amount (USD)', 'Seller code', 'Obligor code', '% total',
                     'Account', 'Late day', 'Per position limit'])

        df2['Advance amount (USD)'] = df2['Advance amount (USD)'].astype(float)

        df = pd.concat([df, df2])

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
             'Per position limit']]


        data = df.to_dict(orient='records')  # Here's our added param..

        Download.collection_search_portfolio.delete_many({})
        if df.empty!=True:
            Download.collection_search_portfolio.insert_many(data)


    @staticmethod
    def search_portfolio_front(i=datetime.today()):
        Download.search_portfolio(Download.date_qupital_portfolio(Download.read_qupital_local_blotter(), i),
        Download.date_fundpark_portfolio(Download.read_fundpark_local_blotter(), i),
        Download.date_culum_portfolio(Download.read_culum_local_blotter(), i),
        Download.date_incomlend_portfolio(Download.read_incomlend_local_blotter(), i),i)

    '''
    @staticmethod
    def date_portfolio(dfQ, dfF, dfC, dfI):
        dataQ = dfQ.to_dict(orient='records')  # Here's our added param..
        dataF = dfF.to_dict(orient='records')
        dataC = dfC.to_dict(orient='records')
        dataI = dfI.to_dict(orient='records')

        Download.collection_search_portfolio.delete_many({})
        Download.collection_search_portfolio.insert_many(dataQ)
        Download.collection_search_portfolio.insert_many(dataF)
        Download.collection_search_portfolio.insert_many(dataC)
        Download.collection_search_portfolio.insert_many(dataI)
    '''

    @staticmethod
    def date_qupital_portfolio(df, date):
        '''
        indexNames = df[df['Status'] == 'Remitted'].index
        df.drop(indexNames, inplace=True)
        '''

        df['Advanced Date'] = pd.to_datetime(df['Advanced Date'])
        df = df[(df['Advanced Date'] <= date)]
        df['Advanced Date'] = df['Advanced Date'].astype(str)

        df['Remitted date'] = pd.to_datetime(df['Remitted date'], errors='coerce')
        df2 = df[(df['Status'] == "Traded")]

        df = df[(df['Remitted date'] > date)]
        df = pd.concat([df,df2])

        df['Remitted date'] = df['Remitted date'].astype(str)


        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
        df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount'] / 7.8),
                                              df['Advanced Amount'])

        df['Account'] = 'Qupital'
        df['% total'] = "NA"

        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)",
                           "Advanced Date": "Start date", "Due date": "End date",
                           "Net Return (% pa)": "Annualized return (%)", "Seller No": "Seller code",
                           "Obligor": "Obligor code"}, inplace=True)

        df = df[['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
                 'Annualized return (%)', 'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account',
                 'Per position limit']]

        return df


    @staticmethod
    def date_fundpark_portfolio(df, date):
        '''
        indexNames = df[df['Actual Repayment Date'] != ''].index
        df.drop(indexNames, inplace=True)
        '''

        df['Trade Date'] = pd.to_datetime(df['Trade Date'])
        df = df[(df['Trade Date'] <= date)]
        df['Trade Date'] = df['Trade Date'].astype(str)

        df2 = df[(df['Actual Repayment Date'] == "")]
        df['Actual Repayment Date'] = pd.to_datetime(df['Actual Repayment Date'],errors='coerce')

        df = df[(df['Actual Repayment Date'] > date)]

        df = pd.concat([df,df2])


        df['Actual Repayment Date'] = df['Actual Repayment Date'].astype(str)

        df['Currency'] = df['Requested Loan Amount'].str.slice(0, 3)
        df['Advanced Amount'] = df['Requested Loan Amount'].str.slice(5, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = (12 * df['Interest Rate (per month)'] - 0.01) * 100
        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount'] / 7.8),
                                              df['Advanced Amount'])

        df['End date'] = pd.to_datetime(df['Trade Date']) + pd.to_timedelta(df['Expected Tenor (days)'], unit='D')
        df['End date'] = df['End date'].astype(str)

        df['Seller code'] = df['Trade ID'].str.slice(2, 6)

        df['Account'] = 'Fundpark'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(
            columns={"Expected Tenor (days)": "Tenor (days)", "Trade Date": "Start date", "Buyer": "Obligor code"},
            inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def date_culum_portfolio(df, date):
        '''
        indexNames = df[df['Acutal payment date'] != 'NA'].index
        df.drop(indexNames, inplace=True)
        '''

        df['Purchase date'] = pd.to_datetime(df['Purchase date'])
        df = df[(df['Purchase date'] <= date)]
        df['Purchase date'] = df['Purchase date'].astype(str)

        df2 = df[(df['Acutal payment date'] == 'NA')]
        df['Acutal payment date'] = pd.to_datetime(df['Acutal payment date'], errors = 'coerce')
        #df['Acutal payment date'] = np.where(df['Acutal payment date'] != 'NA', pd.to_datetime(df['Acutal payment date']),"")


        df = df[(df['Acutal payment date'] > date)]
        df['Acutal payment date'] = df['Acutal payment date'].astype(str)

        df = pd.concat([df, df2])



        df['Currency'] = df['Total investment'].str.slice(0, 3)

        df['Advanced Amount'] = df['Total investment'].str.slice(3, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = 100 * df['Return on investment (annualised)']

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                              df['Advanced Amount'], (df['Advanced Amount'] / 1.36))

        df['Account'] = 'Culum'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Investment No": "Trade ID", "Tenor": "Tenor (days)", "Purchase date": "Start date",
                           "Expected payment date": "End date", "Seller": "Seller code", "Obligor": "Obligor code"},
                  inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def date_incomlend_portfolio(df, date):
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

        df['Account'] = 'Incomlend'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Financing period": "Tenor (days)", "Effective Date": "Start date",
                           "Expected repayment date": "End date", "Invoice": "Seller code",
                           "Supplier name": "Obligor code"}, inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df



    @staticmethod
    def loan_transaction(writer, start_date, end_date):

        dfQ = Download.read_qupital_local_blotter()

        dfQ['Trade date'] = dfQ['Advanced Date']
        dfQ['Settlement date'] = dfQ['Advanced Date']
        dfQ['Intended maturity date'] = dfQ['Due date']
        dfQ['Counterpart'] = (dfQ['Seller No']+'+' + dfQ['Obligor'])
        dfQ['Day count method'] = "act/365"
        dfQ['Initial charge date'] = dfQ['Advanced Date']
        dfQ['Company Bank'] = "QUPITAL LIMITED"
        dfQ['Company bank account'] = "QUP 016-478-788652496"
        dfQ['Comment'] = np.where(dfQ['Status'] == 'Traded',
                                              dfQ['Expected Duration (day(s))'], dfQ['Aucutal Duration (day(s))'])

        dfQ.rename(columns={"Auction no": "External reference", "Currency": "CCY",
                           "Advanced Amount": "Starting amount",
                            "Gross Return (% pa)": "Interest rate fixed"}, inplace=True)


        dfF = Download.read_fundpark_local_blotter()

        dfF['Settlement date'] = dfF['Trade Date']

        dfF['Intended maturity date'] = pd.to_datetime(dfF['Trade Date']) + pd.to_timedelta(dfF['Expected Tenor (days)'], unit='D')
        dfF['Intended maturity date'] = dfF['Intended maturity date'].astype(str)
        dfF['Counterpart'] = dfF['Trade ID'].str.slice(2, 10)

        dfF['Currency'] = dfF['Requested Loan Amount'].str.slice(0, 3)

        dfF['Interest rate fixed'] = (12 * dfF['Interest Rate (per month)'] - 0.01) * 100

        dfF['Day count method'] = "act/360"
        dfF['Initial charge date'] = dfF['Trade Date']

        dfF['Company Bank'] = "MKA LAW OFFICE (CLIENTS'-A/C FUNDPARK LTD)"
        dfF['Company bank account'] = "FP 012-737-0-013389-7"

        dfF['Starting amount'] = dfF['Requested Loan Amount'].str.slice(5, 30)
        dfF['Starting amount'] = dfF['Starting amount'].str.replace(',', '')
        dfF['Starting amount'] = dfF['Starting amount'].str.replace(' ', '').astype(float)


        dfF['Comment'] = np.where(dfF['Actual Repayment Date'] == "",
                                              dfF['Expected Tenor (days)'], dfF['Actual Tenor (day(s))'])

        dfF.rename(columns={"Trade Date":"Trade date","Currency": "CCY",
            "Trade ID": "External reference"}, inplace=True)


        dfC = Download.read_culum_local_blotter()

        dfC['Settlement date'] = dfC['Purchase date']

        dfC['Intended maturity date'] = dfC['Expected payment date']
        dfC['Intended maturity date'] = dfC['Intended maturity date'].astype(str)

        dfC['Counterpart'] = (dfC['Seller']+'+' + dfC['Obligor'])

        dfC['CCY'] = dfC['Total investment'].str.slice(0, 3)

        dfC['Starting amount'] = dfC['Total investment'].str.slice(3, 30)
        dfC['Starting amount'] = dfC['Starting amount'].str.replace(',', '')
        dfC['Starting amount'] = dfC['Starting amount'].str.replace(' ', '').astype(float)

        dfC2 = dfC[(dfC['Acutal payment date'] != 'NA')]
        dfC2['Actual Tenor (day(s))'] = pd.to_datetime(dfC2['Acutal payment date'])-pd.to_datetime(dfC2['Purchase date'])
        dfC2['Actual Tenor (day(s))'] = dfC2['Actual Tenor (day(s))'].astype(str)
        dfC2['Actual Tenor (day(s))'] = dfC2['Actual Tenor (day(s))'].str.split(' ').str[0]

        dfC = dfC[(dfC['Acutal payment date'] == 'NA')]
        dfC['Comment'] = dfC['Tenor']
        dfC2['Comment'] = dfC2['Actual Tenor (day(s))']


        dfC = pd.concat([dfC,dfC2])

        dfC['Interest rate fixed'] = 100 * dfC['Return on investment (annualised)']

        dfC['Day count method'] = "act/365"

        dfC['Initial charge date'] = dfC['Purchase date']

        dfC['Company Bank'] = "Vistra Trust - Culum Capital Pte Ltd"
        dfC['Company bank account'] = "CUL 003-946095-3"

        dfC.rename(columns={"Purchase date": "Trade date",
                            "Investment No": "External reference"}, inplace=True)


        dfI = Download.read_incomlend_local_blotter()

        dfI['Settlement date'] = dfI['Effective Date']

        dfI['Intended maturity date'] = dfI['Expected repayment date']
        dfI['Intended maturity date'] = dfI['Intended maturity date'].astype(str)

        dfI['Counterpart'] = (dfI['Invoice'] + '+' + dfI['Supplier name'])

        dfI['CCY'] = dfI['Amount'].str.slice(0, 3)

        dfI['Starting amount'] = dfI['Allocation Amount'].str.slice(4, 15)
        dfI['Starting amount'] = dfI['Starting amount'].str.replace(',', '')
        dfI['Starting amount'] = dfI['Starting amount'].str.replace(' ', '').astype(float)

        dfI['Comment'] = dfI['Financing period']

        '''
        dfI2 = dfI[(dfI['Acutal payment date'] != 'NA')]
        dfI2['Actual Tenor (day(s))'] = pd.to_datetime(dfI2['Acutal payment date']) - pd.to_datetime(
            dfI2['Purchase date'])
        dfI2['Actual Tenor (day(s))'] = dfI2['Actual Tenor (day(s))'].astype(str)

        dfI = dfI[(dfI['Acutal payment date'] == 'NA')]
        dfI['Comment'] = dfI['Tenor']
        dfI2['Comment'] = dfI2['Actual Tenor (day(s))']   

        dfI = pd.concat([dfI, dfI2])
        '''
        dfI['Interest rate fixed'] = ((1 / (1 - dfI['Discount rate'])) ** 12 - 1) * 100

        dfI['Day count method'] = "act/360"

        dfI['Initial charge date'] = dfI['Effective Date']

        dfI['External reference'] = ('L' + dfI['Supplier Invoice Ref Number'])

        dfI['Company Bank'] = "AMICORP TRUSTEES - Incomlend PL Funder"
        dfI['Company bank account'] = "INC 003-956032-0"

        dfI.rename(columns={"Effective Date":"Trade date"}, inplace=True)



        df = pd.concat([dfQ, dfF, dfC, dfI])


        df['Interest rate fixed'] = df['Interest rate fixed']/100


        df['Company'] = ""
        df['Maturity date'] = ""
        df['Trade type'] = ""
        df['Product type'] = ""
        df['Float rate SM'] = ""
        df['Float rate set frequency'] = ""
        df['Floating margin'] = ""
        df['Charging freq'] = ""
        df['Ea group'] = ""
        df['Counterpart bank'] = ""
        df['Counterpart bank account'] = ""
        df['Counterpart clearing code type'] = ""
        df['Counterpart clearing code'] = ""



        df = df[['Trade date',	'Settlement date',	'Company',	'Maturity date',	'Intended maturity date',	'Counterpart',
                'Trade type',	'Product type',	'CCY',	'Starting amount',	'Interest rate fixed',	'Float rate SM',
                'Float rate set frequency',	'Floating margin',	'Day count method',	'Charging freq',	'Initial charge date',
                'Ea group',	'Comment',	'External reference',	'Company Bank',	'Company bank account',	'Counterpart bank',
                'Counterpart bank account',	'Counterpart clearing code type',	'Counterpart clearing code']]


        df['Settlement date'] = df['Settlement date'].str.split(' ').str[0]
        df['Intended maturity date'] = df['Intended maturity date'].str.split(' ').str[0]
        df['Initial charge date'] = df['Initial charge date'].str.split(' ').str[0]

        '''
        df['Advanced Date'] = pd.to_datetime(df['Advanced Date'])
        df = df[(df['Advanced Date'] <= date)]
        df['Advanced Date'] = df['Advanced Date'].astype(str)
        '''

        i = datetime.strptime(start_date, '%Y-%m-%d')
        j = datetime.strptime(end_date, '%Y-%m-%d')


        df['Trade date'] = pd.to_datetime(df['Trade date'])
        df = df[(df['Trade date'] >= i)]
        df = df[(df['Trade date'] <= j)]
        df['Trade date'] = df['Trade date'].astype(str)

        format1 = writer.book.add_format({'num_format': '#,##0.00'})
        format2 = writer.book.add_format({'num_format': '0.00%'})

        df.to_excel(writer, index=False)

        writer.sheets['Sheet1'].set_column(0, 0, 9.5, format1)
        writer.sheets['Sheet1'].set_column(1, 1, 14, format1)
        writer.sheets['Sheet1'].set_column(2, 2, 9.5, format1)
        writer.sheets['Sheet1'].set_column(3, 3, 12, format1)
        writer.sheets['Sheet1'].set_column(4, 4, 20, format1)
        writer.sheets['Sheet1'].set_column(5, 5, 20, format1)
        writer.sheets['Sheet1'].set_column(7, 7, 11, format1)
        writer.sheets['Sheet1'].set_column(9, 9, 14, format1)
        writer.sheets['Sheet1'].set_column(10, 10, 15, format2)
        writer.sheets['Sheet1'].set_column(11, 16, 15, format1)
        writer.sheets['Sheet1'].set_column(12, 12, 20, format1)
        writer.sheets['Sheet1'].set_column(19, 20, 15, format1)
        writer.sheets['Sheet1'].set_column(21, 21, 20, format1)
        writer.sheets['Sheet1'].set_column(22, 22, 15, format1)
        writer.sheets['Sheet1'].set_column(23, 25, 25, format1)


    @staticmethod
    def save_xls(i, writer):
        format1 = writer.book.add_format({'num_format': '#,##0.00'})
        format2 = writer.book.add_format({'num_format': '0.00%'})
        format3 = writer.book.add_format({'bold': True, 'font_color': '#FF8C00'})
        format4 = writer.book.add_format({'bold': True, 'font_color': '#FFA500'})

        df = pd.concat([Download.date_qupital_portfolio(Download.read_qupital_local_blotter(), i),
                        Download.date_fundpark_portfolio(Download.read_fundpark_local_blotter(), i),
                        Download.date_culum_portfolio(Download.read_culum_local_blotter(), i),
                        Download.date_incomlend_portfolio(Download.read_incomlend_local_blotter(), i)])


        df['Late day'] = np.where(i > pd.to_datetime(df['End date']),
                                  (i - pd.to_datetime(df['End date'])).dt.days, pd.to_timedelta("0"))

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
             'Per position limit']]

        total_advance_USD = Download.read_total_advance_USD()
        #total_advance_USD = 13000000

        cash_advance_USD = total_advance_USD - df['Advance amount (USD)'].sum()

        invested_advance_USD = df['Advance amount (USD)'].sum()

        df['% total'] = (df['Advance amount (USD)'] / total_advance_USD)



        df['Annualized return (%)'] = df['Annualized return (%)'] / 100

        cash_ratio = (cash_advance_USD / total_advance_USD)

        df['temp'] = (df['Annualized return (%)'] * df['% total'])
        portfolio_gross_return = df['temp'].sum()
        weighted_average_return = portfolio_gross_return / (1 - (cash_ratio))

        df2 = pd.DataFrame(
            np.array([["Cash", "", "", "", "", "", 0, cash_advance_USD, "Cash", "Cash", cash_ratio, "Cash", "", ""],
                      ["Total", "", "", "", "", "Weighted average return (%)", weighted_average_return,
                       total_advance_USD,
                       "", "", 1, "", "", ""],
                      ["Invested", "", "", "", "", "Portfolio gross return (%)", portfolio_gross_return,
                       invested_advance_USD, "", "", 1-cash_ratio, "", "", ""]]),
            columns=['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date',
                     'End date',
                     'Annualized return (%)',
                     'Advance amount (USD)', 'Seller code', 'Obligor code', '% total',
                     'Account', 'Late day', 'Per position limit'])

        df2['Advance amount (USD)'] = df2['Advance amount (USD)'].astype(float)
        df2['Annualized return (%)'] = df2['Annualized return (%)'].astype(float)
        df2['% total'] = df2['% total'].astype(float)


        df = pd.concat([df, df2])


        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
             'Per position limit']]


        df['Weighted Average'] = df['Advance amount (USD)']*df['Annualized return (%)']

        df3 = df.groupby(['Seller code'], as_index=False).sum()
        df3 = df3[['Seller code', 'Advance amount (USD)', '% total', 'Weighted Average']]

        #total_weighted_average = df3['Weighted Average'].sum()

        df3['Weighted Average'] = df3['Weighted Average']/df3['Advance amount (USD)']

        df3 = df3.iloc[1:]
        df33 = pd.DataFrame(np.array([["Seller code", "SUM of Advance amount (USD)", "SUM of % total","Weighted Average Return"]]),
                            columns=['Seller code', 'Advance amount (USD)', '% total','Weighted Average'])

        df34 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1, weighted_average_return]]),
                            columns=['Seller code', 'Advance amount (USD)', '% total','Weighted Average'])




        df3 = pd.concat([df33, df3, df34])
        df3.rename(columns={"Advance amount (USD)": "Advance amount (USD) S", "% total": "% total S"},
                   inplace=True)


        df4 = df.groupby(['Account'], as_index=False).sum()
        df4 = df4[['Account', 'Advance amount (USD)', '% total']]
        df4 = df4.iloc[1:]

        df44 = pd.DataFrame(np.array([["Account", "SUM of Advance amount (USD)", "SUM of % total"]]),
                            columns=['Account', 'Advance amount (USD)', '% total'])

        df45 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                            columns=['Account', 'Advance amount (USD)', '% total'])

        df4 = pd.concat([df44, df4, df45])

        df4.rename(columns={"Advance amount (USD)": "Advance amount (USD) A", "% total": "% total A"},
                   inplace=True)

        df5 = df.groupby(['Obligor code'], as_index=False).sum()
        df5 = df5[['Obligor code', 'Advance amount (USD)', '% total']]
        df5 = df5.iloc[1:]

        df55 = pd.DataFrame(np.array([["Obligor code", "SUM of Advance amount (USD)", "SUM of % total"]]),
                            columns=['Obligor code', 'Advance amount (USD)', '% total'])

        df56 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                            columns=['Obligor code', 'Advance amount (USD)', '% total'])

        df5 = pd.concat([df55, df5, df56])

        df5.rename(columns={"Advance amount (USD)": "Advance amount (USD) O", "% total": "% total O"},
                   inplace=True)




        df6 = df[['Trade ID', 'Advance amount (USD)', '% total','Late day']]
        indexNames = df6[df6['Trade ID'] == 'Invested'].index
        df6.drop(indexNames, inplace=True)
        indexNames = df6[df6['Trade ID'] == 'Total'].index
        df6.drop(indexNames, inplace=True)

        df66 = pd.DataFrame(np.array([["Trade ID", "SUM of Advance amount (USD)", "SUM of % total","Late day"]]),
                            columns=['Trade ID', 'Advance amount (USD)', '% total','Late day'])

        df67 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1,""]]),
                            columns=['Trade ID', 'Advance amount (USD)', '% total','Late day'])

        df6 = pd.concat([df66, df6, df67])

        df6.rename(columns={"Advance amount (USD)": "Advance amount (USD) T", "% total": "% total T"}, inplace=True)

        df7 = pd.concat([df3, df5, df6, df4])

        df7 = df7[
            ['Seller code', 'Advance amount (USD) S', '% total S','Weighted Average', 'Obligor code', 'Advance amount (USD) O',
             '% total O',
             'Trade ID', 'Advance amount (USD) T', '% total T','Late day', 'Account', 'Advance amount (USD) A', '% total A']]

        sheet_name = i.strftime('%Y-%m-%d')
        RM = sheet_name + "RM"

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
             'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
             'Per position limit']]

        df['End date'] = df['End date'].str.split(' ').str[0]



        df.to_excel(writer, sheet_name=sheet_name, index=False)

        writer.sheets[i.strftime('%Y-%m-%d')].set_column(0, 1, 10, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(2, 2, 16, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(3, 3, 10, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(4, 5, 9.2, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(6, 6, 20, format2)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(7, 7, 20, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(8, 8, 10, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(9, 9, 20, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(10, 10, 10, format2)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(11, 12, 10, format1)
        writer.sheets[i.strftime('%Y-%m-%d')].set_column(13, 13, 20, format1)

        df7.to_excel(writer, sheet_name=RM, index=False)

        len3 = len(df3.index)
        len5 = len(df5.index) + len3
        len6 = len(df6.index) + len5
        len4 = len(df4.index) + len6

        chart = writer.book.add_chart({'type': 'pie'})
        chart.add_series({
            'categories': [RM, 2, 0, len3 - 1, 0],
            'values': [RM, 2, 1, len3 - 1, 1]
        })

        # writer.sheets[RM].insert_chart('D2', chart)
        writer.sheets[RM].insert_chart(1, 4, chart)

        chart = writer.book.add_chart({'type': 'pie'})
        chart.add_series({
            'categories': [RM, len3 + 2, 4, len5 - 1, 4],
            'values': [RM, len3 + 2, 5, len5 - 1, 5]
        })

        # writer.sheets[RM].insert_chart('G100', chart)
        writer.sheets[RM].insert_chart(len3 + 1, 7, chart)

        chart = writer.book.add_chart({'type': 'pie'})
        chart.add_series({
            'categories': [RM, len5 + 2, 7, len6 - 1, 7],
            'values': [RM, len5 + 2, 8, len6 - 1, 8]
        })

        # writer.sheets[RM].insert_chart('J150', chart)
        writer.sheets[RM].insert_chart(len5 + 1, 11, chart)

        chart = writer.book.add_chart({'type': 'pie'})
        chart.add_series({
            'categories': [RM, len6 + 2, 11, len4 - 1, 11],
            'values': [RM, len6 + 2, 12, len4 - 1, 12]
        })

        # writer.sheets[RM].insert_chart('M200', chart)
        writer.sheets[RM].insert_chart(len6 + 1, 14, chart)

        writer.sheets[RM].set_row(0, None, None, {'hidden': True})
        writer.sheets[RM].set_row(1, None, format3)
        writer.sheets[RM].set_row(len3, None, format4)
        writer.sheets[RM].set_row(len3 + 1, None, format3)
        writer.sheets[RM].set_row(len5, None, format4)
        writer.sheets[RM].set_row(len5 + 1, None, format3)
        writer.sheets[RM].set_row(len6, None, format4)
        writer.sheets[RM].set_row(len6 + 1, None, format3)
        writer.sheets[RM].set_row(len4, None, format4)

        writer.sheets[RM].set_column(0, 1, 20, format1)
        writer.sheets[RM].set_column(2, 3, 20, format2)
        writer.sheets[RM].set_column(4, 5, 20, format1)
        writer.sheets[RM].set_column(6, 6, 20, format2)
        writer.sheets[RM].set_column(7, 8, 20, format1)
        writer.sheets[RM].set_column(9, 9, 20, format2)
        writer.sheets[RM].set_column(10, 12, 20, format1)
        writer.sheets[RM].set_column(13, 13, 20, format2)




'''
    df3 = df.groupby(['Seller code'],as_index=False).sum()
    df3 = df3[['Seller code','Advance amount (USD)', '% total']]
    df3 = df3.iloc[1:]

    df4 = df.groupby(['Account'], as_index=False).sum()
    df4 = df4[['Account', 'Advance amount (USD)', '% total']]
    df4 = df4.iloc[1:]

    df5 = df.groupby(['Obligor code'], as_index=False).sum()
    df5 = df5[['Obligor code', 'Advance amount (USD)', '% total']]
    df5 = df5.iloc[1:]

    df6 = df.groupby(['Trade ID'], as_index=False).sum()
    df6 = df6[['Trade ID', 'Advance amount (USD)', '% total']]
    indexNames = df6[df6['Trade ID'] == 'Invested'].index
    df6.drop(indexNames, inplace=True)
    indexNames = df6[df6['Trade ID'] == 'Total'].index
    df6.drop(indexNames, inplace=True)

    RMS = sheet_name + "RM Seller"
    RMA = sheet_name + "RM Account"
    RMO = sheet_name + "RM Obligor"
    RMT = sheet_name + "RM Trade"

    len3 = len(df3.index)
    len4 = len(df4.index)
    len5 = len(df5.index)
    len6 = len(df6.index)


    df3.to_excel(writer, sheet_name=RMS, index=False)

    chart = writer.book.add_chart({'type': 'pie'})
    chart.add_series({
        'categories': [RMS, 2, 0, len3, 0],
        'values': [RMS, 2, 1, len3, 1 ]
    })

    writer.sheets[RMS].insert_chart('D2', chart)



    df4.to_excel(writer, sheet_name=RMA, index=False)

    chart = writer.book.add_chart({'type': 'pie'})
    chart.add_series({
        'categories': [RMA,2, 0, len4, 0],
        'values': [RMA, 2, 1, len4, 1]
    })

    writer.sheets[RMA].insert_chart('D2', chart)

    df5.to_excel(writer, sheet_name=RMO, index=False)

    chart = writer.book.add_chart({'type': 'pie'})
    chart.add_series({
        'categories': [RMO, 2, 0, len5, 0],
        'values': [RMO, 2, 1, len5, 1]
    })

    writer.sheets[RMO].insert_chart('D2', chart)


    df6.to_excel(writer, sheet_name=RMT, index=False)

    chart = writer.book.add_chart({'type': 'pie'})
    chart.add_series({
        'categories': [RMT,1, 0, len6, 0],
        'values': [RMT,1, 1, len6, 1]
    })

    writer.sheets[RMT].insert_chart('D2', chart)


    writer.sheets[RMS].set_column(0, 1, 20, format1)
    writer.sheets[RMS].set_column(2, 2, 20, format2)
    writer.sheets[RMA].set_column(0, 1, 20, format1)
    writer.sheets[RMA].set_column(2, 2, 20, format2)
    writer.sheets[RMO].set_column(0, 1, 20, format1)
    writer.sheets[RMO].set_column(2, 2, 20, format2)
    writer.sheets[RMT].set_column(0, 1, 20, format1)
    writer.sheets[RMT].set_column(2, 2, 20, format2)

    

i = datetime.strptime(date1, '%Y-%m-%d')
j = datetime.strptime(date2, '%Y-%m-%d')

with ExcelWriter(xls_path, engine='xlsxwriter',options={'strings_to_numbers': True}) as writer:

    while i <= j:
        Download.save_xls(i, writer)
        print(i)
        #Database.insert("progress", {"data": i.strftime('%Y-%m-%d')})
        i = i + timedelta(days=1)

    writer.save()


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

'''
