import pandas as pd
import numpy as np
import time
from pymongo import MongoClient
from pandas import ExcelWriter
from datetime import datetime
from datetime import timedelta
from src.database import Database

class Download():
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


    @staticmethod
    def read_qupital_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Qupital", converters={'Advanced Date': str, 'Due date': str, 'Advanced Amount':str}, keep_default_na = False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    @staticmethod
    def read_fundpark_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Fundpark", converters={'Trade Date': str, 'Actual Repayment Date':str}, keep_default_na = False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    @staticmethod
    def read_culum_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Culum", converters={'Purchase date': str, 'Expected payment date':str}, keep_default_na = False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    @staticmethod
    def read_incomlend_local_blotter():
        dataExcel = pd.read_excel(r'C:\Users\Solomon\Desktop\IACF trade blotter.xlsx', sheet_name = "Incomlend", converters={'Effective Date': str, 'Expected repayment date':str}, keep_default_na = False)
        dfExcel = pd.DataFrame(dataExcel)
        dfExcel.fillna("NA", inplace=True)

        return dfExcel

    ###################################

    @staticmethod
    def read_qupital_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_qupital_blotter.find()),
                              columns=['Auction no', 'Obligor', 'Seller No', 'Currency', 'Advanced Amount', 'Gross gain',
                                       'Platform Fee', 'Net total to be received', 'Gross Return (% pa)',
                                       'Net Return (% pa)', 'Advanced Date', 'Due date', 'Remitted date',
                                       'Late day (day(s))', 'Expected Duration (day(s))', 'Aucutal Duration (day(s))',
                                       'Status', 'Insured invoice', 'Obligor notification', 'Rationale'])

        return dfData

    @staticmethod
    def read_fundpark_db_blotter():

        dfData = pd.DataFrame(list(Download.collection_fundpark_blotter.find()),
                              columns=['Trade ID',	'Requested Loan Amount',	'Expected Tenor (days)',	'Interest Rate (per month)',	'(Expected) Interest Income',	'Buyer',	'Trade Date',	'Actual Repayment Date',	'Actual Tenor (day(s))',	'Rationale'])

        return dfData

    @staticmethod
    def read_culum_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_culum_blotter.find()),
                              columns=['Investment No',	'Obligor',	'Seller',	'Total investment',	'Return on investment (annualised)',	'Credit grade',	'Purchase date',	'Expected payment date',	'Unrealized gain',	'Acutal payment date',	'In recovery',	'Realized gain',	'Tenor',	'Rationale'])

        return dfData

    @staticmethod
    def read_incomlend_db_blotter():
        dfData = pd.DataFrame(list(Download.collection_incomlend_blotter.find()),
                              columns=['Effective Date',	'Transaction Type',	'Invoice',	'Supplier Invoice Ref Number',	'Amount',	'Allocation Amount',	'Allocation Funder Name',	'External Reference',	'Trustee Approved',	'Status',	'Discount rate',	'Expected repayment date',	'Financing period',	'Credit insured', 'Supplier name'])
        return dfData

    ###################################

    @staticmethod
    def update_qupital_blotter():
        Download.collection_qupital_blotter.delete_many({})
        data = Download.read_qupital_local_blotter().to_dict(orient='records')  # Here's our added param..
        Download.collection_qupital_blotter.insert_many(data)

    @staticmethod
    def update_fundpark_blotter():
        Download.collection_fundpark_blotter.delete_many({})
        data = Download.read_fundpark_local_blotter().to_dict(orient='records')  # Here's our added param..
        Download.collection_fundpark_blotter.insert_many(data)

    @staticmethod
    def update_culum_blotter():
        Download.collection_culum_blotter.delete_many({})
        data = Download.read_culum_local_blotter().to_dict(orient='records')  # Here's our added param..
        Download.collection_culum_blotter.insert_many(data)

    @staticmethod
    def update_incomlend_blotter():
        Download.collection_incomlend_blotter.delete_many({})
        data = Download.read_incomlend_local_blotter().to_dict(orient='records')  # Here's our added param..
        Download.collection_incomlend_blotter.insert_many(data)

    ###################################
    @staticmethod
    def update_qupital_portfolio(df):

        indexNames = df[df['Status'] == 'Remitted'].index
        df.drop(indexNames, inplace=True)

        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
        df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount']/7.8), df['Advanced Amount'])

        df['Account'] = 'Qupital'
        df['% total'] = "NA"

        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')


        df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)","Advanced Date":"Start date","Due date":"End date","Net Return (% pa)":"Annualized return (%)","Seller No":"Seller code","Obligor":"Obligor code"},inplace=True)

        df = df[['Trade ID',	'Currency',	'Advanced Amount',	'Tenor (days)',	'Start date',	'End date',	'Annualized return (%)','Advance amount (USD)',	'Seller code',	'Obligor code',	'% total',	'Account',	'Per position limit']]

        return df

    @staticmethod
    def update_fundpark_portfolio(df):

        indexNames = df[df['Actual Repayment Date'] != ''].index
        df.drop(indexNames, inplace=True)

        df['Currency'] = df['Requested Loan Amount'].str.slice(0,3)
        df['Advanced Amount'] = df['Requested Loan Amount'].str.slice(5, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = (12 * df['Interest Rate (per month)'] - 0.01)*100
        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount']/7.8),df['Advanced Amount'])

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

    @staticmethod
    def update_culum_portfolio(df):

        indexNames = df[df['Acutal payment date'] != 'NA'].index
        df.drop(indexNames, inplace=True)

        df['Currency'] = df['Total investment'].str.slice(0,3)

        df['Advanced Amount'] = df['Total investment'].str.slice(3, 30)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = 100 * df['Return on investment (annualised)']

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                              df['Advanced Amount'], (df['Advanced Amount']/1.36))

        df['Account'] = 'Culum'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Investment No": "Trade ID", "Tenor":"Tenor (days)", "Purchase date":"Start date","Expected payment date":"End date","Seller":"Seller code", "Obligor":"Obligor code"}, inplace=True)


        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def update_incomlend_portfolio(df):

        df['Trade ID'] = ('L' + df['Supplier Invoice Ref Number'])

        df['Currency'] = df['Amount'].str.slice(0, 3)

        df['Advanced Amount'] = df['Allocation Amount'].str.slice(4, 15)
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '').astype(float)

        df['Annualized return (%)'] = ((1 /(1-df['Discount rate']))**12-1)*100

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'USD',
                                              df['Advanced Amount'], (df['Advanced Amount']/1.36))


        df['Account'] = 'Incomlend'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Financing period": "Tenor (days)", "Effective Date":"Start date","Expected repayment date":"End date","Invoice":"Seller code","Supplier name":"Obligor code"}, inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    @staticmethod
    def search_portfolio(dfQ,dfF,dfC,dfI):

        dataQ = dfQ.to_dict(orient='records')  # Here's our added param..
        dataF = dfF.to_dict(orient='records')
        dataC = dfC.to_dict(orient='records')
        dataI = dfI.to_dict(orient='records')

        Download.search_portfolio.delete_many({})
        Download.search_portfolio.insert_many(dataQ)
        Download.search_portfolio.insert_many(dataF)
        Download.search_portfolio.insert_many(dataC)
        Download.search_portfolio.insert_many(dataI)



    @staticmethod
    def date_portfolio(dfQ, dfF, dfC, dfI):
        dataQ = dfQ.to_dict(orient='records')  # Here's our added param..
        dataF = dfF.to_dict(orient='records')
        dataC = dfC.to_dict(orient='records')
        dataI = dfI.to_dict(orient='records')

        Download.search_portfolio.delete_many({})
        Download.search_portfolio.insert_many(dataQ)
        Download.search_portfolio.insert_many(dataF)
        Download.search_portfolio.insert_many(dataC)
        Download.search_portfolio.insert_many(dataI)

    @staticmethod
    def date_qupital_portfolio(df,date):

        indexNames = df[df['Status'] == 'Remitted'].index
        df.drop(indexNames, inplace=True)

        df['Advanced Date'] = pd.to_datetime(df['Advanced Date'])
        df = df[(df['Advanced Date'] <= date)]
        df['Advanced Date'] = df['Advanced Date'].astype(str)

        df['Advanced Amount'] = df['Advanced Amount'].str.replace(',', '')
        df['Advanced Amount'] = df['Advanced Amount'].str.replace(' ', '')
        df["Advanced Amount"] = pd.to_numeric(df["Advanced Amount"])

        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount']/7.8), df['Advanced Amount'])

        df['Account'] = 'Qupital'
        df['% total'] = "NA"

        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')


        df.rename(columns={"Auction no": "Trade ID", "Expected Duration (day(s))": "Tenor (days)","Advanced Date":"Start date","Due date":"End date","Net Return (% pa)":"Annualized return (%)","Seller No":"Seller code","Obligor":"Obligor code"},inplace=True)

        df = df[['Trade ID',	'Currency',	'Advanced Amount',	'Tenor (days)',	'Start date',	'End date',	'Annualized return (%)','Advance amount (USD)',	'Seller code',	'Obligor code',	'% total',	'Account',	'Per position limit']]

        return df

    @staticmethod
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
        df['Advance amount (USD)'] = np.where(df['Currency'] == 'HKD', (df['Advanced Amount']/7.8),
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

    @staticmethod
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
                                              df['Advanced Amount'], (df['Advanced Amount']/1.36))

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


    @staticmethod
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

        df['Account'] = 'Incomlend'
        df['% total'] = "NA"
        df['Per position limit'] = np.where(df['Advance amount (USD)'] < 300000, 'Yes', 'Out of limit')

        df.rename(columns={"Financing period": "Tenor (days)", "Effective Date": "Start date",
                            "Expected repayment date": "End date", "Invoice": "Seller code","Supplier name":"Obligor code"}, inplace=True)

        df = df[
            ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date', 'Annualized return (%)',
             'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Per position limit']]

        return df

    '''
    @staticmethod
    def save_xls(date1, date2, xls_path):
        i = datetime.strptime(date1, '%Y-%m-%d')
        j = datetime.strptime(date2, '%Y-%m-%d')
        with ExcelWriter(xls_path) as writer:
            while i <= j:
                df = pd.concat([Download.date_qupital_portfolio(Download.read_qupital_local_blotter(),i), Download.date_fundpark_portfolio(Download.read_fundpark_local_blotter(), i),Download.date_culum_portfolio(Download.read_culum_local_blotter(), i),Download.date_incomlend_portfolio(Download.read_incomlend_local_blotter(), i)])
                df.to_excel(writer, sheet_name=i.strftime('%Y-%m-%d'), index=False)
                #writer.save()
                print(i)
                i = i + timedelta(days=1)

            writer.save()
    '''           
    def save_xls(date1, date2, xls_path):
        i = datetime.strptime(date1, '%Y-%m-%d')
        j = datetime.strptime(date2, '%Y-%m-%d')

        with ExcelWriter(xls_path, engine='xlsxwriter',options={'strings_to_numbers': True}) as writer:
            
            format1 = writer.book.add_format({'num_format': '#,##0.00'})
            format2 = writer.book.add_format({'num_format': '0.00%'})

            format3 = writer.book.add_format({'bold': True, 'font_color': '#FF8C00'})

            format4 = writer.book.add_format({'bold': True, 'font_color': '#FFA500'})


            while i <= j:
                df = pd.concat([Download.date_qupital_portfolio(Download.read_qupital_local_blotter(),i), Download.date_fundpark_portfolio(Download.read_fundpark_local_blotter(), i),Download.date_culum_portfolio(Download.read_culum_local_blotter(), i),Download.date_incomlend_portfolio(Download.read_incomlend_local_blotter(), i)])
                df['Late day'] = np.where(datetime.today() > pd.to_datetime(df['End date']), (datetime.today()-pd.to_datetime(df['End date'])).dt.days, pd.to_timedelta("0"))
                df = df[
                    ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
                     'Annualized return (%)',
                     'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account','Late day', 'Per position limit']]

                cash_advance_USD = ((7000000+9850000)/7.8+10100000) - df['Advance amount (USD)'].sum()
                total_advance_USD = ((7000000+9850000)/7.8+10100000)
                invested_advance_USD = df['Advance amount (USD)'].sum()

                df['% total'] = (df['Advance amount (USD)'] / total_advance_USD)

                df['Annualized return (%)']=df['Annualized return (%)']/100

                cash_ratio = (cash_advance_USD/total_advance_USD)

                df['temp'] = (df['Annualized return (%)']*df['% total'])
                portfolio_gross_return = df['temp'].sum()
                weighted_average_return = portfolio_gross_return/(1-(cash_ratio))

                df2 = pd.DataFrame(np.array([["Cash", "", "", "", "", "", "", cash_advance_USD, "Cash", "Cash", cash_ratio, "Cash", "", ""],
                                             ["Total", "", "", "", "", "Weighted average return (%)", weighted_average_return, total_advance_USD, "", "", 1, "", "", ""],
                                             ["Invested", "", "", "", "", "Portfolio gross return (%)", portfolio_gross_return, invested_advance_USD, "", "", "", "", "", ""]]),
                                             columns = ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date',
                                              'End date',
                                              'Annualized return (%)',
                                              'Advance amount (USD)', 'Seller code', 'Obligor code', '% total',
                                              'Account', 'Late day', 'Per position limit'])

                df = pd.concat([df, df2])

                df = df[
                    ['Trade ID', 'Currency', 'Advanced Amount', 'Tenor (days)', 'Start date', 'End date',
                     'Annualized return (%)',
                     'Advance amount (USD)', 'Seller code', 'Obligor code', '% total', 'Account', 'Late day',
                     'Per position limit']]


                df3 = df.groupby(['Seller code'],as_index=False).sum()
                df3 = df3[['Seller code','Advance amount (USD)', '% total']]
                df3 = df3.iloc[1:]
                df33 = pd.DataFrame(np.array([["Seller code", "SUM of Advance amount (USD)", "SUM of % total"]]),
                                             columns = ['Seller code','Advance amount (USD)', '% total'])

                df34 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                                    columns=['Seller code', 'Advance amount (USD)', '% total'])

                df3 = pd.concat([df33, df3,df34])
                df3.rename(columns={"Advance amount (USD)": "Advance amount (USD) S", "% total": "% total S"},
                           inplace=True)

                df4 = df.groupby(['Account'], as_index=False).sum()
                df4 = df4[['Account', 'Advance amount (USD)', '% total']]
                df4 = df4.iloc[1:]

                df44 = pd.DataFrame(np.array([["Account", "SUM of Advance amount (USD)", "SUM of % total"]]),
                                             columns = ['Account','Advance amount (USD)', '% total'])

                df45 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                                    columns=['Account', 'Advance amount (USD)', '% total'])

                df4 = pd.concat([df44, df4,df45])

                df4.rename(columns={"Advance amount (USD)": "Advance amount (USD) A", "% total": "% total A"},
                           inplace=True)

                df5 = df.groupby(['Obligor code'], as_index=False).sum()
                df5 = df5[['Obligor code', 'Advance amount (USD)', '% total']]
                df5 = df5.iloc[1:]

                df55 = pd.DataFrame(np.array([["Obligor code", "SUM of Advance amount (USD)", "SUM of % total"]]),
                                             columns = ['Obligor code','Advance amount (USD)', '% total'])

                df56 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                                    columns=['Obligor code', 'Advance amount (USD)', '% total'])

                df5 = pd.concat([df55, df5,df56])

                df5.rename(columns={"Advance amount (USD)": "Advance amount (USD) O", "% total": "% total O"},
                           inplace=True)


                df6 = df.groupby(['Trade ID'], as_index=False).sum()
                df6 = df6[['Trade ID', 'Advance amount (USD)', '% total']]
                indexNames = df6[df6['Trade ID'] == 'Invested'].index
                df6.drop(indexNames, inplace=True)
                indexNames = df6[df6['Trade ID'] == 'Total'].index
                df6.drop(indexNames, inplace=True)

                df66 = pd.DataFrame(np.array([["Trade ID", "SUM of Advance amount (USD)", "SUM of % total"]]),
                                             columns = ['Trade ID','Advance amount (USD)', '% total'])

                df67 = pd.DataFrame(np.array([["Grand Total", total_advance_USD, 1]]),
                                    columns=['Trade ID', 'Advance amount (USD)', '% total'])

                df6 = pd.concat([df66, df6,df67])

                df6.rename(columns={"Advance amount (USD)": "Advance amount (USD) T", "% total": "% total T"}, inplace=True)


                df7 = pd.concat([df3, df5, df6, df4])

                df7 = df7[
                    ['Seller code','Advance amount (USD) S', '% total S','Obligor code','Advance amount (USD) O', '% total O','Trade ID','Advance amount (USD) T', '% total T','Account', 'Advance amount (USD) A', '% total A']]

                sheet_name = i.strftime('%Y-%m-%d')
                RM = sheet_name + "RM"

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
                    'categories': [RM, 2, 0, len3-1, 0],
                    'values': [RM, 2, 1, len3-1, 1 ]
                })

                #writer.sheets[RM].insert_chart('D2', chart)
                writer.sheets[RM].insert_chart(1, 3, chart)



                chart = writer.book.add_chart({'type': 'pie'})
                chart.add_series({
                    'categories': [RM, len3+2, 3, len5-1, 3],
                    'values': [RM, len3+2, 4, len5-1, 4]
                })

                #writer.sheets[RM].insert_chart('G100', chart)
                writer.sheets[RM].insert_chart(len3+1, 6, chart)



                chart = writer.book.add_chart({'type': 'pie'})
                chart.add_series({
                    'categories': [RM, len5+2, 6, len6-1, 6],
                    'values': [RM, len5+2, 7, len6-1, 7]
                })

                #writer.sheets[RM].insert_chart('J150', chart)
                writer.sheets[RM].insert_chart(len5+1, 9, chart)


                chart = writer.book.add_chart({'type': 'pie'})
                chart.add_series({
                    'categories': [RM, len6+2, 9, len4-1, 9],
                    'values': [RM, len6+2, 10, len4-1, 10]
                })

                #writer.sheets[RM].insert_chart('M200', chart)
                writer.sheets[RM].insert_chart(len6+1, 12, chart)


                writer.sheets[RM].set_column(0, 1, 20, format1)
                writer.sheets[RM].set_column(2, 2, 20, format2)
                writer.sheets[RM].set_column(3, 4, 20, format1)
                writer.sheets[RM].set_column(5, 5, 20, format2)
                writer.sheets[RM].set_column(6, 7, 20, format1)
                writer.sheets[RM].set_column(8, 8, 20, format2)
                writer.sheets[RM].set_column(9, 10, 20, format1)
                writer.sheets[RM].set_column(11, 11, 20, format2)


                writer.sheets[RM].set_row(0, None, None, {'hidden': True})
                writer.sheets[RM].set_row(1, None, format3)
                writer.sheets[RM].set_row(len3, None, format4)
                writer.sheets[RM].set_row(len3+1, None, format3)
                writer.sheets[RM].set_row(len5, None, format4)
                writer.sheets[RM].set_row(len5+1, None, format3)
                writer.sheets[RM].set_row(len6, None, format4)
                writer.sheets[RM].set_row(len6+1, None, format3)
                writer.sheets[RM].set_row(len4, None, format4)


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
                
                '''

                #table = pd.pivot_table(df, values='Advance amount (USD)',columns = ['Account'], aggfunc = np.sum)
                #print(table)

                print(i)
                Database.insert("progress", {"data": i.strftime('%Y-%m-%d')})
                i = i + timedelta(days=1)
            writer.save()

    ###a = r'C:\Users\Solomon\Desktop\Export Test June.xlsx'

    '''

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





