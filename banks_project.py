# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=["Bank_Name","MC_USD_Billion"]
csv_path='./exchange_rate.csv'
output_path='./Largest_banks_data.csv'
table_name='Largest_banks'
sql_connection=sqlite3.connect('Banks.db')

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp_format = "%Y-%h-%d-%H-%M-%S"
    now=datetime.now()
    timestamp=now.strftime(time_stamp_format)
    with open("code_log.txt","a") as ts:
        ts.write(timestamp +" Message: "+message+"\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page,'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find('table',{'class':'wikitable'}).find('tbody')
    rows = tables.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
                bank_name = cols[1].find_all('a')[1]['title'] 
                market_cap = cols[2].text.strip()
                data_dict = {"Bank_Name": bank_name,
                         "MC_USD_Billion": market_cap}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace('\n','').astype(float)
    
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    #Read the CSV file into a new Pandas DataFrame
    dataframe = pd.read_csv(csv_path)
    #conver the DataFrame to Dict
    exchange_rate_dict = dataframe.set_index('Currency').to_dict()['Rate']
    
    #Add columns to the DataFrame
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output=pd.read_sql(query_statement,sql_connection)
    print(query_output)
    
    
log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, csv_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_path)
log_progress("Data saved to CSV file")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statement=f'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement=f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)

query_statement=f'SELECT Bank_Name from Largest_banks LIMIT 5'
run_query(query_statement, sql_connection)

log_progress("Process Complete")


log_progress("Server Connection closed")
sql_connection.close

