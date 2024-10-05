import mysql.connector as mysql
import csv
import pandas as pd
from sqlalchemy import create_engine
import os
import configparser
import logging

COL = []

#--------------------------------------------------------------
#configuration information


config = configparser.ConfigParser()
config.read('config.ini')

db_host = config['database_1']['host']
db_user = config['database_1']['user']
db_password = config['database_1']['password']
db_port = config['database_1']['port']
db_name_1 = config['database_1']['database']    #datewisedb
db_name_2 = config['database_2']['database']    #stockwisedb

#---------------------------------------------------------------

# Configure logging
log_file = "bhavcopy_upload_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


list_of_files = os.listdir(os.path.join(os.getcwd(), 'Bhavcopy'))        # Files is a array containing name of all the bhavcopy

list_of_stocks = os.listdir(os.path.join(os.getcwd(), 'ISIN_CSVs'))
 
def is_file_uploaded(filename):
    """
    Check if the file has been uploaded by reading the log file.
    
    :param filename: The name of the file to check.
    :return: True if the file is found in the log file, False otherwise.
    """
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                if filename in line and 'successfully uploaded' in line:
                    return True
    return False                       
  
def update_stocks(name):            #updates list of stock when new bhavcopy is there

    name_of_file = os.path.join(os.getcwd(), 'Bhavcopy', name)

    columns = ['Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm']
    
    stocks = pd.read_csv(name_of_file, usecols=columns)
    
    database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_1}'
            
    engine = create_engine(database_url)
    
    database_url_2 = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_2}'
            
    engine_2 = create_engine(database_url_2)
    
    try:
        stocks.to_sql('stocklist', con=engine, if_exists='replace', index=False)
        stocks.to_sql('stocklist', con=engine_2, if_exists='replace', index=False)
        
    except ValueError as e:
        print ("Error occured while updating stock list")


def file_to_stock():                #uploades stock wise data to database 2 - 'stockwisedb' where data of perticular stock is uploded
    
    i = 0

    for filename in list_of_stocks:

        csv_file_path = os.path.join(os.getcwd(), 'ISIN_CSVs', filename)

        columns_to_read = [
        'TradDt', 'Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm', 'OpnPric', 
        'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
        'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd'
        ]

        data = pd.read_csv(csv_file_path, usecols=columns_to_read)

        data['PerChange'] = (data['ClsPric'] - data['PrvsClsgPric'])/data['PrvsClsgPric']

        database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_2}'
        
        engine = create_engine(database_url)
        
        try:
            data.to_sql(filename.replace(".csv","").lower(), con=engine, if_exists='replace', index=False)
            print(f"{i} out of {len(list_of_stocks)} files completed.")
            i = i+1
                
        except ValueError as e:
            print (f"Error uploading {filename}: {e}") 

        except Exception as e:
            # Log any other errors during the process
            print(f"Error uploading {filename}: {e}")
    
 
def file_to_table():                #uplodes daily bhavcopy to database1 - 'datewisedb'

    for filename in list_of_files:

        if is_file_uploaded(filename):
            print(f"Skipping {filename} already uploaded.")
            continue

        csv_file_path = os.path.join(os.getcwd(), 'Bhavcopy', filename)

        columns_to_read = ['Sgmt', 'ISIN', 'TckrSymb', 'SctySrs', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd']
        
        data = pd.read_csv(csv_file_path, usecols=columns_to_read)

        data['PerChange'] = (data['ClsPric'] - data['PrvsClsgPric'])/data['PrvsClsgPric']

        database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_1}'
        
        engine = create_engine(database_url)
        
        try:
            data.to_sql(filename.replace(".csv",""), con=engine, if_exists='fail', index=False)

            logging.info(f"{filename} successfully uploaded to the database.")
            print(f"{filename} successfully uploaded.")

            update_stocks(filename)
                
        except ValueError as e:
            logging.warning(f"{filename} file is already uploaded: {e}")
            print (filename + " file is already uploaded") 

        except Exception as e:
            # Log any other errors during the process
            logging.error(f"Error uploading {filename}: {e}")
            print(f"Error uploading {filename}: {e}")

 

file_to_table()                 #uplodes daily bhavcopy to database 1 - 'datewisedb'

file_to_stock()                 #uploades stock wise data to database 2 - 'stockwisedb' where data of perticular stock is uploded

#use 7 zip on all files and extract the files
#use *.* in search bar cut all csv file and paste it in new folder
#rename all files using cmd command - rename "BhavCopy_NSE_CM_0_0_0_*.csv" "//////////////////////*.csv"
#rename all files using cmd command - rename *.* ????????.*

#3rd database - stocknames is used for getting names using ISIN