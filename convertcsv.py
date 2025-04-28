import pandas as pd
import os
import logging
from sqlalchemy import create_engine
import configparser
import mysql.connector as mysql

#--------------------------------------------------------------
#configuration information


config = configparser.ConfigParser()
config.read('config.ini')

db_host = config['database_1']['host']
db_user = config['database_1']['user']
db_password = config['database_1']['password']
db_port = config['database_1']['port']
db_name_2 = config['database_2']['database']    #stockwisedb

#---------------------------------------------------------------

# Configure logging
log_file = "stock_csv_upload_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def stock_to_database(data, tablename):
    
    database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_2}'
        
    engine = create_engine(database_url)

    try:
        data.to_sql(name=tablename.lower(), con=engine, if_exists='append', index=False)
                
    except ValueError as e:
        print (f"Error inserting {data}: {e}") 

    except Exception as e:
        # Log any other errors during the process   
        print(f"Error inserting {data}: {e}")


def is_file_uploaded(file):
    """
    Check if the file has been uploaded by reading the log file.
    
    :param filename: The name of the file to check.
    :return: True if the file is found in the log file, False otherwise.
    """
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                if file in line and 'Finished processing' in line:
                    return True
    return False

def bhavcopy_to_csv(file):

    # Full path to the Bhavcopy file
    file_path = os.path.join(os.getcwd(), 'Bhavcopy', file)

    try:

        data = pd.read_csv(file_path)
        data = data[data[' SERIES'] == ' EQ']
        data['PER_CHANGE'] = round(((data[' CLOSE_PRICE'] - data[' PREV_CLOSE'])/data[' PREV_CLOSE'])*100,2)

        #columns_of_interest = ['SYMBOL', ' SERIES',' DATE1', ' PREV_CLOSE', ' OPEN_PRICE', ' HIGH_PRICE', ' LOW_PRICE', ' LAST_PRICE', ' CLOSE_PRICE', ' AVG_PRICE', ' TTL_TRD_QNTY', ' TURNOVER_LACS', ' NO_OF_TRADES', ' DELIV_QTY', ' DELIV_PER']
        
        # Filter the data to keep only the relevant columns
        #filtered_data = data[columns_of_interest]

        # Create a directory to store the CSV files
        output_dir = 'Stocks'
        os.makedirs(output_dir, exist_ok=True)

        # Group the data by symbol and create separate CSV files for each symbol
        unique_stocks = data['SYMBOL'].unique()

        for stock in unique_stocks:

            file_name = f"{output_dir}/{stock}.csv"
            file_exists = os.path.isfile(file_name)

            stock_data = data[data['SYMBOL'] == stock]
            stock_data.to_csv(file_name, mode='a', index=False, header=not file_exists)

            stock_to_database(stock_data, stock)

    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")


# Load the CSV file
bhavcopy_directory = os.path.join(os.getcwd(), 'Bhavcopy')
list_of_files = os.listdir(bhavcopy_directory)  # Replace with your file path


for file in list_of_files:
    if(is_file_uploaded(file)):
        print(f"{file} is already converted to csv")
        pass
    else:
        print(f"processing :{file}")
        logging.info(f"Starting processing for: {file}")
        bhavcopy_to_csv(file)
        logging.info(f"Finished processing for: {file}")