import pandas as pd
import os
import logging

# Configure logging
log_file = "stock_csv_upload_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


def is_file_uploaded(file):
    """
    Check if the file has been uploaded by reading the log file.
    
    :param filename: The name of the file to check.
    :return: True if the file is found in the log file, False otherwise.
    """
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                if file in line and 'successfully uploaded' in line:
                    return True
    return False

def bhavcopy_to_csv(file):

    # Full path to the Bhavcopy file
    file_path = os.path.join(os.getcwd(), 'Bhavcopy', file)

    try:

        data = pd.read_csv(file_path)

        columns_of_interest = [
        'TradDt', 'Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm', 'OpnPric', 
        'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
        'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd'
        ]

        # Filter the data to keep only the relevant columns
        filtered_data = data[columns_of_interest]

        # Create a directory to store the CSV files
        output_dir = 'ISIN_CSVs'
        os.makedirs(output_dir, exist_ok=True)

        # Group the data by ISIN and create separate CSV files for each ISIN
        unique_isins = filtered_data['ISIN'].unique()

        for isin in unique_isins:

            file_name = f"{output_dir}/{isin}.csv"
            file_exists = os.path.isfile(file_name)

            isin_data = filtered_data[filtered_data['ISIN'] == isin]
            isin_data.to_csv(file_name, mode='a', index=False, header=not file_exists)

    except Exception as e:
        logging.error(f"Error processing {file_path}: {str(e)}")



# Load the CSV file
bhavcopy_directory = os.path.join(os.getcwd(), 'Bhavcopy')
list_of_files = os.listdir(bhavcopy_directory)  # Replace with your file path

for file in list_of_files:
    if(is_file_uploaded(file)):
        pass
    else:
        print(f"processing :{file}")
        logging.info(f"Starting processing for: {file}")
        bhavcopy_to_csv(file)
        logging.info(f"Finished processing for: {file}")