import requests
import os
from time import sleep
from datetime import date, datetime, timedelta
import logging

log_file = "bhavcopy_download_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

start_date = datetime(2020, 1, 1)
end_date = datetime.combine(date.today(), datetime.min.time())
current_date = start_date

holiday = ['14042021', '21042021', '13052021', '21072021', '26012021', '11032021', 
 '29032021', '02042021', '10092021', '15102021', '04112021', '05112021', '19112021',
 '21022020', '10032020', '02042020', '06042020', '10042020', '14042020', '01052020',
 '25052020', '02102020', '16112020', '30112020', '25122020', '26012022', '01032022',
 '18032022', '14042022', '15042022', '03052022', '09082022', '15082022', '31082022', 
 '05102022', '24102022', '26102022', '08112022', '25122022', '26012023', '07032023',
 '30032023', '04042023', '07042023', '14042023', '01052023', '28062023', '15082023',
 '19092023', '02102023', '24102023', '14112023', '27112023', '25122023', '26012024',
 '08032024', '25032024', '29032024', '11042024', '17042024', '01052024', '17062024', 
 '17062024', '15082024', '02102024', '01112024', '15112024', '25122024', '26022025', 
 '14032025', '31032025', '10042025', '14042025', '18042025', '01052025', '15082025',
 '27082025', '02102025', '21102025', '22102025', '05112025', '25122025']


def is_file_downloaded(date_str):
    if os.path.exists(log_file):
        with open(log_file, 'r') as log:
            for line in log:
                if date_str in line and 'successfully downloaded' in line:
                    return True
    return False

# Function to download the file with retries
def download_file(url, date_str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0 Safari/537.36"
    }

    path = os.path.join(os.getcwd(), 'Bhavcopy', date_str+'.csv')

    if(not is_file_downloaded(date_str)):
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()  # Raise HTTPError for bad responses
                with open(path, "wb") as file:
                    file.write(response.content)
                logging.info(f"{date_str} successfully downloaded.")
                print(f"File successfully downloaded: {path}")
                return
            except requests.exceptions.RequestException as e:
                logging.info(f"{date_str} not downloaded.")
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    sleep(2)  # Wait before retrying
        print(f"Failed to download file after {retries} attempts: {url}")
    else:
        print(f"{date_str} is already downloaded")



while current_date <= end_date:
    
    if current_date.weekday() < 5 and current_date.strftime("%d%m%Y") not in holiday:
        date_str = current_date.strftime("%d%m%Y")
        
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
        
        try:
            download_file(url, date_str)
        except Exception as e:
            print(e)
    current_date += timedelta(days=1)

    #break #remove when all done