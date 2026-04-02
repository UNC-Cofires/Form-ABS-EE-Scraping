import numpy as np
import pandas as pd
import requests
import time
import os

### *** HELPER FUNCTIONS *** ###

def download_asset_data(url,filepath):
    """
    This function downloads Form ABS-EE submissions from SEC EDGAR and saves 
    the result as a plain text file. 

    param: url: URL of submission (usually sourced from index files). 
    param: filepath: path to where output should be saved on local machine.
    returns: success_status: bool denoting whether download was successful or not. 
    """
    # Initialize status variable
    success_status = False

    # Need to specify user-agent in header when accessing EDGAR via API
    headers = {'User-Agent':'Kieran Fitzmason kfitzmason@unc.edu'}
    sleep_seconds = 0.1

    # Attempt to donwload file
    res = requests.get(url,headers=headers)
    time.sleep(sleep_seconds)

    # If it worked, save result to file
    if res.ok:

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(res.text)
                
            success_status = True
            
        except:
            pass

    return success_status

### *** INITIAL SETUP *** ###

# Get current working directory 
pwd = os.getcwd()

# Load index file
CMBS_index_file_path = os.path.join(pwd,'index_files/clean/CMBS_ABS-EE_index_file.parquet')
CMBS_index_file = pd.read_parquet(CMBS_index_file_path)

# Get list of CIKs
CMBS_CIK_list = CMBS_index_file['CIK'].drop_duplicates().to_list()

# Set up folders
raw_folder = os.path.join(pwd,'asset_data/CMBS/raw')
for CIK in CMBS_CIK_list:
    os.makedirs(os.path.join(raw_folder,CIK),exist_ok=True)

# Specify names of output files
CMBS_index_file['Output Path'] = raw_folder 
CMBS_index_file['Output Path'] += '/' + CMBS_index_file['CIK']
CMBS_index_file['Output Path'] += '/' + CMBS_index_file['File Name'].apply(lambda x: x.split('/')[-1])

# Determine which files we've already downloaded
CMBS_index_file['Downloaded'] = CMBS_index_file['Output Path'].apply(lambda x: os.path.exists(x))

# Drop entries that have already been downloaded
CMBS_index_file = CMBS_index_file[~CMBS_index_file['Downloaded']].reset_index(drop=True)

# Update list of CIKs
CMBS_CIK_list = CMBS_index_file['CIK'].drop_duplicates().to_list()

### *** DOWNlOAD DATA *** ###

# Get URL of files to download

CMBS_index_file['File URL'] = 'https://www.sec.gov/Archives/' + CMBS_index_file['File Name']
CMBS_index_file['Success Status'] = False

for deal_num,CIK in enumerate(CMBS_CIK_list):

    # Print deal-level info
    associated_indices = CMBS_index_file.index.values[(CMBS_index_file['CIK']==CIK)]
    dealname = CMBS_index_file.loc[associated_indices[0],'Company Name']
    
    print(f'DEAL {deal_num + 1} / {len(CMBS_CIK_list)}: {dealname} (CIK {CIK})',flush=True)

    # Downlaod all ABS-EE filings associated with deal
    for idx in associated_indices:
        
        url = CMBS_index_file.loc[idx,'File URL']
        filepath = CMBS_index_file.loc[idx,'Output Path']
        success_status = download_asset_data(url,filepath)
        CMBS_index_file.loc[idx,'Success Status'] = success_status


### *** PRINT SUMMARY OF RESULTS FOR USER *** ###

num_attempted_downloads = len(CMBS_index_file)
num_successful_downloads = CMBS_index_file['Success Status'].sum()
percentage_successful = 100*(num_successful_downloads/num_attempted_downloads)

print('\n\n###--------- SUMMARY ---------###\n')

print(f'Number of attempted downloads: {num_attempted_downloads}')
print(f'Number of successful downloads: {num_successful_downloads}')
print(f'Number of failed downloads: {num_attempted_downloads - num_successful_downloads}')
print(f'Percentage successful: {percentage_successful:.2f}%')