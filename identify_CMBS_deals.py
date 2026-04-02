import numpy as np
import pandas as pd
import requests
import time
import os

### *** HELPER FUNTIONS *** ###

def identify_auto_abs(company_name):
    """
    This function identifies auto-loan ABS based on keywords in company name. 
    Please note that this method has high specificity but low sensitivity. 
    As such, there may be auto loan ABS that do not contain identifying keywords 
    in their name that would not be detected. This function is intended to be used
    as a pre-processing step to filter out auto ABS so that the user can focus on 
    other types of ABS such as CMBS. 

    param: company name: string
    returns: name_indicates_auto_abs: variable that's true if  keywords are present in company name.
    """

    keywords = ['AUTO',
                'VEHICLE',
                'CARMAX',
                'TOYOTA',
                'FORD',
                'HYUNDAI',
                'HONDA',
                'NISSAN',
                'DAIMLER',
                'MERCEDES',
                'HARLEY-DAVIDSON',
                'VOLKSWAGEN']

    company_name = company_name.upper()

    if any(kw in company_name for kw in keywords):
        name_indicates_auto_abs = True
    else:
        name_indicates_auto_abs = False

    return name_indicates_auto_abs

def check_if_CMBS(file_name):
    
    """
    This function downloads the full text submission of ABS-EE filings and 
    checks for CMBS-specific asset data fields. 

    param: file_name: Value of "File Name" field from SEC flat index file. 
    returns: successful_request: bool denoting whether HTTPS request was successful.
    returns: CMBS_flag: bool denoting whether entry is a CMBS. Undefined if request fails. 
    """

    CMBS_flag = pd.NA
    
    url = 'https://www.sec.gov/Archives/' + file_name

    # Need to specify user-agent in header when accessing EDGAR via API
    headers = {'User-Agent':'Kieran Fitzmason kfitzmason@unc.edu'}
    sleep_seconds = 0.1

    res = requests.get(url,headers = headers)
    time.sleep(sleep_seconds)

    # List of CMBS-specific data fields to check for
    CMBS_specific_fields = ['<NumberProperties>','<propertyName>','<propertyAddress>']

    if res.ok:
        
        successful_request = True
        
        if any(field in res.text for field in CMBS_specific_fields):
            CMBS_flag = True
        else:
            CMBS_flag = False
    else:
        successful_request = False
    
    return successful_request,CMBS_flag

### *** INITIAL SETUP *** ###

# Get current working directory 
pwd = os.getcwd()

# Load index file
index_file_path = os.path.join(pwd,'index_files/clean/ABS-EE_index_file.parquet')
index_file = pd.read_parquet(index_file_path)

# Filter out entries that are clearly from auto loan ABS
index_file = index_file[~index_file['Company Name'].apply(identify_auto_abs)].reset_index(drop=True)

# Get most recent ABS-EE filing for each ABS deal. 
# We'll use this file to check whether it's from a CMBS. 
ABS_deals = index_file.groupby('CIK').last().reset_index()

### *** DETERMINE WHICH DEALS CORRESPOND TO CMBS *** ###

ABS_deals['Request Successful'] = pd.NA
ABS_deals['CMBS Flag'] = pd.NA

for idx in ABS_deals.index.values:
    
    row = ABS_deals.loc[idx]

    successful_request,CMBS_flag = check_if_CMBS(row['File Name'])

    ABS_deals.loc[idx,'Request Successful'] = successful_request
    ABS_deals.loc[idx,'CMBS Flag'] = CMBS_flag

    print(f'{row['Company Name']} (CIK {row['CIK']})')
    print(f'    Request Successful: {successful_request}')
    print(f'    CMBS Flag: {CMBS_flag}\n')

### *** PRINT SUMMARY OF RESULTS FOR USER *** ###

num_deals = len(ABS_deals)
num_successful_requests = ABS_deals['Request Successful'].sum()
num_CMBS = (ABS_deals['CMBS Flag']).sum()
non_CMBS_deals = ABS_deals[ABS_deals['CMBS Flag'] != True]['Company Name'].to_list()

print('\n\n###--------- SUMMARY ---------###\n')
print(f'Number of successful requests: {num_successful_requests} / {num_deals}')
print(f'Number of identified as CMBS: {num_CMBS} / {num_deals}\n')
print('Non-CMBS deals excluded from final dataset:\n')
for dealname in non_CMBS_deals:
    print(f'    {dealname}')

### *** SAVE RESULTS *** ###

CMBS_CIK_list = ABS_deals[ABS_deals['CMBS Flag'] == True]['CIK'].to_list()
CMBS_index_file = index_file[index_file['CIK'].isin(CMBS_CIK_list)].reset_index(drop=True)

clean_folder = os.path.join(pwd,'index_files/clean')

# Save as parquet
outname = os.path.join(clean_folder,'CMBS_ABS-EE_index_file.parquet')
CMBS_index_file.to_parquet(outname)

# And also as CSV
outname = os.path.join(clean_folder,'CMBS_ABS-EE_index_file.csv')
CMBS_index_file.to_csv(outname,index=False)