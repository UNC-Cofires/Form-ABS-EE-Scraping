import numpy as np
import pandas as pd
import os

### *** INITIAL SETUP *** ###

# Get current working directory 
pwd = os.getcwd()

# Specify paths to folders for input/output
clean_folder = os.path.join(pwd,'asset_data/CMBS/clean')

# Get list of CMBS trusts with data
trust_CIKs = [x for x in np.sort(os.listdir(clean_folder)) if os.path.isdir(os.path.join(clean_folder,x))]

### *** CONCATENATE DATA *** ###

loan_filepaths = [os.path.join(clean_folder,f'{CIK}/{CIK}_loan.parquet') for CIK in trust_CIKs]
prop_filepaths = [os.path.join(clean_folder,f'{CIK}/{CIK}_prop.parquet') for CIK in trust_CIKs]
failed_filepaths = [os.path.join(clean_folder,f'{CIK}/{CIK}_failed.parquet') for CIK in trust_CIKs]

loan_data = pd.read_parquet(loan_filepaths).reset_index(drop=True)
prop_data = pd.read_parquet(prop_filepaths).reset_index(drop=True)
failed_data = pd.read_parquet(failed_filepaths).reset_index(drop=True)

### *** SAVE RESULTS *** ###

loan_outname = os.path.join(clean_folder,'loan.parquet')
prop_outname = os.path.join(clean_folder,'prop.parquet')
failed_outname = os.path.join(clean_folder,'failed.parquet')

loan_data.to_parquet(loan_outname)
prop_data.to_parquet(prop_outname)
failed_data.to_parquet(failed_outname)