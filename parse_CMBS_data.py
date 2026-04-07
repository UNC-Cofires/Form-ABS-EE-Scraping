import numpy as np
import pandas as pd
from itertools import combinations
from copy import deepcopy
import networkx as nx
import xmltodict
import time
import os

### *** HELPER FUNTIONS *** ###

def convert_to_bool(x):
    """
    This function takes a string or integer representation of a boolean 
    and converts it to a boolean data type. 
    """

    tvals = ['True','true','T','1',1]
    fvals = ['False','false','F','0',0]

    if pd.isna(x):
        return pd.NA
    elif x in tvals:
        return True
    elif x in fvals:
        return False
    else:
        return pd.NA

def clean_data_types(dataset,data_dict):
    """
    This function formats the columns of dataset based  
    on the data types listed in the data_dict.

    param: dataset: pandas dataframe containing dataset. 
    param: data_dict: pandas dataframe listing data types of columns in dataset.  
    """

    # Add columns for variables that are missing 
    missing_vars = [var for var in data_dict['variable'] if var not in dataset.columns]
    dataset[missing_vars] = pd.NA
    
    # Reorder columns according to data dict
    dataset = dataset[data_dict['variable'].tolist()]

    # Get list of variables falling under each type
    string_vars = data_dict[data_dict['data_type']=='string']['variable'].tolist()
    float_vars = data_dict[data_dict['data_type']=='float']['variable'].tolist()
    int_vars = data_dict[data_dict['data_type']=='int']['variable'].tolist()
    boolean_vars = data_dict[data_dict['data_type']=='boolean']['variable'].tolist()
    datetime_vars = data_dict[data_dict['data_type']=='datetime']['variable'].tolist()
    
    dataset[string_vars] = dataset[string_vars].astype('string[pyarrow]')
    dataset[float_vars] = dataset[float_vars].astype('float32[pyarrow]')
    dataset[int_vars] = dataset[int_vars].astype('int64[pyarrow]')
    
    for var in boolean_vars:
        dataset[var] = dataset[var].apply(convert_to_bool).astype('boolean')
    
    for var in datetime_vars:
        dataset[var] = pd.to_datetime(dataset[var],errors='coerce').astype('date32[pyarrow]')

    return dataset

def identify_depositors(df):
    """
    This function identifies the CIK of companies that are acting as depositors for 
    multiple CMBS deals based on data from SEC EDGAR index files for ABS-EE filings. 

    For example, Wells Fargo Commercial Mortgage Securities Inc. (CIK 0000850779) is 
    jointly listed on the ABS-EE filings of multiple CMBS trusts, including: 
    
        Wells Fargo Commercial Mortgage Trust 2016-C37
        Wells Fargo Commercial Mortgage Trust 2017-RC1
        Wells Fargo Commercial Mortgage Trust 2017-RB1
        Wells Fargo Commercial Mortgage Trust 2017-C38
        Wells Fargo Commercial Mortgage Trust 2017-C39
        ...(and many more)

    Because of this, we will have multiple copies of the asset data file from each deal
    (one from the depositor's filings, and one from the trust's filings). 

    To avoid duplication and confusion in our data, we want to exclude the ABS-EE filings 
    from depositors and keep only those associated with the trusts. 

    We can identify companies that act as depositors by mapping how they are connected to 
    other companies through ABS-EE filings. Each trust should only have one depositor, while
    depositors can be connected to multiple trusts. By represnting these connections as an 
    undirected graph, we can identify depositors as nodes with multiple incident edges. 

    param: df: pandas dataframe of SEC EDGAR flat index file for ABS-EE filings. 
    returns: depositor_CIKs: list of CIKs of companies acting as depositors. 
    returns: graph_summary: pandas dataframe summarizing number of connections for each company.    
    returns: G: networkx undirected graph object representing connections between companies. 
    """

    # Exctract accession number from file name
    df['Accession Number'] = df['File Name'].apply(lambda x: x.split('/')[-1].strip('.txt'))
    
    # Get unique combos of CIK and accession number
    df = df[['Accession Number','CIK']].drop_duplicates()
    
    # Get entries with accession numbers that show up multiple times
    df = df[df['Accession Number'].duplicated(keep=False)]
    
    # Get list of unique CIKs and accession numbers
    CIK_list = df['CIK'].unique()
    ANUM_list = df['Accession Number'].unique()
    
    # Group entries by accession number
    grouping = df.groupby('Accession Number')
    
    # Create undirected graph to represent how different CIKs  
    # are connected with one another through joint filings. 
    
    G = nx.Graph()
    G.add_nodes_from(CIK_list)
    
    # Add edges to graph based on filings
    
    for ANUM in ANUM_list:
    
        associated_CIKs = grouping.get_group(ANUM)['CIK'].tolist()
        G.add_edges_from(list(combinations(associated_CIKs,2)))
    
    # Calculate degree of each node in graph
    # Nodes with degree >1 likely correspond to depositors
    graph_summary = pd.DataFrame(G.degree,columns=['CIK','num_edges'])
    depositor_CIKs = graph_summary[graph_summary['num_edges'] > 1]['CIK'].to_list()

    return(depositor_CIKs,graph_summary,G)

def extract_ABS_EE_data(filepath):
    """
    This function extracts structured XML data from ABS-EE filings and returns the 
    result as a python dictionary object. 

    param: filepath: path to downloaded ABS-EE filing (plain text version). 
    returns: asset_data: python dictionary of asset-level data. 
    """

    with open(filepath,'r') as f:
        lines = f.readlines()

    startline = None
    endline = None
    
    for i,line in enumerate(lines):
        if '<assetData' in line:
            startline = i
        if '</assetData>' in line:
            endline = i
            break
    
    asset_data = xmltodict.parse("".join(lines[startline:endline+1]))

    return(asset_data)

def process_ABS_EE_data(CIK,trustname,form_info,loan_data_dict,prop_data_dict):

    """
    This function extracts loan- and property-level data related to single-property
    CMBS loans from ABS-EE filings and returns the result as a pandas dataframe. 

    Please note that the current version of this function excludes multi-property loans, which 
    introduce many edge cases due to their inconsistent representation across ABS-EE filings from
    different deals. As such, the results returned by this function include only single-property loans
    and do not represent the complete loan pool associated with a deal. Future versions of the 
    code will attempt to address this limitation. 
    
    param: CIK: CIK number of CMBS trust [string]
    param: trustname: name of CMBS trust [string]
    param: form_info: row of SEC index file corresponding to ABS-EE filing [row of pandas dataframe]
    param: loan_data_dict: dataframe describing data types of loan variables [pandas dataframe]
    param: prop_data_dict: dataframe describing data types of property variables [pandas dataframe]
    returns: loan_df: loan-level data from ABS-EE filings [pandas dataframe]. 
    returns: prop_df: property-level data from ABS-EE filings [pandas dataframe]. 
    """

    # Get information on filing
    prospectus_date = form_info['prospectusDate']
    form_type = form_info['Form Type']
    date_filed = form_info['Date Filed']
    filepath = form_info['File Path']
    accession_number = filepath.split('/')[-1].strip('.txt')
    
    asset_data = extract_ABS_EE_data(filepath)
    assets = asset_data['assetData']['assets']

    # List of loan-specific fields
    loan_specific_fields = ['originalLoanAmount',
                            'originationDate',
                            'maturityDate',
                            'originalInterestRatePercentage',
                            'interestRateSecuritizationPercentage',
                            'NumberProperties',
                            'NumberPropertiesSecuritization']

    # List of fields that can potentially have multiple values (most likely represented as a list)
    multiple_value_fields = ['repurchaseReplacementReasonCode',
                             'liquidationPrepaymentCode',
                             'workoutStrategyCode',
                             'modificationCode']
    
    loan_data_list = []
    prop_data_list = []
    
    for asset in assets:
    
        # Check whether entry corresponds to a loan or whether the entry corresponds
        # to property-specific details for loans collateralized by multiple properties. 
        if any(field in asset.keys() for field in loan_specific_fields) and ('property' in asset.keys()):
    
            # Use deepcopy to avoid python memory issues with nested dicts
            loan_data = deepcopy(asset)
    
            # Create unique identifier for each loan that consists of
            # the CIK of the trust plus the prospectus loan id
            asset_id = loan_data['assetNumber']
            loan_id = CIK + '-' + asset_id
    
            loan_data['CIK'] = CIK
            loan_data['trustName'] = trustname
            loan_data['prospectusDate'] = prospectus_date
            loan_data['formType'] = form_type
            loan_data['dateFiled'] = date_filed
            loan_data['accessionNumber'] = accession_number
            loan_data['loanID'] = loan_id

            # Consolidate fields that can potentially have multiple values
            for field in multiple_value_fields:
                if field in loan_data.keys():
                    if type(loan_data[field]==list):
                        loan_data[field] = ','.join(loan_data[field])

            # Determine whether property information is a dict or list
            prop_data_type = type(loan_data['property'])

            # Determine number of properties securing loan
            if 'NumberProperties' in loan_data.keys():
                num_properties = int(loan_data['NumberProperties'])
            elif 'NumberPropertiesSecuritization' in loan_data.keys():
                num_properties = int(loan_data['NumberPropertiesSecuritization'])
            elif prop_data_type == list:
                num_properties = len(loan_data['property'])
            else:
                # If all else fails, try to extract data as if it's a single-property loan
                num_properties = 1

            # Entries corresponding to multi-property loans will sometimes resemble 
            # a series of single-property loans with related asset numbers
            # (e.g. "3-001","3-002", "3-003",etc.). 
            # When this occurs, the loan information is duplicated for each property, 
            # which can cause us to erroneously count the same loan multiple times. 
            # However, we can avoid this issue by dropping loans whose assetNumbers
            # include hyphens or periods, which is usually the telltale sign of this. 
            if (num_properties == 1) and not any(char in asset_id for char in ['.','-']):

                # Extract property-level data for single-property loans
                prop_data = loan_data.pop('property')
                if prop_data_type == list:
                    prop_data = prop_data[0]

                # Add form-specific info
                prop_data['CIK'] = CIK
                prop_data['trustName'] = trustname
                prop_data['prospectusDate'] = prospectus_date
                prop_data['formType'] = form_type
                prop_data['dateFiled'] = date_filed
                prop_data['accessionNumber'] = accession_number
                prop_data['loanID'] = loan_id

                # For now, propertyID is same as loanID. 
                # This may change in future updates that 
                # add support for multi-property loans. 
                prop_data['propertyID'] = loan_id
                prop_data['assetNumber'] = asset_id

                # Append data to list
                loan_data_list.append(loan_data)
                prop_data_list.append(prop_data)

    # Convert data into dataframe
    loan_df = pd.DataFrame(loan_data_list)
    prop_df = pd.DataFrame(prop_data_list)

    # Clean and format data types
    loan_df = clean_data_types(loan_df,loan_data_dict)
    prop_df = clean_data_types(prop_df,prop_data_dict)

    return(loan_df,prop_df)

def quality_control_checks(loan_df,prop_df):
    """
    This function verifies the integrity of longitudinal data
    on loans and properties scraped from ABS-EE filings and 
    removes observations that fail basic quality-control checks. 

    The following checks are performed: 
    (1) removal of duplicate observations
    (2) removal of observations that have been superseded by ABS-EE/A filings
    (3) removal of observations with inconsistent reporting of the original loan balance

    param: loan_df: loan-level dataset with repeated observations of each loan. 
    param: prop_df: property-level dataset with repeated observations of each property.
    returns: loan_df_clean: cleaned version of loan-level dataset
    returns: prop_df_clean: cleaned version of property-level dataset
    """
    
    # Group observations by loanID
    loan_ids = loan_df['loanID'].unique()
    loan_gb_object = loan_df.groupby('loanID')
    prop_gb_object = prop_df.groupby('loanID')

    loan_ts_list = []
    prop_ts_list = []

    for loan_id in loan_ids:
    
        # Get longitudinal data on specific loan and associated property
        loan_timeseries = loan_gb_object.get_group(loan_id)
        prop_timeseries = prop_gb_object.get_group(loan_id)
        
        # Drop loan observations whose reporting periods are the same, keeping only the one with the most recent date filed. 
        # This can occur when a form ABS-EE/A is filed that corrects errors in previous filings. By keeping the 
        # last filing, we should end up with the corrected loan data for the period of interest. 
        loan_timeseries = loan_timeseries.drop_duplicates(subset=['reportingPeriodBeginningDate','reportingPeriodEndDate'],keep='last')
        
        # Check to see that the original loan balance is reported consistently over time
        # while allowing for an error tolerance of +/- $100. 
        # Drop any loan observations that fall outside of this tolerance. 
        loan_timeseries['roundedOriginalLoanAmount'] = loan_timeseries['originalLoanAmount'].round(-2).astype('int64[pyarrow]')
        rounded_original_balance = loan_timeseries['roundedOriginalLoanAmount'].mode()[0]
        loan_timeseries = loan_timeseries[loan_timeseries['roundedOriginalLoanAmount']==rounded_original_balance]
        loan_timeseries.drop(columns='roundedOriginalLoanAmount',inplace=True)
        
        # Only keep property observations that were derived from the same filings as final set of loan observations
        prop_timeseries = prop_timeseries[prop_timeseries['accessionNumber'].isin(loan_timeseries['accessionNumber'])]

        # Append to list
        loan_ts_list.append(loan_timeseries)
        prop_ts_list.append(prop_timeseries)

    # Concatenate data
    loan_df_clean = pd.concat(loan_ts_list).reset_index(drop=True)
    prop_df_clean = pd.concat(prop_ts_list).reset_index(drop=True)

    return loan_df_clean,prop_df_clean

### *** INITIAL SETUP *** ###

# Get current working directory 
pwd = os.getcwd()

# Specify paths to folders for input/output
raw_folder = os.path.join(pwd,'asset_data/CMBS/raw')
clean_folder = os.path.join(pwd,'asset_data/CMBS/clean')

# Load index of ABS-EE filings from CMBS deals
CMBS_index_file_path = os.path.join(pwd,'index_files/clean/CMBS_ABS-EE_index_file.parquet')
CMBS_index_file = pd.read_parquet(CMBS_index_file_path)

# Load index of broader set of filings from ABS companies that have filed a form ABS-EE
company_index_file_path = os.path.join(pwd,'index_files/clean/ABS_company_index_file.parquet')
company_index_file = pd.read_parquet(company_index_file_path)

# Load data dictionaries
data_dict_path = os.path.join(pwd,'CMBS_ABS-EE_scraping_data_dict.xlsx')
loan_data_dict = pd.read_excel(data_dict_path,sheet_name='loan')
prop_data_dict = pd.read_excel(data_dict_path,sheet_name='property')

### *** DETERMINE WHICH FILES TO PROCESS *** ###

# Identify which companies are acting as depositors vs. trusts in CMBS deals
depositor_CIKs,extra1,extra2 = identify_depositors(CMBS_index_file)
depositor_mask = CMBS_index_file['CIK'].isin(depositor_CIKs)
depositor_info = CMBS_index_file[depositor_mask].drop_duplicates(subset=['CIK'],keep='last')[['Company Name','CIK']]
depositor_names = depositor_info['Company Name'] + ' (CIK ' + depositor_info['CIK'] + ')'

# Drop depositors from our dataset and keep only the trusts.
# (Prevents duplication of asset data)
CMBS_index_file = CMBS_index_file[~depositor_mask]
print(f'Excluded {len(depositor_names)} companies identified as depositors:\n',flush=True)
for name in depositor_names:
    print(f'    {name}',flush=True)

# Specify paths to downloaded raw ABS-EE filings
CMBS_index_file['File Path'] = raw_folder 
CMBS_index_file['File Path'] += '/' + CMBS_index_file['CIK']
CMBS_index_file['File Path'] += '/' + CMBS_index_file['File Name'].apply(lambda x: x.split('/')[-1])

# Determine which files we've already downloaded
CMBS_index_file['Downloaded'] = CMBS_index_file['File Path'].apply(lambda x: os.path.exists(x))

# Determine which deals we have complete data for
share_downloaded_by_CIK = CMBS_index_file[['CIK','Downloaded']].groupby('CIK').mean().reset_index()
complete_CIKs = share_downloaded_by_CIK[share_downloaded_by_CIK['Downloaded']==1]['CIK'].to_list()
incomplete_mask = (~CMBS_index_file['CIK'].isin(complete_CIKs))
incomplete_info = CMBS_index_file[incomplete_mask].drop_duplicates(subset=['CIK'],keep='last')[['Company Name','CIK']]
incomplete_names = incomplete_info['Company Name'] + ' (CIK ' + incomplete_info['CIK'] + ')'

# Print which companies we're missing data for
print(f'\nExcluded {len(incomplete_names)} companies with incomplete downloads of ABS-EE data:\n',flush=True)
for name in incomplete_names:
    print(f'    {name}',flush=True)

# Sometimes companies will upload an ABS-EE form with their preliminary prospectus
# whose details differ from those in the final offering. As such, we should exclude 
# ABS-EE filings from before the date that the final prospectus (Form 424B2) was filed. 

# Get index of prospectus filings
company_index_file = company_index_file[company_index_file['CIK'].isin(complete_CIKs)].reset_index(drop=True)
prospectus_filings = company_index_file[company_index_file['Form Type']=='424B2']

# Get date on which prospectus was filed
prospectus_dates = prospectus_filings[['CIK','Date Filed']].drop_duplicates(subset=['CIK'])
prospectus_dates = prospectus_dates.rename(columns={'Date Filed':'prospectusDate'})

# Attach this info to index of ABS-EE filings
CMBS_index_file = pd.merge(CMBS_index_file,prospectus_dates,how='left',on='CIK')

# Keep only ABS-EE filings from after the final prospectus was filed.
# (helps to filter out preliminary versions of asset-level data that can have incorrect info.) 
CMBS_index_file = CMBS_index_file[CMBS_index_file['Date Filed'] > CMBS_index_file['prospectusDate']].reset_index(drop=True)

# Print update for user on number of deals included in final dataset
trust_CIKs = CMBS_index_file['CIK'].unique().tolist()
num_filings = len(CMBS_index_file)
num_trusts = len(trust_CIKs)

print(f'\nFinal dataset consists of {num_filings} ABS-EE filings from {num_trusts} CMBS trusts.\n',flush=True)

### *** CREATE LONGITUDINAL DATASET FOR EACH CMBS TRUST *** ###

for trust_num,CIK in enumerate(trust_CIKs):

    # Create folder for output
    outfolder = os.path.join(clean_folder,CIK)
    os.makedirs(outfolder,exist_ok=True)
    
    # Get info on ABS-EE filings associated with trust
    CIK_index_file = CMBS_index_file[CMBS_index_file['CIK']==CIK]
    trustname = CIK_index_file['Company Name'].iloc[-1]
    
    loan_df_list = []
    prop_df_list = []
    
    failed_extractions = []
    reason_failed = []
    
    # Extract data from filings
    for i in range(len(CIK_index_file)):
    
        # Get info on form to extract data from
        form_info = CIK_index_file.iloc[i]
    
        try: 
            # Try to extract the data
            loan_df,prop_df = process_ABS_EE_data(CIK,trustname,form_info,loan_data_dict,prop_data_dict)
            
            loan_df_list.append(loan_df)
            prop_df_list.append(prop_df)
            
        except Exception as e:
            # If it fails, note the form accession number so we can check why.
            # Oftentimes this occurs due to errors in ABS-EE submissions, which are 
            # typically corrected by a form ABS-EE/A filed soon after. 
            failed_extractions.append(form_info['Accession Number'])
            error_text = str(e)
            reason_failed.append(error_text)
    
    # Concatenate data
    loan_df = pd.concat(loan_df_list)
    prop_df = pd.concat(prop_df_list)
    
    # Remove loan and property observations that fail basic quality-control checks
    loan_df,prop_df = quality_control_checks(loan_df,prop_df)
    
    # Record filings that we were unable to parse data from
    failed_index_file = CIK_index_file[CIK_index_file['Accession Number'].isin(failed_extractions)].reset_index(drop=True)
    failed_index_file['Reason Failed'] = reason_failed
    
    # Save results
    loan_outname = os.path.join(outfolder,f'{CIK}_loan.parquet')
    prop_outname = os.path.join(outfolder,f'{CIK}_prop.parquet')
    failed_outname = os.path.join(outfolder,f'{CIK}_failed.parquet')
    loan_df.to_parquet(loan_outname)
    prop_df.to_parquet(prop_outname)
    failed_index_file.to_parquet(failed_outname)
    
    # Print update for user
    num_filings = len(CIK_index_file)
    num_failed = len(failed_index_file)
    num_successful = num_filings - num_failed
    
    print(f'TRUST {trust_num + 1} / {num_trusts}: {trustname} (CIK {CIK})',flush=True)
    print(f'    Number of ABS-EE filings: {len(CIK_index_file)}',flush=True)
    print(f'    Number parsed successfully: {num_successful}',flush=True)
    print(f'    Number failed: {num_failed}\n',flush=True)