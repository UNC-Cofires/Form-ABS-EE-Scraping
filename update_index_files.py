import numpy as np
import pandas as pd
import requests
import time
import io
import os

### *** HELPER FUNCTIONS *** ###

def parse_edgar_index_file(filepath):
    """
    Parse an SEC EDGAR full-index form.idx file into a pandas DataFrame.
    These files are available at: https://www.sec.gov/Archives/edgar/full-index/

    param: filepath: path to local form.idx file
    returns: df: pandas dataframe listing company name, CIK, form type, and filing date
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # --- Step 1: Locate the column header line and dashes separator --- #
    header_line_idx = None
    dash_line_idx = None

    for i, line in enumerate(lines):
        if line.strip().startswith('Form Type'):
            header_line_idx = i
        if line.strip().startswith('---'):
            dash_line_idx = i
            break  # dashes always follow the header, so we can stop here

    if header_line_idx is None or dash_line_idx is None:
        raise ValueError(f"Could not find column header or separator line in: {filepath}")

    # --- Step 2: Derive fixed-width column positions from the header line --- #
    header_line = lines[header_line_idx]
    col_names = ['Form Type', 'Company Name', 'CIK', 'Date Filed', 'File Name']

    # Find the starting character position of each column name
    col_starts = [0,16,78,90,102]

    # Build colspecs: list of (start, end) tuples required by read_fwf
    colspecs = [(col_starts[i], col_starts[i + 1]) for i in range(len(col_starts) - 1)]
    colspecs.append((col_starts[-1], None))  # last column runs to end of line

    # --- Step 3: Extract data rows and parse --- #
    data_str = ''.join(lines[dash_line_idx + 1:])

    df = pd.read_fwf(
        io.StringIO(data_str),
        colspecs=colspecs,
        names=col_names,
        dtype='string[pyarrow]'
    )

    # --- Step 4: Clean up --- #
    df = df.apply(lambda s: s.str.strip())
    df = df.dropna(how='all').reset_index(drop=True)
    df['Date Filed'] = pd.to_datetime(df['Date Filed'])
    df['CIK'] = df['CIK'].str.zfill(10)

    return df

### *** INITIAL SETUP *** ###

# Get current working directory 
pwd = os.getcwd()

# Specify period of interest
start_date = pd.Timestamp('2016-11-23')   # Regulation AB II compliance date
today_date = pd.Timestamp('today')

# Determine which quarters fall within this period
# (only download data for those that have been completed). 
start_quarter = start_date.to_period('Q')
current_quarter = today_date.to_period('Q')
end_quarter = current_quarter - 1

# Get range of data to scrape
periods = pd.period_range(start_quarter,end_quarter)

### *** SCRAPE INDEX FILES FOR EACH QUARTER *** ###

# URL of flat index files
base_url = 'https://www.sec.gov/Archives/edgar/full-index'

# Need to specify user-agent in header when accessing EDGAR via API
headers = {'User-Agent':'Kieran Fitzmason kfitzmason@unc.edu'}

# Limited to 10 requests per second, so build in a delay between requests
max_req_per_sec = 10
sleep_seconds = 1/max_req_per_sec

# Create folder for output if it doesn't already exist
raw_folder = os.path.join(pwd,f'index_files/raw')
clean_folder = os.path.join(pwd,f'index_files/clean')
os.makedirs(raw_folder,exist_ok=True)
os.makedirs(clean_folder,exist_ok=True)

# Iterate over periods of interest

for period in periods:
    
    year = period.year
    quarter = period.quarter

    # Check to see if we've already downloaded the data before scraping
    filepath = os.path.join(raw_folder,f'{year}Q{quarter}_form.idx')
    if os.path.exists(filepath):
        print(f'{year}Q{quarter} - Already present')
    else:
        
        # Scrape data if not already present
        url = f'{base_url}/{year}/QTR{quarter}/form.idx'
        res = requests.get(url,headers=headers)

        # Build in pause to avoid going over rate limit
        time.sleep(sleep_seconds)

        if res.ok:
            
            with open(filepath, 'w') as f:
                f.write(res.text)

            print(f'{year}Q{quarter} - Success')

        else:

            print(f'{year}Q{quarter} - Failed')

### *** EXTRACT DATA ON ABS-EE FILINGS *** ###

filepaths = [os.path.join(raw_folder,f) for f in np.sort(os.listdir(raw_folder)) if f.endswith('form.idx')]

form_ABSEE_df_list = []

for filepath in filepaths:
    
    print('Extracting ABS-EE data from:',filepath.split('/')[-1])

    form_ABSEE_df = parse_edgar_index_file(filepath)
    form_ABSEE_df = form_ABSEE_df[form_ABSEE_df['Form Type'].isin(['ABS-EE','ABS-EE/A'])]
    form_ABSEE_df_list.append(form_ABSEE_df)

form_ABSEE_df = pd.concat(form_ABSEE_df_list)
form_ABSEE_df = form_ABSEE_df.sort_values(by=['CIK','Date Filed']).reset_index(drop=True)

### *** ALSO PULL DATA ON ALL FILINGS OF COMPANIES WITH ABS-EE FORMS *** ###

ABS_company_CIKs = form_ABSEE_df['CIK'].unique()

ABS_company_df_list = []

for filepath in filepaths:
    
    print('Extracting ABS company data from:',filepath.split('/')[-1])

    ABS_company_df = parse_edgar_index_file(filepath)
    ABS_company_df = ABS_company_df[ABS_company_df['CIK'].isin(ABS_company_CIKs)]
    ABS_company_df_list.append(ABS_company_df)

ABS_company_df = pd.concat(ABS_company_df_list)
ABS_company_df = ABS_company_df.sort_values(by=['CIK','Date Filed']).reset_index(drop=True)

### *** SAVE RESULTS *** ###

## Form ABS-EE filings

# Save as parquet
outname = os.path.join(clean_folder,'ABS-EE_index_file.parquet')
form_ABSEE_df.to_parquet(outname)

# And also as CSV
outname = os.path.join(clean_folder,'ABS-EE_index_file.csv')
form_ABSEE_df.to_csv(outname,index=False)

## All filings of companies that have submitted a form ABS-EE

# Save as parquet
outname = os.path.join(clean_folder,'ABS_company_index_file.parquet')
ABS_company_df.to_parquet(outname)

# And also as CSV
outname = os.path.join(clean_folder,'ABS_company_index_file.csv')
ABS_company_df.to_csv(outname,index=False)