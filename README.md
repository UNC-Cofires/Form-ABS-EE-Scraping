# Form ABS-EE Scraping

A Python-based pipeline for scraping and processing loan- and property-level data from SEC Form ABS-EE filings of Commercial Mortgage-Backed Securities (CMBS) trusts.

## Overview

This project automates the extraction of structured asset-level data from Form ABS-EE filings submitted to the SEC's EDGAR system. It specifically focuses on CMBS transactions, filtering out other types of asset-backed securities (e.g., auto loans) and extracting detailed information about commercial real estate loans and properties. The current version includes support for single-property loans; adding support for multi-property loans will be a focus of future work. 

The pipeline handles the complete workflow from identifying relevant filings to producing longitudinal datasets suitable for analysis.

## Features
- **Automated SEC EDGAR scraping:** Retrieves Form ABS-EE index files for specified date ranges
- **CMBS identification:** Distinguishes CMBS deals from other ABS types using keyword filtering
- **XML parsing:** Extracts structured loan and property data from Form ABS-EE submissions
- **Longitudinal tracking:** Creates time-series datasets for individual loans and properties
- **Standardized output:** Exports data in Parquet format with consistent schemas

## Requirements and Dependencies
- At least 32 GB of disk space
- At least 16 GB of RAM
- Python ≥3.12 with the following libraries installed: 
```
numpy
pandas
requests
xmltodict
networkx
pyarrow  # for Parquet support
openpyxl  # for Excel data dictionary
```
## Project Structure
```
Form-ABS-EE-Scraping/
├── update_index_files.py          # Step 1: Download SEC EDGAR index files
├── identify_CMBS_deals.py         # Step 2: Identify CMBS deals vs. other ABS
├── download_CMBS_data.py          # Step 3: Download raw ABS-EE filings
├── parse_CMBS_data.py             # Step 4: Parse XML and extract data
├── concatenate_CMBS_data.py       # Step 5: Combine data across all trusts
├── run_all.sh                     # SLURM script to run entire pipeline
├── CMBS_ABS-EE_scraping_data_dict.xlsx  # Data schema definitions
├── index_files/
│   ├── raw/                       # Downloaded SEC index files
│   └── clean/                     # Processed index files
│       ├── ABS-EE_index_file.parquet
│       ├── ABS_company_index_file.parquet
│       └── CMBS_ABS-EE_index_file.parquet
└── asset_data/CMBS/
    ├── raw/                       # Downloaded ABS-EE filings (by CIK)
    └── clean/                     # Processed data
        ├── [CIK]/                 # Individual trust datasets
        │   ├── [CIK]_loan.parquet
        │   ├── [CIK]_prop.parquet
        │   └── [CIK]_failed.parquet
        ├── loan.parquet           # Combined loan dataset
        ├── prop.parquet           # Combined property dataset
        └── failed.parquet         # Failed parsing records
```

## Usage
### Running the Complete Pipeline
The easiest way to run the entire pipeline is using the provided SLURM script (for HPC environments):
```
sbatch < run_all.sh
```
This executes all five scripts in sequence with appropriate memory allocation (32 GB) and time limits (2 days).

### Running Individual Steps
You can also run scripts individually:
```
# Step 1: Update SEC EDGAR index files
python update_index_files.py

# Step 2: Identify CMBS deals (excludes auto ABS, etc.)
python identify_CMBS_deals.py

# Step 3: Download raw ABS-EE filings
python download_CMBS_data.py

# Step 4: Parse and extract structured data
python parse_CMBS_data.py

# Step 5: Concatenate data from all trusts
python concatenate_CMBS_data.py
```

### Configuration

**Date Range:** By default, the pipeline scrapes data from November 23, 2016 ([Regulation AB II](https://www.sec.gov/newsroom/whats-new/regabii-asset-level-requirements-compliance) compliance date) to the end of the most recently concluded quarter. To modify this, edit `update_index_files.py`.

**User Agent:** This pipeline downloads data via the [SEC EDGAR API](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) which requires specification of the `User-Agent` in the request header. Please update the default `User-Agent` header in the `update_index_files.py`,`identify_CMBS_deals.py`, and `download_CMBS_data.py` scripts to reflect your name and email before running the pipeline. 

**Data Dictionary:** Variable names, data types, and descriptions are defined in `CMBS_ABS-EE_scraping_data_dict.xlsx` with separate sheets for loan and property data fields. Variable names and descriptions are derived from the SEC's [XML technical specifications](https://www.sec.gov/submit-filings/technical-specifications#abs) for ABS-EE filings. 

### Data Output
- **Loan-level data (`loan.parquet`):** Contains information about individual commercial real estate loans, including:
  - Original loan amount and balance
  - Interest rates
  - Origination and maturity dates
  - Loan status and modifications
  - Servicer information
  - Delinquency and default indicators
- **Property-level data (`prop.parquet`):** Contains information about properties securing the loans, including:
  - Property name and address
  - Property type (office, retail, multifamily, etc.)
  - Square footage and units
  - Occupancy rates
  - Financial performance (revenue, operating expenses, NOI, etc.)
- **Failed records (`failed.parquet`):** Logs ABS-EE filings that could not be parsed, along with error messages for debugging. This mainly occurs due to formatting errors in the asset-level data file submitted by CMBS trusts to the SEC, which are typically corrected by subsequent ABS-EE/A filings.

## Limitations
- **Multi-property loans:** Current version excludes loans secured by multiple properties due to inconsistent reporting across deals. Future versions will attempt to address this limitation.
- **Historical coverage:** Limited to post-Regulation AB II period (November 2016+).
- **Data quality control:** Imperfect reporting practices can lead to errors and inconsistencies that become incorporated into the data outputs of the pipeline. While the pipeline includes several data quality controls (e.g., removal of duplicated or superseded reports, consistency checks for original loan amounts) further data cleaning is likely to be necessary prior to analysis.

## Citation

If you use this code in your research, please cite:
```
<add citation once Zenodo record is created>
```
