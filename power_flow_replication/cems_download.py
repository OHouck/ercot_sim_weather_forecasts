import pandas as pd
import numpy as np
from pathlib import Path
import requests
import zipfile
from io import BytesIO
from tqdm import tqdm
import time

# Define paths
RAW_DATA = Path('./test_data/CEMS_hourly/CEMS_csv')
ASSEMBLED_DATA = Path('./test_assembled_data')

# Create directories if they don't exist
RAW_DATA.mkdir(parents=True, exist_ok=True)
ASSEMBLED_DATA.mkdir(exist_ok=True)

# Define states and years
STATES = ['ar', 'co', 'ia', 'il', 'in', 'ks', 'ky', 'la', 'mi', 'mn', 
          'mo', 'ms', 'mt', 'nd', 'ne', 'nm', 'ok', 'sd', 'tx', 'wi']
MONTHS = [f'{i:02d}' for i in range(1, 13)]  # ['01', '02', ..., '12']
YEARS_2016_2021 = range(2016, 2022)  # 2016-2021
YEARS_2022 = [2022]

# Columns to drop
COLS_TO_DROP = [
    'so2_mass_measure_flg', 'so2_ratelbsmmbtu', 'so2_rate_measure_flg',
    'nox_ratelbsmmbtu', 'nox_rate_measure_flg', 'nox_mass_measure_flg',
    'co2_mass_measure_flg', 'co2_ratetonsmmbtu', 'co2_rate_measure_flg'
]

def download_cems_data(year, state, month):
    """
    Download CEMS data from EPA FTP server (commented out in original code)
    """
    url = f"ftp://newftp.epa.gov/DMDnLoad/emissions/hourly/monthly/{year}/{year}{state}{month}.zip"
    output_path = RAW_DATA / f"{year}{state}{month}.zip"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {year}{state}{month}.zip")
            return True
        else:
            print(f"Failed to download: {year}{state}{month}.zip (Status: {response.status_code})")
            return False
    except Exception as e:
        print(f"Error downloading {year}{state}{month}.zip: {str(e)}")
        return False

def process_cems_file_2016_2021(year, state, month):
    """
    Process individual CEMS CSV file for years 2016-2021
    
    Parameters:
    -----------
    year : int
        Year of data
    state : str
        Two-letter state code
    month : str
        Two-digit month ('01', '02', etc.)
    
    Returns:
    --------
    pd.DataFrame or None
    """
    filepath = RAW_DATA / f"{year}{state}{month}.csv"
    
    if not filepath.exists():
        print(f"Warning: File not found - {filepath}")
        return None
    
    try:
        # Read CSV
        df = pd.read_csv(filepath, low_memory=False)
        
        # Drop unnecessary columns (if they exist)
        cols_to_drop_actual = [col for col in COLS_TO_DROP if col in df.columns]
        df = df.drop(columns=cols_to_drop_actual, errors='ignore')
        
        # Convert op_date to datetime
        if 'op_date' in df.columns:
            df['date'] = pd.to_datetime(df['op_date'], format='%m/%d/%Y', errors='coerce')
            df = df.drop(columns=['op_date'])
        
        # Rename op_hour to hour
        if 'op_hour' in df.columns:
            df = df.rename(columns={'op_hour': 'hour'})
        
        # Save as pickle (more efficient than CSV for Python)
        output_path = ASSEMBLED_DATA / f"CEMS_hourly_{year}_{state}_{month}.pkl"
        df.to_pickle(output_path)
        
        return df
        
    except Exception as e:
        print(f"Error processing {year}{state}{month}: {str(e)}")
        return None

def process_cems_file_2022(year, state):
    """
    Process CEMS CSV file for 2022 (different format/naming convention)
    
    Parameters:
    -----------
    year : int
        Year of data (2022)
    state : str
        Two-letter state code
    
    Returns:
    --------
    pd.DataFrame or None
    """
    filepath = RAW_DATA / f"emissions-hourly-{year}-{state}.csv"
    
    if not filepath.exists():
        print(f"Warning: File not found - {filepath}")
        return None
    
    try:
        # Read CSV
        df = pd.read_csv(filepath, low_memory=False)
        
        # Rename columns to match earlier years
        rename_dict = {
            'facilityname': 'facility_name',
            'facilityid': 'orispl_code',
            'operatingtime': 'op_time',
            'grossloadmw': 'gloadmw',
            'steamload1000lbhr': 'sload1000lbhr',
            'so2masslbs': 'so2_masslbs',
            'noxmasslbs': 'nox_masslbs',
            'co2massshorttons': 'co2_masstons',
            'heatinputmmbtu': 'heat_inputmmbtu',
            'date': 'op_date'
        }
        
        df = df.rename(columns=rename_dict)
        
        # Keep only necessary columns
        keep_cols = ['state', 'facility_name', 'orispl_code', 'unitid', 'hour', 
                    'op_time', 'gloadmw', 'sload1000lbhr', 'so2_masslbs', 
                    'nox_masslbs', 'co2_masstons', 'heat_inputmmbtu', 'op_date']
        df = df[[col for col in keep_cols if col in df.columns]]
        
        # Convert op_date to datetime
        if 'op_date' in df.columns:
            df['date'] = pd.to_datetime(df['op_date'], format='%Y-%m-%d', errors='coerce')
            df = df.drop(columns=['op_date'])
        
        return df
        
    except Exception as e:
        print(f"Error processing {year}{state}: {str(e)}")
        return None

def combine_year_data_2016_2021(year):
    """
    Combine all state-month files for a given year (2016-2021)
    
    Parameters:
    -----------
    year : int
        Year to combine
    
    Returns:
    --------
    pd.DataFrame
    """
    print(f"\nCombining data for year {year}...")
    
    all_dfs = []
    total_files = len(STATES) * len(MONTHS)
    
    with tqdm(total=total_files, desc=f"Year {year}") as pbar:
        for state in STATES:
            for month in MONTHS:
                filepath = ASSEMBLED_DATA / f"CEMS_hourly_{year}_{state}_{month}.pkl"
                
                if filepath.exists():
                    try:
                        df = pd.read_pickle(filepath)
                        all_dfs.append(df)
                    except Exception as e:
                        print(f"Error reading {filepath}: {str(e)}")
                
                pbar.update(1)
    
    if not all_dfs:
        print(f"Warning: No data found for year {year}")
        return pd.DataFrame()
    
    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Drop fac_id and unit_id if they exist (as in original Stata code)
    combined_df = combined_df.drop(columns=['fac_id', 'unit_id'], errors='ignore')
    
    # Save combined year file
    output_path = ASSEMBLED_DATA / f"CEMS_hourly_{year}.pkl"
    combined_df.to_pickle(output_path)
    
    print(f"Saved: {output_path}")
    print(f"Total rows: {len(combined_df):,}")
    
    return combined_df

def combine_year_data_2022(year):
    """
    Combine all state files for 2022
    
    Parameters:
    -----------
    year : int
        Year to combine (2022)
    
    Returns:
    --------
    pd.DataFrame
    """
    print(f"\nCombining data for year {year}...")
    
    all_dfs = []
    
    with tqdm(total=len(STATES), desc=f"Year {year}") as pbar:
        for state in STATES:
            df = process_cems_file_2022(year, state)
            if df is not None:
                all_dfs.append(df)
            pbar.update(1)
    
    if not all_dfs:
        print(f"Warning: No data found for year {year}")
        return pd.DataFrame()
    
    # Combine all dataframes
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Save combined year file
    output_path = ASSEMBLED_DATA / f"CEMS_hourly_{year}.pkl"
    combined_df.to_pickle(output_path)
    
    print(f"Saved: {output_path}")
    print(f"Total rows: {len(combined_df):,}")
    
    return combined_df

def cleanup_intermediate_files(year):
    """
    Delete intermediate state-month files after combining
    
    Parameters:
    -----------
    year : int
        Year to clean up
    """
    print(f"Cleaning up intermediate files for {year}...")
    
    count = 0
    for state in STATES:
        for month in MONTHS:
            filepath = ASSEMBLED_DATA / f"CEMS_hourly_{year}_{state}_{month}.pkl"
            if filepath.exists():
                filepath.unlink()
                count += 1
    
    print(f"Deleted {count} intermediate files")

def process_all_cems_data(cleanup=True):
    """
    Main function to process all CEMS data
    
    Parameters:
    -----------
    cleanup : bool
        Whether to delete intermediate files after combining
    """
    print("=" * 60)
    print("Processing EPA CEMS Hourly Data")
    print("=" * 60)
    
    # Process 2016-2021 (old format)
    for year in YEARS_2016_2021:
        print(f"\n{'='*60}")
        print(f"Processing Year {year}")
        print('='*60)
        
        # Process individual files
        for state in tqdm(STATES, desc="States"):
            for month in MONTHS:
                process_cems_file_2016_2021(year, state, month)
        
        # Combine into single year file
        combine_year_data_2016_2021(year)
        
        # Clean up intermediate files
        if cleanup:
            cleanup_intermediate_files(year)
    
    # Process 2022 (new format)
    for year in YEARS_2022:
        print(f"\n{'='*60}")
        print(f"Processing Year {year}")
        print('='*60)
        
        combine_year_data_2022(year)
    
    print("\n" + "="*60)
    print("CEMS data processing complete!")
    print("="*60)

def load_cems_year(year):
    """
    Helper function to load processed CEMS data for a specific year
    
    Parameters:
    -----------
    year : int
        Year to load
    
    Returns:
    --------
    pd.DataFrame
    """
    filepath = ASSEMBLED_DATA / f"CEMS_hourly_{year}.pkl"
    
    if not filepath.exists():
        raise FileNotFoundError(f"CEMS data for year {year} not found. Run process_all_cems_data() first.")
    
    return pd.read_pickle(filepath)

def get_cems_summary(year):
    """
    Get summary statistics for CEMS data
    
    Parameters:
    -----------
    year : int
        Year to summarize
    """
    df = load_cems_year(year)
    
    print(f"\nCEMS Data Summary - Year {year}")
    print("=" * 60)
    print(f"Total rows: {len(df):,}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nMissing values:\n{df.isnull().sum()}")
    print(f"\nNumeric columns summary:")
    print(df.describe())

# Example usage functions
def example_basic_usage():
    """Example: Process all CEMS data"""
    process_all_cems_data(cleanup=True)

def example_load_and_analyze():
    """Example: Load and analyze a specific year"""
    # Load data for 2020
    df_2020 = load_cems_year(2020)
    
    # Get summary
    get_cems_summary(2020)
    
    # Example analysis
    print("\nExample Analysis:")
    print(f"Unique facilities: {df_2020['orispl_code'].nunique() if 'orispl_code' in df_2020.columns else 'N/A'}")
    print(f"Unique states: {df_2020['state'].nunique() if 'state' in df_2020.columns else 'N/A'}")

def example_process_single_year():
    """Example: Process just one year"""
    year = 2020
    
    print(f"Processing only year {year}...")
    
    # Process individual files
    for state in tqdm(STATES, desc="States"):
        for month in MONTHS:
            process_cems_file_2016_2021(year, state, month)
    
    # Combine
    combine_year_data_2016_2021(year)
    
    # Cleanup
    cleanup_intermediate_files(year)
    
    print("Done!")

if __name__ == "__main__":
    # Run the full processing pipeline
    # process_all_cems_data(cleanup=True)
    
    # Or run one of the examples:
    example_process_single_year()
    # example_load_and_analyze()