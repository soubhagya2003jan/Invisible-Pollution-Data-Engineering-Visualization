import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import csv
import pandas as pd
from io import StringIO
import gzip

# S3 details
bucket_name = 'openaq-data-archive'
folder_in_bucket = 'records/csv.gz/'  # base folder path

# Initialize boto3 client (public access - no credentials needed)
s3 = boto3.client('s3', region_name='ap-south-1', config=Config(signature_version=UNSIGNED))

# Example: Local folder where you want to save
local_download_folder = 'data'

# Make sure local folder exists
os.makedirs(local_download_folder, exist_ok=True)

# Read the location IDs from the Location_id.csv file
location_ids = {}
with open('Location_id.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        location_ids[row['ID']] = row['Location']  # Map ID to Location name

# Check if the location IDs have been correctly read
if not location_ids:
    print("No location IDs found in Location_id.csv!")
else:
    print(f"Found {len(location_ids)} location IDs.")

# Define the years you want to download data for
years = ['2024', '2025']
months = [f"{i:02d}" for i in range(1, 13)]  # Generates a list of months: 01, 02, ..., 12

# List to hold all data frames
dfs = []

# Loop through each location ID and each year/month
for location_id, location_name in location_ids.items():
    for year in years:
        for month in months:
            # Construct the S3 file path for each combination
            prefix = f'{folder_in_bucket}locationid={location_id}/year={year}/month={month}/'
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            # Debugging: print the S3 prefix to check the path
            print(f"Checking path: {prefix}")

            # Check if files are found
            if 'Contents' not in response:
                print(f"No files found for {location_name} ({location_id}) in {year}-{month}")
                continue  # Skip to the next location ID or month

            # Download and process the .csv.gz files
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv.gz'):  # only process .csv.gz files
                    print(f"Found file: {key}")  # Debugging line
                    file_name = os.path.basename(key)
                    
                    # Download the file into memory
                    file_obj = StringIO()
                    s3.download_fileobj(bucket_name, key, file_obj)
                    file_obj.seek(0)  # Move to the start of the file

                    # Decompress the .csv.gz file
                    with gzip.open(file_obj, mode='rt') as f:
                        try:
                            # Read the CSV into a pandas dataframe
                            df = pd.read_csv(f)
                            if not df.empty:
                                print(f"Data for Location {location_name} successfully loaded.")
                                df['Location'] = location_name
                                dfs.append(df)
                            else:
                                print(f"No data in file: {file_name}")
                        except Exception as e:
                            print(f"Error reading {file_name}: {e}")

# Check if any data frames were added
if not dfs:
    print("No data frames to concatenate.")
else:
    # Concatenate all dataframes into a single dataframe
    try:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv('combined_data.csv', index=False)
        print("All files processed and combined into 'combined_data.csv'")
    except Exception as e:
        print(f"Error during concatenation: {e}")
