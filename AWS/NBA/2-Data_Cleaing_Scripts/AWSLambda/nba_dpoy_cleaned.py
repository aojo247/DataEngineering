import json
import requests
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd
import time
import boto3
import os

def lambda_handler(event, context):
    
    # S3 Bucket Configuration
    s3_bucket_name = os.environ['S3_BUCKET_NAME']  # Set the S3 bucket name in Lambda's environment variables
    source_s3_object_key = 'NBA/raw/nba_dpoy_list.csv'  # Set the desired object key for the raw file (remember- you still need to run a cleaning script)
    destination_s3_object_key = 'NBA/cleaned/nba_dpoy_list.csv'
    
    
    # S3 client
    s3_client = boto3.client('s3')

    # Get the object from S3
    csv_obj = s3_client.get_object(Bucket=s3_bucket_name, Key=source_s3_object_key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    # Use Pandas to read the CSV data
    df = pd.read_csv(StringIO(csv_string))

    # Promote the top row as the header
    df.columns = df.iloc[0]
    
    # Remove the top row (old header)
    df = df[1:]
    
    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)
    
    # Drop rows containing NaN values in the specified column
    df = df.dropna(subset=["Player"])
    
    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)
    
    # Creating a list of column names, excluding the one that is a float and NaN
    valid_columns = [col for col in df.columns if not (isinstance(col, float) and np.isnan(col))]
    
    # Creating a new DataFrame with only the valid columns
    df = df[valid_columns]

    # Convert DataFrame to CSV and upload to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(s3_bucket_name, destination_s3_object_key).put(Body=csv_buffer.getvalue())

    return f"Data cleaned and uploaded to {s3_bucket_name}/{destination_s3_object_key}"


