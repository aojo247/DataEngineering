import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import boto3
import os


def lambda_handler(event, context):
    # S3 Bucket Configuration
    s3_bucket_name = os.environ['S3_BUCKET_NAME']  # Set the S3 bucket name in Lambda's environment variables
    s3_object_key = 'NBA/raw/NBA_roy_List.csv'  # Set the desired object key for the raw file (remember- you still need to run a cleaning script)
    
    # Create a list to store DataFrames
    dataframes = []
    url = 'https://www.basketball-reference.com/awards/roy.html'
    headers = {"User-Agent": "Mozilla/5.0"}  # Replace with any user agent of your choice

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for request errors
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
       
        if table:
            html_content = str(table)
            html_io = StringIO(html_content)
            df = pd.read_html(html_io)[0]
            dataframes.append(df)
            print("Data processed.")
        else:
            print("No table found.")
            return {"statusCode": 400, "body": "No table found"}

    except requests.exceptions.RequestException as e:
        print("Error scraping data.")
        return {"statusCode": 500, "body": "Error scraping data"}

 
    # Concatenate all the DataFrames into one
    nba_roy_df = pd.concat(dataframes, ignore_index=True)

 

    # Convert the DataFrame to CSV format and send it to S3
    csv_buffer = StringIO()
    nba_roy_df.to_csv(csv_buffer, index=False)
   
    # AWS S3 Upload
    session = boto3.Session()
    s3 = session.resource('s3')
    s3.Object(s3_bucket_name, s3_object_key).put(Body=csv_buffer.getvalue())

    return {"statusCode": 200, "body": "Data successfully saved to S3"}