import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time 
import boto3



def save_to_csv(data_frame, save_location):
    csv_file_path = save_location
    data_frame.to_csv(csv_file_path, index=False)
    print("Data saved to " + csv_file_path)



def save_to_s3_bucket(data_frame, s3_bucket_name, s3_object_key, aws_profile_name):
    bucket_name = s3_bucket_name
    object_key = s3_object_key


    session = boto3.Session(profile_name=aws_profile_name)
    s3Client = session.client('s3')

    # Convert the DataFrame to CSV format and send it to S3
    csv_buffer = data_frame.to_csv(index=False)
    s3Client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer)
    print(f"Data saved to  + {bucket_name}/{object_key}.")


# Specify the range of data that will be analyzed (Specified 2024 since the last year of a range is omitted.)
years = range(1985, 2024)

# Create a list to store DataFrames for each year
dataframes = []

# Set a delay between requests to avoid overloading the server
request_delay = 10  # You can adjust this value as needed (in seconds)

for year in years:
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
    
    headers = {"User-Agent": "Mozilla/5.0"}  # Replace with your user agent
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for request errors
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        
        if table:
            html_content = str(table)
            html_io = StringIO(html_content)
            df = pd.read_html(html_io)[0]
            df['Year'] = year
            dataframes.append(df)
            print(f"{year} data processed.")
        else:
            print(f"No table found for {year}")

    except requests.exceptions.RequestException as e:
        print(f"Error scraping data for {year}: {e}")
    
    # Add a delay before the next request to avoid rate-limiting
    time.sleep(request_delay)

# Concatenate all the DataFrames into one
final_df = pd.concat(dataframes, ignore_index=True)



# NOTE: You will need to uncomment one of the blocks of code below - depending on whether you wish to save to s3, or loacally.

#for csv, specify your file path. The file will be saved as nba_stats.csv unless specified otherwise.
#Example "C:\\Users\\bob\\Downloads\\nba_stats.csv"

'''
csv = save_to_csv(final_df, "<Local file path here >\\nba_stats.csv" )
'''


#for S3, specify your bucket name, and profile if neccesary. The file will be saved as nba_stats.csv unless specified otherwise.
'''
#change profile name from default if neccesary
s3 = save_to_s3_bucket(final_df, <s3_bucket_name here >, 'NBA/raw/nba_stats.csv', 'default')
'''

