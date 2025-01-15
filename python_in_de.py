import os
import json
import requests
from datetime import datetime
import boto3
import pandas as pd
from botocore.exceptions import ClientError

### add variables into your environment by running
# export FINNHUB_API_KEY=XXXXXXX
# export AWS_ACCESS_KEY_ID=XXXXXXX
# export AWS_SECRET_ACCESS_KEY=XXXXXXX

FINNHUB_API_URL = "https://finnhub.io/api/v1/quote"  # API endpoint for stock data
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
STOCK_SYMBOL = "AAPL" 
S3_BUCKET_NAME = "python-in-de" 
S3_KEY_PREFIX = "stock-data/"
S3_REGION = "us-east-1"


# Function to create bucket if it doesn't exist
def ensure_bucket_exists(bucket_name, region):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3.create_bucket(
                Bucket=bucket_name
            )
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            raise


# Function to fetch stock data from Finnhub API
def fetch_stock_data():
    params = {"symbol": STOCK_SYMBOL, "token": FINNHUB_API_KEY}
    response = requests.get(FINNHUB_API_URL, params=params)
    response.raise_for_status()
    return response.json()


# Function to transform data
def transform_data(raw_data):
    return {
        "symbol": STOCK_SYMBOL,
        "current_price": raw_data["c"],
        "high_price": raw_data["h"],
        "low_price": raw_data["l"],
        "open_price": raw_data["o"],
        "previous_close": raw_data["pc"],
        "timestamp": datetime.now().isoformat()
    }


# Function to convert data to CSV
def convert_to_csv(data):
    df = pd.DataFrame([data])  # Convert a single dictionary to a pandas DataFrame
    file_name = f"apple_stock_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    csv_path = os.path.join("/tmp", file_name)  # Temporary directory to store the file locally
    df.to_csv(csv_path, index=False)
    return csv_path, file_name


# Function to upload file to S3
def upload_to_s3(file_path, file_name):
    s3 = boto3.client('s3', region_name=S3_REGION)
    s3_key = f"{S3_KEY_PREFIX}{file_name}"
    with open(file_path, "rb") as file:
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=file
        )
    print(f"File {file_name} uploaded to S3 bucket {S3_BUCKET_NAME} successfully.")


# Main job function
def stock_data_job():
    try:
        ensure_bucket_exists(S3_BUCKET_NAME, S3_REGION)  # Ensure bucket exists
        raw_data = fetch_stock_data()
        transformed_data = transform_data(raw_data)
        csv_path, csv_file_name = convert_to_csv(transformed_data)  # Convert to CSV
        upload_to_s3(csv_path, csv_file_name)  # Upload CSV to S3
        print(f"Job completed successfully at {datetime.now()}")
    except Exception as e:
        print(f"Job failed: {str(e)}")

if __name__ == "__main__":
    print("Executing stock data fetch job...")
    stock_data_job()
