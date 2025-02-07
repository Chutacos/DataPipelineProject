import requests
import json
import boto3
import config
import pandas as pd


s3 = boto3.client(
    "s3",
    aws_access_key_id=config.aws_access_key_id,
    aws_secret_access_key=config.aws_secret_access_key,
    region_name=config.region_name
)

def fetch_data():
    """Fetch football fixture data from API"""
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"date": "2021-01-29"}

    response = requests.get(url, headers=config.headers, params=querystring)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def json_to_csv(data, filename="football_fixtures.csv"):
    """Convert JSON data into CSV file."""
    if not data or "response" not in data:
        print("Invalid or empty data")
        return None
    
    fixtures = data["response"]
    
    fixture_list = []
    for fixture in fixtures:
        fixture_info = {
            "Fixture ID": fixture["fixture"]["id"],
            "Date": fixture["fixture"]["date"],
            "Status": fixture["fixture"]["status"]["long"],
            "Home Team": fixture["teams"]["home"]["name"],
            "Away Team": fixture["teams"]["away"]["name"],
            "Home Score": fixture["goals"]["home"],
            "Away Score": fixture["goals"]["away"],
            "League": fixture["league"]["name"],
            "Country": fixture["league"]["country"]
        }
        fixture_list.append(fixture_info)
    
    df = pd.DataFrame(fixture_list)
    df.to_csv(filename, index=False)
    return filename  # Return the filename for upload

def upload_to_s3(file_name, bucket_name, s3_file_name):
    """Upload file to AWS S3 bucket."""
    try:
        with open(file_name, "rb") as data:
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_file_name,
                Body=data,
                ContentType="text/csv"
            )
        print(f"File {file_name} uploaded successfully to S3 as {s3_file_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Main execution
football_data = fetch_data()
if football_data:
    csv_filename = json_to_csv(football_data)
    if csv_filename:
        upload_to_s3(csv_filename, "ads507-footballapi", "football_fixtures.csv")
