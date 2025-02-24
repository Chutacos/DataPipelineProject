import requests
import json
import boto3
import config
import pandas as pd
from tqdm import tqdm
import time
import datetime
from datetime import date

current_date = datetime.date.today()
begin_range = current_date - datetime.timedelta(days=current_date.weekday() + 7)
end_range = begin_range + datetime.timedelta(days=6)

s3 = boto3.client(
    "s3",
    aws_access_key_id=config.aws_access_key_id,
    aws_secret_access_key=config.aws_secret_access_key,
    region_name=config.region_name
)

def upload_to_s3(filename, bucket_name, object_name=None):
    if object_name is None:
        object_name = filename
    try:
        s3.upload_file(filename, bucket_name, object_name)
        print(f"Uploaded {filename} to {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading {filename}: {e}")

def fetch_data(url, querystring, headers): 
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def fetch_fixtures(): 
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    querystring = {"league": "39", "season": "2024", "from": begin_range, "to": end_range}
    data = fetch_data(url, querystring, config.headers)
    if not data or "response" not in data:
        print("Invalid or empty data")
        return None
    df = pd.json_normalize(data["response"], sep='.')
    filename = "fixtures.csv"
    df.to_csv(filename, index=False)
    upload_to_s3(filename, config.s3_bucket_name)
    return df

def fetch_fixture_stats(fixtures_df): 
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    fix_stats = []
    fixture_id_column = "fixture.id" if "fixture.id" in fixtures_df.columns else "fixture"
    for fix_id in tqdm(sorted(set(fixtures_df[fixture_id_column]))):
        querystring = {"fixture": fix_id}
        stats_obj = fetch_data(url, querystring, config.headers)
        time.sleep(2)
        if stats_obj and "response" in stats_obj:
            stats_obj["response"][0]["fixture"] = stats_obj['parameters']['fixture']
            stats_obj["response"][1]["fixture"] = stats_obj['parameters']['fixture']
            fix_stats.extend(stats_obj["response"])
    df = pd.json_normalize(fix_stats, sep='.')
    filename = "fixture_statistics.csv"
    df.to_csv(filename, index=False)
    upload_to_s3(filename, config.s3_bucket_name)
    return df

def fetch_fixture_lineups(fixtures_df): 
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"
    fix_lineups = []
    fixture_id_column = "fixture.id" if "fixture.id" in fixtures_df.columns else "fixture"
    for fix_id in tqdm(sorted(set(fixtures_df[fixture_id_column]))):
        querystring = {"fixture": fix_id}
        lineups_obj = fetch_data(url, querystring, config.headers)
        time.sleep(2)
        if lineups_obj and "response" in lineups_obj:
            fix_lineups.extend(lineups_obj["response"])
    df = pd.json_normalize(fix_lineups, sep='.')
    filename = "fixture_lineups_with_match_id.csv"
    df.to_csv(filename, index=False)
    upload_to_s3(filename, config.s3_bucket_name)
    return df

def process_and_save_all():
    fixtures = fetch_fixtures()
    if fixtures is not None:
        fetch_fixture_stats(fixtures)
        fetch_fixture_lineups(fixtures)

if __name__ == "__main__":
    process_and_save_all()
