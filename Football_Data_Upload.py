import requests
import json
import boto3
import config
import pandas as pd

from tqdm import tqdm
import time
from datetime import date


s3 = boto3.client(
    "s3",
    aws_access_key_id=config.aws_access_key_id,
    aws_secret_access_key=config.aws_secret_access_key,
    region_name=config.region_name
)


def fetch_data(url, querystring, headers): 
    response = requests.get(url, headers = headers, params=querystring)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def fetch_fixtures(): 

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    #test query 
    querystring = {"league": "39", "season": "2024"}

    #live query
    #current_date = str(date.today())
    #querystring = {"league": "39", "season": "2024", "date": current_date}
    
    data = fetch_data(url, querystring, config.headers)

    if not data or "response" not in data:
        print("Invalid or empty data")
        return None

    fixtures = data["response"]

    fixture_list = []

    #Split rows up by team 
    for fixture in fixtures: 
        home_info = {
            "Fixture ID": fixture["fixture"]["id"],
            "Date": fixture["fixture"]["date"],
            "Team ID": fixture["teams"]["home"]["id"],
            "Team Name": fixture["teams"]["home"]["name"],
            "Opponent ID": fixture["teams"]["away"]["id"],
            "Opponent Name": fixture["teams"]["away"]["name"],
            "Goals Scored": fixture["goals"]["home"],
            "Goals Conceded": fixture["goals"]["away"],
            "Win Flag": fixture["teams"]["home"]["winner"]
        }

        away_info = {
            "Fixture ID": fixture["fixture"]["id"],
            "Date": fixture["fixture"]["date"],
            "Team ID": fixture["teams"]["away"]["id"],
            "Team Name": fixture["teams"]["away"]["name"],
            "Opponent ID": fixture["teams"]["home"]["id"],
            "Opponent Name": fixture["teams"]["home"]["name"],
            "Goals Scored": fixture["goals"]["away"],
            "Goals Conceded": fixture["goals"]["home"],
            "Win Flag": fixture["teams"]["away"]["winner"]
        }

        fixture_list.append(home_info)
        fixture_list.append(away_info)
    
    df = pd.DataFrame(fixture_list)

    return df

def fetch_fixture_stats(fixtures_df, filename="Fixture_statistics.csv"): 

    fix_ids = pd.Series(fixtures_df['Fixture ID'])
    fix_stats = []

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
    
    for fix_id in tqdm(sorted(set(fix_ids))):
        querystring = {"fixture": fix_id}
        
        stats_obj = fetch_data(url, querystring, headers)
        time.sleep(2)

        home_info = {
            "Fixture ID": stats_obj['parameters']['fixture'],
            "Team ID": stats_obj['response'][0]['team']['id'],
            stats_obj['response'][0]['statistics'][0]['type']: stats_obj['response'][0]['statistics'][0]['value'],
            stats_obj['response'][0]['statistics'][1]['type']: stats_obj['response'][0]['statistics'][1]['value'],
            stats_obj['response'][0]['statistics'][2]['type']: stats_obj['response'][0]['statistics'][2]['value'],
            stats_obj['response'][0]['statistics'][3]['type']: stats_obj['response'][0]['statistics'][3]['value'],
            stats_obj['response'][0]['statistics'][4]['type']: stats_obj['response'][0]['statistics'][4]['value'],
            stats_obj['response'][0]['statistics'][5]['type']: stats_obj['response'][0]['statistics'][5]['value'],
            stats_obj['response'][0]['statistics'][6]['type']: stats_obj['response'][0]['statistics'][6]['value'],
            stats_obj['response'][0]['statistics'][7]['type']: stats_obj['response'][0]['statistics'][7]['value'],
            stats_obj['response'][0]['statistics'][8]['type']: stats_obj['response'][0]['statistics'][8]['value'],
            stats_obj['response'][0]['statistics'][9]['type']: stats_obj['response'][0]['statistics'][9]['value'],
            stats_obj['response'][0]['statistics'][10]['type']: stats_obj['response'][0]['statistics'][10]['value'],
            stats_obj['response'][0]['statistics'][11]['type']: stats_obj['response'][0]['statistics'][11]['value'],
            stats_obj['response'][0]['statistics'][12]['type']: stats_obj['response'][0]['statistics'][12]['value'],
            stats_obj['response'][0]['statistics'][13]['type']: stats_obj['response'][0]['statistics'][13]['value'],
            stats_obj['response'][0]['statistics'][14]['type']: stats_obj['response'][0]['statistics'][14]['value'],
            stats_obj['response'][0]['statistics'][15]['type']: stats_obj['response'][0]['statistics'][15]['value'],
            stats_obj['response'][0]['statistics'][16]['type']: stats_obj['response'][0]['statistics'][16]['value'],
            stats_obj['response'][0]['statistics'][17]['type']: stats_obj['response'][0]['statistics'][17]['value']
        }

        away_info = {
            "Fixture ID": stats_obj['parameters']['fixture'],
            "Team ID": stats_obj['response'][1]['team']['id'],
            stats_obj['response'][1]['statistics'][0]['type']: stats_obj['response'][1]['statistics'][0]['value'],
            stats_obj['response'][1]['statistics'][1]['type']: stats_obj['response'][1]['statistics'][1]['value'],
            stats_obj['response'][1]['statistics'][2]['type']: stats_obj['response'][1]['statistics'][2]['value'],
            stats_obj['response'][1]['statistics'][3]['type']: stats_obj['response'][1]['statistics'][3]['value'],
            stats_obj['response'][1]['statistics'][4]['type']: stats_obj['response'][1]['statistics'][4]['value'],
            stats_obj['response'][1]['statistics'][5]['type']: stats_obj['response'][1]['statistics'][5]['value'],
            stats_obj['response'][1]['statistics'][6]['type']: stats_obj['response'][1]['statistics'][6]['value'],
            stats_obj['response'][1]['statistics'][7]['type']: stats_obj['response'][1]['statistics'][7]['value'],
            stats_obj['response'][1]['statistics'][8]['type']: stats_obj['response'][1]['statistics'][8]['value'],
            stats_obj['response'][1]['statistics'][9]['type']: stats_obj['response'][1]['statistics'][9]['value'],
            stats_obj['response'][1]['statistics'][10]['type']: stats_obj['response'][1]['statistics'][10]['value'],
            stats_obj['response'][1]['statistics'][11]['type']: stats_obj['response'][1]['statistics'][11]['value'],
            stats_obj['response'][1]['statistics'][12]['type']: stats_obj['response'][1]['statistics'][12]['value'],
            stats_obj['response'][1]['statistics'][13]['type']: stats_obj['response'][1]['statistics'][13]['value'],
            stats_obj['response'][1]['statistics'][14]['type']: stats_obj['response'][1]['statistics'][14]['value'],
            stats_obj['response'][1]['statistics'][15]['type']: stats_obj['response'][1]['statistics'][15]['value'],
            stats_obj['response'][1]['statistics'][16]['type']: stats_obj['response'][1]['statistics'][16]['value'],
            stats_obj['response'][1]['statistics'][17]['type']: stats_obj['response'][1]['statistics'][17]['value']
        }

        fix_stats.append(home_info)
        fix_stats.append(away_info)

    df = pd.DataFrame(fix_stats)
    df.to_csv(filename, index=False)

    return df  


def fetch_fixture_lineups(fixtures_df, filename = "fixture_lineups.csv"): 

    fix_ids = pd.Series(fixtures_df['Fixture ID'])
    fix_lineups = []

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/lineups"
    
    for fix_id in tqdm(sorted(set(fix_ids))):
        querystring = {"fixture": fix_id}
        
        lineups_obj = fetch_data(url, querystring, headers)
        time.sleep(2)

        home_info = {
            "Fixture ID": lineups_obj['parameters']['fixture'],
            "Team ID": lineups_obj['response'][0]['team']['id'],
            "Formation": lineups_obj['response'][0]['formation'],
            "Player 1 Name": lineups_obj['response'][0]['startXI'][0]['player']['name'],
            "Player 1 Pos": lineups_obj['response'][0]['startXI'][0]['player']['pos'],
            "Player 2 Name": lineups_obj['response'][0]['startXI'][1]['player']['name'],
            "Player 2 Pos": lineups_obj['response'][0]['startXI'][1]['player']['pos'],
            "Player 3 Name": lineups_obj['response'][0]['startXI'][2]['player']['name'],
            "Player 3 Pos": lineups_obj['response'][0]['startXI'][2]['player']['pos'],
            "Player 4 Name": lineups_obj['response'][0]['startXI'][3]['player']['name'],
            "Player 4 Pos": lineups_obj['response'][0]['startXI'][3]['player']['pos'],
            "Player 5 Name": lineups_obj['response'][0]['startXI'][4]['player']['name'],
            "Player 5 Pos": lineups_obj['response'][0]['startXI'][4]['player']['pos'],
            "Player 6 Name": lineups_obj['response'][0]['startXI'][5]['player']['name'],
            "Player 6 Pos": lineups_obj['response'][0]['startXI'][5]['player']['pos'],
            "Player 7 Name": lineups_obj['response'][0]['startXI'][6]['player']['name'],
            "Player 7 Pos": lineups_obj['response'][0]['startXI'][6]['player']['pos'],
            "Player 8 Name": lineups_obj['response'][0]['startXI'][7]['player']['name'],
            "Player 8 Pos": lineups_obj['response'][0]['startXI'][7]['player']['pos'],
            "Player 9 Name": lineups_obj['response'][0]['startXI'][8]['player']['name'],
            "Player 9 Pos": lineups_obj['response'][0]['startXI'][8]['player']['pos'],
            "Player 10 Name": lineups_obj['response'][0]['startXI'][9]['player']['name'],
            "Player 10 Pos": lineups_obj['response'][0]['startXI'][9]['player']['pos'],
            "Player 11 Name": lineups_obj['response'][0]['startXI'][10]['player']['name'],
            "Player 11 Pos": lineups_obj['response'][0]['startXI'][10]['player']['pos']
        }

        away_info = {
            "Fixture ID": lineups_obj['parameters']['fixture'],
            "Team ID": lineups_obj['response'][1]['team']['id'],
            "Formation": lineups_obj['response'][1]['formation'],
            "Player 1 Name": lineups_obj['response'][1]['startXI'][0]['player']['name'],
            "Player 1 Pos": lineups_obj['response'][1]['startXI'][0]['player']['pos'],
            "Player 2 Name": lineups_obj['response'][1]['startXI'][1]['player']['name'],
            "Player 2 Pos": lineups_obj['response'][1]['startXI'][1]['player']['pos'],
            "Player 3 Name": lineups_obj['response'][1]['startXI'][2]['player']['name'],
            "Player 3 Pos": lineups_obj['response'][1]['startXI'][2]['player']['pos'],
            "Player 4 Name": lineups_obj['response'][1]['startXI'][3]['player']['name'],
            "Player 4 Pos": lineups_obj['response'][1]['startXI'][3]['player']['pos'],
            "Player 5 Name": lineups_obj['response'][1]['startXI'][4]['player']['name'],
            "Player 5 Pos": lineups_obj['response'][1]['startXI'][4]['player']['pos'],
            "Player 6 Name": lineups_obj['response'][1]['startXI'][5]['player']['name'],
            "Player 6 Pos": lineups_obj['response'][1]['startXI'][5]['player']['pos'],
            "Player 7 Name": lineups_obj['response'][1]['startXI'][6]['player']['name'],
            "Player 7 Pos": lineups_obj['response'][1]['startXI'][6]['player']['pos'],
            "Player 8 Name": lineups_obj['response'][1]['startXI'][7]['player']['name'],
            "Player 8 Pos": lineups_obj['response'][1]['startXI'][7]['player']['pos'],
            "Player 9 Name": lineups_obj['response'][1]['startXI'][8]['player']['name'],
            "Player 9 Pos": lineups_obj['response'][1]['startXI'][8]['player']['pos'],
            "Player 10 Name": lineups_obj['response'][1]['startXI'][9]['player']['name'],
            "Player 10 Pos": lineups_obj['response'][1]['startXI'][9]['player']['pos'],
            "Player 11 Name": lineups_obj['response'][1]['startXI'][10]['player']['name'],
            "Player 11 Pos": lineups_obj['response'][1]['startXI'][10]['player']['pos']
        }
            

        fix_lineups.append(home_info)
        fix_lineups.append(away_info)

    df = pd.DataFrame(fix_lineups)
    df.to_csv(filename, index=False)

    return df


def data_to_csv(data, filename="football_fixtures.csv"):
    """Compile data into CSV file."""

    if not isinstance(data, pd.DataFrame):
        print("Invalid or empty data")
        return None

    stats = fetch_fixture_stats(data)
    lineups = fetch_fixture_lineups(data)

    data['Fixture ID'] = data['Fixture ID'].astype(int)
    data['Team ID'] = data['Team ID'].astype(int)
    stats['Fixture ID'] = stats['Fixture ID'].astype(int)
    stats['Team ID'] = stats['Team ID'].astype(int)
    lineups['Fixture ID'] = lineups['Fixture ID'].astype(int)
    lineups['Team ID'] = lineups['Team ID'].astype(int)

    merge_stats = pd.merge(data, stats, on=['Fixture ID', 'Team ID'], how='left')
    merge_final = pd.merge(merge_stats, lineups, on=['Fixture ID', 'Team ID'], how='left')
    merge_final = merge_final.loc[:, ~merge_final.columns.duplicated()]

    df.to_csv(filename, index=False)
    return filename  # Return the filename for upload
    

