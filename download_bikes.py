import requests
import zipfile
import json
import io
import os
import pandas as pd
from datetime import datetime
import warnings

def download_trips(date):
    url = f'https://s3.amazonaws.com/tripdata/{date}-citibike-tripdata.csv.zip'
    alternative_url = f'https://s3.amazonaws.com/tripdata/{date}-citbike-tripdata.csv.zip'
    destination_path = '/home/vagrant/project/InputData/TempBikes'
    response = requests.get(url)

    if response.status_code != 200:
        response = requests.get(alternative_url)
        if response.status_code != 200:
            print("Both URLs are unavailable.")
            return

    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    extracted_file = zip_file.namelist()[0] 
    extracted_filepath = os.path.join(destination_path, f'bike_trips_{date}.csv')
    zip_file.extract(extracted_file, path=destination_path)
    os.rename(os.path.join(destination_path, extracted_file), extracted_filepath)
    zip_file.close()
    return extracted_filepath

def transform_datetime(original_datetime):
    year = original_datetime.year
    month = original_datetime.month
    day = original_datetime.day
    hour = original_datetime.hour
    minute = original_datetime.minute
    second = original_datetime.second
    transformed_datetime = datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
    return transformed_datetime.strftime('%Y-%m-%d %H:%M:%S')

def rename_and_map_columns(bike_df):
    if 'ride_id' not in bike_df.columns:
        bike_df['ride_id'] = 'bla_id'

    good_trip_cols = ['ride_id','rideable_type','start_time','stop_time','start_station_name','start_station_id','end_station_name','end_station_id','start_lat','start_lng','end_lat','end_lng','member_casual']

    if 'starttime' in bike_df.columns:
        bike_df.rename(columns={'starttime':'start_time'}, inplace=True)
        bike_df['start_time'] = pd.to_datetime(bike_df['start_time'])
        bike_df['start_time'] = bike_df['start_time'].apply(transform_datetime)

    if 'started_at' in bike_df.columns:
        bike_df.rename(columns={'started_at':'start_time'}, inplace=True)
        bike_df['start_time'] = pd.to_datetime(bike_df['start_time'])
        bike_df['start_time'] = bike_df['start_time'].apply(transform_datetime)
        
    if 'stoptime' in bike_df.columns:
        bike_df.rename(columns={'stoptime':'stop_time'}, inplace=True)
        bike_df['stop_time'] = pd.to_datetime(bike_df['stop_time'])
        bike_df['stop_time'] = bike_df['stop_time'].apply(transform_datetime)

    if 'ended_at' in bike_df.columns:
        bike_df.rename(columns={'ended_at':'stop_time'}, inplace=True)
        bike_df['stop_time'] = pd.to_datetime(bike_df['stop_time'])
        bike_df['stop_time'] = bike_df['stop_time'].apply(transform_datetime)
    
    if 'start station id' in bike_df.columns:
        bike_df.rename(columns={'start station id':'start_station_id'}, inplace=True)
    if 'start station name' in bike_df.columns:
        bike_df.rename(columns={'start station name':'start_station_name'}, inplace=True)
    if 'start station latitude' in bike_df.columns:
        bike_df.rename(columns={'start station latitude':'start_lat'}, inplace=True)
    if 'start station longitude' in bike_df.columns:
        bike_df.rename(columns={'start station longitude':'start_lng'}, inplace=True)
    if 'start_station_id' in bike_df.columns:
        bike_df['start_station_id'] = pd.to_numeric(bike_df['start_station_id'], errors='coerce')
    if 'end_station_id' in bike_df.columns:
        bike_df['end_station_id'] = pd.to_numeric(bike_df['end_station_id'], errors='coerce')
    if 'end station id' in bike_df.columns:
        bike_df.rename(columns={'end station id':'end_station_id'}, inplace=True)
    if 'end station name' in bike_df.columns:
        bike_df.rename(columns={'end station name':'end_station_name'}, inplace=True)
    if 'end station latitude' in bike_df.columns:
        bike_df.rename(columns={'end station latitude':'end_lat'}, inplace=True)
    if 'end station longitude' in bike_df.columns:
        bike_df.rename(columns={'end station longitude':'end_lng'}, inplace=True)
    if 'usertype' in bike_df.columns:
        bike_df.rename(columns={'usertype':'member_casual'}, inplace=True)
        bike_df['member_casual'] = bike_df['member_casual'].map({'Subscriber':'member', 'Customer':'casual'})
    if 'rideable_type' not in bike_df.columns:
        bike_df['rideable_type'] = 'docked_bike'
    bike_df = bike_df.dropna(subset=['start_station_id', 'end_station_id'])
    bike_df = bike_df[good_trip_cols]
    return bike_df



def main():
    warnings.simplefilter('ignore')
    dfs = []

    for year in [2021, 2022]:
        for month in range(1, 13):
            date = f'{year}{month:02d}'
            print(f"Processing data for {date}...")
        
            file_path = download_trips(date)
            if file_path:
                df = pd.read_csv(file_path)
                updated_df = rename_and_map_columns(df)
                sample_df = updated_df.sample(frac=0.1, random_state=42)
                dfs.append(sample_df)
    
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv('/home/vagrant/project/InputData/Bikes/bike_trips.csv', index=False)
    
    print("Data processing completed.")
if __name__ == '__main__':
    main()