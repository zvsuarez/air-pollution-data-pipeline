import requests
import pandas as pd
from datetime import datetime
import credentials as crd
import uuid
import os
import s3fs

def airpol_etl():
    # Air Pollution API key
    api = crd.API_KEY
    
    # Reverse geocode with SimpleMaps dataset
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(current_dir, 'coordinates.csv')
    coordinates_df = pd.read_csv(csv_file_path).convert_dtypes()

    # test call // disable when in prod
    #loc_test = coord.head()
    
    # Loop through the csv and plug in coordinates to API request and filter data
    data = []
    for row in coordinates_df.itertuples(index=False):
        col1 = row.lat
        col2 = row.lng
        # change this epoch time or refactor with s3 checking operators for catchup
        start = 1680350400 # April 1, 2023 12:00 P.M GMT/UTC
        end = 1685556000 # May 31, 2023 12:00 P.M GMT/UTC
        
        try:
            req = requests.get(f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={col1}&lon={col2}&start={start}&end={end}&appid={api}')
            content = req.json()
        except:
            continue

        for key in content['list']:
            filtered_req = {
                'id': str(uuid.uuid4())[:8],
                'country': row.country,
                'city': row.city_ascii,
                'latitude': col1,
                'longitude': col2,
                'population': row.population,
                'date_time': datetime.utcfromtimestamp(key['dt']).strftime("%Y-%m-%d %H:%M:%S"),
                'year': datetime.utcfromtimestamp(key['dt']).year,
                'month': datetime.utcfromtimestamp(key['dt']).month,
                'day': datetime.utcfromtimestamp(key['dt']).day,
                'weekday': datetime.utcfromtimestamp(key['dt']).weekday(),
                'air_index': key['main']['aqi'],
                'o3': key['components']['o3'],
                'nh3': key['components']['nh3'],
                'co': key['components']['co'],
                'no': key['components']['no'],
                'no2': key['components']['no2'],
                'so2': key['components']['so2'],
                'pm2_5': key['components']['pm2_5'],
                'pm10': key['components']['pm10']
                }
            data.append(filtered_req)

    aqi_map = {1:'Good', 2:'Fair', 3:'Moderate', 4:'Poor', 5:'Very Poor'}
    weekday_map = {0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}
    month_map = {1:'January', 2:'February', 3:'March', 4:'April', 5:'May', 6:'June', 7:'July', 8:'August', 9:'September', 10:'October', 11:'November', 12:'December'}
    
    df = pd.DataFrame(data)
    df.insert(11, 'air_quality',df['air_index'].map(aqi_map))
    df['month'].replace(month_map, inplace=True)
    df['weekday'].replace(weekday_map, inplace=True)
    name_format = f'{datetime.utcfromtimestamp(start).month}-{datetime.utcfromtimestamp(start).day}-{datetime.utcfromtimestamp(start).year}_{datetime.utcfromtimestamp(end).month}-{datetime.utcfromtimestamp(end).day}-{datetime.utcfromtimestamp(end).year}'
    df.to_csv(f's3://{crd.BUCKET}/data/airpol_{name_format}.csv', index=False) #--change to S3 bucket path
