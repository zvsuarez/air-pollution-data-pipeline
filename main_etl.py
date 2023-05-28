import requests
import pandas as pd
from datetime import datetime
import credentials as creds
import uuid
import os
import s3fs

def airpol_etl():
    # OpenWeather Air Pollution API key
    api = creds.API_KEY

    # Reverse geocode with SimpleMaps database
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(current_dir, 'coordinates.csv')
    coord = pd.read_csv(csv_file_path).convert_dtypes()
    #loc_test = coord.head() #for test

    # Loop through the csv and plug in coordinates to API request and filter data
    data = []
    
    for row in coord.itertuples(index=False):
        col1 = row.lat
        col2 = row.lng
        req = requests.get(f'http://api.openweathermap.org/data/2.5/air_pollution?lat={col1}&lon={col2}&appid={api}')
        
        try:
            content = req.json()
        except:
            continue
        
        filtered_req = {
            "id": str(uuid.uuid4())[:8],
            "country": row.country,
            "city": row.city_ascii,
            "latitude": content['coord']['lat'],
            "longitude": content['coord']['lon'],
            "population": row.population,
            "date_time": datetime.fromtimestamp(content['list'][0]['dt']).strftime("%Y-%m-%d %H:%M:%S"),
            "year": datetime.fromtimestamp(content['list'][0]['dt']).year,
            "month": datetime.fromtimestamp(content['list'][0]['dt']).month,
            "day": datetime.fromtimestamp(content['list'][0]['dt']).day,
            "weekday": datetime.fromtimestamp(content['list'][0]['dt']).weekday(),
            "air_index": content['list'][0]['main']['aqi'],
            "o3": content['list'][0]['components']['o3'],
            "nh3": content['list'][0]['components']['nh3'],
            "co": content['list'][0]['components']['co'],
            "no": content['list'][0]['components']['no'],
            "no2": content['list'][0]['components']['no2'],
            "so2": content['list'][0]['components']['so2'],
            "pm2_5": content['list'][0]['components']['pm2_5'],
            "pm10": content['list'][0]['components']['pm10']
        }
        data.append(filtered_req)

    # map values to proper description
    aqi_map = {1:'Good', 2:'Fair', 3:'Moderate', 4:'Poor', 5:'Very Poor'}
    weekday_map = {0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}
    month_map = {1: "January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June",
        7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}

    df = pd.DataFrame(data)
    df.insert(11, 'air_quality',df['air_index'].map(aqi_map))
    df['month'].replace(month_map, inplace=True)
    df['weekday'].replace(weekday_map, inplace=True)
    df.to_csv(f's3://{creds.BUCKET}/data/air_pollution_data_{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.csv', index=False) #--change to S3 bucket path