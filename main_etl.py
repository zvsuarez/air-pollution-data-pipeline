import requests
import pandas as pd
from datetime import datetime
import credentials

def airpol_etl():
    # OpenWeather Air Pollution API key
    api = credentials.API_KEY

    # Reverse geocode with SimpleMaps database
    coord = pd.read_csv('coordinates.csv').convert_dtypes()
    loc_test = coord    #add .heads() for test

    # Loop through the csv and plug in coordinates to API request and filter data
    data = []
    
    for row in loc_test.itertuples(index=False):
        col1 = row.lat
        col2 = row.lng
        req = requests.get(f'http://api.openweathermap.org/data/2.5/air_pollution?lat={col1}&lon={col2}&appid={api}')
        
        try:
            content = req.json()
        except:
            continue
        
        filtered_req = {
            "country": row.country,
            "city": row.city_ascii,
            "latitude": content['coord']['lat'],
            "longitude": content['coord']['lon'],
            "date": datetime.fromtimestamp(content['list'][0]['dt']).strftime("%Y-%m-%d %H:%M:%S"),
            "year": datetime.fromtimestamp(content['list'][0]['dt']).year,
            "month": datetime.fromtimestamp(content['list'][0]['dt']).month,
            "day": datetime.fromtimestamp(content['list'][0]['dt']).day,
            "weekday": datetime.fromtimestamp(content['list'][0]['dt']).weekday(),
            "air_index": content['list'][0]['main']['aqi'],
            "ozone": content['list'][0]['components']['o3'],
            "ammonia": content['list'][0]['components']['nh3'],
            "carbon_monoxide": content['list'][0]['components']['co'],
            "nitrogen_monoxide": content['list'][0]['components']['no'],
            "nitrogen_dioxide": content['list'][0]['components']['no2'],
            "sulfur_dioxide": content['list'][0]['components']['so2'],
            "fine_particles": content['list'][0]['components']['pm2_5'],
            "coarse_particulate": content['list'][0]['components']['pm10']
        }
        data.append(filtered_req)

    # map values to proper description
    aqi_map = {1:'Good', 2:'Fair', 3:'Moderate', 4:'Poor', 5:'Very Poor'}
    weekday_map = {0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 5:'Saturday', 6:'Sunday'}
    month_map = {1: "January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June",
        7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}

    df = pd.DataFrame(data)
    df.insert(9, 'air_quality',df['air_index'].map(aqi_map))
    df['month'].replace(month_map, inplace=True)
    df['weekday'].replace(weekday_map, inplace=True)
    df.to_csv(f's3://openweather-zvsuarez/data/air_pollution_data_{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}.csv') #--change to S3 bucket path

#air_pol_etl()