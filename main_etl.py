import json
import requests
import pandas as pd
from datetime import datetime
import air_pol_api

# OpenWeather Air Pollution API key
api = air_pol_api.API_KEY

# Reverse geocode with SimpleMaps database
coord = pd.read_csv('coordinates.csv').convert_dtypes()
loc_test = coord

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
        "date": datetime.fromtimestamp(content['list'][0]['dt']).strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": content['coord']['lat'],
        "longitude": content['coord']['lon'],
        "air_quality_index": content['list'][0]['main']['aqi'],
        "carbon_monoxide": content['list'][0]['components']['co'],
        "nitrogen_monoxide": content['list'][0]['components']['no'],
        "nitrogen_dioxide": content['list'][0]['components']['no2'],
        "ozone": content['list'][0]['components']['o3'],
        "sulfur_dioxide": content['list'][0]['components']['so2'],
        "fine_particles": content['list'][0]['components']['pm2_5'],
        "coarse_particulates": content['list'][0]['components']['pm10'],
        "ammonia": content['list'][0]['components']['nh3']
    }
    data.append(filtered_req)

df = pd.DataFrame(data)
df.to_csv('air_pollution_data.csv')