import json
import requests
import pandas as pd
from datetime import datetime
import air_pol_api

# OpenWeather Air Pollution API key
api = air_pol_api.API_KEY

# Reverse geocode with SimpleMaps database
coord = pd.read_csv('coordinates.csv').convert_dtypes()
loc_test = coord.head()

# Response
for row in loc_test.itertuples(index=False):
    col1 = row.lat
    col2 = row.lng
    #print(col1, col2)
    req = requests.get('')
    try:
        content = req.json()

    except:
        print('Response failed...')



# current
# http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API key}