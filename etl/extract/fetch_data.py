import os
import time
import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

response = requests.get(BASE_URL, params=params)
data = response.json()
print(data)