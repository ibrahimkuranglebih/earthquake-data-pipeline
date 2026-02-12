import os
import time
import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

params = {
    "format": "geojson",
    "starttime": "2026-01-01",
    "endtime": "2026-02-01",
    "minmagnitude": 4
}

response = requests.get(BASE_URL, params=params)
data = response.json()
print(data)