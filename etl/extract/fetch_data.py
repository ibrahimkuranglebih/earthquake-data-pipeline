import os
import time
import requests

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

def fetch_earthquake_data():
    try : 
        params = {
            "format" : "geojson",
            "orderby" : "time"
        }
        response = requests.get(BASE_URL, params=params,timeout=30)
        response.raise_for_status()
        return response.json()
    
    except Exception as e:
        print(f"Error fetching earthquake data: {e}")
        return None