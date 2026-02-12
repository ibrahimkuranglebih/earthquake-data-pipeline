from datetime import datetime

def transform_data(raw_data: dict):

    event_id = raw_data['id']
    props = raw_data['properties']
    geom = raw_data['geometry']

    magnitude = props['mag']
    mag_type = props['magType']
    place = props["place"]
    tsunami = props["tsunami"]
    sig = props["sig"]
    status = props["status"]
    nst = props.get("nst")
    gap = props.get("gap")
    rms = props.get("rms")

    longitude = geom["coordinates"][0]
    latitude = geom["coordinates"][1]
    depth = geom["coordinates"][2]

    timestamp = datetime.utcfromtimestamp(props["time"] / 1000)
    inserted_at = datetime.utcnow()

    time_dim = {
        "time_key": int(timestamp.strftime("%Y%m%d")),
        "full_timestamp": timestamp,
        "year": timestamp.year,
        "month": timestamp.month,
        "day": timestamp.day,
        "hour": timestamp.hour,
        "inserted_at": inserted_at
    }

    dim_location = {
        "place": place,
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth,
        "inserted_at": inserted_at
    }

    dim_magnitude = {
        "mag": magnitude,
        "mag_type": mag_type,
        "inserted_at": inserted_at
    }

    dim_status = {
        "status": status,
        "inserted_at": inserted_at
    }

    fact = {
        "event_id": event_id,
        "time_key": time_dim["time_key"],
        "tsunami": tsunami,
        "sig": sig,
        "nst": nst,
        "gap": gap,
        "rms": rms,
        "inserted_at": inserted_at
    }

    return {
        "fact": fact,
        "dim_time": time_dim,
        "dim_location": dim_location,
        "dim_magnitude": dim_magnitude,
        "dim_status": dim_status,
    }
