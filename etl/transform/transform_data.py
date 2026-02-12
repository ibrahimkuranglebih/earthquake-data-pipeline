from datetime import datetime


def transform_data(ti):

    raw_data = ti.xcom_pull(task_ids="extract_data_earthquake")

    if not raw_data:
        return []

    features = raw_data["features"]
    transformed_records = []

    for feature in features:

        event_id = feature["id"]
        props = feature['properties']
        geom = feature['geometry']

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

        transformed_records.append({
            "event_id": event_id,
            "time_key": int(timestamp.strftime("%Y%m%d")),
            "full_timestamp": timestamp,
            "year": timestamp.year,
            "month": timestamp.month,
            "day": timestamp.day,
            "hour": timestamp.hour,
            "place": place,
            "latitude": latitude,
            "longitude": longitude,
            "depth_km": depth,
            "mag": magnitude,
            "mag_type": mag_type,
            "status": status,
            "tsunami": tsunami,
            "sig": sig,
            "nst": nst,
            "gap": gap,
            "rms": rms,
            "inserted_at": inserted_at
        })

    return transformed_records
