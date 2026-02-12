import os
import psycopg2

def load_to_postgres(transformed_data):

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT")
    )

    try:
        cur = conn.cursor()

        # ============================================
        # DIM TIME (UPSERT SAFE)
        # ============================================
        cur.execute("""
            INSERT INTO warehouse.dim_time
            (time_key, full_timestamp, year, month, day, hour)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (time_key) DO NOTHING
        """, (
            transformed_data["dim_time"]["time_key"],
            transformed_data["dim_time"]["full_timestamp"],
            transformed_data["dim_time"]["year"],
            transformed_data["dim_time"]["month"],
            transformed_data["dim_time"]["day"],
            transformed_data["dim_time"]["hour"],
        ))

        # ============================================
        # DIM LOCATION (UPSERT + RETURN KEY)
        # ============================================
        cur.execute("""
            INSERT INTO warehouse.dim_location
            (place, latitude, longitude, depth_km)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (place, latitude, longitude, depth_km)
            DO UPDATE SET place = EXCLUDED.place
            RETURNING location_key
        """, (
            transformed_data["dim_location"]["place"],
            transformed_data["dim_location"]["latitude"],
            transformed_data["dim_location"]["longitude"],
            transformed_data["dim_location"]["depth_km"],
        ))
        location_key = cur.fetchone()[0]

        # ============================================
        # DIM MAGNITUDE
        # ============================================
        cur.execute("""
            INSERT INTO warehouse.dim_magnitude
            (mag, mag_type)
            VALUES (%s,%s)
            ON CONFLICT (mag, mag_type)
            DO UPDATE SET mag = EXCLUDED.mag
            RETURNING magnitude_key
        """, (
            transformed_data["dim_magnitude"]["mag"],
            transformed_data["dim_magnitude"]["mag_type"],
        ))
        magnitude_key = cur.fetchone()[0]

        # ============================================
        # DIM STATUS
        # ============================================
        cur.execute("""
            INSERT INTO warehouse.dim_status
            (status)
            VALUES (%s)
            ON CONFLICT (status)
            DO UPDATE SET status = EXCLUDED.status
            RETURNING status_key
        """, (
            transformed_data["dim_status"]["status"],
        ))
        status_key = cur.fetchone()[0]

        # ============================================
        # FACT TABLE
        # ============================================
        fact = transformed_data["fact"]

        cur.execute("""
            INSERT INTO warehouse.fact_earthquake
            (event_id, time_key, location_key, magnitude_key, status_key,
             tsunami, sig, nst, gap, rms)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            fact["event_id"],
            fact["time_key"],
            location_key,
            magnitude_key,
            status_key,
            fact["tsunami"],
            fact["sig"],
            fact["nst"],
            fact["gap"],
            fact["rms"],
        ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cur.close()
        conn.close()
