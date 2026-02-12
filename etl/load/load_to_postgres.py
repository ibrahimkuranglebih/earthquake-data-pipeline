import os
import psycopg2
from psycopg2.extras import execute_values


def load_to_postgres(ti):

    records = ti.xcom_pull(task_ids="transform_data_earthquake")

    if not records:
        print("No transformed data found. Skipping load.")
        return

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT", 5432)
    )

    try:
        cur = conn.cursor()

        # ================= DIM TIME =================
        time_rows = list({
            (r["time_key"], r["full_timestamp"], r["year"],
             r["month"], r["day"], r["hour"])
            for r in records
        })

        execute_values(cur, """
            INSERT INTO warehouse.dim_time
            (time_key, full_timestamp, year, month, day, hour)
            VALUES %s
            ON CONFLICT (time_key) DO NOTHING
        """, time_rows)


        # ================= DIM LOCATION =================
        location_rows = list({
            (r["place"], r["latitude"], r["longitude"], r["depth_km"])
            for r in records
        })

        execute_values(cur, """
            INSERT INTO warehouse.dim_location
            (place, latitude, longitude, depth_km)
            VALUES %s
            ON CONFLICT (place, latitude, longitude, depth_km)
            DO NOTHING
        """, location_rows)


        # ================= DIM MAGNITUDE =================
        magnitude_rows = list({
            (r["mag"], r["mag_type"])
            for r in records
        })

        execute_values(cur, """
            INSERT INTO warehouse.dim_magnitude
            (mag, mag_type)
            VALUES %s
            ON CONFLICT (mag, mag_type)
            DO NOTHING
        """, magnitude_rows)


        # ================= DIM STATUS =================
        status_rows = list({
            (r["status"],)
            for r in records
        })

        execute_values(cur, """
            INSERT INTO warehouse.dim_status
            (status)
            VALUES %s
            ON CONFLICT (status)
            DO NOTHING
        """, status_rows)


        # ================= BUILD DIMENSION MAPPING =================

        # LOCATION MAP
        cur.execute("""
            SELECT location_key, place, latitude, longitude, depth_km
            FROM warehouse.dim_location
        """)
        location_map = {
            (row[1], row[2], row[3], row[4]): row[0]
            for row in cur.fetchall()
        }

        # MAGNITUDE MAP
        cur.execute("""
            SELECT magnitude_key, mag, mag_type
            FROM warehouse.dim_magnitude
        """)
        magnitude_map = {
            (row[1], row[2]): row[0]
            for row in cur.fetchall()
        }

        # STATUS MAP
        cur.execute("""
            SELECT status_key, status
            FROM warehouse.dim_status
        """)
        status_map = {
            row[1]: row[0]
            for row in cur.fetchall()
        }


        # ================= FACT TABLE =================
        fact_rows = []

        for r in records:
            location_key = location_map[
                (r["place"], r["latitude"], r["longitude"], r["depth_km"])
            ]

            magnitude_key = magnitude_map[
                (r["mag"], r["mag_type"])
            ]

            status_key = status_map[
                r["status"]
            ]

            fact_rows.append((
                r["event_id"],
                r["time_key"],
                location_key,
                magnitude_key,
                status_key,
                r["tsunami"],
                r["sig"],
                r["nst"],
                r["gap"],
                r["rms"]
            ))

        execute_values(cur, """
            INSERT INTO warehouse.fact_earthquake
            (event_id, time_key, location_key, magnitude_key, status_key,
             tsunami, sig, nst, gap, rms)
            VALUES %s
            ON CONFLICT (event_id) DO NOTHING
        """, fact_rows)


        conn.commit()
        print(f"{len(records)} records successfully loaded into warehouse.")

    except Exception as e:
        conn.rollback()
        print("Error loading to Postgres:", e)
        raise e

    finally:
        cur.close()
        conn.close()
