-- ============================================
-- CREATE SCHEMA
-- ============================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================
-- DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_key INT PRIMARY KEY,
    full_timestamp TIMESTAMP NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_key SERIAL PRIMARY KEY,
    place TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_location UNIQUE(place, latitude, longitude, depth_km)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_magnitude (
    magnitude_key SERIAL PRIMARY KEY,
    mag DOUBLE PRECISION,
    mag_type TEXT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_mag UNIQUE(mag, mag_type)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_status (
    status_key SERIAL PRIMARY KEY,
    status TEXT NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_status UNIQUE(status)
);

-- ============================================
-- FACT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS warehouse.fact_earthquake (
    event_id TEXT PRIMARY KEY,
    time_key INT NOT NULL REFERENCES warehouse.dim_time(time_key),
    location_key INT NOT NULL REFERENCES warehouse.dim_location(location_key),
    magnitude_key INT NOT NULL REFERENCES warehouse.dim_magnitude(magnitude_key),
    status_key INT NOT NULL REFERENCES warehouse.dim_status(status_key),
    tsunami INT,
    sig INT,
    nst INT,
    gap DOUBLE PRECISION,
    rms DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEX FOR ANALYTICS PERFORMANCE
-- ============================================

CREATE INDEX IF NOT EXISTS idx_fact_time_key
ON warehouse.fact_earthquake(time_key);

CREATE INDEX IF NOT EXISTS idx_fact_location_key
ON warehouse.fact_earthquake(location_key);
