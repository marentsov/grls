-- scripts/init-database.sql
CREATE TABLE IF NOT EXISTS analysis_sessions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    source_file VARCHAR(500),
    total_records INTEGER,
    substances_found INTEGER,
    preparations_found INTEGER,
    consumers_found INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS substance_manufacturers (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES analysis_sessions(id),
    substance_name VARCHAR(500),
    manufacturers JSONB
);

CREATE TABLE IF NOT EXISTS substance_consumers (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES analysis_sessions(id),
    substance_name VARCHAR(500),
    preparation_trade_name VARCHAR(500),
    preparation_inn_name VARCHAR(500),
    preparation_manufacturer VARCHAR(500),
    preparation_country VARCHAR(100),
    registration_number VARCHAR(100),
    registration_date VARCHAR(50),
    release_forms TEXT
);