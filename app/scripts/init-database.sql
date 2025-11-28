-- Прогоны сервиса (сессии)
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

-- Производители субстанций с версионированием
CREATE TABLE IF NOT EXISTS substance_manufacturers (
    id SERIAL PRIMARY KEY,
    substance_name VARCHAR(500) NOT NULL,
    manufacturers JSONB NOT NULL,
    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1
);

-- Журнал изменений производителей
CREATE TABLE IF NOT EXISTS substance_manufacturer_changes (
    id SERIAL PRIMARY KEY,
    substance_name VARCHAR(500) NOT NULL,
    old_manufacturers JSONB,
    new_manufacturers JSONB,
    change_type VARCHAR(50), -- 'added', 'removed', 'modified'
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id INTEGER REFERENCES analysis_sessions(id)
);

-- Препараты с версионированием
CREATE TABLE IF NOT EXISTS substance_consumers (
    id SERIAL PRIMARY KEY,
    substance_name VARCHAR(500) NOT NULL,
    preparation_trade_name VARCHAR(500),
    preparation_inn_name VARCHAR(500),
    preparation_manufacturer VARCHAR(500),
    preparation_country VARCHAR(100),
    registration_number VARCHAR(100),
    registration_date VARCHAR(50),
    release_forms TEXT,
    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1,
    UNIQUE(substance_name, preparation_trade_name, preparation_manufacturer, registration_number)
);

-- Журнал изменений препаратов
CREATE TABLE IF NOT EXISTS substance_consumer_changes (
    id SERIAL PRIMARY KEY,
    substance_name VARCHAR(500) NOT NULL,
    preparation_trade_name VARCHAR(500),
    preparation_inn_name VARCHAR(500),
    preparation_manufacturer VARCHAR(500),
    preparation_country VARCHAR(100),
    registration_number VARCHAR(100),
    change_type VARCHAR(50), -- 'added', 'removed', 'modified'
    changed_fields JSONB, -- Какие поля изменились
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id INTEGER REFERENCES analysis_sessions(id)
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_substance_manufacturers_name ON substance_manufacturers(substance_name);
CREATE INDEX IF NOT EXISTS idx_substance_manufacturers_current ON substance_manufacturers(is_current);
CREATE INDEX IF NOT EXISTS idx_substance_consumers_current ON substance_consumers(is_current);
CREATE INDEX IF NOT EXISTS idx_substance_consumers_composite ON substance_consumers(substance_name, preparation_trade_name, registration_number);