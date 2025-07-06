-- schema.sql (recommended)
CREATE TABLE IF NOT EXISTS prices (
    id TEXT PRIMARY KEY,
    symbol TEXT,
    price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    timestamp TIMESTAMP
);
