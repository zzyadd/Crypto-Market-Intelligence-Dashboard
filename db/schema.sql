-- Create the PostgreSQL table to store crypto streaming data
CREATE TABLE IF NOT EXISTS prices (
    id TEXT,
    symbol TEXT,
    price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    timestamp TIMESTAMP
);
