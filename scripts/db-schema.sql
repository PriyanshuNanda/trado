-- Create extension for TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Topics table to store unique topics
CREATE TABLE IF NOT EXISTS topics (
  topic_id SERIAL PRIMARY KEY,
  topic_name TEXT NOT NULL UNIQUE,
  index_name TEXT,
  type TEXT,
  strike NUMERIC
);

-- Create index on topic_name for faster lookups
CREATE INDEX IF NOT EXISTS idx_topics_topic_name ON topics(topic_name);

-- LTP data table to store market data
CREATE TABLE IF NOT EXISTS ltp_data (
  id SERIAL,
  topic_id INTEGER NOT NULL REFERENCES topics(topic_id),
  ltp NUMERIC(10,2) NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, received_at)
);

-- Create index on topic_id and received_at
CREATE INDEX IF NOT EXISTS idx_ltp_data_topic_time ON ltp_data(topic_id, received_at DESC);

-- Convert ltp_data to a TimescaleDB hypertable
SELECT create_hypertable('ltp_data', 'received_at', if_not_exists => TRUE); 