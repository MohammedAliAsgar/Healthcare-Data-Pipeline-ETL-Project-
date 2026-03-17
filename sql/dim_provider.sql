CREATE TABLE IF NOT EXISTS dim_provider (
  provider_sk BIGINT PRIMARY KEY,
  provider_id VARCHAR(32) NOT NULL
);
