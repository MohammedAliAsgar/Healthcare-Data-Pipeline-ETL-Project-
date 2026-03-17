CREATE TABLE IF NOT EXISTS dim_date (
  date_sk BIGINT PRIMARY KEY,
  date DATE,
  year INT,
  month INT,
  day INT
);
