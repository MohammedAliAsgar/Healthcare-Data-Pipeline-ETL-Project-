CREATE TABLE IF NOT EXISTS dim_member (
  member_sk BIGINT PRIMARY KEY,
  member_key VARCHAR(64) NOT NULL,
  gender VARCHAR(10)
);
