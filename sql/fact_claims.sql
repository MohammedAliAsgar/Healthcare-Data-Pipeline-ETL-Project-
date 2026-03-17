CREATE TABLE IF NOT EXISTS fact_claims (
  claim_id VARCHAR(32),
  member_sk BIGINT,
  provider_sk BIGINT,
  diagnosis_sk BIGINT,
  date_sk BIGINT,
  paid_amount DOUBLE PRECISION,
  allowed_amount DOUBLE PRECISION,
  final_procedure VARCHAR(32)
);
