# Healthcare Claims & EHR Data Integration Pipeline (PySpark)

This project demonstrates an end-to-end, HIPAA-aware integration pipeline that ingests **claims** + **EHR** data,
performs **data quality checks**, applies **PHI masking**, builds **fact/dimension** models, and (optionally) loads
to a warehouse via JDBC.

## Architecture

Raw Zone (`data/raw`) → Staging (`data/staging`) → Curated (`data/curated`)  
Bad/failed records → Quarantine (`data/quarantine`)

Jobs:
1. `ingest_claims.py` – read claims raw files → staging parquet
2. `ingest_ehr.py` – read EHR raw files → staging parquet
3. `data_quality.py` – validate, dedupe, standardize → curated clean + quarantine
4. `phi_masking.py` – hash/mask PHI fields → curated safe zone
5. `integrate_and_model.py` – integrate claims + EHR; build dims + facts

## Quickstart (local)

### 1) Create a virtualenv
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run with Spark
Make sure you have Spark installed (or use Databricks). Then run:
```bash
spark-submit spark_jobs/ingest_claims.py --input data/raw/claims --output data/staging/claims
spark-submit spark_jobs/ingest_ehr.py --input data/raw/ehr --output data/staging/ehr
spark-submit spark_jobs/data_quality.py --claims data/staging/claims --ehr data/staging/ehr --curated data/curated --quarantine data/quarantine
spark-submit spark_jobs/phi_masking.py --input data/curated --output data/curated_safe --salt "my_salt_value"
spark-submit spark_jobs/integrate_and_model.py --claims data/curated_safe/claims_clean --ehr data/curated_safe/ehr_clean --output data/curated_safe/model
```

### 3) Optional: Load to a warehouse
See `spark_jobs/load_to_warehouse.py` for a JDBC example.

## Data model

Curated tables:
- `dim_member`
- `dim_provider`
- `dim_diagnosis`
- `dim_date`
- `fact_claims`

See `sql/` for warehouse DDL templates.

## HIPAA note

This repo is a **demo**. Do not use real PHI. The masking step hashes PHI-like fields and drops raw identifiers.
In production, you would use KMS-managed encryption, IAM policies, audit logs, and stricter controls.

