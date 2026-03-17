"""
Demo Airflow DAG (works in Composer/Airflow with SparkSubmitOperator if configured).
This is a template: adjust connection IDs and Spark cluster settings to your environment.
"""
from datetime import datetime
from airflow import DAG

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except Exception:
    SparkSubmitOperator = None

with DAG(
    dag_id="healthcare_claims_ehr_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["healthcare","pyspark","etl"],
) as dag:
    if SparkSubmitOperator:
        ingest_claims = SparkSubmitOperator(
            task_id="ingest_claims",
            application="spark_jobs/ingest_claims.py",
            application_args=["--input","data/raw/claims","--output","data/staging/claims"],
        )

        ingest_ehr = SparkSubmitOperator(
            task_id="ingest_ehr",
            application="spark_jobs/ingest_ehr.py",
            application_args=["--input","data/raw/ehr","--output","data/staging/ehr"],
        )

        dq = SparkSubmitOperator(
            task_id="data_quality",
            application="spark_jobs/data_quality.py",
            application_args=[
                "--claims","data/staging/claims",
                "--ehr","data/staging/ehr",
                "--curated","data/curated",
                "--quarantine","data/quarantine",
                "--config","config.json"
            ],
        )

        mask = SparkSubmitOperator(
            task_id="phi_masking",
            application="spark_jobs/phi_masking.py",
            application_args=["--input","data/curated","--output","data/curated_safe","--salt","change_me"],
        )

        model = SparkSubmitOperator(
            task_id="integrate_and_model",
            application="spark_jobs/integrate_and_model.py",
            application_args=[
                "--claims","data/curated_safe/claims_clean",
                "--ehr","data/curated_safe",
                "--output","data/curated_safe/model"
            ],
        )

        ingest_claims >> ingest_ehr >> dq >> mask >> model
