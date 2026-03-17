from __future__ import annotations

import argparse
from pyspark.sql.functions import col
from _common import build_spark, add_common_args, set_log_level

def parse_args():
    p = argparse.ArgumentParser(description="Ingest claims raw CSV to staging Parquet.")
    p.add_argument("--input", required=True, help="Input folder for raw claims CSV files")
    p.add_argument("--output", required=True, help="Output path for staging parquet")
    add_common_args(p)
    return p.parse_args()

def main():
    args = parse_args()
    spark = build_spark("ingest_claims")
    set_log_level(spark, args.log_level)

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    # Minimal typing/cleanup: ensure expected columns exist
    expected = ["claim_id","member_id","provider_id","service_date","paid_amount","allowed_amount","icd10_code","cpt_code"]
    for c in expected:
        if c not in df.columns:
            df = df.withColumn(c, col(c))

    df.write.mode("overwrite").parquet(args.output)
    spark.stop()

if __name__ == "__main__":
    main()
