from __future__ import annotations

import argparse
from _common import build_spark, add_common_args, set_log_level

def parse_args():
    p = argparse.ArgumentParser(description="Ingest EHR raw CSV to staging Parquet.")
    p.add_argument("--input", required=True, help="Input folder for raw EHR CSV files")
    p.add_argument("--output", required=True, help="Output base path for staging parquet")
    add_common_args(p)
    return p.parse_args()

def main():
    args = parse_args()
    spark = build_spark("ingest_ehr")
    set_log_level(spark, args.log_level)

    patients = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{args.input}/patients.csv")
    )
    encounters = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{args.input}/encounters.csv")
    )

    patients.write.mode("overwrite").parquet(f"{args.output}/patients")
    encounters.write.mode("overwrite").parquet(f"{args.output}/encounters")

    spark.stop()

if __name__ == "__main__":
    main()
