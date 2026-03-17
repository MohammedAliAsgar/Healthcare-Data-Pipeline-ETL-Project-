from __future__ import annotations

import argparse, json
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, to_date, lit
from _common import build_spark, add_common_args, set_log_level

def parse_args():
    p = argparse.ArgumentParser(description="Data quality checks + standardization for claims and EHR.")
    p.add_argument("--claims", required=True, help="Staging path for claims parquet")
    p.add_argument("--ehr", required=True, help="Staging path for EHR parquet base (contains patients/encounters)")
    p.add_argument("--curated", required=True, help="Output base path for curated clean data")
    p.add_argument("--quarantine", required=True, help="Output base path for quarantined (bad) data")
    p.add_argument("--config", default="config.json", help="Config JSON with DQ rules")
    add_common_args(p)
    return p.parse_args()

def standardize_codes(df: DataFrame, cols: list[str]) -> DataFrame:
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, upper(trim(col(c))))
    return df

def dq_not_null(df: DataFrame, cols: list[str]) -> tuple[DataFrame, DataFrame]:
    bad = None
    for c in cols:
        cond = col(c).isNull() | (trim(col(c)) == "")
        bad = df.filter(cond) if bad is None else bad.unionByName(df.filter(cond))
    good = df.join(bad.select(*df.columns).distinct(), on=df.columns, how="left_anti") if bad is not None else df
    return good, (bad.distinct() if bad is not None else df.limit(0))

def dq_non_negative(df: DataFrame, cols: list[str]) -> tuple[DataFrame, DataFrame]:
    bad = None
    for c in cols:
        if c in df.columns:
            cond = col(c).cast("double") < 0
            bad = df.filter(cond) if bad is None else bad.unionByName(df.filter(cond))
    good = df.join(bad.select(*df.columns).distinct(), on=df.columns, how="left_anti") if bad is not None else df
    return good, (bad.distinct() if bad is not None else df.limit(0))

def main():
    args = parse_args()
    spark = build_spark("data_quality")
    set_log_level(spark, args.log_level)

    cfg = json.load(open(args.config, "r"))
    uppercase_cols = cfg.get("standardization", {}).get("uppercase_cols", [])

    claims = spark.read.parquet(args.claims)
    ehr_patients = spark.read.parquet(f"{args.ehr}/patients")
    ehr_encounters = spark.read.parquet(f"{args.ehr}/encounters")

    # Parse dates
    claims = claims.withColumn("service_date", to_date(col("service_date")))
    ehr_encounters = ehr_encounters.withColumn("encounter_date", to_date(col("encounter_date")))

    # Standardize codes
    claims = standardize_codes(claims, uppercase_cols)
    ehr_encounters = standardize_codes(ehr_encounters, uppercase_cols)

    # DQ rules
    c_rules = cfg["dq_rules"]["claims"]
    e_rules = cfg["dq_rules"]["ehr"]

    claims_good, claims_bad1 = dq_not_null(claims, c_rules["not_null"])
    claims_good, claims_bad2 = dq_non_negative(claims_good, c_rules["non_negative"])
    claims_bad = claims_bad1.unionByName(claims_bad2).withColumn("reason", lit("claims_dq_failed"))

    ehr_good, ehr_bad = dq_not_null(ehr_encounters, e_rules["not_null"])
    ehr_bad = ehr_bad.withColumn("reason", lit("ehr_dq_failed"))

    # Deduplicate
    claims_good = claims_good.dropDuplicates(["claim_id", "service_date"])
    ehr_good = ehr_good.dropDuplicates(["encounter_id"])

    # Write outputs
    claims_good.write.mode("overwrite").parquet(f"{args.curated}/claims_clean")
    ehr_patients.write.mode("overwrite").parquet(f"{args.curated}/patients_clean")
    ehr_good.write.mode("overwrite").parquet(f"{args.curated}/encounters_clean")

    if claims_bad.count() > 0:
        claims_bad.write.mode("overwrite").parquet(f"{args.quarantine}/claims")
    if ehr_bad.count() > 0:
        ehr_bad.write.mode("overwrite").parquet(f"{args.quarantine}/encounters")

    spark.stop()

if __name__ == "__main__":
    main()
