from __future__ import annotations

import argparse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, monotonically_increasing_id, year, month, dayofmonth
from _common import build_spark, add_common_args, set_log_level

def parse_args():
    p = argparse.ArgumentParser(description="Integrate claims + EHR and build dims/fact tables.")
    p.add_argument("--claims", required=True, help="Path to PHI-safe claims_clean parquet")
    p.add_argument("--ehr", required=True, help="Base path to PHI-safe EHR (patients_clean/encounters_clean)")
    p.add_argument("--output", required=True, help="Output base path for star schema tables")
    add_common_args(p)
    return p.parse_args()

def build_dim(df: DataFrame, cols: list[str], key_name: str) -> DataFrame:
    dim = df.select(*cols).dropDuplicates(cols)
    dim = dim.withColumn(key_name, monotonically_increasing_id())
    return dim

def main():
    args = parse_args()
    spark = build_spark("integrate_and_model")
    set_log_level(spark, args.log_level)

    claims = spark.read.parquet(args.claims)
    patients = spark.read.parquet(f"{args.ehr}/patients_clean")
    encounters = spark.read.parquet(f"{args.ehr}/encounters_clean")

    # Integrate: bring EHR diagnosis/procedure when claims missing
    # Claims has icd10_code/cpt_code, EHR has diagnosis_code/procedure_code
    # Link via member_key
    integrated = claims.join(
        encounters.select("member_key","diagnosis_code","procedure_code","encounter_date"),
        on="member_key",
        how="left"
    )

    integrated = integrated.withColumn("final_diagnosis", coalesce(col("icd10_code"), col("diagnosis_code"))) \
                           .withColumn("final_procedure", coalesce(col("cpt_code"), col("procedure_code")))

    # Dimensions
    dim_member = build_dim(patients, ["member_key","gender"], "member_sk")
    dim_provider = build_dim(integrated, ["provider_id"], "provider_sk")
    dim_diagnosis = build_dim(integrated, ["final_diagnosis"], "diagnosis_sk")

    # Date dimension based on service_date
    dim_date = (
        integrated.select(col("service_date").alias("date"))
        .dropna()
        .dropDuplicates(["date"])
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("date_sk", monotonically_increasing_id())
    )

    # Fact table: join surrogate keys
    fact = integrated \
        .join(dim_member.select("member_key","member_sk"), on="member_key", how="left") \
        .join(dim_provider, on="provider_id", how="left") \
        .join(dim_diagnosis, on="final_diagnosis", how="left") \
        .join(dim_date.select(col("date").alias("service_date"), "date_sk"), on="service_date", how="left") \
        .select(
            "claim_id",
            "member_sk",
            "provider_sk",
            "diagnosis_sk",
            "date_sk",
            col("paid_amount").cast("double").alias("paid_amount"),
            col("allowed_amount").cast("double").alias("allowed_amount"),
            "final_procedure"
        )

    # Write
    dim_member.write.mode("overwrite").parquet(f"{args.output}/dim_member")
    dim_provider.write.mode("overwrite").parquet(f"{args.output}/dim_provider")
    dim_diagnosis.write.mode("overwrite").parquet(f"{args.output}/dim_diagnosis")
    dim_date.write.mode("overwrite").parquet(f"{args.output}/dim_date")
    fact.write.mode("overwrite").parquet(f"{args.output}/fact_claims")

    spark.stop()

if __name__ == "__main__":
    main()
