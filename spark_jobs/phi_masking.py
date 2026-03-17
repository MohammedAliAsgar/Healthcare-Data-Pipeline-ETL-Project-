from __future__ import annotations

import argparse
from pyspark.sql.functions import sha2, concat_ws, col, lit
from _common import build_spark, add_common_args, set_log_level

PHI_COLS = ["first_name","last_name","dob","zip"]

def parse_args():
    p = argparse.ArgumentParser(description="Mask PHI-like columns in curated data.")
    p.add_argument("--input", required=True, help="Input curated base path (claims_clean/patients_clean/encounters_clean)")
    p.add_argument("--output", required=True, help="Output base path for PHI-safe curated data")
    p.add_argument("--salt", required=True, help="Salt value for hashing (demo only)")
    add_common_args(p)
    return p.parse_args()

def main():
    args = parse_args()
    spark = build_spark("phi_masking")
    set_log_level(spark, args.log_level)

    claims = spark.read.parquet(f"{args.input}/claims_clean")
    patients = spark.read.parquet(f"{args.input}/patients_clean")
    encounters = spark.read.parquet(f"{args.input}/encounters_clean")

    # Create stable hashed patient key using (patient_id + salt)
    patients = patients.withColumn(
        "member_key",
        sha2(concat_ws("||", col("patient_id"), lit(args.salt)), 256)
    )

    # Drop PHI columns
    for c in PHI_COLS:
        if c in patients.columns:
            patients = patients.drop(c)

    # Replace raw ids in encounters/claims with hashed key where possible
    # Note: claims uses member_id, patients uses patient_id
    mapping = patients.select(col("patient_id").alias("member_id"), "member_key")

    claims = claims.join(mapping, on="member_id", how="left").drop("member_id")
    encounters = encounters.join(patients.select("patient_id","member_key"), encounters.patient_id == patients.patient_id, "left") \
                           .drop(encounters.patient_id).drop(patients.patient_id)

    claims.write.mode("overwrite").parquet(f"{args.output}/claims_clean")
    patients.write.mode("overwrite").parquet(f"{args.output}/patients_clean")
    encounters.write.mode("overwrite").parquet(f"{args.output}/encounters_clean")

    spark.stop()

if __name__ == "__main__":
    main()
