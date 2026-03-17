from __future__ import annotations

import argparse
from _common import build_spark, add_common_args, set_log_level

def parse_args():
    p = argparse.ArgumentParser(description="Example JDBC load to a warehouse (Postgres/Redshift/etc).")
    p.add_argument("--input", required=True, help="Input parquet path (one table)")
    p.add_argument("--jdbc_url", required=True, help="JDBC URL")
    p.add_argument("--table", required=True, help="Target table name")
    p.add_argument("--user", required=True, help="DB username")
    p.add_argument("--password", required=True, help="DB password")
    p.add_argument("--driver", default="org.postgresql.Driver", help="JDBC driver class")
    add_common_args(p)
    return p.parse_args()

def main():
    args = parse_args()
    spark = build_spark("load_to_warehouse")
    set_log_level(spark, args.log_level)

    df = spark.read.parquet(args.input)
    (df.write.format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", args.table)
        .option("user", args.user)
        .option("password", args.password)
        .option("driver", args.driver)
        .mode("append")
        .save()
    )
    spark.stop()

if __name__ == "__main__":
    main()
