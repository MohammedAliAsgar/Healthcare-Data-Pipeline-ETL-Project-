"""
Common Spark utilities: session builder, arg parsing helpers, and safe writing.
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--log_level", default="WARN", help="Spark log level (ERROR/WARN/INFO)")
    return parser

def set_log_level(spark: SparkSession, level: str) -> None:
    spark.sparkContext.setLogLevel(level)

