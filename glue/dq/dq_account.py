import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "record_date",
    "account_id",
    "customer_id",
    "account_type",
    "account_status",
    "currency",
    "balance",
    "credit_limit",
    "opened_date",
    "branch_code",
    "city",
    "last_updated_ts"
]

NON_NULL_COLUMNS = ["account_id", "account_type", "last_updated_ts"]

NUMERIC_STRICT = ["balance"]               # must be numeric and not blank
NUMERIC_ALLOW_EMPTY = ["credit_limit"]     # blank allowed, but if present must be numeric


def fail(msg: str):
    raise Exception(f"[DQ_FAILED] {msg}")


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_bucket", "input_key"])
    input_bucket = args["input_bucket"]
    input_key = args["input_key"]
    input_path = f"s3://{input_bucket}/{input_key}"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    print(f"[DQ] Reading input: {input_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )

    total = df.count()
    print(f"[DQ] Total records: {total}")
    if total == 0:
        fail("Input file has 0 records")

    # 1) Required columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        fail(f"Missing required columns: {missing}")

    # 2) Non-null checks
    for c in NON_NULL_COLUMNS:
        bad = df.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count()
        if bad > 0:
            fail(f"Column '{c}' has {bad} null/blank values")

    # Numeric regex: allows negative and decimals
    numeric_regex = r"^-?\d+(\.\d+)?$"

    # 3) Strict numeric checks
    for c in NUMERIC_STRICT:
        bad = df.filter(
            F.col(c).isNull()
            | (F.trim(F.col(c)) == "")
            | (~F.trim(F.col(c)).rlike(numeric_regex))
        ).count()
        if bad > 0:
            fail(f"Column '{c}' has {bad} non-numeric/blank values")

    # 4) Allow-empty numeric checks
    for c in NUMERIC_ALLOW_EMPTY:
        bad = df.filter(
            (F.col(c).isNotNull())
            & (F.trim(F.col(c)) != "")
            & (~F.trim(F.col(c)).rlike(numeric_regex))
        ).count()
        if bad > 0:
            fail(f"Column '{c}' has {bad} non-numeric values (blank allowed)")

    # 5) Timestamp parse check for last_updated_ts
    # If your last_updated_ts is ISO format, this works well
    ts_bad = (
        df.withColumn("parsed_ts", F.to_timestamp("last_updated_ts"))
          .filter(F.col("parsed_ts").isNull())
          .count()
    )
    if ts_bad > 0:
        fail(f"Column 'last_updated_ts' has {ts_bad} values not parseable as timestamp")

    print("[DQ_SUCCESS] All checks passed.")


if __name__ == "__main__":
    main()