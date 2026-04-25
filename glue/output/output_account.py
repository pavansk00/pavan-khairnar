import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

# Output schema = input schema + last_updated_ts
OUTPUT_COLUMNS = [
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

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "business_date", "hudi_base_path", "output_path"])
    business_date = args["business_date"]
    hudi_base_path = args["hudi_base_path"].rstrip("/") + "/"
    output_path = args["output_path"].rstrip("/") + "/"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    print(f"[OUTPUT] Reading Hudi from: {hudi_base_path}")
    df = spark.read.format("hudi").load(hudi_base_path)

    # Filter only today's business_date
    df_today = df.filter(F.col("business_date") == business_date)

    count_today = df_today.count()
    print(f"[OUTPUT] Records for business_date={business_date}: {count_today}")

    # Select columns (same as input)
    # NOTE: Do NOT include feed/business_date in the parquet files (you chose partition-folder only).
    df_out = df_today.select(*[c for c in OUTPUT_COLUMNS if c in df_today.columns])

    print(f"[OUTPUT] Writing Parquet to: {output_path} (overwrite)")
    (
        df_out.write
            .mode("overwrite")
            .format("parquet")
            .save(output_path)
    )

    print("[OUTPUT_SUCCESS] Output Parquet generated successfully.")


if __name__ == "__main__":
    main()