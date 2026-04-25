import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "input_bucket", "input_key", "business_date", "hudi_base_path", "hudi_table"]
    )

    input_bucket = args["input_bucket"]
    input_key = args["input_key"]
    business_date = args["business_date"]
    hudi_base_path = args["hudi_base_path"].rstrip("/") + "/"
    hudi_table = args["hudi_table"]

    input_path = f"s3://{input_bucket}/{input_key}"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    print(f"[HUDI] Reading input: {input_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )

    # Standardize/derive columns
    # business_date from pipeline, last_updated_ts cast to timestamp
    df2 = (
        df.withColumn("business_date", F.lit(business_date))
          .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
          .withColumn("record_key", F.concat_ws("#", F.col("account_id"), F.col("business_date")))
    )

    # Optional: Cast numeric columns (recommended for Hudi schema stability)
    df2 = (
        df2.withColumn("balance", F.col("balance").cast("double"))
           .withColumn("credit_limit", F.when(F.trim(F.col("credit_limit")) == "", None)
                                      .otherwise(F.col("credit_limit").cast("double")))
           .withColumn("record_date", F.to_date("record_date"))
           .withColumn("opened_date", F.to_date("opened_date"))
    )

    print(f"[HUDI] Writing to: {hudi_base_path} table={hudi_table}")
    print("[HUDI] Partitions: account_type, business_date")
    print("[HUDI] record_key = account_id#business_date, precombine = last_updated_ts")

    hudi_options = {
        "hoodie.table.name": hudi_table,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",

        "hoodie.datasource.write.recordkey.field": "record_key",
        "hoodie.datasource.write.precombine.field": "last_updated_ts",

        "hoodie.datasource.write.partitionpath.field": "account_type,business_date",
        "hoodie.datasource.write.hive_style_partitioning": "true",

        # Performance (starter defaults)
        "hoodie.upsert.shuffle.parallelism": "20",
        "hoodie.insert.shuffle.parallelism": "20",
    }

    (
        df2.write.format("hudi")
           .options(**hudi_options)
           .mode("append")
           .save(hudi_base_path)
    )

    print("[HUDI_SUCCESS] Hudi upsert completed successfully.")


if __name__ == "__main__":
    main()