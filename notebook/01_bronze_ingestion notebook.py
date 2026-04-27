from pyspark.sql.functions import current_timestamp, lit

raw_base_path = "/Volumes/workspace/default/raw_data"
bronze_base_path = "/Volumes/workspace/default/raw_data/bronze"

files = {
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "sales_transactions": "sales_transactions.csv"
}

for table_name, file_name in files.items():
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(f"{raw_base_path}/{file_name}")

    bronze_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("source_file", lit(file_name)) \
                  .withColumn("pipeline_stage", lit("bronze"))

    bronze_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{bronze_base_path}/{table_name}")

    print(f"Bronze table recreated: {table_name}")
