#Load all four Bronze tables
from pyspark.sql.functions import col, expr


bronze_base_path = "/Volumes/workspace/default/raw_data/bronze"
silver_base_path = "/Volumes/workspace/default/raw_data/silver"


bronze_customers = spark.read.format("delta").load(f"{bronze_base_path}/customers")
bronze_products = spark.read.format("delta").load(f"{bronze_base_path}/products")
bronze_stores = spark.read.format("delta").load(f"{bronze_base_path}/stores")
bronze_sales = spark.read.format("delta").load(f"{bronze_base_path}/sales_transactions")


#Lightly clean the master tables
customers_clean = bronze_customers.dropDuplicates(["customer_id"])
products_clean = bronze_products.dropDuplicates(["product_id"])
stores_clean = bronze_stores.dropDuplicates(["store_id"])


#Basic clean sales table
sales_clean = bronze_sales \
    .withColumn("sale_date", expr("try_to_date(sale_date, 'yyyy-MM-dd')")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount", col("discount").cast("double")) \
    .filter(col("sale_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("store_id").isNotNull()) \
    .filter(col("sale_date").isNotNull()) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .filter((col("discount") >= 0) & (col("discount") <= 1)) \
    .dropDuplicates(["sale_id"])


#Add net amount
sales_clean = sales_clean.withColumn(
    "net_amount",
    col("quantity") * col("unit_price") * (1 - col("discount"))
)


#Save all Silver tables
customers_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/customers")
products_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/products")
stores_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/stores")
sales_clean.write.format("delta").mode("overwrite").save(f"{silver_base_path}/sales_transactions")
