# Load all four Bronze tables
from pyspark.sql.functions import col, expr, when, current_timestamp

bronze_base_path = "/Volumes/workspace/default/raw_data/bronze"
silver_base_path = "/Volumes/workspace/default/raw_data/silver"

bronze_customers = spark.read.format("delta").load(f"{bronze_base_path}/customers")
bronze_products = spark.read.format("delta").load(f"{bronze_base_path}/products")
bronze_stores = spark.read.format("delta").load(f"{bronze_base_path}/stores")
bronze_sales = spark.read.format("delta").load(f"{bronze_base_path}/sales_transactions")


# Lightly clean the master tables
customers_clean = bronze_customers.dropDuplicates(["customer_id"])
products_clean = bronze_products.dropDuplicates(["product_id"])
stores_clean = bronze_stores.dropDuplicates(["store_id"])


# Standardize sales table
sales_standardized = bronze_sales \
    .withColumn("sale_date", expr("try_to_date(sale_date, 'yyyy-MM-dd')")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount", col("discount").cast("double"))


# Add rejection reason
sales_validated = sales_standardized.withColumn(
    "rejection_reason",
    when(col("sale_id").isNull() | (col("sale_id") == ""), "Missing sale_id")
    .when(col("customer_id").isNull() | (col("customer_id") == ""), "Missing customer_id")
    .when(col("product_id").isNull() | (col("product_id") == ""), "Missing product_id")
    .when(col("store_id").isNull() | (col("store_id") == ""), "Missing store_id")
    .when(col("sale_date").isNull(), "Invalid or missing sale_date")
    .when(col("quantity") <= 0, "Invalid quantity")
    .when(col("unit_price") <= 0, "Invalid unit_price")
    .when((col("discount") < 0) | (col("discount") > 1), "Invalid discount")
    .otherwise(None)
)


# Split clean and rejected sales
sales_clean = sales_validated \
    .filter(col("rejection_reason").isNull()) \
    .drop("rejection_reason") \
    .dropDuplicates(["sale_id"])

sales_rejected = sales_validated \
    .filter(col("rejection_reason").isNotNull()) \
    .withColumn("rejected_timestamp", current_timestamp())


# Add net amount to clean sales
sales_clean = sales_clean.withColumn(
    "net_amount",
    col("quantity") * col("unit_price") * (1 - col("discount"))
)


# Save all Silver tables
customers_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/customers")

products_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/products")

stores_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/stores")

sales_clean.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/sales_transactions")

sales_rejected.write.format("delta") \
    .mode("overwrite") \
    .save(f"{silver_base_path}/rejected_sales")


# Validate output
print("Customers:", customers_clean.count())
print("Products:", products_clean.count())
print("Stores:", stores_clean.count())
print("Clean Sales:", sales_clean.count())
print("Rejected Sales:", sales_rejected.count())

display(sales_clean)
display(sales_rejected)
