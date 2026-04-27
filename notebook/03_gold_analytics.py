from pyspark.sql.functions import col


# Read Silver tables
silver_base_path = "/Volumes/workspace/default/raw_data/silver"


sales = spark.read.format("delta").load(f"{silver_base_path}/sales_transactions")
customers = spark.read.format("delta").load(f"{silver_base_path}/customers")
products = spark.read.format("delta").load(f"{silver_base_path}/products")
stores = spark.read.format("delta").load(f"{silver_base_path}/stores")


# Use aliases to avoid duplicate column names during joins
s = sales.alias("s")
c = customers.alias("c")
p = products.alias("p")
st = stores.alias("st")


# Join tables to create enriched Gold dataset
gold_df = s \
    .join(c, col("s.customer_id") == col("c.customer_id"), "left") \
    .join(p, col("s.product_id") == col("p.product_id"), "left") \
    .join(st, col("s.store_id") == col("st.store_id"), "left") \
    .select(
        col("s.sale_id"),
        col("s.customer_id"),
        col("c.customer_name"),
        col("c.city").alias("customer_city"),
        col("c.state").alias("customer_state"),


        col("s.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.brand"),


        col("s.store_id"),
        col("st.store_name"),
        col("st.city").alias("store_city"),
        col("st.state").alias("store_state"),
        col("st.region"),


        col("s.sale_date"),
        col("s.quantity"),
        col("s.unit_price"),
        col("s.discount"),
        col("s.payment_type"),
        col("s.net_amount")
    )


# Preview schema and data
gold_df.printSchema()


# Create business metrics
sales_by_product = gold_df.groupBy("product_name") \
    .sum("net_amount") \
    .withColumnRenamed("sum(net_amount)", "total_sales")


sales_by_region = gold_df.groupBy("region") \
    .sum("net_amount") \
    .withColumnRenamed("sum(net_amount)", "total_sales")


sales_by_date = gold_df.groupBy("sale_date") \
    .sum("net_amount") \
    .withColumnRenamed("sum(net_amount)", "daily_sales")


# Save Gold tables
gold_base_path = "/Volumes/workspace/default/raw_data/gold"


gold_df.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/sales_enriched")


sales_by_product.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/sales_by_product")


sales_by_region.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/sales_by_region")


sales_by_date.write.format("delta") \
    .mode("overwrite") \
    .save(f"{gold_base_path}/sales_by_date")

# Setting up Gold star schema: dimension and fact tables


from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, year


gold_base_path = "/Volumes/workspace/default/raw_data/gold"


# Dimension: Customers
dim_customers = customers.select(
    col("customer_id"),
    col("customer_name"),
    col("city").alias("customer_city"),
    col("state").alias("customer_state"),
    col("signup_date")
).dropDuplicates(["customer_id"])




# Dimension: Products
dim_products = products.select(
    col("product_id"),
    col("product_name"),
    col("category"),
    col("brand"),
    col("cost_price"),
    col("selling_price")
).dropDuplicates(["product_id"])




# Dimension: Stores
dim_stores = stores.select(
    col("store_id"),
    col("store_name"),
    col("city").alias("store_city"),
    col("state").alias("store_state"),
    col("region"),
    col("open_date")
).dropDuplicates(["store_id"])




# Dimension: Date
dim_date = sales.select("sale_date").dropDuplicates() \
    .withColumn("date_key", date_format(col("sale_date"), "yyyyMMdd").cast("int")) \
    .withColumn("day", dayofmonth(col("sale_date"))) \
    .withColumn("month", month(col("sale_date"))) \
    .withColumn("quarter", quarter(col("sale_date"))) \
    .withColumn("year", year(col("sale_date"))) \
    .select(
        "date_key",
        "sale_date",
        "day",
        "month",
        "quarter",
        "year"
    )




# Fact: Sales
fact_sales = sales.withColumn(
    "date_key", date_format(col("sale_date"), "yyyyMMdd").cast("int")
).select(
    col("sale_id"),
    col("customer_id"),
    col("product_id"),
    col("store_id"),
    col("date_key"),
    col("sale_date"),
    col("quantity"),
    col("unit_price"),
    col("discount"),
    col("payment_type"),
    col("net_amount")
)




# Save Gold star schema tables (FIX: overwriteSchema added)
dim_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold_base_path}/dim_customers")


dim_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold_base_path}/dim_products")


dim_stores.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold_base_path}/dim_stores")


dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold_base_path}/dim_date")


fact_sales.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{gold_base_path}/fact_sales")


print("Gold star schema tables created successfully.")
