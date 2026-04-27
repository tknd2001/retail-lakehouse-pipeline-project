/*Loading path and data into notebook */
gold_base_path = "/Volumes/workspace/default/raw_data/gold"


fact_sales = spark.read.format("delta").load(f"{gold_base_path}/fact_sales")
dim_customers = spark.read.format("delta").load(f"{gold_base_path}/dim_customers")
dim_products = spark.read.format("delta").load(f"{gold_base_path}/dim_products")
dim_stores = spark.read.format("delta").load(f"{gold_base_path}/dim_stores")
dim_date = spark.read.format("delta").load(f"{gold_base_path}/dim_date")

/*Sales by category*/
sales_by_category = fact_sales \
    .join(dim_products, "product_id") \
    .groupBy("category") \
    .sum("net_amount")

display(sales_by_category)

/*Sales by month*/
sales_by_month = fact_sales \
    .join(dim_date, "date_key") \
    .groupBy("year", "month") \
    .agg(_sum("net_amount").alias("total_sales")) \
    .orderBy("year", "month")

display(sales_by_month)

/*Top 10 Products*/
from pyspark.sql.functions import col, sum as _sum

top_10_products = fact_sales \
    .join(dim_products, "product_id") \
    .groupBy("product_name") \
    .agg(_sum("net_amount").alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .limit(10)

display(top_10_products)

/*Repeat Customers*/
from pyspark.sql.functions import col, countDistinct, sum as _sum

repeat_customers = fact_sales \
    .join(dim_customers, "customer_id") \
    .groupBy("customer_id", "customer_name") \
    .agg(
        countDistinct("sale_id").alias("number_of_transactions"),
        _sum("net_amount").alias("total_spent")
    ) \
    .filter(col("number_of_transactions") > 1) \
    .orderBy(col("number_of_transactions").desc(), col("total_spent").desc())

display(repeat_customers)

/*Discount Impact by Category*/
from pyspark.sql.functions import col, sum as _sum, avg

discount_impact_by_category = fact_sales \
    .join(dim_products, "product_id") \
    .groupBy("category") \
    .agg(
        _sum("net_amount").alias("sales_after_discount"),
        _sum(col("quantity") * col("unit_price")).alias("sales_before_discount"),
        _sum(col("quantity") * col("unit_price") * col("discount")).alias("discount_amount"),
        avg("discount").alias("average_discount_rate")
    ) \
    .withColumn(
        "discount_impact_percent",
        (col("discount_amount") / col("sales_before_discount")) * 100
    ) \
    .orderBy(col("discount_impact_percent").desc())

display(discount_impact_by_category)

/*Average basket size*/
from pyspark.sql.functions import sum as _sum, countDistinct, col

avg_basket_size = fact_sales \
    .agg(
        (_sum("quantity") / countDistinct("sale_id")).alias("avg_basket_size")
    )

display(avg_basket_size)

/*Sales trend before/after promotion*/
sales_before_after_by_category = fact_sales \
    .join(dim_products, "product_id") \
    .withColumn(
        "promotion_period",
        when(col("sale_date") < lit(promotion_start_date), "Before Promotion")
        .otherwise("After Promotion")
    ) \
    .groupBy("category", "promotion_period") \
    .agg(
        _sum("net_amount").alias("total_sales")
    ) \
    .orderBy("category", "promotion_period")

display(sales_before_after_by_category)
