This project is about creating a pipeline that ingests raw data, and transforms it to being business ready for analysis purposes.
The objective is to simulate a real world data engineering workflow, by:
- Ingesting raw data into a Bronze layer using Delta Lake  
- Cleaning and validating data in the Silver layer using business rules  
- Modeling data in the Gold layer using a star schema (fact and dimension tables)  
- Generating business KPIs to support analytical decision-making

This project is about showcasing how raw data can be transformed into structured, reliable and insight ready datasets with the use of PySpark in Databricks.

## Architecture

The project follows a layered Lakehouse architecture, separating data processing into distinct stages:

```text
Raw Data (CSV)
        ↓
     Bronze Layer (Delta)
        ↓
     Silver Layer (Cleaned & Validated)
        ↓
     Gold Layer (Star Schema)
        ↓
     KPI Layer (Business Insights)
```

Bronze Layer- Ingests raw CSV data into delta format, stores data in its original form with minimal transformation, adds metadata such as ingestion timestamp.

Siver Layer- Cleaning and validation using business rules, handles flaws such as missing values, invalid records, and duplicates, standardizes data types and formats, produces reliable, structured datasets.

Gold layers models the data using a star schema. We create a central table(fact_sales) and multiple surrounding tables(dim_customers, dim_products, dim_stores, dim_date), optimized for analytical queries.

KPI Layer builds business metrics using data from gold tables. Sales by category, top 10 product, repeat customers, discount impact by category, average basket size, sales trend before/after promotion are all queries used as examples for testing, the syntax and results will be present in the KPI queries folder.

## Data Model (Star Schema)

The Gold layer is modeled as a star schema, for the purpose of increasing query efficiency. The schema consists of a central fact_sales table, surrounded by dimension tables.

```text
                dim_customers
                     |
dim_products — fact_sales — dim_stores
                     |
                  dim_date
```

fact_sales contains transactional data, and inludes sale_id, customer_id, product_id, store_id, data_key, quantity, unit_price, discount, net_amount.

dim_customers contains customer data, includes customer_id, customer_name, customer_city, customer_state, signup_date.

dim_products contains product data, includes product_id, product_name, category, brand, cost_price, selling_price.

dim_stores contains store data, includes store_id, store_name, store_city, store_state, region, open_date.

dim_date contains time based data from the transaction date, includes date_key, sale_date, day, month, quarter, year.

To connect fact_sales to the dimension tables, we establish the following connections:
fact_sales.customer_id → dim_customers.customer_id

fact_sales.product_id → dim_products.product_id

fact_sales.store_id → dim_stores.store_id

fact_sales.date_key → dim_date.date_key

## Technologies Used

- **Databricks** – Data processing and notebook environment  
- **PySpark** – Distributed data processing and transformations  
- **Delta Lake** – Storage layer for reliable and scalable data handling  
- **Apache Spark** – Underlying engine for large-scale data processing  
- **Python** – Programming language used for implementation  
- **GitHub** – Version control and project hosting

## Key KPIs

The following key performance indicators (KPIs) were derived from the Gold layer to generate business insights:

- **Total Sales by Category**  
  Aggregates revenue across product categories to identify segment performance.

- **Top 10 Products by Revenue**  
  Identifies the highest-performing products based on total sales.

- **Repeat Customers**  
  Highlights customers with multiple transactions, indicating customer retention and loyalty.

- **Discount Impact by Category**  
  Measures how discounts affect revenue across different product categories.

- **Average Basket Size**  
  Calculates the average number of items purchased per transaction.

- **Sales Trend Before and After Promotion**  
  Compares revenue patterns before and after promotional periods to evaluate effectiveness.

These KPIs showcase how structured data can be used to support business decision-making and performance analysts.
