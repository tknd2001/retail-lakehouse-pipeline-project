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
