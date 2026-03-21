# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Clean Slate
# MAGIC Drop all Bronze + Silver tables, volumes, and reset checkpoints.
# MAGIC Run this before Full Refresh on Bronze + Silver pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Drop Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bharatmart_databricks.default.bharatmart_churn_model;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_brands;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_campaign_responses;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_cart_events;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_categories;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_commissions;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_customers;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_geo_dimension;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_inventory;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_orders;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_payments;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_products;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_refunds;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_reviews;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_sellers;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_sessions;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_shipments;
# MAGIC DROP TABLE IF EXISTS bharatmart.bronze.brz_warehouses;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Drop Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_brands;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_campaign_responses;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_cart_events;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_categories;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_commissions;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_customers;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_geo;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_inventory;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_orders;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_payments;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_products;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_refunds;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_reviews;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_sellers;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_sessions;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_shipments;
# MAGIC DROP TABLE IF EXISTS bharatmart.silver.slv_warehouses;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Drop Bronze Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS bharatmart.bronze.landing;
# MAGIC DROP VOLUME IF EXISTS bharatmart.bronze.`__databricks_ingestion_gateway_staging_data-6a7df98a-9274-4cd4-a788-d9`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.agg_customer_360;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.agg_daily_sales;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.agg_seller_performance;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_customer;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_date;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_geo;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_product;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_seller;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.dim_warehouse;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_campaign_responses;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_cart_events;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_commissions;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_inventory;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_orders;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_payments;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_refunds;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_reviews;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_sessions;
# MAGIC DROP TABLE IF EXISTS bharatmart.gold.fact_shipments;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Reset AutoLoader Checkpoints

# COMMAND ----------

checkpoint_path = "abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/checkpoints/"

try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print(f"checkpoints cleared")
except Exception as e:
    print(f"no checkpoints found or already cleared: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Verify everything is gone

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bharatmart.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bharatmart.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bharatmart.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — Next Steps
# MAGIC 1. Run `04b_sql_to_bronze.py` — reload Azure SQL dimension tables via CDC
# MAGIC 2. Bronze pipeline → Full Refresh
# MAGIC 3. Silver pipeline → Full Refresh
