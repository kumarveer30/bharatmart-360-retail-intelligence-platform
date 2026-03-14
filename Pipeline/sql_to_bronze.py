# Databricks notebook source
# MAGIC %md
# MAGIC # 04b — Azure SQL Master Data → Bronze
# MAGIC
# MAGIC LakeFlow Connect was the designed pattern but compute quota blocked it.
# MAGIC This reads the 6 master tables from Azure SQL via JDBC and writes them
# MAGIC directly to bharatmart.bronze as Delta tables.
# MAGIC
# MAGIC Run once on personal compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("sql_server",   "", "Azure SQL server")
dbutils.widgets.text("sql_database", "bharatmart_db", "SQL database")
dbutils.widgets.text("sql_username", "veer", "SQL username")
dbutils.widgets.text("sql_password", "", "SQL password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widgets

# COMMAND ----------

SQL_SERVER = dbutils.widgets.get("sql_server")
SQL_DB     = dbutils.widgets.get("sql_database")
SQL_USER   = dbutils.widgets.get("sql_username")
SQL_PASS   = dbutils.widgets.get("sql_password")

print(f"server: {SQL_SERVER}")
print(f"database: {SQL_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC config

# COMMAND ----------

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}:1433;"
    f"database={SQL_DB};"
    f"encrypt=true;trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;loginTimeout=30"
)
JDBC_PROPS = {
    "user":     SQL_USER,
    "password": SQL_PASS,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print("jdbc ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make sure bronze schema exists

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bharatmart.bronze")
print("schema ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Load 6 tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## customers

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

customers_df = spark.read.jdbc(JDBC_URL, "dbo.customers", properties=JDBC_PROPS)
print(f"rows: {customers_df.count()}")

# COMMAND ----------

display(customers_df.limit(5))

# COMMAND ----------

(customers_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_customers")
)
print("brz_customers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## products

# COMMAND ----------

products_df = spark.read.jdbc(JDBC_URL, "dbo.products", properties=JDBC_PROPS)
print(f"rows: {products_df.count()}")

# COMMAND ----------

display(products_df.limit(5))

# COMMAND ----------

(products_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_products")
)
print("brz_products written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## sellers

# COMMAND ----------

sellers_df = spark.read.jdbc(JDBC_URL, "dbo.sellers", properties=JDBC_PROPS)
print(f"rows: {sellers_df.count()}")

# COMMAND ----------

display(sellers_df.limit(5))

# COMMAND ----------

(sellers_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_sellers")
)
print("brz_sellers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## brands

# COMMAND ----------

brands_df = spark.read.jdbc(JDBC_URL, "dbo.brands", properties=JDBC_PROPS)
print(f"rows: {brands_df.count()}")

# COMMAND ----------

display(brands_df.limit(5))

# COMMAND ----------

(brands_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_brands")
)
print("brz_brands written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## categories

# COMMAND ----------

categories_df = spark.read.jdbc(JDBC_URL, "dbo.categories", properties=JDBC_PROPS)
print(f"rows: {categories_df.count()}")

# COMMAND ----------

display(categories_df.limit(5))

# COMMAND ----------

(categories_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_categories")
)
print("brz_categories written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## warehouses

# COMMAND ----------

warehouses_df = spark.read.jdbc(JDBC_URL, "dbo.warehouses", properties=JDBC_PROPS)
print(f"rows: {warehouses_df.count()}")

# COMMAND ----------

display(warehouses_df.limit(5))

# COMMAND ----------

(warehouses_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("azure_sql_jdbc"))
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("bharatmart.bronze.brz_warehouses")
)
print("brz_warehouses written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all 6 tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'brz_customers'   AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_customers   UNION ALL
# MAGIC SELECT 'brz_products'    AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_products    UNION ALL
# MAGIC SELECT 'brz_sellers'     AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_sellers     UNION ALL
# MAGIC SELECT 'brz_brands'      AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_brands      UNION ALL
# MAGIC SELECT 'brz_categories'  AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_categories  UNION ALL
# MAGIC SELECT 'brz_warehouses'  AS tbl, COUNT(*) AS rows FROM bharatmart.bronze.brz_warehouses
# MAGIC ORDER BY tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done — Bronze is complete
# MAGIC
# MAGIC All 17 Bronze tables now populated:
# MAGIC - 11 tables from pipeline (ADLS + Event Hub)
# MAGIC - 6 tables from this notebook (Azure SQL via JDBC)
# MAGIC
# MAGIC Next: Silver EDA → 07a_silver_eda.py
