# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Exploration
# MAGIC
# MAGIC Before we build the Bronze pipeline we need to understand what we actually have.
# MAGIC Three sources to explore:
# MAGIC - ADLS landing/ - CSV files from 03c
# MAGIC - Event Hub - live JSON messages from 03b
# MAGIC - Azure SQL - master data tables from 03a
# MAGIC
# MAGIC This notebook is READ ONLY. No writes, no ingestion.
# MAGIC Run this on personal compute, look at the output, then build the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("adls_path", "abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/landing", "ADLS landing path")
dbutils.widgets.text("eh_namespace", "", "Event Hub namespace")
dbutils.widgets.text("eh_connection_string", "", "Event Hub connection string")
dbutils.widgets.text("sql_server", "", "Azure SQL server")
dbutils.widgets.text("sql_database", "bharatmart_db", "SQL database")
dbutils.widgets.text("sql_username", "veer", "SQL username")
dbutils.widgets.text("sql_password", "", "SQL password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widgets

# COMMAND ----------

ADLS_PATH  = dbutils.widgets.get("adls_path")
EH_NS      = dbutils.widgets.get("eh_namespace")
EH_CONN    = dbutils.widgets.get("eh_connection_string")
SQL_SERVER = dbutils.widgets.get("sql_server")
SQL_DB     = dbutils.widgets.get("sql_database")
SQL_USER   = dbutils.widgets.get("sql_username")
SQL_PASS   = dbutils.widgets.get("sql_password")

print("config ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — ADLS Landing Zone
# MAGIC
# MAGIC What folders exist? How many files? What does the data look like?

# COMMAND ----------

# MAGIC %md
# MAGIC ## What folders are in landing/?

# COMMAND ----------

folders = dbutils.fs.ls(ADLS_PATH)
for f in folders:
    print(f.name, f.size)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many files in each folder?

# COMMAND ----------

for folder in folders:
    try:
        # count files recursively
        all_files = dbutils.fs.ls(folder.path)
        print(f"{folder.name:<25} {len(all_files)} top-level items")
    except:
        print(f"{folder.name:<25} could not list")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check orders folder structure

# COMMAND ----------

order_files = dbutils.fs.ls(f"{ADLS_PATH}/orders/")
print(f"year partitions: {len(order_files)}")
for f in order_files:
    print(f.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drill into one month of orders

# COMMAND ----------

sample_month = dbutils.fs.ls(f"{ADLS_PATH}/orders/2020/01/")
print(f"files in 2020/01: {len(sample_month)}")
for f in sample_month[:5]:
    print(f.name, round(f.size/1024, 1), "KB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read a sample orders CSV — see what columns we get

# COMMAND ----------

orders_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/orders/2020/01/")
print(f"rows: {orders_sample.count()}")

# COMMAND ----------

orders_sample.printSchema()

# COMMAND ----------

display(orders_sample.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for nulls in orders

# COMMAND ----------

from pyspark.sql.functions import col, count, when, isnan

null_counts = orders_sample.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in orders_sample.columns
])
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check order_status distribution

# COMMAND ----------

display(orders_sample.groupBy("order_status").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read payments sample

# COMMAND ----------

payments_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/payments/2020/01/")
print(f"rows: {payments_sample.count()}")

# COMMAND ----------

display(payments_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read shipments sample

# COMMAND ----------

shipments_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/shipments/2020/01/")
print(f"rows: {shipments_sample.count()}")

# COMMAND ----------

display(shipments_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read reviews sample

# COMMAND ----------

reviews_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/reviews/2020/01/")
print(f"rows: {reviews_sample.count()}")

# COMMAND ----------

display(reviews_sample.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check review_text nulls — should be ~40%

# COMMAND ----------

total  = reviews_sample.count()
nulls  = reviews_sample.filter(col("review_text").isNull()).count()
print(f"total: {total}, null review_text: {nulls}, pct: {nulls/total*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check review_date format — should be DD-MM-YYYY

# COMMAND ----------

display(reviews_sample.select("review_date").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for ghost order_ids in reviews — should be ~3%

# COMMAND ----------

ghost = reviews_sample.filter(col("order_id").startswith("ORD_GHOST_")).count()
print(f"ghost order_ids: {ghost} out of {total} = {ghost/total*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read refunds sample

# COMMAND ----------

refunds_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/refunds/2020/01/")
print(f"rows: {refunds_sample.count()}")

# COMMAND ----------

display(refunds_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read commissions sample

# COMMAND ----------

commissions_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/commissions/2020/01/")
print(f"rows: {commissions_sample.count()}")

# COMMAND ----------

display(commissions_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read inventory sample

# COMMAND ----------

inventory_sample = spark.read.option("header", "true").csv(f"{ADLS_PATH}/inventory/2020/01/")
print(f"rows: {inventory_sample.count()}")

# COMMAND ----------

display(inventory_sample.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check last_updated format in inventory — should be DD-MM-YYYY HH:MM

# COMMAND ----------

display(inventory_sample.select("last_updated").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read geo_dimension

# COMMAND ----------

geo_df = spark.read.option("header", "true").csv(f"{ADLS_PATH}/geo_dimension/")
print(f"rows: {geo_df.count()}")

# COMMAND ----------

display(geo_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check all regions present in geo

# COMMAND ----------

display(geo_df.groupBy("region").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS summary — count files across all folders

# COMMAND ----------

datasets = ["orders", "payments", "shipments", "reviews", "refunds", "commissions", "inventory"]

print(f"{'dataset':<20} {'files':<10} {'sample rows'}")
print("-" * 45)
for ds in datasets:
    try:
        files = dbutils.fs.ls(f"{ADLS_PATH}/{ds}/")
        sample_df = spark.read.option("header","true").csv(f"{ADLS_PATH}/{ds}/2020/01/")
        print(f"{ds:<20} {len(files):<10} {sample_df.count()}")
    except Exception as e:
        print(f"{ds:<20} error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 — Event Hub (Kafka)
# MAGIC
# MAGIC What topics exist? What does a raw message look like?
# MAGIC We read a small batch using readStream with a timeout.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka connection options

# COMMAND ----------

kafka_opts = {
    "kafka.bootstrap.servers": f"{EH_NS}.servicebus.windows.net:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONN}";'
    ),
    "startingOffsets": "earliest",
    "endingOffsets": "latest",   # batch read — not streaming
}

print("kafka options ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample raw messages from orders topic

# COMMAND ----------

# batch read so we don't need a streaming query
orders_raw = (spark.read
    .format("kafka")
    .options(**{**kafka_opts, "subscribe": "orders"})
    .load()
)
print(f"messages in orders topic: {orders_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## See what a raw Kafka message looks like

# COMMAND ----------

display(orders_raw.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decode the value column to see JSON

# COMMAND ----------

from pyspark.sql.functions import col

orders_decoded = orders_raw.withColumn("value_str", col("value").cast("string"))
display(orders_decoded.select("value_str").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample payments topic

# COMMAND ----------

payments_raw = (spark.read
    .format("kafka")
    .options(**{**kafka_opts, "subscribe": "payments"})
    .load()
    .withColumn("value_str", col("value").cast("string"))
)
print(f"messages: {payments_raw.count()}")

# COMMAND ----------

display(payments_raw.select("value_str").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample sessions topic

# COMMAND ----------

sessions_raw = (spark.read
    .format("kafka")
    .options(**{**kafka_opts, "subscribe": "sessions"})
    .load()
    .withColumn("value_str", col("value").cast("string"))
)
print(f"messages: {sessions_raw.count()}")

# COMMAND ----------

display(sessions_raw.select("value_str").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample cart-events topic

# COMMAND ----------

cart_raw = (spark.read
    .format("kafka")
    .options(**{**kafka_opts, "subscribe": "cart-events"})
    .load()
    .withColumn("value_str", col("value").cast("string"))
)
print(f"messages: {cart_raw.count()}")

# COMMAND ----------

display(cart_raw.select("value_str").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample campaign-responses topic

# COMMAND ----------

campaign_raw = (spark.read
    .format("kafka")
    .options(**{**kafka_opts, "subscribe": "campaign-responses"})
    .load()
    .withColumn("value_str", col("value").cast("string"))
)
print(f"messages: {campaign_raw.count()}")

# COMMAND ----------

display(campaign_raw.select("value_str").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Hub summary

# COMMAND ----------

topics = ["orders", "payments", "sessions", "cart-events", "campaign-responses"]
print(f"{'topic':<25} {'messages'}")
print("-" * 35)
for topic in topics:
    try:
        df = spark.read.format("kafka").options(**{**kafka_opts, "subscribe": topic}).load()
        print(f"{topic:<25} {df.count()}")
    except Exception as e:
        print(f"{topic:<25} error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 — Azure SQL Tables
# MAGIC
# MAGIC What tables exist? How many rows? What columns?
# MAGIC CDC enabled on which tables?

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
    "user": SQL_USER,
    "password": SQL_PASS,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
print("jdbc ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List all tables in dbo schema

# COMMAND ----------

all_tables = spark.read.jdbc(
    JDBC_URL,
    "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_TYPE='BASE TABLE') t",
    properties=JDBC_PROPS
)
display(all_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts for every table

# COMMAND ----------

table_names = [r[0] for r in all_tables.collect()]

print(f"{'table':<25} {'rows'}")
print("-" * 35)
for t in table_names:
    cnt = spark.read.jdbc(JDBC_URL, f"(SELECT COUNT(*) AS n FROM dbo.{t}) t", properties=JDBC_PROPS).collect()[0][0]
    print(f"{t:<25} {cnt:>10,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample customers table

# COMMAND ----------

customers_df = spark.read.jdbc(JDBC_URL, "(SELECT TOP 10 * FROM dbo.customers) t", properties=JDBC_PROPS)
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample products table

# COMMAND ----------

products_df = spark.read.jdbc(JDBC_URL, "(SELECT TOP 10 * FROM dbo.products) t", properties=JDBC_PROPS)
display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample sellers table

# COMMAND ----------

sellers_df = spark.read.jdbc(JDBC_URL, "(SELECT TOP 10 * FROM dbo.sellers) t", properties=JDBC_PROPS)
display(sellers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check which tables have CDC enabled

# COMMAND ----------

cdc_tables = spark.read.jdbc(
    JDBC_URL,
    "(SELECT name, is_tracked_by_cdc FROM sys.tables WHERE is_tracked_by_cdc = 1) t",
    properties=JDBC_PROPS
)
display(cdc_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check columns for each table

# COMMAND ----------

for t in table_names:
    cols = spark.read.jdbc(
        JDBC_URL,
        f"(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{t}' AND TABLE_SCHEMA='dbo') t",
        properties=JDBC_PROPS
    )
    print(f"\n--- {t} ---")
    for r in cols.collect():
        print(f"  {r[0]:<30} {r[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary — What We Found
# MAGIC
# MAGIC Fill this in after running the notebook.
# MAGIC Use it to decide what goes into the Bronze pipeline.

# COMMAND ----------

print("""
ADLS landing/
--------------
folders found        : check output above
orders files         : check output above
dirty data confirmed :
  - reviews null text ~40%   YES / NO
  - reviews DD-MM-YYYY dates YES / NO
  - ghost order_ids ~3%      YES / NO
  - inventory wrong datetime YES / NO

Event Hub topics
--------------
orders messages      : check output above
payments messages    : check output above
sessions messages    : check output above
cart-events messages : check output above
campaign-responses   : check output above

Azure SQL tables
--------------
tables found         : check output above
CDC enabled on       : check output above
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Findings confirmed — ready to build Bronze pipeline
# MAGIC
# MAGIC Run `04_bronze_ingestion.py` as a Databricks Pipeline next.
# MAGIC
