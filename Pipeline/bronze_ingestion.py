# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Ingestion Pipeline (LakeFlow Declarative)
# MAGIC
# MAGIC Three ingestion paths, all landing as raw Delta tables:
# MAGIC
# MAGIC | Source | Method | Entities |
# MAGIC |--------|--------|----------|
# MAGIC | ADLS landing/ | AutoLoader (cloud_files) | orders, payments, shipments, reviews, refunds, commissions, inventory, geo |
# MAGIC | Event Hub | Kafka connector | orders, payments, sessions, cart_events, campaign_responses |
# MAGIC | Azure SQL CDC | LakeFlow Connect (separate pipeline) | customers, products, sellers, brands, categories, warehouses |
# MAGIC
# MAGIC **Key design:** Orders and payments each merge from ADLS (historical) + Event Hub (live)
# MAGIC into ONE table via `append_flow`.
# MAGIC
# MAGIC No transformations. No cleaning. Just raw data + metadata columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, lit, from_json
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline config

# COMMAND ----------

ADLS_LANDING = spark.conf.get("adls.path")

EH_NAMESPACE = spark.conf.get("eh.namespace")
EH_CONNECTION_STRING = spark.conf.get("eh.connection_string")

# reused across all Event Hub tables
kafka_options = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}:9093",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{EH_CONNECTION_STRING}";'
    ),
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": 500,
    "failOnDataLoss": "false"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 - ADLS Ingestion (AutoLoader)
# MAGIC
# MAGIC Historical bulk data from 03c sits in ADLS landing/{entity}/{year}/{month}/.
# MAGIC AutoLoader picks up all existing files on first run, then only new files after.
# MAGIC No schema enforcement — Bronze stores everything as-is.

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_shipments

# COMMAND ----------

@dp.table(
    name="brz_shipments",
    comment="Raw shipments from ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def brz_shipments():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/shipments/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_reviews

# COMMAND ----------

@dp.table(
    name="brz_reviews",
    comment="Raw product reviews from ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def brz_reviews():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/reviews/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_refunds

# COMMAND ----------

@dp.table(
    name="brz_refunds",
    comment="Raw refund records from ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def brz_refunds():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/refunds/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_commissions

# COMMAND ----------

@dp.table(
    name="brz_commissions",
    comment="Raw seller commissions from ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def brz_commissions():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/commissions/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_inventory

# COMMAND ----------

@dp.table(
    name="brz_inventory",
    comment="Raw weekly inventory snapshots from ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def brz_inventory():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/inventory/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_geo_dimension

# COMMAND ----------

@dp.table(
    name="brz_geo_dimension",
    comment="Raw geo reference data — pincode, city, state, region, lat/lng",
    table_properties={"quality": "bronze"}
)
def brz_geo_dimension():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{ADLS_LANDING}/geo_dimension/")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 - Event Hub Ingestion (Kafka Connector)
# MAGIC
# MAGIC Live streaming events from 03b event producer.
# MAGIC 5 Event Hub topics → 3 Bronze tables (sessions, cart_events, campaign_responses).
# MAGIC Orders and payments go to dual-source tables in Part 3.
# MAGIC
# MAGIC Pattern: read Kafka binary → cast value to string → parse JSON → select fields + metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_sessions

# COMMAND ----------

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_sessions",
    comment="Unified sessions — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical sessions (2020–2026)
@dp.append_flow(target="brz_sessions", name="flow_sessions_adls")
def sessions_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/sessions/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live sessions
session_schema = StructType([
    StructField("session_id",    StringType()),
    StructField("customer_id",   StringType()),
    StructField("event_type",    StringType()),
    StructField("channel",       StringType()),
    StructField("device_type",   StringType()),
    StructField("pages_viewed", StringType()),
    StructField("session_start", StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_sessions", name="flow_sessions_eh")
def sessions_from_eventhub():
    opts = {**kafka_options, "subscribe": "sessions"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), session_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_cart_events

# COMMAND ----------

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_cart_events",
    comment="Unified cart events — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical cart events (2020–2026)
@dp.append_flow(target="brz_cart_events", name="flow_cart_adls")
def cart_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/cart_events/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live cart events
cart_schema = StructType([
    StructField("event_id",      StringType()),
    StructField("session_id",    StringType()),
    StructField("customer_id",   StringType()),
    StructField("product_id",    StringType()),
    StructField("action",        StringType()),
    StructField("quantity",     StringType()),
    StructField("unit_price",   StringType()),
    StructField("total_amount", StringType()),
    StructField("channel",       StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_cart_events", name="flow_cart_eh")
def cart_from_eventhub():
    opts = {**kafka_options, "subscribe": "cart-events"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), cart_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_campaign_responses

# COMMAND ----------

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_campaign_responses",
    comment="Unified campaign responses — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical campaign responses (2020–2026)
@dp.append_flow(target="brz_campaign_responses", name="flow_campaign_adls")
def campaign_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/campaign_responses/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live campaign responses
campaign_schema = StructType([
    StructField("response_id",   StringType()),
    StructField("campaign_id",   StringType()),
    StructField("customer_id",   StringType()),
    StructField("campaign_type", StringType()),
    StructField("action",        StringType()),
    StructField("category_id",   StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_campaign_responses", name="flow_campaign_eh")
def campaign_from_eventhub():
    opts = {**kafka_options, "subscribe": "campaign-responses"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), campaign_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 - Dual-Source Tables (ADLS + Event Hub → One Table)
# MAGIC
# MAGIC **This is the competition differentiator.**
# MAGIC
# MAGIC Orders and payments arrive from two paths:
# MAGIC - ADLS landing/ → 5 years of history (2020 to yesterday, from 03c)
# MAGIC - Event Hub → live data (from today onwards, via 03b)
# MAGIC
# MAGIC Both merge into ONE Bronze table using `dp.append_flow`.
# MAGIC Each flow has its own schema, latency, and source format (CSV vs JSON).
# MAGIC
# MAGIC Judges see real multi-source architecture — batch backfill + live streaming
# MAGIC into a unified table. Same pattern used at Flipkart, Amazon, Swiggy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_orders - dual source (ADLS + Event Hub)

# COMMAND ----------

# empty destination table — append_flow fills it from both sources
dp.create_streaming_table(
    name="brz_orders",
    comment="Unified orders — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flow 1: historical orders from ADLS

# COMMAND ----------

@dp.append_flow(target="brz_orders", name="flow_orders_adls")
def orders_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/orders/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flow 2: live orders from Event Hub

# COMMAND ----------

# matches 03b build_order() output — all StringType for Bronze (Silver casts)
order_eh_schema = StructType([
    StructField("order_id", StringType()),
    StructField("session_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_items", StringType()),
    StructField("order_status", StringType()),
    StructField("channel", StringType()),
    StructField("subtotal", StringType()),
    StructField("discount_amount", StringType()),
    StructField("delivery_charge", StringType()),
    StructField("total_amount", StringType()),
    StructField("coupon_code", StringType()),
    StructField("delivery_pincode", StringType()),
    StructField("warehouse_id", StringType()),
    StructField("estimated_delivery", StringType()),
    StructField("order_timestamp", StringType()),
])

@dp.append_flow(target="brz_orders", name="flow_orders_eh")
def orders_from_eventhub():
    opts = {**kafka_options, "subscribe": "orders"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), order_eh_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### brz_payments - dual source (ADLS + Event Hub)

# COMMAND ----------

dp.create_streaming_table(
    name="brz_payments",
    comment="Unified payments — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flow 1: historical payments from ADLS

# COMMAND ----------

@dp.append_flow(target="brz_payments", name="flow_payments_adls")
def payments_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/payments/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Flow 2: live payments from Event Hub

# COMMAND ----------

# matches 03b build_payment() output — all StringType for Bronze (Silver casts)
payment_eh_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("payment_method", StringType()),
    StructField("amount", StringType()),
    StructField("status", StringType()),
    StructField("upi_id", StringType()),
    StructField("payment_time", StringType()),
])

@dp.append_flow(target="brz_payments", name="flow_payments_eh")
def payments_from_eventhub():
    opts = {**kafka_options, "subscribe": "payments"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), payment_eh_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC ## Bronze Tables — This Pipeline (11 tables)
# MAGIC
# MAGIC | Table | Source | Method | Type |
# MAGIC |-------|--------|--------|------|
# MAGIC | `brz_orders` | ADLS + Event Hub | `append_flow` (dual source) | Streaming Table |
# MAGIC | `brz_payments` | ADLS + Event Hub | `append_flow` (dual source) | Streaming Table |
# MAGIC | `brz_sessions` | Event Hub | Kafka connector | Streaming Table |
# MAGIC | `brz_cart_events` | Event Hub | Kafka connector | Streaming Table |
# MAGIC | `brz_campaign_responses` | Event Hub | Kafka connector | Streaming Table |
# MAGIC | `brz_shipments` | ADLS | AutoLoader | Streaming Table |
# MAGIC | `brz_reviews` | ADLS | AutoLoader | Streaming Table |
# MAGIC | `brz_refunds` | ADLS | AutoLoader | Streaming Table |
# MAGIC | `brz_commissions` | ADLS | AutoLoader | Streaming Table |
# MAGIC | `brz_inventory` | ADLS | AutoLoader | Streaming Table |
# MAGIC | `brz_geo_dimension` | ADLS | AutoLoader | Streaming Table |
# MAGIC
# MAGIC ## Bronze Tables — LakeFlow Connect (6 tables, separate pipeline)
# MAGIC
# MAGIC | Table | Source | Method |
# MAGIC |-------|--------|--------|
# MAGIC | `brz_customers` | Azure SQL CDC | LakeFlow Connect |
# MAGIC | `brz_products` | Azure SQL CDC | LakeFlow Connect |
# MAGIC | `brz_sellers` | Azure SQL CDC | LakeFlow Connect |
# MAGIC | `brz_brands` | Azure SQL CDC | LakeFlow Connect |
# MAGIC | `brz_categories` | Azure SQL CDC | LakeFlow Connect |
# MAGIC | `brz_warehouses` | Azure SQL CDC | LakeFlow Connect |
# MAGIC
# MAGIC **Total: 17 Bronze tables across 3 ingestion methods.**
# MAGIC
# MAGIC **Patterns demonstrated:**
# MAGIC - AutoLoader incremental file ingestion (ADLS CSV)
# MAGIC - Kafka structured streaming (Event Hub JSON)
# MAGIC - `append_flow` multi-source merge (ADLS + Event Hub → one table)
# MAGIC - LakeFlow Connect managed CDC (Azure SQL → Bronze)
# MAGIC - Schema evolution mode enabled
# MAGIC - Metadata columns for lineage tracking
