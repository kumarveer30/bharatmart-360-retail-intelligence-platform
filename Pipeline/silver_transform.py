# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Clean, Validate, Enrich (LakeFlow Declarative)
# MAGIC
# MAGIC **What Silver does:**
# MAGIC - Type casting (Bronze stores everything as strings for dual-source)
# MAGIC - Deduplication (T2 dirty data: ~1% dupes injected in 03c)
# MAGIC - Date parsing (T3: mixed YYYY-MM-DD and DD-MM-YYYY formats)
# MAGIC - NULL handling (T1: ~2% NULL customer_ids in orders)
# MAGIC - Ghost FK flagging (T7: ~3% phantom order_ids in shipments/reviews/refunds)
# MAGIC - Column reconciliation (ADLS vs Event Hub have different column names for orders/payments)
# MAGIC - Data quality expectations (warn / drop / fail)
# MAGIC
# MAGIC **Every Bronze table gets a Silver table. No exceptions.**
# MAGIC
# MAGIC **All Silver tables are materialized views** — batch recompute from Bronze snapshots.
# MAGIC Bronze streaming tables are read in batch mode via `dp.read()`.
# MAGIC
# MAGIC **Pipeline config:** Separate pipeline from Bronze.
# MAGIC Silver pipeline reads from `bharatmart.bronze.*` and writes to `bharatmart.silver.*`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, lit, when, round as spark_round,
    to_date, to_timestamp, current_timestamp, regexp_replace,
    row_number, count, sum as spark_sum, avg, max as spark_max
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: robust date parser
# MAGIC
# MAGIC 03c injects T3 dirty data — 15% of order_date flipped to DD-MM-YYYY,
# MAGIC 100% of review_date is DD-MM-YYYY. This handles both formats safely.

# COMMAND ----------

def parse_date(column):
    """Try YYYY-MM-DD first, fall back to DD-MM-YYYY."""
    return coalesce(
        to_date(col(column), "yyyy-MM-dd"),
        to_date(col(column), "dd-MM-yyyy")
    )

def parse_timestamp(column):
    """Try ISO timestamp first, fall back to DD-MM-YYYY HH:mm."""
    return coalesce(
        to_timestamp(col(column), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(column), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(column), "dd-MM-yyyy HH:mm"),
        to_timestamp(col(column))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: dedup by primary key
# MAGIC
# MAGIC Keeps the latest row per PK based on _ingested_at.
# MAGIC Handles T2 dirty data (exact duplicates injected at ~1%).

# COMMAND ----------

def dedup(df, pk_col):
    """Keep latest row per primary key. Removes T2 dupes."""
    w = Window.partitionBy(pk_col).orderBy(col("_ingested_at").desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — Dimension Tables (from Azure SQL via Bronze)
# MAGIC
# MAGIC These 6 tables came from Azure SQL (03a) into Bronze via JDBC/LakeFlow Connect.
# MAGIC Clean data — no dirty injection. Just trim strings, enforce types, dedup as safety net.

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_customers

# COMMAND ----------

@dp.materialized_view(
    name="slv_customers",
    comment="Cleaned customer dimension — from Azure SQL CDC"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email", "email IS NOT NULL")
def slv_customers():
    return (spark.table("bharatmart.bronze.brz_customers")
        .select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            trim(col("email")).alias("email"),
            trim(col("phone_number")).alias("phone"),
            to_date(col("date_of_birth")).alias("date_of_birth"),
            trim(col("gender")).alias("gender"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("pincode")).alias("pincode"),
            trim(col("loyalty_tier")).alias("loyalty_tier"),
            to_date(col("registration_date")).alias("registration_date"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_products

# COMMAND ----------

@dp.materialized_view(
    name="slv_products",
    comment="Cleaned product dimension — from Azure SQL CDC"
)
@dp.expect("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_category", "category_id IS NOT NULL")
@dp.expect("positive_cost", "cost_price > 0")
def slv_products():
    return (spark.table("bharatmart.bronze.brz_products")
        .select(
            trim(col("product_id")).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category_id")).alias("category_id"),
            trim(col("brand_id")).alias("brand_id"),
            regexp_replace(col("retail_price"), "[^0-9.]", "").cast("double").alias("retail_price"),
            col("cost_price").cast("double").alias("cost_price"),
            col("weight_kg").cast("double").alias("weight_kg"),
            col("stock_quantity").cast("int").alias("stock_quantity"),
            trim(col("unit_size")).alias("unit_size"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["product_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_sellers

# COMMAND ----------

@dp.materialized_view(
    name="slv_sellers",
    comment="Cleaned seller dimension — from Azure SQL CDC"
)
@dp.expect("valid_seller_id", "seller_id IS NOT NULL")
def slv_sellers():
    return (spark.table("bharatmart.bronze.brz_sellers")
        .select(
            trim(col("seller_id")).alias("seller_id"),
            trim(col("seller_name")).alias("seller_name"),
            trim(col("gstin")).alias("gstin"),
            trim(col("email")).alias("email"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("phone")).alias("phone"),
            col("rating").cast("double").alias("rating"),
            to_date(col("joined_date")).alias("joined_date"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["seller_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_brands

# COMMAND ----------

@dp.materialized_view(
    name="slv_brands",
    comment="Cleaned brand dimension — from Azure SQL CDC"
)
@dp.expect("valid_brand_id", "brand_id IS NOT NULL")
def slv_brands():
    return (spark.table("bharatmart.bronze.brz_brands")
        .select(
            trim(col("brand_id")).alias("brand_id"),
            trim(col("brand_name")).alias("brand_name"),
            trim(col("country_origin")).alias("country_origin"),
            col("is_premium").cast("boolean").alias("is_premium"),
            col("established_year").cast("int").alias("established_year"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["brand_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_categories

# COMMAND ----------

@dp.materialized_view(
    name="slv_categories",
    comment="Cleaned category dimension — from Azure SQL CDC"
)
@dp.expect("valid_category_id", "category_id IS NOT NULL")
def slv_categories():
    return (spark.table("bharatmart.bronze.brz_categories")
        .select(
            trim(col("category_id")).alias("category_id"),
            trim(col("category_name")).alias("category_name"),
            trim(col("parent_category")).alias("parent_category"),
            col("level").cast("int").alias("level"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["category_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_warehouses

# COMMAND ----------

@dp.materialized_view(
    name="slv_warehouses",
    comment="Cleaned warehouse dimension — from Azure SQL CDC"
)
@dp.expect("valid_warehouse_id", "warehouse_id IS NOT NULL")
def slv_warehouses():
    return (spark.table("bharatmart.bronze.brz_warehouses")
        .select(
            trim(col("warehouse_id")).alias("warehouse_id"),
            trim(col("warehouse_name")).alias("warehouse_name"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            col("capacity_sqft").cast("int").alias("capacity_sqft"),
            col("is_active").cast("boolean").alias("is_active"),
        )
        .dropDuplicates(["warehouse_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 - Fact Tables (from ADLS + Event Hub Bronze)
# MAGIC
# MAGIC These tables have dirty data injected. Silver must fix:
# MAGIC - T1: NULL customer_ids (~2%) → drop row
# MAGIC - T2: Exact duplicates (~1%) → dedup by PK
# MAGIC - T3: Mixed date formats (YYYY-MM-DD + DD-MM-YYYY) → robust parser
# MAGIC - T4: Zero amounts from Event Hub → flag, keep for analysis
# MAGIC - T7: Ghost order_ids (~3% in shipments/reviews/refunds) → flag, keep

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_orders (DUAL SOURCE - ADLS + Event Hub reconciled)
# MAGIC
# MAGIC **The hardest Silver table.** Two sources with completely different schemas:
# MAGIC
# MAGIC | Field | ADLS (03c) | Event Hub (03b) |
# MAGIC |-------|-----------|-----------------|
# MAGIC | Amount | order_amount | subtotal |
# MAGIC | Shipping | freight_value | delivery_charge |
# MAGIC | Date | order_date | order_timestamp |
# MAGIC | Items | one row per item (product_id col) | JSON array (order_items) |
# MAGIC | Payment | payment_method, payment_status | (separate table) |
# MAGIC
# MAGIC Strategy: COALESCE matching fields, keep source-specific fields as-is.

# COMMAND ----------

@dp.materialized_view(
    name="slv_orders",
    comment="Cleaned, reconciled orders — ADLS historical + Event Hub live merged"
)
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect("has_customer", "customer_id IS NOT NULL")
@dp.expect("positive_amount", "order_amount > 0")
def slv_orders():
    raw = spark.table("bharatmart.bronze.brz_orders")

    reconciled = raw.select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        # ADLS has product_id directly; EH has it inside order_items JSON
        trim(col("product_id")).alias("product_id"),
        trim(col("seller_id")).alias("seller_id"),
        trim(col("order_status")).alias("order_status"),

        # amount reconciliation: ADLS=order_amount, EH=subtotal
        coalesce(
            col("order_amount").cast("double"),
            col("subtotal").cast("double")
        ).alias("order_amount"),

        # shipping: ADLS=freight_value, EH=delivery_charge
        coalesce(
            col("freight_value").cast("double"),
            col("delivery_charge").cast("double"),
            lit(0.0)
        ).alias("freight_value"),

        col("discount_amount").cast("double").alias("discount_amount"),
        col("total_amount").cast("double").alias("total_amount"),

        # date reconciliation: ADLS=order_date (T3 mixed format), EH=order_timestamp
        coalesce(
            parse_date("order_date"),
            to_date(col("order_timestamp"))
        ).alias("order_date"),

        parse_date("estimated_delivery").alias("estimated_delivery"),

        # ADLS-only fields
        trim(col("payment_method")).alias("payment_method"),

        # EH-only fields
        trim(col("session_id")).alias("session_id"),
        trim(col("channel")).alias("channel"),
        trim(col("coupon_code")).alias("coupon_code"),
        trim(col("delivery_pincode")).alias("delivery_pincode"),
        trim(col("warehouse_id")).alias("warehouse_id"),
        col("order_items").alias("order_items_json"),

        # lineage
        col("_source"),
        col("_ingested_at"),
    )

    # dedup: keep latest per order_id
    w = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
    deduped = (reconciled
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    return deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_payments (DUAL SOURCE - ADLS + Event Hub reconciled)
# MAGIC
# MAGIC | Field | ADLS (03c) | Event Hub (03b) |
# MAGIC |-------|-----------|-----------------|
# MAGIC | Amount | payment_amount | amount |
# MAGIC | Status | payment_status | status |
# MAGIC | Date | payment_date | payment_time |
# MAGIC | Gateway | gateway | (not present) |
# MAGIC | UPI | (not present) | upi_id |

# COMMAND ----------

@dp.materialized_view(
    name="slv_payments",
    comment="Cleaned, reconciled payments — ADLS historical + Event Hub live merged"
)
@dp.expect_or_drop("valid_payment_id", "payment_id IS NOT NULL")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect("positive_amount", "payment_amount > 0")
def slv_payments():
    raw = spark.table("bharatmart.bronze.brz_payments")

    reconciled = raw.select(
        trim(col("payment_id")).alias("payment_id"),
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("payment_method")).alias("payment_method"),

        # amount: ADLS=payment_amount, EH=amount
        coalesce(
            col("payment_amount").cast("double"),
            col("amount").cast("double")
        ).alias("payment_amount"),

        # status: ADLS=payment_status, EH=status
        coalesce(
            trim(col("payment_status")),
            trim(col("status"))
        ).alias("payment_status"),

        # date: ADLS=payment_date, EH=payment_time
        coalesce(
            parse_date("payment_date"),
            to_date(col("payment_time"))
        ).alias("payment_date"),

        # source-specific fields
        trim(col("gateway")).alias("gateway"),
        trim(col("upi_id")).alias("upi_id"),

        col("_source"),
        col("_ingested_at"),
    )

    # dedup by payment_id
    w = Window.partitionBy("payment_id").orderBy(col("_ingested_at").desc())
    return (reconciled
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_shipments
# MAGIC
# MAGIC Dirty data: T2 dupes (1%), T7 ghost order_ids (3%).
# MAGIC Ghost orders are flagged, not dropped - useful for data quality dashboards.

# COMMAND ----------

@dp.materialized_view(
    name="slv_shipments",
    comment="Cleaned shipments — ghost order_ids flagged, dupes removed"
)
@dp.expect_or_drop("valid_shipment_id", "shipment_id IS NOT NULL")
@dp.expect("valid_order_ref", "order_id IS NOT NULL")
@dp.expect("delivery_after_ship", "actual_delivery IS NULL OR actual_delivery >= shipped_date")
def slv_shipments():
    raw = spark.table("bharatmart.bronze.brz_shipments")

    cleaned = raw.select(
        trim(col("shipment_id")).alias("shipment_id"),
        trim(col("order_id")).alias("order_id"),
        trim(col("warehouse_id")).alias("warehouse_id"),
        trim(col("carrier")).alias("carrier"),
        trim(col("tracking_number")).alias("tracking_number"),
        parse_date("shipped_date").alias("shipped_date"),
        parse_date("estimated_delivery").alias("estimated_delivery"),
        parse_date("actual_delivery").alias("actual_delivery"),
        trim(col("status")).alias("status"),
        col("late_delivery_risk").cast("boolean").alias("late_delivery_risk"),
        col("weight_kg").cast("double").alias("weight_kg"),
        # T7 flag: ghost order_ids start with ORD_GHOST_
        when(col("order_id").startswith("ORD_GHOST_"), lit(True))
            .otherwise(lit(False)).alias("_is_ghost_order"),
        col("_ingested_at"),
    )

    return dedup(cleaned, "shipment_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_reviews
# MAGIC
# MAGIC Dirty data: T2 dupes (1%), T3 date format (100% DD-MM-YYYY), T7 ghost order_ids (3%).

# COMMAND ----------

@dp.materialized_view(
    name="slv_reviews",
    comment="Cleaned reviews — dates parsed from DD-MM-YYYY, ghosts flagged"
)
@dp.expect_or_drop("valid_review_id", "review_id IS NOT NULL")
@dp.expect("valid_rating", "rating BETWEEN 1 AND 5")
def slv_reviews():
    raw = spark.table("bharatmart.bronze.brz_reviews")

    cleaned = raw.select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("seller_id")).alias("seller_id"),
        col("rating").cast("int").alias("rating"),
        trim(col("review_text")).alias("review_text"),
        col("helpful_votes").cast("int").alias("helpful_votes"),
        # 100% of review_date is DD-MM-YYYY from 03c flip_date_format
        parse_date("review_date").alias("review_date"),
        when(col("order_id").startswith("ORD_GHOST_"), lit(True))
            .otherwise(lit(False)).alias("_is_ghost_order"),
        col("_ingested_at"),
    )

    return dedup(cleaned, "review_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_refunds
# MAGIC
# MAGIC Dirty data: T2 dupes (1%), T7 ghost order_ids (3%).

# COMMAND ----------

@dp.materialized_view(
    name="slv_refunds",
    comment="Cleaned refunds — ghosts flagged, dupes removed"
)
@dp.expect_or_drop("valid_refund_id", "refund_id IS NOT NULL")
@dp.expect("positive_refund", "refund_amount > 0")
def slv_refunds():
    raw = spark.table("bharatmart.bronze.brz_refunds")

    cleaned = raw.select(
        trim(col("refund_id")).alias("refund_id"),
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("product_id")).alias("product_id"),
        col("refund_amount").cast("double").alias("refund_amount"),
        trim(col("reason")).alias("reason"),
        trim(col("status")).alias("status"),
        parse_date("requested_date").alias("requested_date"),
        parse_date("processed_date").alias("processed_date"),
        when(col("order_id").startswith("ORD_GHOST_"), lit(True))
            .otherwise(lit(False)).alias("_is_ghost_order"),
        col("_ingested_at"),
    )

    return dedup(cleaned, "refund_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_commissions
# MAGIC
# MAGIC Dirty data: T2 dupes (1%) only.

# COMMAND ----------

@dp.materialized_view(
    name="slv_commissions",
    comment="Cleaned seller commissions — dupes removed"
)
@dp.expect_or_drop("valid_commission_id", "commission_id IS NOT NULL")
@dp.expect("valid_rate", "commission_rate BETWEEN 0 AND 1")
@dp.expect("positive_amount", "commission_amount >= 0")
def slv_commissions():
    raw = spark.table("bharatmart.bronze.brz_commissions")

    cleaned = raw.select(
        trim(col("commission_id")).alias("commission_id"),
        trim(col("seller_id")).alias("seller_id"),
        trim(col("order_id")).alias("order_id"),
        col("sale_amount").cast("double").alias("sale_amount"),
        col("commission_rate").cast("double").alias("commission_rate"),
        col("commission_amount").cast("double").alias("commission_amount"),
        trim(col("month_year")).alias("month_year"),
        trim(col("payment_status")).alias("payment_status"),
        col("_ingested_at"),
    )

    return dedup(cleaned, "commission_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 - Event Tables (from Event Hub Bronze)
# MAGIC
# MAGIC These came from 03b event producer. Dirty data: T3 string timestamps, T4 zero amounts.
# MAGIC Small volume (~1K-2K rows) but proper streaming architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_sessions

# COMMAND ----------

@dp.materialized_view(
    name="slv_sessions",
    comment="Cleaned sessions — 03c historical + 03b live, unified schema"
)
@dp.expect_or_drop("valid_session_id", "session_id IS NOT NULL")
@dp.expect("valid_customer",           "customer_id IS NOT NULL")
def slv_sessions():
    raw = spark.table("bharatmart.bronze.brz_sessions")

    cleaned = raw.select(
        trim(col("session_id")).alias("session_id"),
        trim(col("customer_id")).alias("customer_id"),

        # 03b has event_type="session_start"
        # 03c has converted=1/0 — derive event_type from it
        coalesce(
            trim(col("event_type")),
            when(col("converted").cast("int") == 1, lit("session_start"))
            .otherwise(lit("session_bounce"))
        ).alias("event_type"),

        trim(col("channel")).alias("channel"),
        trim(col("device_type")).alias("device_type"),

        # utm_source from 03c — not in 03b
        trim(col("utm_source")).alias("utm_source"),

        col("pages_viewed").cast("int").alias("pages_viewed"),

        # 03c has session_duration_seconds — 03b does not
        col("session_duration_seconds").cast("int").alias("session_duration_seconds"),

        # session_start — both sources have this
        parse_timestamp("session_start").alias("session_start"),

        # session_end — 03c only
        parse_timestamp("session_end").alias("session_end"),

        # event_time — 03b only. For 03c use session_start as fallback
        coalesce(
            parse_timestamp("event_time"),
            parse_timestamp("session_start")
        ).alias("event_time"),

        # converted flag — 03c only (1=purchased, 0=bounced)
        coalesce(
            col("converted").cast("int"),
            lit(1)   # 03b sessions always result in purchase
        ).alias("converted"),

        # session_date — 03c only. For 03b derive from session_start
        coalesce(
            parse_date("session_date"),
            to_date(col("session_start"))
        ).alias("session_date"),

        col("_source"),
        col("_ingested_at"),
    )

    return cleaned.dropDuplicates(["session_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_cart_events
# MAGIC
# MAGIC T4 dirty: some total_amount = 0. Flagged, not dropped.

# COMMAND ----------

@dp.materialized_view(
    name="slv_cart_events",
    comment="Cleaned cart events — 03c historical + 03b live, unified schema"
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect("has_product",            "product_id IS NOT NULL")
def slv_cart_events():
    raw = spark.table("bharatmart.bronze.brz_cart_events")

    cleaned = raw.select(
        # 03b = event_id, 03c = cart_event_id
        coalesce(
            trim(col("event_id")),
            trim(col("cart_event_id"))
        ).alias("event_id"),

        trim(col("session_id")).alias("session_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("product_id")).alias("product_id"),

        # 03b = action, 03c = event_type
        coalesce(
            trim(col("action")),
            trim(col("event_type"))
        ).alias("action"),

        col("quantity").cast("int").alias("quantity"),

        # 03b = unit_price, 03c = price_at_event
        coalesce(
            col("unit_price").cast("double"),
            col("price_at_event").cast("double")
        ).alias("unit_price"),

        # 03b = total_amount, 03c = price_at_event (no separate total)
        coalesce(
            col("total_amount").cast("double"),
            col("price_at_event").cast("double")
        ).alias("total_amount"),

        # channel — 03b has it, 03c does not
        coalesce(
            trim(col("channel")),
            lit("organic_search")   # 03c default — most historical traffic
        ).alias("channel"),

        # event_time — 03b = event_time, 03c = event_timestamp
        coalesce(
            parse_timestamp("event_time"),
            parse_timestamp("event_timestamp")
        ).alias("event_time"),

        # event_date — 03c only. For 03b derive from event_time
        coalesce(
            parse_date("event_date"),
            to_date(col("event_time"))
        ).alias("event_date"),

        # T4 flag: zero amounts from DC.maybe_zero_amount (03b)
        # or price_at_event = 0 (edge case in 03c)
        when(
            coalesce(col("total_amount").cast("double"),
                     col("price_at_event").cast("double")) == 0.0,
            lit(True)
        ).otherwise(lit(False)).alias("_is_zero_amount"),

        col("_source"),
        col("_ingested_at"),
    )

    return cleaned.dropDuplicates(["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_campaign_responses

# COMMAND ----------

@dp.materialized_view(
    name="slv_campaign_responses",
    comment="Cleaned campaign responses — 03c historical + 03b live, unified schema"
)
@dp.expect_or_drop("valid_response_id", "response_id IS NOT NULL")
@dp.expect("valid_customer",            "customer_id IS NOT NULL")
def slv_campaign_responses():
    raw = spark.table("bharatmart.bronze.brz_campaign_responses")

    cleaned = raw.select(
        trim(col("response_id")).alias("response_id"),

        # campaign_id — 03b has it, 03c does not
        # for 03c rows generate a placeholder so Gold doesn't get nulls
        coalesce(
            trim(col("campaign_id")),
            lit("HIST_CAMPAIGN")
        ).alias("campaign_id"),

        trim(col("customer_id")).alias("customer_id"),

        # campaign_type — 03b has campaign_type, 03c has channel
        coalesce(
            trim(col("campaign_type")),
            trim(col("channel"))
        ).alias("campaign_type"),

        # action — 03b has action string (sent/delivered/opened/clicked/converted)
        # 03c has boolean columns: sent, opened, clicked, converted
        # derive action from highest funnel stage reached
        coalesce(
            trim(col("action")),
            when(col("converted").cast("int") == 1, lit("converted"))
            .when(col("clicked").cast("int")   == 1, lit("clicked"))
            .when(col("opened").cast("int")    == 1, lit("opened"))
            .when(col("sent").cast("int")      == 1, lit("sent"))
            .otherwise(lit("sent"))
        ).alias("action"),

        # category_id — 03b has it, 03c does not
        trim(col("category_id")).alias("category_id"),

        # order_id — 03c has it (for converted responses), 03b does not
        trim(col("order_id")).alias("order_id"),

        # coupon_code — 03c has it, 03b does not
        trim(col("coupon_code")).alias("coupon_code"),

        # boolean funnel columns from 03c — useful for ML features
        coalesce(col("sent").cast("int"),      lit(1)).alias("was_sent"),
        coalesce(col("opened").cast("int"),    lit(0)).alias("was_opened"),
        coalesce(col("clicked").cast("int"),   lit(0)).alias("was_clicked"),
        coalesce(col("converted").cast("int"), lit(0)).alias("was_converted"),

        # event_time — 03b = event_time, 03c = response_date / campaign_date
        coalesce(
            parse_timestamp("event_time"),
            parse_timestamp("response_date"),
            parse_timestamp("campaign_date")
        ).alias("event_time"),

        col("_source"),
        col("_ingested_at"),
    )

    return cleaned.dropDuplicates(["response_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4 - Reference & Snapshot Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_inventory
# MAGIC
# MAGIC Weekly snapshots. last_updated uses DD-MM-YYYY HH:MM format from 03c.

# COMMAND ----------

@dp.materialized_view(
    name="slv_inventory",
    comment="Cleaned weekly inventory snapshots"
)
@dp.expect_or_drop("valid_inventory_id", "inventory_id IS NOT NULL")
@dp.expect("reserved_lte_available", "quantity_reserved <= quantity_available")
def slv_inventory():
    return (spark.table("bharatmart.bronze.brz_inventory")
        .select(
            trim(col("inventory_id")).alias("inventory_id"),
            trim(col("product_id")).alias("product_id"),
            trim(col("warehouse_id")).alias("warehouse_id"),
            col("quantity_available").cast("int").alias("quantity_available"),
            col("quantity_reserved").cast("int").alias("quantity_reserved"),
            col("reorder_level").cast("int").alias("reorder_level"),
            parse_timestamp("last_updated").alias("last_updated"),
            parse_date("snapshot_date").alias("snapshot_date"),
        )
        .dropDuplicates(["inventory_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### slv_geo

# COMMAND ----------

@dp.materialized_view(
    name="slv_geo",
    comment="Cleaned geo reference — pincode, city, state, region, coordinates"
)
@dp.expect_or_drop("valid_pincode", "pincode IS NOT NULL")
def slv_geo():
    return (spark.table("bharatmart.bronze.brz_geo_dimension")
        .select(
            trim(col("pincode")).alias("pincode"),
            trim(col("city")).alias("city"),
            trim(col("state")).alias("state"),
            trim(col("region")).alias("region"),
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
        )
        .dropDuplicates(["pincode"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC ## Silver Tables — 17 total
# MAGIC
# MAGIC | Table | Source Bronze | Key Transforms | Expectations |
# MAGIC |-------|-------------|----------------|--------------|
# MAGIC | `slv_customers` | `brz_customers` | trim, type cast, dedup | valid_id, valid_email |
# MAGIC | `slv_products` | `brz_products` | trim, type cast, dedup | valid_id, valid_category, positive_cost |
# MAGIC | `slv_sellers` | `brz_sellers` | trim, type cast, dedup | valid_id |
# MAGIC | `slv_brands` | `brz_brands` | trim, type cast, dedup | valid_id |
# MAGIC | `slv_categories` | `brz_categories` | trim, type cast, dedup | valid_id |
# MAGIC | `slv_warehouses` | `brz_warehouses` | trim, type cast, dedup | valid_id |
# MAGIC | `slv_orders` | `brz_orders` | **dual-source reconcile**, date parse, dedup | valid_id (drop), has_customer, positive_amount |
# MAGIC | `slv_payments` | `brz_payments` | **dual-source reconcile**, date parse, dedup | valid_ids (drop), positive_amount |
# MAGIC | `slv_shipments` | `brz_shipments` | date parse, dedup, ghost flag | valid_id (drop), delivery_after_ship |
# MAGIC | `slv_reviews` | `brz_reviews` | DD-MM-YYYY parse, dedup, ghost flag | valid_id (drop), valid_rating |
# MAGIC | `slv_refunds` | `brz_refunds` | date parse, dedup, ghost flag | valid_id (drop), positive_refund |
# MAGIC | `slv_commissions` | `brz_commissions` | type cast, dedup | valid_id (drop), valid_rate, positive_amount |
# MAGIC | `slv_sessions` | `brz_sessions` | timestamp parse, dedup | valid_id (drop), valid_customer |
# MAGIC | `slv_cart_events` | `brz_cart_events` | type cast, zero flag, dedup | valid_id (drop), has_product |
# MAGIC | `slv_campaign_responses` | `brz_campaign_responses` | timestamp parse, dedup | valid_id (drop), valid_customer |
# MAGIC | `slv_inventory` | `brz_inventory` | type cast, timestamp parse, dedup | valid_id (drop), reserved_lte_available |
# MAGIC | `slv_geo` | `brz_geo_dimension` | type cast, dedup | valid_pincode (drop) |
# MAGIC
# MAGIC ## Dirty Data Handling
# MAGIC
# MAGIC | Dirty Type | What 03c/03b Injected | Silver Fix |
# MAGIC |-----------|----------------------|------------|
# MAGIC | T1 NULL customer_id | 2% of orders | Expectation warns (not dropped — recoverable downstream) |
# MAGIC | T2 Duplicates | 1% across all entities | `dedup()` by primary key, keep latest |
# MAGIC | T3 Date formats | 15% order_date, 100% review_date | `parse_date()` tries YYYY-MM-DD then DD-MM-YYYY |
# MAGIC | T4 Zero amounts | EH cart events, order totals | `_is_zero_amount` flag column |
# MAGIC | T7 Ghost FKs | 3% in shipments/reviews/refunds | `_is_ghost_order` flag column |
# MAGIC
