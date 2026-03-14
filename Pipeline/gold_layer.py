# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema for Power BI (LakeFlow Declarative)
# MAGIC
# MAGIC **What Gold does:**
# MAGIC - Denormalized dimension tables (pre-joined, business-friendly names)
# MAGIC - Clean fact tables with FK references to dimensions
# MAGIC - Generated date dimension for Power BI time intelligence
# MAGIC - All materialized views — recomputed when Silver changes
# MAGIC
# MAGIC **Star schema design:**
# MAGIC ```
# MAGIC          dim_date
# MAGIC              │
# MAGIC dim_customer─┼─dim_product
# MAGIC              │
# MAGIC         fact_orders
# MAGIC              │
# MAGIC dim_seller───┼─dim_warehouse
# MAGIC              │
# MAGIC          dim_geo
# MAGIC ```
# MAGIC
# MAGIC **All Gold tables are materialized views** — `@dp.materialized_view`.
# MAGIC Read from `bharatmart.silver.*` via `dp.read()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, lit, concat, concat_ws, coalesce, trim,
    year, month, dayofmonth, dayofweek, dayofyear, quarter,
    date_format, last_day, trunc, expr,
    when, row_number, count, round as spark_round,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    datediff, months_between, current_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — Dimension Tables (denormalized for Power BI)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date (generated calendar — 2020-01-01 to 2026-12-31)
# MAGIC
# MAGIC Every star schema needs a date dimension. Power BI time intelligence depends on it.
# MAGIC Indian fiscal year runs April–March, so we include that.

# COMMAND ----------

@dp.materialized_view(
    name="dim_date",
    comment="Calendar dimension — time intelligence, Indian fiscal year, exact festive day flags"
)
def dim_date():
    from pyspark.sql.functions import to_date

    start = date(2020, 1, 1)
    end   = date(2026, 12, 31)
    dates = []
    d = start
    while d <= end:
        dates.append((d,))
        d += timedelta(days=1)

    df = spark.createDataFrame(dates, ["date_key"])

    return (df
        .withColumn("year",         year("date_key"))
        .withColumn("month",        month("date_key"))
        .withColumn("day",          dayofmonth("date_key"))
        .withColumn("quarter",      quarter("date_key"))
        .withColumn("day_of_week",  dayofweek("date_key"))
        .withColumn("day_of_year",  dayofyear("date_key"))
        .withColumn("month_name",   date_format("date_key", "MMMM"))
        .withColumn("month_short",  date_format("date_key", "MMM"))
        .withColumn("day_name",     date_format("date_key", "EEEE"))
        .withColumn("year_month",   date_format("date_key", "yyyy-MM"))
        .withColumn("year_quarter", concat(
            year("date_key").cast("string"), lit("-Q"), quarter("date_key").cast("string")
        ))
        .withColumn("is_weekend", when(
            dayofweek("date_key").isin(1, 7), True
        ).otherwise(False))

        # ── Indian fiscal year: Apr–Mar ──
        .withColumn("fiscal_year", when(
            month("date_key") >= 4, year("date_key")
        ).otherwise(year("date_key") - 1))
        .withColumn("fiscal_quarter", when(
            month("date_key").isin(4, 5, 6), 1
        ).when(
            month("date_key").isin(7, 8, 9), 2
        ).when(
            month("date_key").isin(10, 11, 12), 3
        ).otherwise(4))

        # ================================================================
        # EXACT FESTIVE DAY FLAGS
        # All dates match FESTIVAL_DATES in 03c exactly
        # ================================================================

        # ── Diwali — exact date per year ──
        .withColumn("is_diwali", when(col("date_key").isin(
            to_date(lit("2020-11-14")), to_date(lit("2021-11-04")),
            to_date(lit("2022-10-24")), to_date(lit("2023-11-12")),
            to_date(lit("2024-11-01")), to_date(lit("2025-10-20")),
            to_date(lit("2026-11-08"))
        ), True).otherwise(False))

        # ── Dhanteras — 2 days before Diwali ──
        .withColumn("is_dhanteras", when(col("date_key").isin(
            to_date(lit("2020-11-13")), to_date(lit("2021-11-02")),
            to_date(lit("2022-10-22")), to_date(lit("2023-11-10")),
            to_date(lit("2024-10-29")), to_date(lit("2025-10-18")),
            to_date(lit("2026-11-06"))
        ), True).otherwise(False))

        # ── BharatMart Dhamaka Days — 10 days starting dhamaka_start ──
        .withColumn("is_dhamaka_days", when(
            col("date_key").between(to_date(lit("2020-11-02")), to_date(lit("2020-11-11"))) |
            col("date_key").between(to_date(lit("2021-10-23")), to_date(lit("2021-11-01"))) |
            col("date_key").between(to_date(lit("2022-10-12")), to_date(lit("2022-10-21"))) |
            col("date_key").between(to_date(lit("2023-10-31")), to_date(lit("2023-11-09"))) |
            col("date_key").between(to_date(lit("2024-10-20")), to_date(lit("2024-10-29"))) |
            col("date_key").between(to_date(lit("2025-10-08")), to_date(lit("2025-10-17"))) |
            col("date_key").between(to_date(lit("2026-10-27")), to_date(lit("2026-11-05")))
        , True).otherwise(False))

        # ── Navratri — 9 days ──
        .withColumn("is_navratri", when(
            col("date_key").between(to_date(lit("2020-10-17")), to_date(lit("2020-10-25"))) |
            col("date_key").between(to_date(lit("2021-10-07")), to_date(lit("2021-10-15"))) |
            col("date_key").between(to_date(lit("2022-10-02")), to_date(lit("2022-10-10"))) |
            col("date_key").between(to_date(lit("2023-10-15")), to_date(lit("2023-10-23"))) |
            col("date_key").between(to_date(lit("2024-10-03")), to_date(lit("2024-10-11"))) |
            col("date_key").between(to_date(lit("2025-09-22")), to_date(lit("2025-09-30"))) |
            col("date_key").between(to_date(lit("2026-10-11")), to_date(lit("2026-10-19")))
        , True).otherwise(False))

        # ── Holi — exact date per year ──
        .withColumn("is_holi", when(col("date_key").isin(
            to_date(lit("2020-03-09")), to_date(lit("2021-03-28")),
            to_date(lit("2022-03-18")), to_date(lit("2023-03-07")),
            to_date(lit("2024-03-25")), to_date(lit("2025-03-14")),
            to_date(lit("2026-03-04"))
        ), True).otherwise(False))

        # ── Republic Day — Jan 26 every year ──
        .withColumn("is_republic_day", when(
            (month("date_key") == 1) & (dayofmonth("date_key") == 26)
        , True).otherwise(False))

        # ── Independence Day — Aug 15 every year ──
        .withColumn("is_independence_day", when(
            (month("date_key") == 8) & (dayofmonth("date_key") == 15)
        , True).otherwise(False))

        # ── Valentine's Day — Feb 14 every year ──
        .withColumn("is_valentines_day", when(
            (month("date_key") == 2) & (dayofmonth("date_key") == 14)
        , True).otherwise(False))

        # ── Christmas — Dec 25 every year ──
        .withColumn("is_christmas", when(
            (month("date_key") == 12) & (dayofmonth("date_key") == 25)
        , True).otherwise(False))

        # ── New Year — Jan 1 every year ──
        .withColumn("is_new_year", when(
            (month("date_key") == 1) & (dayofmonth("date_key") == 1)
        , True).otherwise(False))

        # ── End of Season Sale — 10 days from Mar 20 and Jun 20 ──
        .withColumn("is_eos_sale", when(
            ((month("date_key") == 3) & (dayofmonth("date_key").between(20, 29))) |
            ((month("date_key") == 6) & (dayofmonth("date_key").between(20, 29)))
        , True).otherwise(False))

        # ── Wedding season — Nov 15 to Apr 30, no Jun–Sep ──
        .withColumn("is_wedding_season", when(
            ((month("date_key") == 11) & (dayofmonth("date_key") >= 15)) |
            month("date_key").isin(12, 1, 2) |
            ((month("date_key") == 3) & (dayofmonth("date_key") >= 15)) |
            ((month("date_key") == 4))
        , True).otherwise(False))

        # ── COVID lockdown — Mar 25 to Jun 7 2020 only ──
        .withColumn("is_covid_lockdown", when(
            col("date_key").between(
                to_date(lit("2020-03-25")), to_date(lit("2020-06-07"))
            )
        , True).otherwise(False))

        # ── is_festive_day — any major event active that day ──
        # useful as a single slicer in Power BI
        .withColumn("is_festive_day", when(
            col("is_diwali")          |
            col("is_dhanteras")       |
            col("is_dhamaka_days")    |
            col("is_navratri")        |
            col("is_holi")            |
            col("is_republic_day")    |
            col("is_independence_day")|
            col("is_valentines_day")  |
            col("is_christmas")       |
            col("is_eos_sale")
        , True).otherwise(False))

        # ── festive_event_name — single label for the active event ──
        # priority order: Diwali > Dhamaka > Navratri > others
        .withColumn("festive_event_name", when(col("is_diwali"),           lit("Diwali"))
            .when(col("is_dhanteras"),        lit("Dhanteras"))
            .when(col("is_dhamaka_days"),     lit("Dhamaka Days"))
            .when(col("is_navratri"),         lit("Navratri"))
            .when(col("is_holi"),             lit("Holi"))
            .when(col("is_republic_day"),     lit("Republic Day"))
            .when(col("is_independence_day"), lit("Independence Day"))
            .when(col("is_valentines_day"),   lit("Valentine's Day"))
            .when(col("is_christmas"),        lit("Christmas"))
            .when(col("is_eos_sale"),         lit("End of Season Sale"))
            .otherwise(lit(None))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer (denormalized with geo)

# COMMAND ----------

@dp.materialized_view(
    name="dim_customer",
    comment="Customer dimension — enriched with geo region for Power BI slicing"
)
def dim_customer():
    customers = spark.table("bharatmart.silver.slv_customers")
    geo = spark.table("bharatmart.silver.slv_geo")

    return (customers
        .join(geo, customers.pincode == geo.pincode, "left")
        .select(
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            concat_ws(" ", customers.first_name, customers.last_name).alias("full_name"),
            customers.email,
            customers.phone,
            customers.date_of_birth,
            customers.gender,
            customers.city,
            customers.state,
            customers.pincode,
            coalesce(geo.region, lit("Unknown")).alias("region"),
            coalesce(geo.latitude, lit(0.0)).alias("latitude"),
            coalesce(geo.longitude, lit(0.0)).alias("longitude"),
            customers.loyalty_tier,
            customers.registration_date,
            customers.is_active,
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_product (denormalized with brand + category)

# COMMAND ----------

@dp.materialized_view(
    name="dim_product",
    comment="Product dimension — enriched with brand name, category name for Power BI"
)
def dim_product():
    products = spark.table("bharatmart.silver.slv_products")
    brands = spark.table("bharatmart.silver.slv_brands")
    categories = spark.table("bharatmart.silver.slv_categories")

    return (products
        .join(brands, "brand_id", "left")
        .join(categories, "category_id", "left")
        .select(
            products.product_id,
            products.product_name,
            products.category_id,
            coalesce(categories.category_name, lit("Unknown")).alias("category_name"),
            coalesce(categories.parent_category, lit("")).alias("parent_category"),
            products.brand_id,
            coalesce(brands.brand_name, lit("Unknown")).alias("brand_name"),
            coalesce(brands.is_premium, lit(False)).alias("is_premium_brand"),
            products.retail_price,
            products.cost_price,
            (products.retail_price - products.cost_price).alias("margin"),
            when(products.retail_price > 0,
                spark_round((products.retail_price - products.cost_price) / products.retail_price * 100, 2)
            ).otherwise(lit(0.0)).alias("margin_pct"),
            products.weight_kg,
            products.stock_quantity,
            products.is_active,
            # price tier for Power BI slicing
            when(products.retail_price < 500, "Budget")
            .when(products.retail_price < 2000, "Mid-Range")
            .when(products.retail_price < 10000, "Premium")
            .otherwise("Luxury").alias("price_tier"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_seller

# COMMAND ----------

@dp.materialized_view(
    name="dim_seller",
    comment="Seller dimension for Power BI"
)
def dim_seller():
    return (spark.table("bharatmart.silver.slv_sellers")
        .select(
            col("seller_id"),
            col("seller_name"),
            col("gstin"),
            col("city").alias("seller_city"),
            col("state").alias("seller_state"),
            col("rating").alias("seller_rating"),
            col("joined_date"),
            col("is_active"),
            # seller tier based on rating
            when(col("rating") >= 4.5, "Platinum")
            .when(col("rating") >= 4.0, "Gold")
            .when(col("rating") >= 3.0, "Silver")
            .otherwise("Bronze").alias("seller_tier"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_warehouse

# COMMAND ----------

@dp.materialized_view(
    name="dim_warehouse",
    comment="Warehouse dimension for Power BI"
)
def dim_warehouse():
    return (spark.table("bharatmart.silver.slv_warehouses")
        .select(
            col("warehouse_id"),
            col("warehouse_name"),
            col("city").alias("warehouse_city"),
            col("state").alias("warehouse_state"),
            col("capacity_sqft"),
            col("is_active"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_geo

# COMMAND ----------

@dp.materialized_view(
    name="dim_geo",
    comment="Geography dimension — standalone for cross-entity geo analysis"
)
def dim_geo():
    return (spark.table("bharatmart.silver.slv_geo")
        .select(
            col("pincode"),
            col("city"),
            col("state"),
            col("region"),
            col("latitude"),
            col("longitude"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 — Fact Tables
# MAGIC
# MAGIC Facts reference dimensions via natural keys (customer_id, product_id, etc.).
# MAGIC Power BI creates relationships on these keys.

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_orders

# COMMAND ----------

@dp.materialized_view(
    name="fact_orders",
    comment="Order fact table — grain: one row per order"
)
def fact_orders():
    return (spark.table("bharatmart.silver.slv_orders")
        .select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("seller_id"),
            col("order_status"),
            col("order_amount"),
            col("freight_value"),
            coalesce(col("discount_amount"), lit(0.0)).alias("discount_amount"),
            coalesce(col("total_amount"),
                col("order_amount") + col("freight_value") - coalesce(col("discount_amount"), lit(0.0))
            ).alias("total_amount"),
            col("order_date"),
            col("estimated_delivery"),
            col("payment_method"),
            col("session_id"),
            col("channel"),
            col("coupon_code"),
            col("delivery_pincode"),
            col("warehouse_id"),
            col("_source"),
            # derived flags for Power BI measures
            when(col("coupon_code").isNotNull() & (col("coupon_code") != ""), True)
                .otherwise(False).alias("used_coupon"),
            when(col("order_status") == "cancelled", True)
                .otherwise(False).alias("is_cancelled"),
            when(col("order_status") == "delivered", True)
                .otherwise(False).alias("is_delivered"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_payments

# COMMAND ----------

@dp.materialized_view(
    name="fact_payments",
    comment="Payment fact table — grain: one row per payment"
)
def fact_payments():
    return (spark.table("bharatmart.silver.slv_payments")
        .select(
            col("payment_id"),
            col("order_id"),
            col("customer_id"),
            col("payment_method"),
            col("payment_amount"),
            col("payment_status"),
            col("payment_date"),
            col("gateway"),
            col("upi_id"),
            col("_source"),
            when(col("payment_status") == "success", True)
                .otherwise(False).alias("is_successful"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_shipments

# COMMAND ----------

@dp.materialized_view(
    name="fact_shipments",
    comment="Shipment fact table — delivery performance metrics"
)
def fact_shipments():
    return (spark.table("bharatmart.silver.slv_shipments")
        .filter(col("_is_ghost_order") == False)
        .select(
            col("shipment_id"),
            col("order_id"),
            col("warehouse_id"),
            col("carrier"),
            col("tracking_number"),
            col("shipped_date"),
            col("estimated_delivery"),
            col("actual_delivery"),
            col("status"),
            col("late_delivery_risk"),
            col("weight_kg"),
            # delivery performance
            datediff(col("actual_delivery"), col("shipped_date")).alias("delivery_days"),
            datediff(col("actual_delivery"), col("estimated_delivery")).alias("delay_days"),
            when(col("actual_delivery") > col("estimated_delivery"), True)
                .otherwise(False).alias("is_late"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_reviews

# COMMAND ----------

@dp.materialized_view(
    name="fact_reviews",
    comment="Review fact table — customer satisfaction metrics"
)
def fact_reviews():
    return (spark.table("bharatmart.silver.slv_reviews")
        .filter(col("_is_ghost_order") == False)
        .select(
            col("review_id"),
            col("order_id"),
            col("product_id"),
            col("customer_id"),
            col("seller_id"),
            col("rating"),
            col("review_text"),
            col("helpful_votes"),
            col("review_date"),
            # sentiment proxy from rating
            when(col("rating") >= 4, "Positive")
            .when(col("rating") == 3, "Neutral")
            .otherwise("Negative").alias("sentiment"),
            when(col("review_text").isNotNull() & (col("review_text") != ""), True)
                .otherwise(False).alias("has_review_text"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_refunds

# COMMAND ----------

@dp.materialized_view(
    name="fact_refunds",
    comment="Refund fact table"
)
def fact_refunds():
    return (spark.table("bharatmart.silver.slv_refunds")
        .filter(col("_is_ghost_order") == False)
        .select(
            col("refund_id"),
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("refund_amount"),
            col("reason"),
            col("status"),
            col("requested_date"),
            col("processed_date"),
            datediff(col("processed_date"), col("requested_date")).alias("processing_days"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_commissions

# COMMAND ----------

@dp.materialized_view(
    name="fact_commissions",
    comment="Seller commission fact table"
)
def fact_commissions():
    return (spark.table("bharatmart.silver.slv_commissions")
        .select(
            col("commission_id"),
            col("seller_id"),
            col("order_id"),
            col("sale_amount"),
            col("commission_rate"),
            col("commission_amount"),
            col("month_year"),
            col("payment_status"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_inventory

# COMMAND ----------

@dp.materialized_view(
    name="fact_inventory",
    comment="Inventory snapshot fact — weekly product × warehouse stock levels"
)
def fact_inventory():
    return (spark.table("bharatmart.silver.slv_inventory")
        .select(
            col("inventory_id"),
            col("product_id"),
            col("warehouse_id"),
            col("quantity_available"),
            col("quantity_reserved"),
            (col("quantity_available") - col("quantity_reserved")).alias("quantity_free"),
            col("reorder_level"),
            when(col("quantity_available") <= col("reorder_level"), True)
                .otherwise(False).alias("needs_reorder"),
            col("last_updated"),
            col("snapshot_date"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_sessions

# COMMAND ----------

@dp.materialized_view(
    name="fact_sessions",
    comment="Session fact table — customer engagement metrics"
)
def fact_sessions():
    return (spark.table("bharatmart.silver.slv_sessions")
        .select(
            col("session_id"),
            col("customer_id"),
            col("event_type"),
            col("channel"),
            col("device_type"),
            col("pages_viewed"),
            col("session_start"),
            col("event_time"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_cart_events

# COMMAND ----------

@dp.materialized_view(
    name="fact_cart_events",
    comment="Cart event fact — add/remove/view actions"
)
def fact_cart_events():
    return (spark.table("bharatmart.silver.slv_cart_events")
        .filter(col("_is_zero_amount") == False)
        .select(
            col("event_id"),
            col("session_id"),
            col("customer_id"),
            col("product_id"),
            col("action"),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("channel"),
            col("event_time"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_campaign_responses

# COMMAND ----------

@dp.materialized_view(
    name="fact_campaign_responses",
    comment="Campaign response fact — marketing effectiveness"
)
def fact_campaign_responses():
    return (spark.table("bharatmart.silver.slv_campaign_responses")
        .select(
            col("response_id"),
            col("campaign_id"),
            col("customer_id"),
            col("campaign_type"),
            col("action"),
            col("category_id"),
            col("event_time"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 — Aggregation Tables (pre-computed for dashboard performance)
# MAGIC
# MAGIC Power BI can query fact+dimension joins directly, but pre-aggregated tables
# MAGIC are faster for high-level KPIs and executive dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_customer_360 (RFM + lifetime stats)
# MAGIC
# MAGIC One row per customer — everything a PM needs for segmentation.

# COMMAND ----------

@dp.materialized_view(
    name="agg_customer_360",
    comment="Customer 360 — RFM scores, lifetime stats, for segmentation and churn analysis"
)
def agg_customer_360():
    orders = spark.table("bharatmart.silver.slv_orders")
    reviews = spark.table("bharatmart.silver.slv_reviews").filter(col("_is_ghost_order") == False)
    refunds = spark.table("bharatmart.silver.slv_refunds").filter(col("_is_ghost_order") == False)

    # order-level metrics per customer
    order_stats = (orders
        .filter(col("order_status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("order_amount"), 2).alias("lifetime_revenue"),
            spark_round(avg("order_amount"), 2).alias("avg_order_value"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date"),
            count(when(col("order_status") == "delivered", True)).alias("delivered_orders"),
        )
    )

    # review metrics
    review_stats = (reviews
        .groupBy("customer_id")
        .agg(
            count("review_id").alias("total_reviews"),
            spark_round(avg("rating"), 2).alias("avg_rating"),
        )
    )

    # refund metrics
    refund_stats = (refunds
        .groupBy("customer_id")
        .agg(
            count("refund_id").alias("total_refunds"),
            spark_round(spark_sum("refund_amount"), 2).alias("total_refund_amount"),
        )
    )

    return (order_stats
        .join(review_stats, "customer_id", "left")
        .join(refund_stats, "customer_id", "left")
        .select(
            col("customer_id"),
            col("total_orders"),
            coalesce(col("lifetime_revenue"), lit(0.0)).alias("lifetime_revenue"),
            coalesce(col("avg_order_value"), lit(0.0)).alias("avg_order_value"),
            col("first_order_date"),
            col("last_order_date"),
            col("delivered_orders"),
            datediff(current_date(), col("last_order_date")).alias("days_since_last_order"),
            coalesce(col("total_reviews"), lit(0)).alias("total_reviews"),
            coalesce(col("avg_rating"), lit(0.0)).alias("avg_rating"),
            coalesce(col("total_refunds"), lit(0)).alias("total_refunds"),
            coalesce(col("total_refund_amount"), lit(0.0)).alias("total_refund_amount"),
            # refund rate
            when(col("total_orders") > 0,
                spark_round(coalesce(col("total_refunds"), lit(0)) / col("total_orders") * 100, 2)
            ).otherwise(lit(0.0)).alias("refund_rate_pct"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_daily_sales (daily revenue summary)

# COMMAND ----------

@dp.materialized_view(
    name="agg_daily_sales",
    comment="Daily sales aggregation — revenue, orders, AOV by date"
)
def agg_daily_sales():
    return (spark.table("bharatmart.silver.slv_orders")
        .filter(col("order_date").isNotNull())
        .filter(col("order_status") != "cancelled")
        .groupBy("order_date")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("order_amount"), 2).alias("total_revenue"),
            spark_round(avg("order_amount"), 2).alias("avg_order_value"),
            count(when(col("order_status") == "delivered", True)).alias("delivered_orders"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg_seller_performance (seller scorecards)

# COMMAND ----------

@dp.materialized_view(
    name="agg_seller_performance",
    comment="Seller performance — revenue, orders, avg rating, commission"
)
def agg_seller_performance():
    orders = spark.table("bharatmart.silver.slv_orders").filter(col("order_status") != "cancelled")
    reviews = spark.table("bharatmart.silver.slv_reviews").filter(col("_is_ghost_order") == False)
    commissions = spark.table("bharatmart.silver.slv_commissions")

    order_metrics = (orders
        .filter(col("seller_id").isNotNull())
        .groupBy("seller_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("order_amount"), 2).alias("total_revenue"),
        )
    )

    review_metrics = (reviews
        .groupBy("seller_id")
        .agg(
            spark_round(avg("rating"), 2).alias("avg_rating"),
            count("review_id").alias("total_reviews"),
        )
    )

    commission_metrics = (commissions
        .groupBy("seller_id")
        .agg(
            spark_round(spark_sum("commission_amount"), 2).alias("total_commission")
        )
    )

    return (order_metrics
        .join(review_metrics, "seller_id", "left")
        .join(commission_metrics, "seller_id", "left")
        .select(
            col("seller_id"),
            col("total_orders"),
            coalesce(col("total_revenue"), lit(0.0)).alias("total_revenue"),
            coalesce(col("avg_rating"), lit(0.0)).alias("avg_rating"),
            coalesce(col("total_reviews"), lit(0)).alias("total_reviews"),
            coalesce(col("total_commission"), lit(0.0)).alias("total_commission"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary
# MAGIC
# MAGIC ## Gold Tables — 20 total
# MAGIC
# MAGIC ### Dimensions (6)
# MAGIC | Table | Source Silver Tables | Key Enrichment |
# MAGIC |-------|---------------------|----------------|
# MAGIC | `dim_date` | Generated | Fiscal year, weekends, sale seasons |
# MAGIC | `dim_customer` | slv_customers + slv_geo | Region, coordinates from geo |
# MAGIC | `dim_product` | slv_products + slv_brands + slv_categories | Brand name, category name, price tier |
# MAGIC | `dim_seller` | slv_sellers | Seller tier from rating |
# MAGIC | `dim_warehouse` | slv_warehouses | Direct pass-through |
# MAGIC | `dim_geo` | slv_geo | Direct pass-through |
# MAGIC
# MAGIC ### Facts (11)
# MAGIC | Table | Source | Grain | Key Metrics |
# MAGIC |-------|--------|-------|-------------|
# MAGIC | `fact_orders` | slv_orders | 1 row / order | order_amount, total_amount, is_delivered |
# MAGIC | `fact_payments` | slv_payments | 1 row / payment | payment_amount, is_successful |
# MAGIC | `fact_shipments` | slv_shipments | 1 row / shipment | delivery_days, delay_days, is_late |
# MAGIC | `fact_reviews` | slv_reviews | 1 row / review | rating, sentiment, has_text |
# MAGIC | `fact_refunds` | slv_refunds | 1 row / refund | refund_amount, processing_days |
# MAGIC | `fact_commissions` | slv_commissions | 1 row / commission | commission_amount, rate |
# MAGIC | `fact_inventory` | slv_inventory | 1 row / snapshot | quantity_free, needs_reorder |
# MAGIC | `fact_sessions` | slv_sessions | 1 row / session | pages_viewed, channel |
# MAGIC | `fact_cart_events` | slv_cart_events | 1 row / event | total_amount, action |
# MAGIC | `fact_campaign_responses` | slv_campaign_responses | 1 row / response | campaign_type, action |
# MAGIC
# MAGIC ### Aggregations (3)
# MAGIC | Table | Purpose | Grain |
# MAGIC |-------|---------|-------|
# MAGIC | `agg_customer_360` | RFM + lifetime stats | 1 row / customer |
# MAGIC | `agg_daily_sales` | Daily revenue KPIs | 1 row / date |
# MAGIC | `agg_seller_performance` | Seller scorecards | 1 row / seller |
# MAGIC
# MAGIC **Total: 6 dimensions + 11 facts + 3 aggregations = 20 Gold tables**
