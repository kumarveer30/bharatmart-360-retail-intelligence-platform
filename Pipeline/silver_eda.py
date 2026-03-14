# Databricks notebook source
# MAGIC %md
# MAGIC # 07a — Silver EDA
# MAGIC
# MAGIC Read every Bronze table. Measure every dirty data type that Silver fixes.
# MAGIC Numbers here should match Silver's expectation counts after the pipeline runs.
# MAGIC
# MAGIC No writes. Run on personal compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, when, lit, to_date, to_timestamp,
    min, max, avg, length, trim,
    row_number, sum as spark_sum, expr
)
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def null_report(df, name):
    total = df.count()
    print(f"\n{name} — {total:,} rows")
    print(f"  {'column':<35} {'nulls':>8}  {'%':>6}")
    print("  " + "-" * 52)
    for c in df.columns:
        if c.startswith("_"):
            continue
        n = df.filter(col(c).isNull()).count()
        pct = n / total * 100
        flag = " ←" if pct > 0 else ""
        print(f"  {c:<35} {n:>8,}  {pct:>5.1f}%{flag}")

# COMMAND ----------

def dupe_check(df, pk, label):
    total  = df.count()
    unique = df.select(pk).distinct().count()
    dupes  = total - unique
    pct    = dupes / total * 100
    print(f"{label}: total={total:,}  unique={unique:,}  dupes={dupes:,}  ({pct:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — Dimension Tables (Azure SQL via 04b)
# MAGIC
# MAGIC Silver trims, type casts, dedups as safety net.

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_customers

# COMMAND ----------

cust = spark.read.table("bharatmart.bronze.brz_customers")
print(f"rows: {cust.count():,}")

# COMMAND ----------

display(cust.limit(5))

# COMMAND ----------

null_report(cust, "brz_customers")

# COMMAND ----------

dupe_check(cust, "customer_id", "brz_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_products

# COMMAND ----------

prod = spark.read.table("bharatmart.bronze.brz_products")
print(f"rows: {prod.count():,}")

# COMMAND ----------

null_report(prod, "brz_products")

# COMMAND ----------

dupe_check(prod, "product_id", "brz_products")

# COMMAND ----------

# retail_price stored as varchar — Silver casts to double
bad_price = prod.filter(
    col("retail_price").isNotNull() &
    expr("try_cast(retail_price as double) is null")
).count()
print(f"retail_price not castable to double: {bad_price:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_sellers

# COMMAND ----------

sellers = spark.read.table("bharatmart.bronze.brz_sellers")
print(f"rows: {sellers.count():,}")

# COMMAND ----------

null_report(sellers, "brz_sellers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_brands / brz_categories / brz_warehouses

# COMMAND ----------

for tbl in ["brz_brands", "brz_categories", "brz_warehouses"]:
    df = spark.read.table(f"bharatmart.bronze.{tbl}")
    print(f"{tbl}: {df.count():,} rows — no dirty data expected")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 — brz_orders (Dual Source)
# MAGIC
# MAGIC ADLS rows and EH rows in same table with different columns.
# MAGIC Silver coalesces: order_amount↔subtotal, freight_value↔delivery_charge.

# COMMAND ----------

orders = spark.read.table("bharatmart.bronze.brz_orders")
print(f"rows: {orders.count():,}")

# COMMAND ----------

display(orders.groupBy("_source").count())

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NULL

# COMMAND ----------

adls = orders.filter(col("_source") == "adls_historical")
total_adls = adls.count()
null_cust = adls.filter(col("customer_id").isNull()).count()
print(f"ADLS orders   : {total_adls:,}")
print(f"NULL customer : {null_cust:,}  ({null_cust/total_adls*100:.2f}%)")
print(f"Silver        : @dp.expect warns — row kept")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicates

# COMMAND ----------

dupe_check(orders, "order_id", "brz_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date format

# COMMAND ----------

good = adls.filter(to_date(col("order_date"), "yyyy-MM-dd").isNotNull()).count()
bad  = adls.filter(
    to_date(col("order_date"), "yyyy-MM-dd").isNull() &
    col("order_date").isNotNull()
).count()
print(f"YYYY-MM-DD : {good:,}  ({good/total_adls*100:.1f}%)")
print(f"other fmt  : {bad:,}  ({bad/total_adls*100:.1f}%)  ← Silver parse_date() fixes these")

# COMMAND ----------

display(adls.filter(
    to_date(col("order_date"), "yyyy-MM-dd").isNull() &
    col("order_date").isNotNull()
).select("order_id", "order_date").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zero total_amount

# COMMAND ----------

# DBTITLE 1,Cell 35
eh = orders.filter(col("_source") == "eventhub_live")
total_eh  = eh.count()
zero_amt  = eh.filter(col("total_amount") == "0.0").count()
print(f"EH orders       : {total_eh:,}")
print(f"zero total_amt  : {zero_amt:,}  ({zero_amt/(total_eh if total_eh > 0 else 1)*100:.2f}%)")
print(f"Silver : flagged as _is_zero_amount, not dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## payment_status

# COMMAND ----------

# DBTITLE 1,Cell 37
# 03c drops payment_status before upload
adls_with_ps = 0 if "payment_status" not in adls.columns else adls.filter(col("payment_status").isNotNull()).count()
print(f"ADLS rows with non-null payment_status: {adls_with_ps:,}")
print(f"Expected: 0  —  {'CONFIRMED ✓' if adls_with_ps == 0 else 'UNEXPECTED — investigate'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS vs EH column mismatch - coalesce targets

# COMMAND ----------

display(orders.select(
    spark_sum(when(col("order_amount").isNotNull(), 1).otherwise(0)).alias("order_amount_ADLS"),
    spark_sum(when(col("subtotal").isNotNull(), 1).otherwise(0)).alias("subtotal_EH"),
    spark_sum(when(col("freight_value").isNotNull(), 1).otherwise(0)).alias("freight_value_ADLS"),
    spark_sum(when(col("delivery_charge").isNotNull(), 1).otherwise(0)).alias("delivery_charge_EH"),
    spark_sum(when(col("order_date").isNotNull(), 1).otherwise(0)).alias("order_date_ADLS"),
    spark_sum(when(col("order_timestamp").isNotNull(), 1).otherwise(0)).alias("order_timestamp_EH"),
))

# COMMAND ----------

# EH null delivery_pincode (T1 ~5%)
null_pin = eh.filter(col("delivery_pincode").isNull()).count()
print(f"EH null delivery_pincode: {null_pin:,}  ({null_pin / __builtins__.max(total_eh, 1) * 100:.2f}%)")

# COMMAND ----------

display(orders.groupBy("order_status").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 — brz_payments (Dual Source)
# MAGIC
# MAGIC Silver coalesces: payment_amount↔amount, payment_status↔status, payment_date↔payment_time

# COMMAND ----------

pays = spark.read.table("bharatmart.bronze.brz_payments")
print(f"rows: {pays.count():,}")

# COMMAND ----------

display(pays.groupBy("_source").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS vs EH column pattern

# COMMAND ----------

display(pays.select(
    spark_sum(when(col("payment_amount").isNotNull(), 1).otherwise(0)).alias("payment_amount_ADLS"),
    spark_sum(when(col("amount").isNotNull(), 1).otherwise(0)).alias("amount_EH"),
    spark_sum(when(col("payment_status").isNotNull(), 1).otherwise(0)).alias("payment_status_ADLS"),
    spark_sum(when(col("status").isNotNull(), 1).otherwise(0)).alias("status_EH"),
    spark_sum(when(col("payment_date").isNotNull(), 1).otherwise(0)).alias("payment_date_ADLS"),
    spark_sum(when(col("payment_time").isNotNull(), 1).otherwise(0)).alias("payment_time_EH"),
    spark_sum(when(col("gateway").isNotNull(), 1).otherwise(0)).alias("gateway_ADLS"),
    spark_sum(when(col("upi_id").isNotNull(), 1).otherwise(0)).alias("upi_id_EH"),
))

# COMMAND ----------

dupe_check(pays, "payment_id", "brz_payments")

# COMMAND ----------

# T4 zero amount in EH rows
eh_pays  = pays.filter(col("_source") == "eventhub_live")
zero_pay = eh_pays.filter(col("amount") == "0.0").count()


# COMMAND ----------

import builtins
 
print(f"EH zero amount: {zero_pay:,}  ({zero_pay/builtins.max(eh_pays.count(),1)*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4 — brz_shipments

# COMMAND ----------

ships = spark.read.table("bharatmart.bronze.brz_shipments")
print(f"rows: {ships.count():,}")

# COMMAND ----------

display(ships.limit(3))

# COMMAND ----------

dupe_check(ships, "shipment_id", "brz_shipments")

# COMMAND ----------

# T7 ghost order_ids
total_ships = ships.count()
ghost       = ships.filter(col("order_id").startswith("ORD_GHOST_")).count()
print(f"ghost order_ids: {ghost:,}  ({ghost/total_ships*100:.2f}%)  ← Silver flags _is_ghost_order")

# COMMAND ----------

display(ships.groupBy("status").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 5 — brz_reviews

# COMMAND ----------

revs = spark.read.table("bharatmart.bronze.brz_reviews")
print(f"rows: {revs.count():,}")

# COMMAND ----------

null_report(revs, "brz_reviews")

# COMMAND ----------

dupe_check(revs, "review_id", "brz_reviews")

# COMMAND ----------

# T3 review_date — 100% should be DD-MM-YYYY
total_revs  = revs.count()
dd_mm_yyyy  = revs.filter(
    to_date(col("review_date"), "dd-MM-yyyy").isNotNull() &
    to_date(col("review_date"), "yyyy-MM-dd").isNull()
).count()
yyyy_mm_dd  = revs.filter(to_date(col("review_date"), "yyyy-MM-dd").isNotNull()).count()
print(f"DD-MM-YYYY : {dd_mm_yyyy:,}  ({dd_mm_yyyy/total_revs*100:.1f}%)  ← expected ~100%")
print(f"YYYY-MM-DD : {yyyy_mm_dd:,}  ({yyyy_mm_dd/total_revs*100:.1f}%)  ← expected 0%")

# COMMAND ----------

display(revs.select("review_id", "review_date").limit(5))

# COMMAND ----------

# T7 ghost order_ids
ghost_rev = revs.filter(col("order_id").startswith("ORD_GHOST_")).count()
print(f"ghost order_ids: {ghost_rev:,}  ({ghost_rev/total_revs*100:.2f}%)")

# COMMAND ----------

# NULL review_text — not a problem, Silver keeps as-is
null_text = revs.filter(col("review_text").isNull()).count()
print(f"NULL review_text: {null_text:,}  ({null_text/total_revs*100:.1f}%)  ← kept, not filled")

# COMMAND ----------

display(revs.groupBy("rating").count().orderBy("rating"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 6 — brz_refunds

# COMMAND ----------

refunds = spark.read.table("bharatmart.bronze.brz_refunds")
print(f"rows: {refunds.count():,}")

# COMMAND ----------

dupe_check(refunds, "refund_id", "brz_refunds")

# COMMAND ----------

ghost_ref = refunds.filter(col("order_id").startswith("ORD_GHOST_")).count()
print(f"ghost order_ids: {ghost_ref:,}  ({ghost_ref/refunds.count()*100:.2f}%)")

# COMMAND ----------

display(refunds.groupBy("status").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 7 — brz_commissions

# COMMAND ----------

comms = spark.read.table("bharatmart.bronze.brz_commissions")
print(f"rows: {comms.count():,}")

# COMMAND ----------

dupe_check(comms, "commission_id", "brz_commissions")

# COMMAND ----------

null_report(comms, "brz_commissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 8 — brz_inventory

# COMMAND ----------

inv = spark.read.table("bharatmart.bronze.brz_inventory")
print(f"rows: {inv.count():,}")

# COMMAND ----------

dupe_check(inv, "inventory_id", "brz_inventory")

# COMMAND ----------

display(inv.select("last_updated").limit(5))

# COMMAND ----------

# T3 last_updated format
parseable = inv.filter(
    to_timestamp(col("last_updated"), "dd-MM-yyyy HH:mm").isNotNull()
).count()
total_inv = inv.count()
print(f"DD-MM-YYYY HH:MM parseable: {parseable:,} / {total_inv:,}  ({parseable/total_inv*100:.1f}%)")
print(f"Silver fix: parse_timestamp()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 9 — Event Hub Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_sessions

# COMMAND ----------

sess = spark.read.table("bharatmart.bronze.brz_sessions")
print(f"rows: {sess.count():,}")

# COMMAND ----------

null_report(sess, "brz_sessions")

# COMMAND ----------

# T3 timestamps as plain string
display(sess.select("session_start", "event_time").limit(3))

# COMMAND ----------

parseable_ts = sess.filter(
    to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss").isNotNull()
).count()
print(f"event_time parseable: {parseable_ts:,} / {sess.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_cart_events

# COMMAND ----------

cart = spark.read.table("bharatmart.bronze.brz_cart_events")
print(f"rows: {cart.count():,}")

# COMMAND ----------

null_report(cart, "brz_cart_events")

# COMMAND ----------

# T4 zero total_amount
total_cart = cart.count()
zero_cart  = cart.filter(col("total_amount") == 0.0).count()
print(f"zero total_amount: {zero_cart:,}  ({zero_cart/total_cart*100:.2f}%)  ← Silver flags _is_zero_amount")

# COMMAND ----------

display(cart.groupBy("action").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## brz_campaign_responses

# COMMAND ----------

camp = spark.read.table("bharatmart.bronze.brz_campaign_responses")
print(f"rows: {camp.count():,}")

# COMMAND ----------

null_report(camp, "brz_campaign_responses")

# COMMAND ----------

display(camp.groupBy("campaign_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 10 — brz_geo_dimension

# COMMAND ----------

geo = spark.read.table("bharatmart.bronze.brz_geo_dimension")
print(f"rows: {geo.count():,}")

# COMMAND ----------

null_report(geo, "brz_geo_dimension")

# COMMAND ----------

display(geo.groupBy("region").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary

# COMMAND ----------

print("""
EDA FINDINGS — maps to Silver pipeline (05_silver_transform.py)
================================================================

T1 — NULL customer_id
  brz_orders ADLS: ~2% NULL  →  Silver @dp.expect warns, row kept

T2 — Duplicates (~1%)
  orders, payments, shipments, reviews, refunds, commissions, inventory
  Silver: dedup() by primary key, keep latest _ingested_at

T3 — Wrong date formats
  order_date   ~15% DD-MM-YYYY  →  parse_date() tries YYYY-MM-DD, falls back to DD-MM-YYYY
  review_date  100% DD-MM-YYYY  →  parse_date() handles this
  last_updated DD-MM-YYYY HH:MM →  parse_timestamp() handles this
  EH timestamps plain string    →  parse_timestamp() handles this

T4 — Zero amounts
  EH orders  ~0.5% zero total_amount  →  _is_zero_amount flag
  EH cart    ~0.5% zero total_amount  →  _is_zero_amount flag
  EH payments ~0.5% zero amount       →  @dp.expect warns

T7 — Ghost order_ids (~3%)
  shipments, reviews, refunds  →  _is_ghost_order flag, rows kept

payment_status NULL for all ADLS rows
  03c drops it before upload — confirmed 0 non-null ADLS rows
  Silver coalesce(payment_status, status) → NULL for all ADLS rows

Dual-source reconciliation
  orders  : coalesce(order_amount, subtotal)
            coalesce(freight_value, delivery_charge)
  payments: coalesce(payment_amount, amount)
            coalesce(payment_status, status)

NULL review_text (~7%) — valid data, Silver keeps as-is

No issues: brz_geo_dimension, brz_brands, brz_categories, brz_warehouses, brz_sellers
""")
