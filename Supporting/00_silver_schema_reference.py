# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Schema Reference
# MAGIC Column names, data types, and row counts for all 17 Silver tables.
# MAGIC Run this any time you need to check what a table looks like before building features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Silver Tables — Row Counts

# COMMAND ----------

tables = spark.catalog.listTables("bharatmart.silver")
table_names = sorted([t.name for t in tables])

print(f"{'Table':<35} {'Row Count':>12}")
print("-" * 50)

counts = {}
for tbl in table_names:
    n = spark.table(f"bharatmart.silver.{tbl}").count()
    counts[tbl] = n
    print(f"{tbl:<35} {n:>12,}")

print("-" * 50)
print(f"{'TOTAL':<35} {sum(counts.values()):>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Silver Tables — Schema (Column Name + Data Type)

# COMMAND ----------

for tbl in table_names:
    print(f"\n{'='*55}")
    print(f"  {tbl}   ({counts[tbl]:,} rows)")
    print(f"{'='*55}")
    df = spark.table(f"bharatmart.silver.{tbl}")
    for field in df.schema.fields:
        nullable = "" if field.nullable else "  NOT NULL"
        print(f"  {field.name:<35} {str(field.dataType):<20}{nullable}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Lookup — Single Table
# MAGIC Change the table name here when you need one specific table.

# COMMAND ----------

# change this to whatever table you want to inspect
TABLE = "slv_orders"

df = spark.table(f"bharatmart.silver.{TABLE}")

print(f"Table: bharatmart.silver.{TABLE}")
print(f"Rows:  {df.count():,}")
print()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Lookup — Sample Rows

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ML Reference — Which Silver Tables Feed Which Features

# COMMAND ----------

print("""
ML Feature Source Map
=====================

slv_orders
  -> recency_days         (MAX order_date per customer)
  -> total_orders         (COUNT order_id per customer)
  -> total_spent          (SUM total_amount per customer)
  -> avg_order_value      (total_spent / total_orders)
  -> days_between_orders  (AVG gap between consecutive orders)
  -> order_trend          (orders last 30d / orders prev 30d)
  -> category_diversity   (COUNT DISTINCT category_id per customer)

slv_refunds
  -> refund_count         (COUNT refund_id per customer)
  -> refund_rate          (refund_count / total_orders)

slv_payments
  -> payment_failure_rate (failed payments / total payments)
  -> preferred_payment    (MODE payment_method per customer)

slv_sessions
  -> session_count_30d    (COUNT sessions in last 30 days)
  -> avg_pages_viewed     (AVG pages_viewed per customer)
  -> channel_preference   (MODE channel per customer)

slv_cart_events
  -> cart_abandon_rate    (1 - purchases / cart_adds per customer)

slv_reviews
  -> avg_rating_given     (AVG rating per customer)
  -> review_sentiment     (COUNT negative / COUNT total per customer)
  -> review_frequency     (COUNT reviews / COUNT orders per customer)
""")
