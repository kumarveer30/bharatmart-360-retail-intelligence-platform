# Databricks notebook source
# MAGIC %md
# MAGIC # 03d — Weekly Dependent Data Generator
# MAGIC
# MAGIC 03b pushes live orders + payments to Event Hub → Bronze picks them up.
# MAGIC But 03b does NOT generate shipments, reviews, refunds, or commissions.
# MAGIC
# MAGIC This notebook runs weekly as a Databricks Job:
# MAGIC 1. Reads recent orders from `brz_orders` (Event Hub source, last N days)
# MAGIC 2. Checks watermark — skips orders already processed
# MAGIC 3. Generates dependent tables with all 13 cardinality rules
# MAGIC 4. Writes CSV to ADLS landing/ → AutoLoader picks them up in Bronze
# MAGIC
# MAGIC **Same 13 rules as 03c:**
# MAGIC 1. Customer can't order before registration_date
# MAGIC 2. Failed payment → order cancelled
# MAGIC 3. Only DELIVERED orders get reviews
# MAGIC 4. Review date > actual delivery date
# MAGIC 5. Max 2 reviews per (customer, product)
# MAGIC 6. Only delivered/shipped orders get refunds
# MAGIC 7. Refund reason matches shipment context
# MAGIC 8. No commission on cancelled orders
# MAGIC 9. Commission rate varies by category
# MAGIC 10. Reduced commission on refunded orders
# MAGIC 11. Shipment weight ≈ product weight
# MAGIC 12. quantity_reserved ≤ quantity_available
# MAGIC 13. Full temporal chain preserved
# MAGIC
# MAGIC **Schedule:** Weekly via Databricks Job (or run manually after 03b batches)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("catalog", "bharatmart", "Catalog name")
dbutils.widgets.text("lookback_days", "1", "Days to look back for new orders")
dbutils.widgets.text("adls_path", "abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/landing", "ADLS landing path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widget values

# COMMAND ----------

CATALOG       = dbutils.widgets.get("catalog")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
ADLS_LANDING  = dbutils.widgets.get("adls_path")

print(f"catalog:       {CATALOG}")
print(f"lookback days: {LOOKBACK_DAYS}")
print(f"ADLS landing:  {ADLS_LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import random
import time
from datetime import date, datetime, timedelta
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — Read Recent Orders + Reference Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch new Event Hub orders from Bronze

# COMMAND ----------

cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
print(f"looking for Event Hub orders since: {cutoff}")

recent_orders = (spark.read.table(f"{CATALOG}.bronze.brz_orders")
    .filter(F.col("_source") == "eventhub_live")
    .filter(F.col("_ingested_at") >= cutoff)
)

order_count = recent_orders.count()
print(f"found {order_count} recent Event Hub orders")

if order_count == 0:
    print("no new orders — nothing to do")
    dbutils.notebook.exit("NO_NEW_ORDERS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check watermark — skip already-processed orders
# MAGIC
# MAGIC We keep a simple Delta table that tracks which order_ids have been processed.
# MAGIC First run creates the table. Subsequent runs only process new orders.

# COMMAND ----------

WATERMARK_TABLE = f"{CATALOG}.bronze._03d_watermark"

# create watermark table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        order_id STRING,
        processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT '03d watermark — tracks which orders have dependent data generated'
""")

already_processed = spark.read.table(WATERMARK_TABLE).select("order_id")
print(f"already processed: {already_processed.count()} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter to only new orders

# COMMAND ----------

new_orders = recent_orders.join(already_processed, "order_id", "left_anti")
new_count = new_orders.count()
print(f"new orders to process: {new_count}")

if new_count == 0:
    print("all recent orders already processed — nothing to do")
    dbutils.notebook.exit("ALL_PROCESSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect orders to driver
# MAGIC
# MAGIC Small batch from 03b (~500 per run) so toPandas is safe.

# COMMAND ----------

orders_pdf = new_orders.toPandas()
print(f"collected {len(orders_pdf)} orders to driver")
display(orders_pdf.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load product reference from Bronze

# COMMAND ----------

products_df = spark.read.table(f"{CATALOG}.bronze.brz_products")

# build product_info dict — product_id → {seller_id, category_id, weight_kg}
product_rows = products_df.select("product_id", "seller_id", "category_id", "weight_kg").collect()

product_info = {}
for r in product_rows:
    product_info[r["product_id"]] = {
        "seller_id":    r["seller_id"],
        "category_id":  r["category_id"],
        "weight_kg":    float(r["weight_kg"]) if r["weight_kg"] else 1.0,
    }

print(f"loaded {len(product_info)} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load category → parent mapping

# COMMAND ----------

categories_df = spark.read.table(f"{CATALOG}.bronze.brz_categories")
cat_rows = categories_df.select("category_id", "parent_category").collect()
cat_parent = {r["category_id"]: r["parent_category"] for r in cat_rows}
print(f"loaded {len(cat_parent)} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load warehouse IDs

# COMMAND ----------

warehouses_df = spark.read.table(f"{CATALOG}.bronze.brz_warehouses")
warehouse_ids = [r["warehouse_id"] for r in warehouses_df.select("warehouse_id").collect()]
print(f"loaded {len(warehouse_ids)} warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Commission rate helper (Rule 9)

# COMMAND ----------

COMMISSION_RATES = {
    "Electronics":             (0.04, 0.08),
    "Fashion":                 (0.12, 0.20),
    "Home & Kitchen":          (0.08, 0.15),
    "Sports & Outdoors":       (0.08, 0.14),
    "Books & Media":           (0.05, 0.10),
    "Beauty & Personal Care":  (0.10, 0.18),
    "Grocery & Gourmet":       (0.02, 0.05),
    "Toys & Games":            (0.08, 0.14),
    "Automotive":              (0.06, 0.10),
    "Health & Wellness":       (0.08, 0.14),
}

def get_commission_rate(category_id):
    parent = cat_parent.get(category_id, "")
    lo, hi = COMMISSION_RATES.get(parent, (0.05, 0.12))
    return round(random.uniform(lo, hi), 4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review text pools
# MAGIC
# MAGIC Simple Indian English review templates by rating.
# MAGIC For weekly batches we don't need DS-3 — templates are fine.

# COMMAND ----------

POSITIVE_REVIEWS = [
    "Very happy with this purchase. Delivered on time and good quality.",
    "Excellent product! Packaging was neat and delivery was quick.",
    "Worth every rupee. Would definitely buy again.",
    "Amazing quality for the price. Highly recommended!",
    "Best purchase this month. Family loved it.",
    "Product is exactly as described. Very satisfied customer.",
    "Superb quality and fast shipping. Five stars!",
    "Great value for money. Will order more soon.",
]

NEUTRAL_REVIEWS = [
    "Product is okay. Nothing special but does the job.",
    "Average quality. Expected slightly better for the price.",
    "Decent product. Delivery was a bit slow though.",
    "It works fine. Not great not bad.",
    "Okay purchase. Meets basic expectations.",
]

NEGATIVE_REVIEWS = [
    "Not happy with the quality. Feels cheap.",
    "Product doesn't match the description. Disappointing.",
    "Very poor quality. Will not order again.",
    "Waste of money. Returning this immediately.",
    "Delivery was extremely late and product was damaged.",
    "Terrible experience. Customer support was unhelpful too.",
]

def pick_review_text(rating):
    if random.random() < 0.15:
        return None
    if rating >= 4:
        return random.choice(POSITIVE_REVIEWS)
    elif rating == 3:
        return random.choice(NEUTRAL_REVIEWS)
    else:
        return random.choice(NEGATIVE_REVIEWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review dedup tracker (Rule 5)

# COMMAND ----------

review_tracker = {}

def can_review(cust_id, prod_id):
    key = (cust_id, prod_id)
    return review_tracker.get(key, 0) < 2

def mark_reviewed(cust_id, prod_id):
    key = (cust_id, prod_id)
    review_tracker[key] = review_tracker.get(key, 0) + 1

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 — Generator Functions
# MAGIC
# MAGIC Same logic as 03c but reads from the orders we already have
# MAGIC (no order generation — those came from Event Hub via 03b).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive shipments
# MAGIC
# MAGIC No cancelled orders. Weight from product. Temporal chain.

# COMMAND ----------

CARRIERS = ["Delhivery", "BlueDart", "DTDC", "Ekart", "Shadowfax", "XpressBees", "India Post"]

_shp_counter = [0]

def derive_shipments(orders_pdf):
    # 03b order_status field
    shippable = orders_pdf[orders_pdf["order_status"] != "cancelled"].copy()
    if len(shippable) == 0:
        return []

    rows = []
    for _, o in shippable.iterrows():
        prod_id = o.get("product_id")

        # try to get product_id from order_items JSON if not a direct column
        if not prod_id or str(prod_id) == "nan":
            try:
                import json
                items = json.loads(o.get("order_items", "[]"))
                if items:
                    prod_id = items[0].get("product_id")
            except:
                prod_id = None

        # weight from product (Rule 11)
        prod_weight = product_info.get(prod_id, {}).get("weight_kg", 1.0)
        ship_weight = round(prod_weight * random.uniform(1.0, 1.3), 2)

        # dates (Rule 13)
        order_ts = str(o.get("order_timestamp", o.get("order_date", "")))
        try:
            o_date = datetime.strptime(order_ts[:10], "%Y-%m-%d").date()
        except:
            o_date = date.today() - timedelta(days=random.randint(1, 7))

        shipped_date    = o_date + timedelta(days=random.randint(0, 2))
        days_to_deliver = random.randint(2, 7)
        est_delivery    = shipped_date + timedelta(days=days_to_deliver)

        if o.get("order_status") == "delivered":
            actual_delivery = shipped_date + timedelta(days=days_to_deliver + random.randint(-1, 3))
            actual_delivery = max(actual_delivery, shipped_date + timedelta(days=1))
            status = "delivered"
        elif o.get("order_status") == "shipped":
            actual_delivery = None
            status = "in_transit"
        else:
            actual_delivery = shipped_date + timedelta(days=days_to_deliver)
            status = random.choices(["delivered", "in_transit", "delayed"], weights=[70, 20, 10])[0]
            if status != "delivered":
                actual_delivery = None

        late_risk = 1 if (actual_delivery and actual_delivery > est_delivery) else 0

        _shp_counter[0] += 1

        rows.append({
            "shipment_id":        f"SHP{_shp_counter[0]:010d}",
            "order_id":           o["order_id"],
            "warehouse_id":       o.get("warehouse_id", random.choice(warehouse_ids)),
            "carrier":            random.choice(CARRIERS),
            "tracking_number":    f"TRK{random.randint(100000000, 999999999)}",
            "shipped_date":       shipped_date.strftime("%Y-%m-%d"),
            "estimated_delivery": est_delivery.strftime("%Y-%m-%d"),
            "actual_delivery":    actual_delivery.strftime("%Y-%m-%d") if actual_delivery else None,
            "status":             status,
            "late_delivery_risk": late_risk,
            "weight_kg":          ship_weight,
        })

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive reviews
# MAGIC
# MAGIC Rule 3: only delivered. Rule 4: after delivery. Rule 5: max 2 per (cust, prod).

# COMMAND ----------

_rev_counter = [0]

def derive_reviews(orders_pdf, shipments, review_rate=0.30):
    delivered = orders_pdf[orders_pdf["order_status"] == "delivered"].copy()
    if len(delivered) == 0:
        return []

    # map order_id → actual_delivery
    delivery_dates = {}
    for sh in shipments:
        if sh.get("actual_delivery"):
            delivery_dates[sh["order_id"]] = sh["actual_delivery"]

    n_reviews = max(1, int(len(delivered) * review_rate))
    candidates = delivered.sample(n=min(n_reviews, len(delivered)))

    rows = []
    for _, o in candidates.iterrows():
        cust_id = o["customer_id"]

        # extract product_id
        prod_id = o.get("product_id")
        if not prod_id or str(prod_id) == "nan":
            try:
                import json
                items = json.loads(o.get("order_items", "[]"))
                if items:
                    prod_id = items[0].get("product_id")
            except:
                continue

        seller_id = product_info.get(prod_id, {}).get("seller_id", "UNKNOWN")

        # Rule 5: max 2 reviews per (customer, product)
        if not can_review(cust_id, prod_id):
            continue

        rating = random.choices([5, 4, 3, 2, 1], weights=[35, 30, 15, 12, 8])[0]

        # late delivery → lower rating
        delivery_str = delivery_dates.get(o["order_id"])
        delivery_dt = None
        if delivery_str:
            try:
                delivery_dt = datetime.strptime(str(delivery_str), "%Y-%m-%d").date()
                est_str = str(o.get("estimated_delivery", ""))
                if est_str:
                    est_dt = datetime.strptime(est_str[:10], "%Y-%m-%d").date()
                    if delivery_dt > est_dt and rating > 2:
                        rating = max(1, rating - random.choice([0, 1, 1, 2]))
            except:
                pass

        review_text = pick_review_text(rating)

        # Rule 4: review after delivery
        order_ts = str(o.get("order_timestamp", o.get("order_date", "")))
        try:
            o_date = datetime.strptime(order_ts[:10], "%Y-%m-%d").date()
        except:
            o_date = date.today() - timedelta(days=5)

        if delivery_dt:
            review_date = delivery_dt + timedelta(days=random.randint(1, 15))
        else:
            review_date = o_date + timedelta(days=random.randint(10, 25))

        mark_reviewed(cust_id, prod_id)
        _rev_counter[0] += 1

        rows.append({
            "review_id":     f"REV{_rev_counter[0]:010d}",
            "order_id":      o["order_id"],
            "product_id":    prod_id,
            "customer_id":   cust_id,
            "seller_id":     seller_id,
            "rating":        rating,
            "review_text":   review_text,
            "helpful_votes": random.randint(0, 30),
            "review_date":   review_date.strftime("%Y-%m-%d"),
        })

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive refunds
# MAGIC
# MAGIC Rule 6: delivered/shipped only. Rule 7: context-aware reason.

# COMMAND ----------

REFUND_STATUSES = ["processed", "approved", "requested", "rejected"]
REFUND_WEIGHTS  = [55, 25, 15, 5]

REASONS_DELIVERED = ["damaged_product", "wrong_item", "quality_issue", "changed_mind"]
REASONS_DELAYED   = ["not_received", "damaged_product", "wrong_item"]
REASONS_DEFAULT   = ["damaged_product", "wrong_item", "quality_issue", "changed_mind"]

_ref_counter = [0]

def derive_refunds(orders_pdf, shipments, refund_rate=0.05):
    eligible = orders_pdf[orders_pdf["order_status"].isin(["delivered", "shipped"])].copy()
    if len(eligible) == 0:
        return []

    ship_status = {sh["order_id"]: sh["status"] for sh in shipments}

    n_refunds = max(1, int(len(eligible) * refund_rate))
    refunded = eligible.sample(n=min(n_refunds, len(eligible)))

    rows = []
    for _, o in refunded.iterrows():
        order_ts = str(o.get("order_timestamp", o.get("order_date", "")))
        try:
            o_date = datetime.strptime(order_ts[:10], "%Y-%m-%d").date()
        except:
            o_date = date.today() - timedelta(days=5)

        # Rule 7: reason from shipment context
        s_status = ship_status.get(o["order_id"], "delivered")
        if s_status == "delayed":
            reason = random.choice(REASONS_DELAYED)
        elif s_status == "delivered":
            reason = random.choice(REASONS_DELIVERED)
        else:
            reason = random.choice(REASONS_DEFAULT)

        status = random.choices(REFUND_STATUSES, weights=REFUND_WEIGHTS)[0]

        order_amt = float(o.get("total_amount", o.get("order_amount", 500)))
        refund_pct = random.uniform(0.5, 1.0)
        refund_amount = round(order_amt * refund_pct, 2)

        requested = o_date + timedelta(days=random.randint(5, 20))
        processed = None
        if status in ("processed", "approved"):
            processed = (requested + timedelta(days=random.randint(2, 10))).strftime("%Y-%m-%d")

        # extract product_id (might be inside order_items JSON)
        prod_id = o.get("product_id")
        if not prod_id or str(prod_id) == "nan":
            try:
                import json
                items = json.loads(o.get("order_items", "[]"))
                if items:
                    prod_id = items[0].get("product_id")
            except:
                prod_id = None

        _ref_counter[0] += 1

        rows.append({
            "refund_id":      f"REF{_ref_counter[0]:010d}",
            "order_id":       o["order_id"],
            "customer_id":    o["customer_id"],
            "product_id":     prod_id,
            "refund_amount":  refund_amount,
            "reason":         reason,
            "status":         status,
            "requested_date": requested.strftime("%Y-%m-%d"),
            "processed_date": processed,
        })

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive commissions
# MAGIC
# MAGIC Rule 8: no cancelled. Rule 9: rate by category. Rule 10: reduced if refunded.

# COMMAND ----------

_com_counter = [0]

def derive_commissions(orders_pdf, refunded_order_ids):
    active = orders_pdf[orders_pdf["order_status"] != "cancelled"].copy()
    if len(active) == 0:
        return []

    rows = []
    for _, o in active.iterrows():
        sale_amount = float(o.get("total_amount", o.get("order_amount", 500)))

        prod_id = o.get("product_id")
        if not prod_id or str(prod_id) == "nan":
            try:
                import json
                items = json.loads(o.get("order_items", "[]"))
                if items:
                    prod_id = items[0].get("product_id")
            except:
                prod_id = None

        cat_id = product_info.get(prod_id, {}).get("category_id", "")
        seller_id = product_info.get(prod_id, {}).get("seller_id", o.get("seller_id", "UNKNOWN"))
        commission_rate = get_commission_rate(cat_id)

        # Rule 10: halved if refunded
        if o["order_id"] in refunded_order_ids:
            commission_rate = round(commission_rate * 0.5, 4)

        commission_amt = round(sale_amount * commission_rate, 2)

        order_ts = str(o.get("order_timestamp", o.get("order_date", "")))
        try:
            o_date = datetime.strptime(order_ts[:10], "%Y-%m-%d").date()
        except:
            o_date = date.today()

        _com_counter[0] += 1

        rows.append({
            "commission_id":     f"COM{_com_counter[0]:010d}",
            "seller_id":         seller_id,
            "order_id":          o["order_id"],
            "sale_amount":       sale_amount,
            "commission_rate":   commission_rate,
            "commission_amount": commission_amt,
            "month_year":        f"{o_date.year}-{o_date.month:02d}",
            "payment_status":    random.choices(["paid", "pending", "disputed"], weights=[80, 15, 5])[0],
        })

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dirty data helpers
# MAGIC
# MAGIC Same T1/T2/T3/T7 as 03c — Silver expects these.

# COMMAND ----------

import pandas as pd

def inject_dupes(df, rate=0.01):
    n = max(1, int(len(df) * rate))
    dupes = df.sample(n=min(n, len(df)), replace=True)
    return pd.concat([df, dupes], ignore_index=True)

def inject_ghost_ids(df, col="order_id", rate=0.03):
    if col not in df.columns:
        return df
    for i in range(len(df)):
        if random.random() < rate:
            df.loc[i, col] = f"ORD_GHOST_{random.randint(100000, 999999)}"
    return df

def flip_date_format(df, col, rate=0.15):
    if col not in df.columns:
        return df
    for i in range(len(df)):
        if random.random() < rate:
            try:
                d = datetime.strptime(str(df.loc[i, col]), "%Y-%m-%d")
                df.loc[i, col] = d.strftime("%d-%m-%Y")
            except:
                pass
    return df

print("dirty data helpers ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 — Generate + Upload

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate all dependent tables

# COMMAND ----------

gen_start = time.time()

# 1. shipments
shipments = derive_shipments(orders_pdf)
print(f"shipments: {len(shipments)}")

# 2. reviews
reviews = derive_reviews(orders_pdf, shipments, review_rate=0.30)
print(f"reviews:   {len(reviews)}")

# 3. refunds
refunds = derive_refunds(orders_pdf, shipments, refund_rate=0.05)
print(f"refunds:   {len(refunds)}")

# 4. commissions
refunded_oids = {r["order_id"] for r in refunds}
commissions = derive_commissions(orders_pdf, refunded_oids)
print(f"commissions: {len(commissions)}")

gen_time = time.time() - gen_start
print(f"\ngeneration took {gen_time:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick cardinality checks

# COMMAND ----------

cancelled_ids = set(orders_pdf[orders_pdf["order_status"] == "cancelled"]["order_id"])
delivered_ids = set(orders_pdf[orders_pdf["order_status"] == "delivered"]["order_id"])

# check 1: no shipments for cancelled orders
bad_ships = [s for s in shipments if s["order_id"] in cancelled_ids]
print(f"shipments for cancelled orders: {len(bad_ships)} (should be 0)")

# check 2: reviews only for delivered
bad_revs = [r for r in reviews if r["order_id"] not in delivered_ids]
print(f"reviews for non-delivered: {len(bad_revs)} (should be 0)")

# check 3: no commissions for cancelled
bad_coms = [c for c in commissions if c["order_id"] in cancelled_ids]
print(f"commissions for cancelled: {len(bad_coms)} (should be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to DataFrames

# COMMAND ----------

ships_pdf = pd.DataFrame(shipments) if shipments else pd.DataFrame()
revs_pdf  = pd.DataFrame(reviews) if reviews else pd.DataFrame()
refs_pdf  = pd.DataFrame(refunds) if refunds else pd.DataFrame()
coms_pdf  = pd.DataFrame(commissions) if commissions else pd.DataFrame()

print(f"ships: {len(ships_pdf)}, reviews: {len(revs_pdf)}, refunds: {len(refs_pdf)}, commissions: {len(coms_pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply dirty data

# COMMAND ----------

today_tag = date.today().strftime("%Y%m%d")

if len(ships_pdf) > 0:
    ships_pdf = inject_dupes(ships_pdf, rate=0.01)
    ships_pdf = inject_ghost_ids(ships_pdf, rate=0.03)
    print(f"ships after dirty: {len(ships_pdf)}")

if len(revs_pdf) > 0:
    revs_pdf = inject_dupes(revs_pdf, rate=0.01)
    revs_pdf = flip_date_format(revs_pdf, "review_date", rate=1.0)
    revs_pdf = inject_ghost_ids(revs_pdf, rate=0.03)
    print(f"reviews after dirty: {len(revs_pdf)}")

if len(refs_pdf) > 0:
    refs_pdf = inject_dupes(refs_pdf, rate=0.01)
    refs_pdf = inject_ghost_ids(refs_pdf, rate=0.03)
    print(f"refunds after dirty: {len(refs_pdf)}")

if len(coms_pdf) > 0:
    coms_pdf = inject_dupes(coms_pdf, rate=0.01)
    print(f"commissions after dirty: {len(coms_pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to ADLS landing/
# MAGIC
# MAGIC Using Spark to write CSVs — no extra library needed.
# MAGIC Files land in same folders as 03c historical data.
# MAGIC AutoLoader picks them up automatically (new files only).

# COMMAND ----------

def write_to_adls(pdf, entity_name):
    if len(pdf) == 0:
        print(f"  {entity_name}: empty — skipping")
        return 0
    sdf = spark.createDataFrame(pdf)
    path = f"{ADLS_LANDING}/{entity_name}/incremental/{today_tag}"
    sdf.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  {entity_name}: {len(pdf)} rows → {path}")
    return len(pdf)

# COMMAND ----------

print("writing to ADLS landing/...")
t_ships = write_to_adls(ships_pdf, "shipments")
t_revs  = write_to_adls(revs_pdf, "reviews")
t_refs  = write_to_adls(refs_pdf, "refunds")
t_coms  = write_to_adls(coms_pdf, "commissions")

total = t_ships + t_revs + t_refs + t_coms
print(f"\ntotal rows written: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate inventory snapshot (Rule 12)
# MAGIC
# MAGIC Weekly snapshot — 50 random products × all warehouses.

# COMMAND ----------

product_ids_list = list(product_info.keys())
snap_date = date.today()

inv_rows = []
sampled_pids = random.sample(product_ids_list, min(50, len(product_ids_list)))

for pid in sampled_pids:
    for wid in warehouse_ids:
        qty_avail    = random.randint(0, 500)
        qty_reserved = random.randint(0, min(qty_avail, 50))

        inv_rows.append({
            "inventory_id":       f"INV{random.randint(10000000, 99999999)}",
            "product_id":         pid,
            "warehouse_id":       wid,
            "quantity_available": qty_avail,
            "quantity_reserved":  qty_reserved,
            "reorder_level":      random.choice([10, 20, 30, 50]),
            "last_updated":       f"{snap_date.strftime('%Y-%m-%d')} {random.randint(0,23):02d}:{random.randint(0,59):02d}",
            "snapshot_date":      snap_date.strftime("%Y-%m-%d"),
        })

inv_pdf = pd.DataFrame(inv_rows)

# Rule 12 check
bad_inv = inv_pdf[inv_pdf["quantity_reserved"] > inv_pdf["quantity_available"]]
print(f"inventory rows: {len(inv_pdf)}")
print(f"reserved > available: {len(bad_inv)} (should be 0)")

# COMMAND ----------

inv_pdf = inject_dupes(inv_pdf, rate=0.01)
t_inv = write_to_adls(inv_pdf, "inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4 — Update Watermark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mark processed orders

# COMMAND ----------

processed_ids = orders_pdf[["order_id"]].copy()
processed_ids["processed_at"] = pd.Timestamp.now()

processed_sdf = spark.createDataFrame(processed_ids)
processed_sdf.write.mode("append").saveAsTable(WATERMARK_TABLE)

print(f"watermark updated: {len(processed_ids)} orders marked as processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary

# COMMAND ----------

total_time = time.time() - gen_start

print("=" * 60)
print("03d complete — dependent data for Event Hub orders")
print("=" * 60)
print()
print(f"  orders processed:  {len(orders_pdf):>8,}")
print(f"  shipments:         {t_ships:>8,}")
print(f"  reviews:           {t_revs:>8,}")
print(f"  refunds:           {t_refs:>8,}")
print(f"  commissions:       {t_coms:>8,}")
print(f"  inventory:         {t_inv:>8,}")
print(f"  {'─' * 28}")
print(f"  total rows:        {total + t_inv:>8,}")
print()
print(f"  time: {total_time:.1f}s")
print()
print("Cardinality rules enforced:")
print("  1.  Customer registration (inherited from 03b)")
print("  2.  Payment-order status (inherited from 03b)")
print("  3.  Only delivered orders get reviews")
print("  4.  Review date after delivery date")
print("  5.  Max 2 reviews per (customer, product)")
print("  6.  Only delivered/shipped get refunds")
print("  7.  Refund reason matches shipment context")
print("  8.  No commission on cancelled orders")
print("  9.  Commission rate by category")
print("  10. Reduced commission on refunded orders")
print("  11. Shipment weight matches product weight")
print("  12. Reserved ≤ available in inventory")
print("  13. Full temporal chain preserved")
print()
print("Files written to ADLS landing/ — AutoLoader will pick them up.")
print("Watermark updated — next run skips these orders.")
