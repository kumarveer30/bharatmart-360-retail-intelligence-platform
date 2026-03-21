# Databricks notebook source
# MAGIC %md
# MAGIC # 03c — Generate Historical Bulk Data → ADLS Landing (v2)
# MAGIC
# MAGIC 03a loaded master data into Azure SQL (customers, products, sellers).
# MAGIC 03b is streaming live events from today onwards via Event Hub.
# MAGIC
# MAGIC This script generates 5 years of historical data with **correct cardinality**:
# MAGIC
# MAGIC **Key design: Orders are the spine.**
# MAGIC Every review, shipment, refund, and commission traces back to a real
# MAGIC order with a real customer who bought a real product from the right seller.
# MAGIC
# MAGIC **Cardinality rules enforced:**
# MAGIC 1. Customer can't order before their registration_date
# MAGIC 2. Failed payment → order cancelled (never "delivered")
# MAGIC 3. Only DELIVERED orders get reviews
# MAGIC 4. Review date > actual delivery date
# MAGIC 5. Max 2 reviews per (customer, product) pair
# MAGIC 6. Only delivered/shipped orders get refunds
# MAGIC 7. Refund reason matches shipment context
# MAGIC 8. No commission on cancelled orders
# MAGIC 9. Commission rate varies by category
# MAGIC 10. Reduced commission on refunded orders
# MAGIC 11. Shipment weight ≈ product weight from Azure SQL
# MAGIC 12. quantity_reserved ≤ quantity_available always
# MAGIC 13. Full temporal chain: registration → order → payment → ship → deliver → review/refund
# MAGIC
# MAGIC **CTGAN models used:** model_transactions, model_reviews, model_supply_chain
# MAGIC
# MAGIC **Dirty data:** T1 NULLs, T2 dupes, T3 date formats, T7 ghost IDs
# MAGIC
# MAGIC **History window:** 2020-01-01 to yesterday

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install libraries

# COMMAND ----------

# MAGIC %pip install azure-storage-blob sdv kagglehub pymssql --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restart Python after install

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import io
import os
import pickle
import random
import time
from datetime import date, datetime, timedelta

import pandas as pd
from azure.storage.blob import BlobServiceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets
# MAGIC
# MAGIC Fill in ADLS connection string and SQL password before running.
# MAGIC Kaggle credentials needed too — set KAGGLE_USERNAME + KAGGLE_KEY
# MAGIC as environment variables or place kaggle.json in ~/.kaggle/

# COMMAND ----------

dbutils.widgets.text("adls_connection_string", "", "ADLS connection string")
dbutils.widgets.text("adls_container", "bharatmart", "ADLS container name")
dbutils.widgets.text("sql_server", "bahartmartsql.database.windows.net", "Azure SQL server")
dbutils.widgets.text("sql_database", "bharatmart_db", "SQL database")
dbutils.widgets.text("sql_username", "veer", "SQL username")
dbutils.widgets.text("sql_password", "", "SQL password")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read widget values

# COMMAND ----------

ADLS_CONN  = dbutils.widgets.get("adls_connection_string")
CONTAINER  = dbutils.widgets.get("adls_container")
SQL_SERVER = dbutils.widgets.get("sql_server")
SQL_DB     = dbutils.widgets.get("sql_database")
SQL_USER   = dbutils.widgets.get("sql_username")
SQL_PASS   = dbutils.widgets.get("sql_password")

HISTORY_START = date(2020, 1, 1)
HISTORY_END   = date.today() - timedelta(days=1)

print(f"history window: {HISTORY_START} to {HISTORY_END}")
print(f"container: {CONTAINER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kaggle credentials

# COMMAND ----------

import os
os.environ["KAGGLE_USERNAME"] = "kumarveer"
os.environ["KAGGLE_KEY"]      = ""  # paste your Kaggle API key here

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 1 — Load External Resources

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-download DS-3 Flipkart Reviews

# COMMAND ----------

import kagglehub

ds3_path = kagglehub.dataset_download("niraliivaghani/flipkart-dataset")
print(f"downloaded to: {ds3_path}")

# COMMAND ----------

for f in os.listdir(ds3_path):
    print(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load review text

# COMMAND ----------

csv_files = [f for f in os.listdir(ds3_path) if f.endswith(".csv")]
ds3_file  = os.path.join(ds3_path, csv_files[0])

ds3_df = pd.read_csv(ds3_file, nrows=50000, encoding="ISO-8859-1")
print(f"loaded {len(ds3_df)} rows")
print(ds3_df.columns.tolist())

# COMMAND ----------

display(ds3_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucket reviews by length

# COMMAND ----------

text_col = None
for col in ds3_df.columns:
    if col.lower() in ("review", "summary", "review_text", "text"):
        text_col = col
        break
if text_col is None:
    # fallback: use longest-average column
    str_cols = ds3_df.select_dtypes(include="object").columns
    text_col = max(str_cols, key=lambda c: ds3_df[c].astype(str).str.len().mean())
print(f"using text column: {text_col}")

# COMMAND ----------

ds3_df = ds3_df.dropna(subset=[text_col])
ds3_df = ds3_df[ds3_df[text_col].str.strip() != ""]
print(f"rows with actual text: {len(ds3_df)}")

# COMMAND ----------

ds3_df["text_len"] = ds3_df[text_col].str.len()

short_reviews  = ds3_df[ds3_df["text_len"] < 50][text_col].tolist()
medium_reviews = ds3_df[(ds3_df["text_len"] >= 50) & (ds3_df["text_len"] < 150)][text_col].tolist()
long_reviews   = ds3_df[ds3_df["text_len"] >= 150][text_col].tolist()

print(f"short:  {len(short_reviews)}")
print(f"medium: {len(medium_reviews)}")
print(f"long:   {len(long_reviews)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CTGAN models

# COMMAND ----------

MODEL_BASE = "/Volumes/bharatmart/ctgan_models/models"

with open(f"{MODEL_BASE}/model_transactions.pkl", "rb") as f:
    transactions_model = pickle.load(f)
print("model_transactions.pkl loaded")

with open(f"{MODEL_BASE}/model_reviews.pkl", "rb") as f:
    reviews_model = pickle.load(f)
print("model_reviews.pkl loaded")

with open(f"{MODEL_BASE}/model_supply_chain.pkl", "rb") as f:
    supply_model = pickle.load(f)
print("model_supply_chain.pkl loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick test — what columns does each model know?

# COMMAND ----------

print("=== model_transactions ===")
t_sample = transactions_model.sample(num_rows=3)
print(t_sample.columns.tolist())
display(t_sample)

# COMMAND ----------

print("=== model_reviews ===")
r_sample = reviews_model.sample(num_rows=3)
print(r_sample.columns.tolist())
display(r_sample)

# COMMAND ----------

print("=== model_supply_chain ===")
s_sample = supply_model.sample(num_rows=3)
print(s_sample.columns.tolist())
display(s_sample)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC config for Azure SQL

# COMMAND ----------

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}:1433;"
    f"database={SQL_DB};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=60"
)
JDBC_PROPS = {
    "user":     SQL_USER,
    "password": SQL_PASS,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("jdbc config ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retry helper for Azure SQL auto-pause

# COMMAND ----------

def read_sql_with_retry(query_alias, max_retries=3, pause=30):
    for attempt in range(1, max_retries + 1):
        try:
            rows = spark.read.jdbc(JDBC_URL, query_alias, properties=JDBC_PROPS).collect()
            return rows
        except Exception as e:
            if attempt < max_retries:
                print(f"  attempt {attempt} failed, retrying in {pause}s... ({e})")
                time.sleep(pause)
            else:
                raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix: ensure products.seller_id exists
# MAGIC
# MAGIC Products table was created without seller_id in 03a.
# MAGIC We add it here and assign each product to a seller round-robin.

# COMMAND ----------

# MAGIC %skip
# MAGIC import pymssql
# MAGIC
# MAGIC conn = pymssql.connect(server=SQL_SERVER, user=SQL_USER, password=SQL_PASS, database=SQL_DB)
# MAGIC cursor = conn.cursor()
# MAGIC
# MAGIC # add column if missing
# MAGIC cursor.execute("""
# MAGIC IF NOT EXISTS (
# MAGIC     SELECT 1 FROM sys.columns
# MAGIC     WHERE object_id = OBJECT_ID('dbo.products') AND name = 'seller_id'
# MAGIC )
# MAGIC ALTER TABLE dbo.products ADD seller_id VARCHAR(20)
# MAGIC """)
# MAGIC conn.commit()
# MAGIC print("step 1 — column added (or already exists)")
# MAGIC
# MAGIC # assign sellers round-robin (only if NULLs exist)
# MAGIC cursor.execute("""
# MAGIC WITH numbered_products AS (
# MAGIC     SELECT product_id,
# MAGIC            ROW_NUMBER() OVER (ORDER BY product_id) AS rn
# MAGIC     FROM dbo.products
# MAGIC     WHERE seller_id IS NULL
# MAGIC ),
# MAGIC numbered_sellers AS (
# MAGIC     SELECT seller_id,
# MAGIC            ROW_NUMBER() OVER (ORDER BY seller_id) AS sn,
# MAGIC            COUNT(*) OVER () AS total
# MAGIC     FROM dbo.sellers
# MAGIC     WHERE is_active = 1
# MAGIC )
# MAGIC UPDATE p
# MAGIC SET p.seller_id = s.seller_id
# MAGIC FROM dbo.products p
# MAGIC JOIN numbered_products np ON p.product_id = np.product_id
# MAGIC JOIN numbered_sellers s   ON ((np.rn - 1) % s.total) + 1 = s.sn
# MAGIC """)
# MAGIC conn.commit()
# MAGIC print(f"step 2 — {cursor.rowcount} products assigned a seller")
# MAGIC conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load customer IDs + registration dates
# MAGIC
# MAGIC Rule 1: customer can't order before their registration_date.

# COMMAND ----------

cust_rows = read_sql_with_retry(
    "(SELECT customer_id, registration_date FROM dbo.customers WHERE is_active=1) t"
)
# dict: customer_id → registration_date
customer_reg = {r[0]: r[1] for r in cust_rows}
customer_ids = list(customer_reg.keys())
print(f"loaded {len(customer_ids)} active customers with registration dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load product → seller + category + weight
# MAGIC
# MAGIC Rule 9: commission rate by category.
# MAGIC Rule 11: shipment weight ≈ product weight.

# COMMAND ----------

ps_rows = read_sql_with_retry(
    "(SELECT product_id, seller_id, category_id, weight_kg FROM dbo.products WHERE is_active=1) t"
)
# dict for easy lookup
product_info = {}
product_seller_pairs = []
for r in ps_rows:
    pid, sid, cat_id, weight = r[0], r[1], r[2], r[3]
    product_info[pid] = {
        "seller_id":   sid,
        "category_id": cat_id,
        "weight_kg":   float(weight) if weight else round(random.uniform(0.1, 5.0), 2),
    }
    product_seller_pairs.append((pid, sid))

product_ids = list(product_info.keys())
seller_ids  = list(set(p[1] for p in product_seller_pairs))

print(f"loaded {len(product_ids)} products with seller/category/weight")
print(f"unique sellers: {len(seller_ids)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load category → parent mapping for commission rates

# COMMAND ----------

cat_rows = read_sql_with_retry(
    "(SELECT category_id, parent_category FROM dbo.categories) t"
)
cat_parent = {r[0]: r[1] for r in cat_rows}

# commission rates by L1 category (lo, hi)
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

print(f"loaded {len(cat_parent)} category mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load warehouse IDs

# COMMAND ----------

wh_rows       = read_sql_with_retry("(SELECT warehouse_id FROM dbo.warehouses WHERE is_active=1) t")
warehouse_ids = [r[0] for r in wh_rows]
print(f"loaded {len(warehouse_ids)} warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to ADLS

# COMMAND ----------

adls_client = BlobServiceClient.from_connection_string(ADLS_CONN)
print("connected to ADLS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload helper

# COMMAND ----------

def upload_to_adls(df, blob_path):
    buf  = io.StringIO()
    df.to_csv(buf, index=False)
    data = buf.getvalue().encode("utf-8")
    blob = adls_client.get_blob_client(container=CONTAINER, blob=blob_path)
    blob.upload_blob(data, overwrite=True)
    return len(df)

print("upload helper ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 2 — Generator Functions (with cardinality rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTGAN column finder

# COMMAND ----------

def find_col(series_index, *keywords):
    for col in series_index:
        cl = col.lower()
        if all(k in cl for k in keywords):
            return col
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Track review counts per (customer, product)
# MAGIC
# MAGIC Rule 5: max 2 reviews per (customer, product).

# COMMAND ----------

# global tracker — persists across months
review_tracker = {}   # (customer_id, product_id) → count

def can_review(cust_id, prod_id):
    key = (cust_id, prod_id)
    return review_tracker.get(key, 0) < 2

def mark_reviewed(cust_id, prod_id):
    key = (cust_id, prod_id)
    review_tracker[key] = review_tracker.get(key, 0) + 1

print("review tracker ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate one month of orders
# MAGIC
# MAGIC Rule 1: order_date ≥ customer.registration_date
# MAGIC Rule 2: payment status drives order status

# COMMAND ----------

# DBTITLE 1,Diwali month mapping by year
import math

# Exact festival dates — hardcoded from Hindu calendar
# BharatMart Dhamaka Days = 10 days starting 12 days before Diwali

FESTIVAL_DATES = {
    2020: {
        "diwali":          date(2020, 11, 14),
        "navratri_start":  date(2020, 10, 17),
        "dussehra":        date(2020, 10, 25),
        "dhamaka_start":   date(2020, 11,  2),  # BharatMart Dhamaka Days start
        "dhanteras":       date(2020, 11, 13),
        "holi":            date(2020,  3,  9),
        "covid_start":     date(2020,  3, 25),
        "covid_end":       date(2020,  6,  7),
    },
    2021: {
        "diwali":          date(2021, 11,  4),
        "navratri_start":  date(2021, 10,  7),
        "dussehra":        date(2021, 10, 15),
        "dhamaka_start":   date(2021, 10, 23),
        "dhanteras":       date(2021, 11,  2),
        "holi":            date(2021,  3, 28),
        "covid_start":     None,
        "covid_end":       None,
    },
    2022: {
        "diwali":          date(2022, 10, 24),
        "navratri_start":  date(2022, 10,  2),
        "dussehra":        date(2022, 10,  5),
        "dhamaka_start":   date(2022, 10, 12),
        "dhanteras":       date(2022, 10, 22),
        "holi":            date(2022,  3, 18),
        "covid_start":     None,
        "covid_end":       None,
    },
    2023: {
        "diwali":          date(2023, 11, 12),
        "navratri_start":  date(2023, 10, 15),
        "dussehra":        date(2023, 10, 24),
        "dhamaka_start":   date(2023, 10, 31),
        "dhanteras":       date(2023, 11, 10),
        "holi":            date(2023,  3,  7),
        "covid_start":     None,
        "covid_end":       None,
    },
    2024: {
        "diwali":          date(2024, 11,  1),
        "navratri_start":  date(2024, 10,  3),
        "dussehra":        date(2024, 10, 12),
        "dhamaka_start":   date(2024, 10, 20),
        "dhanteras":       date(2024, 10, 29),
        "holi":            date(2024,  3, 25),
        "covid_start":     None,
        "covid_end":       None,
    },
    2025: {
        "diwali":          date(2025, 10, 20),
        "navratri_start":  date(2025,  9, 22),
        "dussehra":        date(2025, 10,  2),
        "dhamaka_start":   date(2025, 10,  8),
        "dhanteras":       date(2025, 10, 18),
        "holi":            date(2025,  3, 14),
        "covid_start":     None,
        "covid_end":       None,
    },
    2026: {
        "diwali":          date(2026, 11, 8),
        "navratri_start":  date(2026, 10, 11),
        "dussehra":        date(2026, 10, 20),
        "dhamaka_start":   date(2026, 10, 27),
        "dhanteras":       date(2026, 11, 6),
        "holi":            date(2026,  3,  4),
        "covid_start":     None,
        "covid_end":       None,
    },
}

# backward compat — derive_refunds and generate_inventory still use DIWALI_MONTH
DIWALI_MONTH = {yr: FESTIVAL_DATES[yr]["diwali"].month for yr in FESTIVAL_DATES}

print("festival calendar ready")
for yr in [2020, 2022, 2024, 2025]:
    d = FESTIVAL_DATES[yr]["diwali"]
    dh = FESTIVAL_DATES[yr]["dhamaka_start"]
    print(f"  {yr}: Diwali={d}  Dhamaka Days start={dh}")

# COMMAND ----------

PAYMENT_METHODS = ["UPI", "credit_card", "debit_card", "net_banking", "COD", "wallet"]

def get_payment_weights(year):
    if year <= 2020:
        return [20, 20, 15, 5, 40, 5]
    elif year == 2021:
        return [30, 20, 14, 5, 25, 6]
    elif year == 2022:
        return [40, 18, 12, 4, 20, 6]
    elif year == 2023:
        return [52, 16, 10, 3, 13, 6]
    elif year == 2024:
        return [62, 15, 8, 3, 8, 4]
    else:
        return [72, 14, 6, 2, 4, 2]

def get_coupon(d, fest_dates):
    r   = random.random()
    yr  = d.year % 100
    diwali      = fest_dates.get("diwali")
    dhamaka     = fest_dates.get("dhamaka_start")
    dhanteras   = fest_dates.get("dhanteras")
    holi        = fest_dates.get("holi")

    if diwali and abs((d - diwali).days) <= 1:
        if r < 0.70:
            return random.choice([f"DIWALI{yr}", f"DIWSALE{yr}", "FESTIVE50", "DHANTERAS20"])
    elif dhamaka and 0 <= (d - dhamaka).days <= 9:
        if r < 0.70:
            return random.choice(["DHAMAKA40", "BHARATFEST30", f"DHAMAKA{yr}", "BIGDEAL35"])
    elif d.month == 1 and d.day == 26:
        if r < 0.50:
            return random.choice(["REPUBLIC20", f"INDIA{yr}SALE", "BHARAT20"])
    elif d.month == 2 and d.day == 14:
        if r < 0.35:
            return random.choice(["LOVE15", f"VALENTINE{yr}"])
    elif holi and abs((d - holi).days) <= 2:
        if r < 0.50:
            return random.choice([f"HOLI{yr}", "COLORSOFFER", "EOS30"])
    elif d.month == 8 and d.day == 15:
        if r < 0.50:
            return random.choice(["FREEDOM15", "AUG15SALE", f"INDIA{d.year}"])
    elif d.month in [11, 12, 1, 2]:
        if r < 0.20:
            return random.choice(["WEDDING15", "SHAADI20"])
    if r < 0.20:
        return random.choice(["WELCOME10", "SAVE5", "FLAT100", "NEWUSER15"])
    return None

# Smooth bell curve — peaks at 1.0 on centre day
# used for Diwali ramp-up and ramp-down
def bell(days_from_peak, ramp_days):
    if abs(days_from_peak) > ramp_days:
        return 0.0
    return math.sin(math.pi * (1 - abs(days_from_peak) / ramp_days) / 2) ** 2

def get_day_multiplier(d):
    year  = d.year
    fyr   = FESTIVAL_DATES.get(year, FESTIVAL_DATES[2025])
    diwali    = fyr["diwali"]
    dhamaka   = fyr["dhamaka_start"]
    navratri  = fyr["navratri_start"]
    dhanteras = fyr["dhanteras"]
    holi      = fyr["holi"]
    covid_s   = fyr["covid_start"]
    covid_e   = fyr["covid_end"]

    mult = 1.0

    # ── COVID lockdown (2020 only) ──
    if covid_s and covid_e and covid_s <= d <= covid_e:
        days_in  = (d - covid_s).days
        days_out = (covid_e - d).days
        if days_in <= 5:
            # sudden crash over 5 days
            mult = 1.0 - (0.75 * days_in / 5)
        elif days_out <= 10:
            # gradual unlock recovery
            mult = 0.25 + (0.45 * (1 - days_out / 10))
        else:
            mult = 0.25
        return max(0.1, mult)  # never zero — essentials still order

    # ── Diwali — smooth bell 14 days before to 3 days after ──
    days_to_diwali = (d - diwali).days
    if -14 <= days_to_diwali <= 3:
        if days_to_diwali <= 0:
            # ramp up — smooth bell
            diwali_boost = 1.0 + 4.0 * bell(days_to_diwali, 14)
        else:
            # sudden drop after — 3 day recovery
            diwali_boost = 1.0 + 4.0 * (1 - days_to_diwali / 3)
        mult = max(mult, diwali_boost)

    # ── Dhanteras — electronics spike 2 days before Diwali ──
    days_to_dhanteras = (d - dhanteras).days
    if days_to_dhanteras in [-1, 0]:
        mult = max(mult, mult * 1.5)

    # ── BharatMart Dhamaka Days — step function, 10 days ──
    days_into_dhamaka = (d - dhamaka).days
    if 0 <= days_into_dhamaka <= 9:
        # sudden launch on day 0, flat, sudden end
        dhamaka_boost = 3.5 if days_into_dhamaka < 9 else 2.0
        mult = max(mult, dhamaka_boost)

    # ── Navratri — 9 days, smooth build ──
    days_into_nav = (d - navratri).days
    if 0 <= days_into_nav <= 8:
        nav_boost = 1.0 + 0.8 * math.sin(math.pi * days_into_nav / 9)
        mult = max(mult, nav_boost)

    # ── Holi — smooth 5 days centred on date ──
    if holi:
        days_to_holi = (d - holi).days
        if -3 <= days_to_holi <= 2:
            holi_boost = 1.0 + 0.5 * bell(days_to_holi, 3)
            mult = max(mult, holi_boost)

    # ── Republic Day — sudden 2 days ──
    if d.month == 1 and d.day in [25, 26, 27]:
        mult = max(mult, 1.4)

    # ── Independence Day — sudden 2 days ──
    if d.month == 8 and d.day in [14, 15, 16]:
        mult = max(mult, 1.5)

    # ── End of Season Sales — 10 day step ──
    eos_summer = date(year, 6, 20)
    eos_winter = date(year, 3, 20)
    if 0 <= (d - eos_summer).days <= 9:
        mult = max(mult, 1.35)
    if 0 <= (d - eos_winter).days <= 9:
        mult = max(mult, 1.3)

    # ── Valentine's Day — smooth 3 days ──
    if d.month == 2 and d.day in [13, 14, 15]:
        mult = max(mult, 1.2)

    # ── Christmas + New Year — gradual Dec 23 to Jan 2 ──
    if (d.month == 12 and d.day >= 23) or (d.month == 1 and d.day <= 2):
        mult = max(mult, 1.6)

    # ── Pre-festive September buildup ──
    if d.month == 9:
        mult = max(mult, 1.3 + 0.3 * (d.day / 30))

    # ── Wedding season background layer ──
    # smooth baseline lift, not a spike
    if d.month in [11, 12]:
        wedding_lift = 0.15 + 0.1 * (d.day / 30)
        mult += wedding_lift
    elif d.month == 1:
        wedding_lift = 0.20
        mult += wedding_lift
    elif d.month == 2:
        wedding_lift = 0.10
        mult += wedding_lift
    elif d.month in [3, 4]:
        # secondary season
        mult += 0.08

    # ── Weekend boost ──
    if d.weekday() in [5, 6]:  # Sat, Sun
        mult *= 1.12

    # ── Post-festive hangover — Jan after Diwali ──
    if d.month == 1 and d.year > 2020:
        mult *= 0.88

    # ── Random daily variance — no two days identical ──
    mult *= random.uniform(0.92, 1.08)

    return max(0.1, mult)

# category weights — same as before
CATEGORY_WEIGHTS_NORMAL  = {"Electronics": 20, "Fashion": 25, "Home & Kitchen": 15, "Beauty & Personal Care": 10, "Grocery & Gourmet": 10, "Toys & Games": 5, "Sports & Outdoors": 5, "Books & Media": 5, "Automotive": 3, "Health & Wellness": 2}
CATEGORY_WEIGHTS_DIWALI  = {"Electronics": 40, "Fashion": 20, "Home & Kitchen": 15, "Beauty & Personal Care": 8,  "Grocery & Gourmet": 6,  "Toys & Games": 4, "Sports & Outdoors": 2, "Books & Media": 2, "Automotive": 2, "Health & Wellness": 1}
CATEGORY_WEIGHTS_HOLI    = {"Electronics": 10, "Fashion": 30, "Home & Kitchen": 10, "Beauty & Personal Care": 25, "Grocery & Gourmet": 12, "Toys & Games": 5, "Sports & Outdoors": 4, "Books & Media": 2, "Automotive": 1, "Health & Wellness": 1}
CATEGORY_WEIGHTS_SUMMER  = {"Electronics": 20, "Fashion": 20, "Home & Kitchen": 18, "Beauty & Personal Care": 10, "Grocery & Gourmet": 10, "Toys & Games": 4, "Sports & Outdoors": 12, "Books & Media": 2, "Automotive": 3, "Health & Wellness": 1}
CATEGORY_WEIGHTS_COVID   = {"Electronics": 25, "Fashion":  5, "Home & Kitchen": 20, "Beauty & Personal Care": 5,  "Grocery & Gourmet": 30, "Toys & Games": 3, "Sports & Outdoors": 2, "Books & Media": 8, "Automotive": 0, "Health & Wellness": 12}
CATEGORY_WEIGHTS_WEDDING = {"Electronics": 18, "Fashion": 30, "Home & Kitchen": 22, "Beauty & Personal Care": 12, "Grocery & Gourmet": 6,  "Toys & Games": 3, "Sports & Outdoors": 3, "Books & Media": 2, "Automotive": 2, "Health & Wellness": 2}

category_products = {}
for pid, info in product_info.items():
    cat_id = info["category_id"]
    parent = cat_parent.get(cat_id, "Other")
    if parent not in category_products:
        category_products[parent] = []
    category_products[parent].append(pid)

def pick_product(d, fyr):
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]
    holi    = fyr["holi"]
    covid_s = fyr["covid_start"]
    covid_e = fyr["covid_end"]

    is_covid   = covid_s and covid_e and covid_s <= d <= covid_e
    is_diwali  = diwali and abs((d - diwali).days) <= 7
    is_dhamaka = dhamaka and 0 <= (d - dhamaka).days <= 9
    is_holi    = holi and abs((d - holi).days) <= 2
    is_summer  = d.month in [4, 5]
    is_wedding = d.month in [11, 12, 1, 2, 3, 4]

    if is_covid:
        wts = CATEGORY_WEIGHTS_COVID
    elif is_diwali or is_dhamaka:
        wts = CATEGORY_WEIGHTS_DIWALI
    elif is_holi:
        wts = CATEGORY_WEIGHTS_HOLI
    elif is_summer:
        wts = CATEGORY_WEIGHTS_SUMMER
    elif is_wedding:
        wts = CATEGORY_WEIGHTS_WEDDING
    else:
        wts = CATEGORY_WEIGHTS_NORMAL

    cats = list(wts.keys())
    weights = list(wts.values())

    for _ in range(5):
        cat = random.choices(cats, weights=weights)[0]
        prods = category_products.get(cat, [])
        if prods:
            return random.choice(prods)
    return random.choice(product_ids)

def get_aov_multiplier(d, fyr):
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]
    if diwali and abs((d - diwali).days) <= 3:
        return random.uniform(1.5, 1.8)
    if dhamaka and 0 <= (d - dhamaka).days <= 9:
        return random.uniform(1.3, 1.6)
    if d.month in [3, 6]:
        return random.uniform(0.85, 1.0)  # clearance — lower AOV
    return 1.0

def get_disc_range(d, fyr):
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]
    if diwali and abs((d - diwali).days) <= 3:
        return (0.25, 0.40)
    if dhamaka and 0 <= (d - dhamaka).days <= 9:
        return (0.25, 0.40)
    if d.month in [3, 6]:
        return (0.20, 0.30)
    if d.month in [1, 8]:
        return (0.15, 0.22)
    return (0.05, 0.16)

_order_counter = [0]

def generate_day_orders(d, n_orders):
    year = d.year
    fyr  = FESTIVAL_DATES.get(year, FESTIVAL_DATES[2025])

    payment_weights = get_payment_weights(year)
    aov_mult        = get_aov_multiplier(d, fyr)
    disc_range      = get_disc_range(d, fyr)

    synth = transactions_model.sample(num_rows=n_orders)
    rows  = []

    for idx in range(n_orders):
        s = synth.iloc[idx]

        # Rule 1: customer registered before order date
        cust_id = None
        for _ in range(5):
            candidate = random.choice(customer_ids)
            reg = customer_reg[candidate]
            if hasattr(reg, 'date'):
                reg = reg.date()
            if reg <= d:
                cust_id = candidate
                break
        if cust_id is None:
            cust_id = random.choice(customer_ids)

        prod_id = pick_product(d, fyr)
        info    = product_info[prod_id]
        sell_id = info["seller_id"]

        price_col = find_col(s.index, "price")
        if price_col and price_col != find_col(s.index, "freight"):
            order_amount = round(max(49, min(99999, abs(float(s[price_col])) * aov_mult)), 2)
        else:
            order_amount = round(random.uniform(99, 14999) * aov_mult, 2)

        freight_col = find_col(s.index, "freight")
        freight = round(max(0, min(999, abs(float(s[freight_col])))), 2) if freight_col else round(random.uniform(20, 200), 2)

        discount_amount = round(order_amount * random.uniform(*disc_range), 2)

        pay_status = random.choices(["completed", "pending", "failed"], weights=[95, 3, 2])[0]

        if pay_status == "failed":
            order_status = "cancelled"
        elif pay_status == "pending":
            order_status = "processing"
        else:
            status_col = find_col(s.index, "status")
            if status_col:
                raw = str(s[status_col]).lower().strip()
                if "deliver" in raw:   order_status = "delivered"
                elif "ship" in raw:    order_status = "shipped"
                elif "cancel" in raw:  order_status = "cancelled"
                elif "process" in raw: order_status = "processing"
                else:                  order_status = "delivered"
            else:
                order_status = random.choices(["delivered","shipped","cancelled","processing"], weights=[85,5,7,3])[0]

        # Idea 10: payment method by year
        payment_method = random.choices(PAYMENT_METHODS, weights=payment_weights)[0]

        # Idea 9: festive electronics → credit card for no-cost EMI
        diwali  = fyr["diwali"]
        dhamaka = fyr["dhamaka_start"]
        cat_parent_name = cat_parent.get(info["category_id"], "")
        is_festive_elec = cat_parent_name == "Electronics" and (
            (diwali and abs((d - diwali).days) <= 7) or
            (dhamaka and 0 <= (d - dhamaka).days <= 9)
        )
        if is_festive_elec:
            payment_method = random.choices(PAYMENT_METHODS, weights=[45, 35, 8, 3, 5, 4])[0]

        hour = random.randint(6, 23)
        _order_counter[0] += 1
        order_id   = f"ORD{int(datetime(d.year, d.month, d.day, hour).timestamp())}{_order_counter[0]:06d}"
        session_id = f"SES{_order_counter[0]:010d}"

        est_delivery = d + timedelta(days=random.randint(3, 10))
        coupon = get_coupon(d, fyr)

        rows.append({
            "order_id":           order_id,
            "session_id":         session_id,
            "customer_id":        cust_id,
            "product_id":         prod_id,
            "seller_id":          sell_id,
            "order_status":       order_status,
            "order_amount":       order_amount,
            "freight_value":      freight,
            "discount_amount":    discount_amount,
            "payment_method":     payment_method,
            "payment_status":     pay_status,
            "order_date":         d.strftime("%Y-%m-%d"),
            "estimated_delivery": est_delivery.strftime("%Y-%m-%d"),
            "coupon_code":        coupon,
        })

    return pd.DataFrame(rows)

print("generate_day_orders ready")
sample = generate_day_orders(date(2023, 11, 12), 100)  # Diwali 2023
print(f"sample: {len(sample)} orders, Diwali day")
print(f"avg order amount: ₹{sample['order_amount'].mean():,.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test order generator

# COMMAND ----------

sample_orders = generate_day_orders(date(2023, 6, 15), 500)  # new style — date object, n
print(f"generated {len(sample_orders)} orders")
display(sample_orders.head(10))

# COMMAND ----------

print("order_status distribution:")
print(sample_orders["order_status"].value_counts())
print()
print("payment_status distribution:")
print(sample_orders["payment_status"].value_counts())

# COMMAND ----------

# Rule 2 check: no failed payment should have delivered status
bad = sample_orders[(sample_orders["payment_status"] == "failed") & (sample_orders["order_status"] == "delivered")]
print(f"failed payment + delivered status: {len(bad)} (should be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive payments
# MAGIC
# MAGIC Payment amount = order_amount + freight. Status from order.

# COMMAND ----------

GATEWAYS        = ["Razorpay", "PayTM", "PhonePe", "Stripe_IN", "CCAvenue"]
GATEWAY_WEIGHTS = [35, 25, 25, 10, 5]

_payment_counter = [0]

def derive_payments(orders_df):
    rows = []
    for _, o in orders_df.iterrows():
        payment_amount = round(o["order_amount"] + o["freight_value"], 2)
        o_date = datetime.strptime(o["order_date"], "%Y-%m-%d").date()
        pay_date = o_date + timedelta(days=random.choice([0, 0, 0, 1]))

        _payment_counter[0] += 1

        rows.append({
            "payment_id":     f"PAY{_payment_counter[0]:010d}",
            "order_id":       o["order_id"],
            "payment_method": o["payment_method"],
            "payment_amount": payment_amount,
            "payment_status": o["payment_status"],
            "payment_date":   pay_date.strftime("%Y-%m-%d"),
            "gateway":        random.choices(GATEWAYS, weights=GATEWAY_WEIGHTS)[0],
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive shipments
# MAGIC
# MAGIC Cancelled orders get NO shipment record.
# MAGIC Rule 11: weight ≈ product weight from Azure SQL.
# MAGIC Rule 13: actual_delivery ≥ shipped_date always.

# COMMAND ----------

CARRIERS = ["Delhivery", "BlueDart", "DTDC", "Ekart", "Shadowfax", "XpressBees", "India Post"]

_shipment_counter = [0]

def derive_shipments(orders_df):
    shippable = orders_df[orders_df["order_status"] != "cancelled"].copy()
    if len(shippable) == 0:
        return pd.DataFrame()

    n = len(shippable)
    synth = supply_model.sample(num_rows=n)

    rows = []
    for i, (_, o) in enumerate(shippable.iterrows()):
        s = synth.iloc[i]

        real_col  = find_col(s.index, "ship", "real") or find_col(s.index, "actual", "day")
        days_real = max(1, min(15, abs(int(float(s[real_col]))))) if real_col else random.randint(2, 7)

        sched_col  = find_col(s.index, "scheduled") or find_col(s.index, "sched")
        days_sched = max(1, min(15, abs(int(float(s[sched_col]))))) if sched_col else random.randint(2, 7)

        stat_col = find_col(s.index, "delivery", "status") or find_col(s.index, "delivery_status")
        if stat_col:
            raw = str(s[stat_col]).lower()
            if "advance" in raw:
                status = "delivered"
            elif "late" in raw:
                status = "delayed"
            elif "cancel" in raw:
                status = "cancelled"
            else:
                status = "delivered"
        else:
            status = random.choices(
                ["delivered", "delayed", "in_transit"],
                weights=[80, 12, 8]
            )[0]

        # match order status
        if o["order_status"] == "delivered":
            status = "delivered"

        late_col  = find_col(s.index, "late", "risk")
        late_risk = int(float(s[late_col])) if late_col else random.randint(0, 1)

        o_date          = datetime.strptime(o["order_date"], "%Y-%m-%d").date()
        shipped_date    = o_date + timedelta(days=random.randint(0, 2))
        est_delivery    = shipped_date + timedelta(days=days_sched)
        # Rule 13: actual_delivery always >= shipped_date
        actual_delivery = shipped_date + timedelta(days=days_real) if status == "delivered" else None

        # Rule 11: weight from product with packaging variance
        prod_weight = product_info.get(o["product_id"], {}).get("weight_kg", 1.0)
        ship_weight = round(prod_weight * random.uniform(1.0, 1.3), 2)

        _shipment_counter[0] += 1

        rows.append({
            "shipment_id":        f"SHP{_shipment_counter[0]:010d}",
            "order_id":           o["order_id"],
            "warehouse_id":       random.choice(warehouse_ids),
            "carrier":            random.choice(CARRIERS),
            "tracking_number":    f"TRK{random.randint(100000000, 999999999)}",
            "shipped_date":       shipped_date.strftime("%Y-%m-%d"),
            "estimated_delivery": est_delivery.strftime("%Y-%m-%d"),
            "actual_delivery":    actual_delivery.strftime("%Y-%m-%d") if actual_delivery else None,
            "status":             status,
            "late_delivery_risk": late_risk,
            "weight_kg":          ship_weight,
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test shipments

# COMMAND ----------

sample_ships = derive_shipments(sample_orders)
cancelled = len(sample_orders[sample_orders["order_status"] == "cancelled"])
print(f"orders: {len(sample_orders)}, cancelled: {cancelled}")
print(f"shipments: {len(sample_ships)} (should be ~{len(sample_orders) - cancelled})")
display(sample_ships.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive reviews
# MAGIC
# MAGIC Rule 3: only DELIVERED orders.
# MAGIC Rule 4: review_date > actual delivery.
# MAGIC Rule 5: max 2 reviews per (customer, product).

# COMMAND ----------

def derive_reviews(orders_df, shipments_df, review_rate=0.30):
    delivered = orders_df[orders_df["order_status"] == "delivered"].copy()
    if len(delivered) == 0:
        return pd.DataFrame()

    # order_id → actual_delivery date
    delivery_dates = {}
    if len(shipments_df) > 0:
        for _, sh in shipments_df.iterrows():
            if sh.get("actual_delivery"):
                delivery_dates[sh["order_id"]] = sh["actual_delivery"]

    n_reviews = max(1, int(len(delivered) * review_rate))
    candidates = delivered.sample(n=min(n_reviews, len(delivered)))

    synth = reviews_model.sample(num_rows=len(candidates))

    rows = []
    for i, (_, o) in enumerate(candidates.iterrows()):
        cust_id = o["customer_id"]
        prod_id = o["product_id"]

        # Rule 5
        if not can_review(cust_id, prod_id):
            continue

        s = synth.iloc[i]

        rating_col = find_col(s.index, "rating")
        if rating_col:
            rating = max(1, min(5, int(round(float(s[rating_col])))))
        else:
            rating = random.randint(1, 5)

        # late delivery → slightly lower rating
        delivery_str = delivery_dates.get(o["order_id"])
        delivery_dt = None
        if delivery_str:
            try:
                delivery_dt = datetime.strptime(str(delivery_str), "%Y-%m-%d").date()
                est_dt = datetime.strptime(o["estimated_delivery"], "%Y-%m-%d").date()
                if delivery_dt > est_dt and rating > 2:
                    rating = max(1, rating - random.choice([0, 0, 1, 1, 2]))
            except:
                pass

        tl_col       = find_col(s.index, "text_length")
        text_len_bin = str(s[tl_col]).strip().lower() if tl_col else "short"

        hr_col   = find_col(s.index, "has_review")
        has_text = str(s[hr_col]).strip().lower() if hr_col else "yes"

        if has_text in ("none", "false", "no", "0"):
            review_text = None
        elif text_len_bin == "long" and long_reviews:
            review_text = random.choice(long_reviews)
        elif text_len_bin == "medium" and medium_reviews:
            review_text = random.choice(medium_reviews)
        else:
            review_text = random.choice(short_reviews) if short_reviews else None

        # Rule 4: review AFTER delivery (or 10-30 days after order if no date)
        o_date = datetime.strptime(o["order_date"], "%Y-%m-%d").date()
        if delivery_dt:
            review_date = delivery_dt + timedelta(days=random.randint(1, 20))
        else:
            review_date = o_date + timedelta(days=random.randint(10, 30))

        mark_reviewed(cust_id, prod_id)

        rows.append({
            "review_id":     f"REV{random.randint(10000000, 99999999)}",
            "order_id":      o["order_id"],
            "product_id":    prod_id,
            "customer_id":   cust_id,
            "seller_id":     o["seller_id"],
            "rating":        rating,
            "review_text":   review_text,
            "helpful_votes": random.randint(0, 50),
            "review_date":   review_date.strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test reviews

# COMMAND ----------

sample_ships_full = derive_shipments(sample_orders)
sample_revs = derive_reviews(sample_orders, sample_ships_full)
delivered_count = len(sample_orders[sample_orders["order_status"] == "delivered"])
print(f"delivered orders: {delivered_count}")
print(f"reviews generated: {len(sample_revs)} ({len(sample_revs)/max(delivered_count,1)*100:.0f}% of delivered)")
display(sample_revs.head(5))

# COMMAND ----------

# check: no review for a non-delivered order
delivered_oids = set(sample_orders[sample_orders["order_status"] == "delivered"]["order_id"])
bad_reviews = [r for _, r in sample_revs.iterrows() if r["order_id"] not in delivered_oids]
print(f"reviews for non-delivered orders: {len(bad_reviews)} (should be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive refunds
# MAGIC
# MAGIC Rule 6: only delivered/shipped orders.
# MAGIC Rule 7: refund reason matches shipment context.

# COMMAND ----------

REFUND_STATUSES = ["processed", "approved", "requested", "rejected"]
REFUND_WEIGHTS  = [55, 25, 15, 5]

REASONS_DELIVERED = ["damaged_product", "wrong_item", "quality_issue", "changed_mind"]
REASONS_DELAYED   = ["not_received", "damaged_product", "wrong_item"]
REASONS_DEFAULT   = ["damaged_product", "wrong_item", "quality_issue", "changed_mind", "duplicate_order"]

# Idea 13: category-wise return rates — fashion highest, grocery lowest
CATEGORY_REFUND_RATES = {
    "Electronics":            0.08,
    "Fashion":                0.12,
    "Home & Kitchen":         0.06,
    "Beauty & Personal Care": 0.05,
    "Grocery & Gourmet":      0.02,
    "Toys & Games":           0.06,
    "Sports & Outdoors":      0.05,
    "Books & Media":          0.03,
    "Automotive":             0.04,
    "Health & Wellness":      0.04,
}

_refund_counter = [0]

def derive_refunds(orders_df, shipments_df, year, month):
    diwali_month = DIWALI_MONTH.get(year, 11)

    # Idea 12: post-festive refund spike in Dec-Jan after Diwali
    if month in [12, 1]:
        base_refund_rate = 0.09
    elif month == diwali_month or month == diwali_month - 1:
        base_refund_rate = 0.07  # slightly higher during festive too
    else:
        base_refund_rate = 0.05

    eligible = orders_df[orders_df["order_status"].isin(["delivered", "shipped"])].copy()
    if len(eligible) == 0:
        return pd.DataFrame()

    ship_status = {}
    if len(shipments_df) > 0:
        for _, sh in shipments_df.iterrows():
            ship_status[sh["order_id"]] = sh["status"]

    rows = []
    for _, o in eligible.iterrows():
        # Idea 13: refund rate by category
        cat_parent_name = cat_parent.get(
            product_info.get(o["product_id"], {}).get("category_id", ""), ""
        )
        refund_rate = CATEGORY_REFUND_RATES.get(cat_parent_name, base_refund_rate)

        # blend with base rate — category rate skews but base month rate anchors
        final_rate = (refund_rate + base_refund_rate) / 2

        if random.random() > final_rate:
            continue  # this order doesn't get a refund

        status = random.choices(REFUND_STATUSES, weights=REFUND_WEIGHTS)[0]
        o_date = datetime.strptime(o["order_date"], "%Y-%m-%d").date()

        s_status = ship_status.get(o["order_id"], "delivered")
        if s_status == "delayed":
            reason = random.choice(REASONS_DELAYED)
        elif s_status == "delivered":
            reason = random.choice(REASONS_DELIVERED)
        else:
            reason = random.choice(REASONS_DEFAULT)

        refund_pct    = random.uniform(0.5, 1.0)
        refund_amount = round(o["order_amount"] * refund_pct, 2)

        requested = o_date + timedelta(days=random.randint(5, 20))
        if status in ("processed", "approved"):
            processed = (requested + timedelta(days=random.randint(2, 10))).strftime("%Y-%m-%d")
        else:
            processed = None

        _refund_counter[0] += 1

        rows.append({
            "refund_id":      f"REF{_refund_counter[0]:010d}",
            "order_id":       o["order_id"],
            "customer_id":    o["customer_id"],
            "product_id":     o["product_id"],
            "refund_amount":  refund_amount,
            "reason":         reason,
            "status":         status,
            "requested_date": requested.strftime("%Y-%m-%d"),
            "processed_date": processed,
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test refunds

# COMMAND ----------

sample_refs = derive_refunds(sample_orders, sample_ships_full, 2023, 6)
eligible_count = len(sample_orders[sample_orders["order_status"].isin(["delivered", "shipped"])])
print(f"eligible: {eligible_count}, refunds: {len(sample_refs)}")
display(sample_refs.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derive commissions
# MAGIC
# MAGIC Rule 8: no cancelled orders.
# MAGIC Rule 9: rate by category.
# MAGIC Rule 10: reduced if refunded.

# COMMAND ----------

_commission_counter = [0]

def derive_commissions(orders_df, refunds_df):
    active_orders = orders_df[orders_df["order_status"] != "cancelled"].copy()
    if len(active_orders) == 0:
        return pd.DataFrame()

    refunded_orders = set()
    if len(refunds_df) > 0:
        refunded_orders = set(refunds_df["order_id"].tolist())

    rows = []
    for _, o in active_orders.iterrows():
        sale_amount = o["order_amount"]

        cat_id = product_info.get(o["product_id"], {}).get("category_id", "")
        commission_rate = get_commission_rate(cat_id)

        # Rule 10: halve commission if refunded
        if o["order_id"] in refunded_orders:
            commission_rate = round(commission_rate * 0.5, 4)

        commission_amt = round(sale_amount * commission_rate, 2)
        o_date = datetime.strptime(o["order_date"], "%Y-%m-%d").date()

        _commission_counter[0] += 1

        rows.append({
            "commission_id":     f"COM{_commission_counter[0]:010d}",
            "seller_id":         o["seller_id"],
            "order_id":          o["order_id"],
            "sale_amount":       sale_amount,
            "commission_rate":   commission_rate,
            "commission_amount": commission_amt,
            "month_year":        f"{o_date.year}-{o_date.month:02d}",
            "payment_status":    random.choices(["paid", "pending", "disputed"], weights=[80, 15, 5])[0],
        })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test commissions

# COMMAND ----------

sample_coms = derive_commissions(sample_orders, sample_refs)
cancelled_count = len(sample_orders[sample_orders["order_status"] == "cancelled"])
print(f"orders: {len(sample_orders)}, cancelled: {cancelled_count}")
print(f"commissions: {len(sample_coms)} (should be {len(sample_orders) - cancelled_count})")
display(sample_coms.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dirty data injection helpers

# COMMAND ----------

def inject_dupes(df, rate=0.01):
    n_dupes = max(1, int(len(df) * rate))
    dupes   = df.sample(n=n_dupes, replace=True)
    return pd.concat([df, dupes], ignore_index=True)


def inject_ghost_order_ids(df, rate=0.03):
    if "order_id" not in df.columns:
        return df
    mask = [random.random() < rate for _ in range(len(df))]
    for i, is_ghost in enumerate(mask):
        if is_ghost:
            df.loc[i, "order_id"] = f"ORD_GHOST_{random.randint(100000, 999999)}"
    return df


def inject_null_customers(df, rate=0.02):
    if "customer_id" not in df.columns:
        return df
    mask = [random.random() < rate for _ in range(len(df))]
    for i, is_null in enumerate(mask):
        if is_null:
            df.loc[i, "customer_id"] = None
    return df


def flip_date_format(df, col, rate=1.0):
    if col not in df.columns:
        return df
    if rate >= 1.0:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d-%m-%Y")
    else:
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

CHANNELS         = ["organic_search", "paid_search", "social_media", "email", "push_notification", "direct", "referral"]
CHANNEL_W_FEST   = [15, 25, 20, 15, 15,  5,  5]  # more paid ads during festive
CHANNEL_W_NORMAL = [30, 15, 20, 15, 10,  8,  2]

DEVICES          = ["mobile", "desktop", "tablet"]
DEVICE_W         = [65, 28, 7]   # mobile dominant in India

_session_counter = [0]

def derive_sessions(orders_df, d):
    year = d.year
    fyr  = FESTIVAL_DATES.get(year, FESTIVAL_DATES[2025])
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]

    is_festive = (
        (diwali  and abs((d - diwali).days)  <= 7) or
        (dhamaka and 0 <= (d - dhamaka).days <= 9) or
        d.month in [9]  # pre-festive buildup
    )

    ch_weights = CHANNEL_W_FEST if is_festive else CHANNEL_W_NORMAL

    rows = []

    # one converted session per order
    for _, o in orders_df.iterrows():
        channel = random.choices(CHANNELS, weights=ch_weights)[0]
        device  = random.choices(DEVICES,  weights=DEVICE_W)[0]

        # Idea 18: deeper browsing during festive
        pages    = random.randint(5, 20) if is_festive else random.randint(2, 10)
        duration = random.randint(300, 1800) if is_festive else random.randint(60, 900)

        s_start = datetime(d.year, d.month, d.day, random.randint(6, 22), random.randint(0, 59))

        rows.append({
            "session_id":               o["session_id"],
            "customer_id":              o["customer_id"],
            "channel":                  channel,
            "device_type":              device,
            "utm_source":               channel.replace("_", "-"),
            "pages_viewed":             pages,
            "session_duration_seconds": duration,
            "session_start":            s_start.strftime("%Y-%m-%d %H:%M:%S"),
            "session_end":              (s_start + timedelta(seconds=duration)).strftime("%Y-%m-%d %H:%M:%S"),
            "converted":                1,
            "session_date":             d.strftime("%Y-%m-%d"),
        })

    # bounce sessions — visited but didn't buy
    # Idea 15: fewer bounces during festive (urgency buying pulls people to checkout faster)
    n_bounces = int(len(orders_df) * (0.15 if is_festive else 0.40))
    for _ in range(n_bounces):
        cust    = random.choice(customer_ids)
        ch      = random.choices(CHANNELS, weights=ch_weights)[0]
        dev     = random.choices(DEVICES,  weights=DEVICE_W)[0]
        dur     = random.randint(30, 300)
        b_start = datetime(d.year, d.month, d.day, random.randint(6, 22), random.randint(0, 59))
        _session_counter[0] += 1

        rows.append({
            "session_id":               f"SES_B{_session_counter[0]:010d}",
            "customer_id":              cust,
            "channel":                  ch,
            "device_type":              dev,
            "utm_source":               ch.replace("_", "-"),
            "pages_viewed":             random.randint(1, 5),
            "session_duration_seconds": dur,
            "session_start":            b_start.strftime("%Y-%m-%d %H:%M:%S"),
            "session_end":              (b_start + timedelta(seconds=dur)).strftime("%Y-%m-%d %H:%M:%S"),
            "converted":                0,
            "session_date":             d.strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(rows)

print("derive_sessions ready")

# COMMAND ----------

CART_EVENTS   = ["cart_add", "cart_view", "cart_remove", "wishlist_add"]
CART_W        = [40, 35, 15, 10]

_cart_counter = [0]

def derive_cart_events(orders_df, d):
    year = d.year
    fyr  = FESTIVAL_DATES.get(year, FESTIVAL_DATES[2025])
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]

    is_festive = (
        (diwali  and abs((d - diwali).days)  <= 7) or
        (dhamaka and 0 <= (d - dhamaka).days <= 9)
    )

    rows = []
    for _, o in orders_df.iterrows():
        # Idea 18: more cart events during festive — deeper consideration
        # cancelled orders get fewer events, end with cart_remove
        if o["order_status"] == "cancelled":
            n_events = random.randint(1, 3)
        elif is_festive:
            n_events = random.randint(3, 8)
        else:
            n_events = random.randint(1, 4)

        for j in range(n_events):
            event_date = d - timedelta(days=random.randint(0, 2))
            event_type = random.choices(CART_EVENTS, weights=CART_W)[0]

            # last event for cancelled = cart_remove
            if o["order_status"] == "cancelled" and j == n_events - 1:
                event_type = "cart_remove"

            _cart_counter[0] += 1
            rows.append({
                "cart_event_id":   f"CRT{_cart_counter[0]:010d}",
                "session_id":      o["session_id"],
                "customer_id":     o["customer_id"],
                "product_id":      o["product_id"],
                "event_type":      event_type,
                "quantity":        random.randint(1, 3),
                "price_at_event":  o["order_amount"],
                "event_timestamp": datetime(
                    event_date.year, event_date.month, event_date.day,
                    random.randint(6, 22), random.randint(0, 59)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "event_date":      event_date.strftime("%Y-%m-%d"),
            })

    return pd.DataFrame(rows)

print("derive_cart_events ready")

# COMMAND ----------

CAMP_CHANNELS  = ["email", "push_notification", "sms", "whatsapp"]
CAMP_W_FEST    = [30, 35, 20, 15]  # more push during festive
CAMP_W_NORMAL  = [40, 30, 20, 10]

_campaign_counter = [0]

def derive_campaign_responses(orders_df, d):
    year = d.year
    fyr  = FESTIVAL_DATES.get(year, FESTIVAL_DATES[2025])
    diwali  = fyr["diwali"]
    dhamaka = fyr["dhamaka_start"]
    holi    = fyr["holi"]

    is_festive = (
        (diwali  and abs((d - diwali).days)  <= 7) or
        (dhamaka and 0 <= (d - dhamaka).days <= 9) or
        (holi    and abs((d - holi).days)    <= 2) or
        (d.month == 1 and d.day in [25, 26, 27]) or
        (d.month == 8 and d.day in [14, 15, 16])
    )

    ch_weights = CAMP_W_FEST if is_festive else CAMP_W_NORMAL

    # more orders have a campaign behind them on festive days
    camp_rate = 0.70 if is_festive else 0.45

    rows = []
    for _, o in orders_df.iterrows():
        if random.random() > camp_rate:
            continue

        channel   = random.choices(CAMP_CHANNELS, weights=ch_weights)[0]
        sent_date = d - timedelta(days=random.randint(0, 2))

        # festive: higher open and click rates — people expect sale emails
        opened    = random.random() < (0.48 if is_festive else 0.28)
        clicked   = opened   and random.random() < (0.38 if is_festive else 0.20)
        converted = clicked  and random.random() < (0.65 if is_festive else 0.45)

        coupon = get_coupon(d, fyr) if clicked else None

        _campaign_counter[0] += 1
        rows.append({
            "response_id":   f"CAM{_campaign_counter[0]:010d}",
            "customer_id":   o["customer_id"],
            "order_id":      o["order_id"] if converted else None,
            "channel":       channel,
            "sent":          1,
            "opened":        1 if opened    else 0,
            "clicked":       1 if clicked   else 0,
            "converted":     1 if converted else 0,
            "coupon_code":   coupon,
            "campaign_date": sent_date.strftime("%Y-%m-%d"),
            "response_date": d.strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(rows)

print("derive_campaign_responses ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 3 — Main Generation Loop
# MAGIC
# MAGIC Month by month: orders → payments → shipments → reviews → refunds → commissions.
# MAGIC Each entity depends on the one before it. Cardinality preserved.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build months list

# COMMAND ----------

days_to_process = []
cur = HISTORY_START
while cur <= HISTORY_END:
    days_to_process.append(cur)
    cur += timedelta(days=1)

print(f"days to process: {len(days_to_process)}")
print(f"from {days_to_process[0]} to {days_to_process[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders per month — platform growth

# COMMAND ----------

# DBTITLE 1,Cell 86
BASE_DAILY = 8000 / 30  # ~267 orders/day baseline in Jan 2020

def orders_for_day(d):
    years_elapsed = (d - date(2020, 1, 1)).days / 365.25
    base = BASE_DAILY * (1.23 ** years_elapsed)
    mult = get_day_multiplier(d)
    return max(50, int(base * mult))

# sanity check — a few representative days
check_days = [
    date(2020, 4, 15),   # COVID lockdown
    date(2020, 11, 14),  # Diwali 2020
    date(2022, 10, 24),  # Diwali 2022
    date(2023, 11, 12),  # Diwali 2023
    date(2024, 10, 20),  # Dhamaka Days start 2024
    date(2025,  3, 14),  # Holi 2025
    date(2025,  6, 15),  # Normal day
]
print("Day multiplier sanity check:")
for cd in check_days:
    n = orders_for_day(cd)
    m = get_day_multiplier(cd)
    print(f"  {cd}  mult={m:.2f}x  orders={n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the main loop

# COMMAND ----------

totals = {
    "orders": 0, "payments": 0, "shipments": 0,
    "reviews": 0, "refunds": 0, "commissions": 0,
    "sessions": 0, "cart_events": 0, "campaign_responses": 0,
}

loop_start   = time.time()
# batch uploads — collect 7 days then upload together (faster ADLS calls)
BATCH_DAYS   = 7
batch_buffer = {k: [] for k in totals}

def flush_batch(batch_buffer, batch_label):
    for entity, frames in batch_buffer.items():
        if not frames:
            continue
        combined = pd.concat(frames, ignore_index=True)
        path = f"landing/{entity}/{batch_label}/{entity}_{batch_label}.csv"
        upload_to_adls(combined, path)
        totals[entity] += len(combined)
    # clear buffer
    for k in batch_buffer:
        batch_buffer[k] = []

batch_days_collected = []

for i, d in enumerate(days_to_process):
    n_orders = orders_for_day(d)

    # 1. ORDERS
    orders_df = generate_day_orders(d, n_orders)

    # 2. PAYMENTS
    payments_df = derive_payments(orders_df)

    # 3. SHIPMENTS
    shipments_df = derive_shipments(orders_df)

    # 4. REVIEWS
    reviews_df = derive_reviews(orders_df, shipments_df, review_rate=0.30)

    # 5. REFUNDS
    refunds_df = derive_refunds(orders_df, shipments_df, d.year, d.month)

    # 6. COMMISSIONS
    commissions_df = derive_commissions(orders_df, refunds_df)

    # 7. SESSIONS
    sessions_df = derive_sessions(orders_df, d)

    # 8. CART EVENTS
    cart_df = derive_cart_events(orders_df, d)

    # 9. CAMPAIGN RESPONSES
    campaign_df = derive_campaign_responses(orders_df, d)

    # === DIRTY DATA ===

    orders_dirty = orders_df.drop(columns=["payment_status"]).copy()
    orders_dirty = inject_null_customers(orders_dirty, rate=0.02)
    orders_dirty = inject_dupes(orders_dirty, rate=0.01)
    orders_dirty = flip_date_format(orders_dirty, "order_date", rate=0.15)

    payments_dirty    = inject_dupes(payments_df,    rate=0.01)
    shipments_dirty   = inject_dupes(shipments_df,   rate=0.01) if len(shipments_df) > 0 else shipments_df
    shipments_dirty   = inject_ghost_order_ids(shipments_dirty, rate=0.03)
    reviews_dirty     = inject_dupes(reviews_df,     rate=0.01) if len(reviews_df)   > 0 else reviews_df
    reviews_dirty     = flip_date_format(reviews_dirty, "review_date", rate=1.0)
    reviews_dirty     = inject_ghost_order_ids(reviews_dirty, rate=0.03)
    refunds_dirty     = inject_dupes(refunds_df,     rate=0.01) if len(refunds_df)   > 0 else refunds_df
    refunds_dirty     = inject_ghost_order_ids(refunds_dirty, rate=0.03)
    commissions_dirty = inject_dupes(commissions_df, rate=0.01) if len(commissions_df) > 0 else commissions_df
    sessions_dirty    = inject_dupes(sessions_df,    rate=0.01)
    cart_dirty        = inject_dupes(cart_df,        rate=0.01)
    campaign_dirty    = inject_dupes(campaign_df,    rate=0.01) if len(campaign_df) > 0 else campaign_df

    # add to batch buffer
    batch_buffer["orders"].append(orders_dirty)
    batch_buffer["payments"].append(payments_dirty)
    if len(shipments_dirty) > 0:   batch_buffer["shipments"].append(shipments_dirty)
    if len(reviews_dirty) > 0:     batch_buffer["reviews"].append(reviews_dirty)
    if len(refunds_dirty) > 0:     batch_buffer["refunds"].append(refunds_dirty)
    if len(commissions_dirty) > 0: batch_buffer["commissions"].append(commissions_dirty)
    batch_buffer["sessions"].append(sessions_dirty)
    batch_buffer["cart_events"].append(cart_dirty)
    if len(campaign_dirty) > 0:    batch_buffer["campaign_responses"].append(campaign_dirty)

    batch_days_collected.append(d)

    # flush every 7 days OR on last day
    is_last = (i == len(days_to_process) - 1)
    if len(batch_days_collected) >= BATCH_DAYS or is_last:
        label = batch_days_collected[0].strftime("%Y%m%d")
        flush_batch(batch_buffer, label)
        batch_days_collected = []

    # progress every 30 days
    if (i + 1) % 30 == 0 or is_last:
        elapsed = (time.time() - loop_start) / 60
        pct     = (i + 1) / len(days_to_process) * 100
        print(f"[{i+1}/{len(days_to_process)}] {d}  {pct:.0f}%  {elapsed:.1f}min  orders_today={n_orders:,}")

    del orders_df, orders_dirty, payments_df, payments_dirty
    del shipments_df, reviews_df, refunds_df, commissions_df
    del sessions_df, cart_df, campaign_df

print()
print("=== TOTALS ===")
for entity, count in totals.items():
    print(f"  {entity:20s}: {count:>12,}")
print(f"  total time: {(time.time() - loop_start)/60:.1f} minutes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check totals

# COMMAND ----------

print("=== Order-linked data ===")
for entity, count in totals.items():
    print(f"  {entity:15s}: {count:>12,}")
print(f"  {'':15s}  {'─'*12}")
print(f"  {'TOTAL':15s}: {sum(totals.values()):>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 4 — Inventory Snapshots
# MAGIC
# MAGIC Weekly. Product × warehouse stock levels.
# MAGIC Rule 12: quantity_reserved ≤ quantity_available always.

# COMMAND ----------

def generate_inventory(snap_date, n_products=50):
    diwali_month = DIWALI_MONTH.get(snap_date.year, 11)

    # Idea 20: festive stock pressure — lower qty_available in Oct/Nov, reorder breach more often
    is_festive = snap_date.month in [diwali_month, diwali_month - 1]
    is_post    = snap_date.month in [12, 1]  # post-festive restock

    sampled_pids = random.sample(product_ids, min(n_products, len(product_ids)))
    rows = []

    for pid in sampled_pids:
        for wid in warehouse_ids:
            if is_festive:
                # stock running low — high demand has depleted inventory
                qty_avail    = random.randint(0, 150)
                reorder_level = random.choice([30, 50, 75, 100])
            elif is_post:
                # restocking after festive
                qty_avail    = random.randint(100, 600)
                reorder_level = random.choice([20, 30, 50])
            else:
                qty_avail    = random.randint(0, 500)
                reorder_level = random.choice([10, 20, 30, 50])

            qty_reserved = random.randint(0, min(qty_avail, 50))

            # T3 dirty: last_updated in DD-MM-YYYY HH:MM format
            last_updated = f"{snap_date.strftime('%d-%m-%Y')} {random.randint(0,23):02d}:{random.randint(0,59):02d}"

            rows.append({
                "inventory_id":       f"INV{random.randint(10000000, 99999999)}",
                "product_id":         pid,
                "warehouse_id":       wid,
                "quantity_available": qty_avail,
                "quantity_reserved":  qty_reserved,
                "reorder_level":      reorder_level,
                "last_updated":       last_updated,
                "snapshot_date":      snap_date.strftime("%Y-%m-%d"),
            })

    return pd.DataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test inventory

# COMMAND ----------

sample_inv = generate_inventory(date(2023, 1, 2))
print(f"generated {len(sample_inv)} rows")

bad_inv = sample_inv[sample_inv["quantity_reserved"] > sample_inv["quantity_available"]]
print(f"reserved > available: {len(bad_inv)} (should be 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload weekly inventory

# COMMAND ----------

print(f"uploading inventory: {HISTORY_START} to {HISTORY_END}")

total_inv_rows = 0
week = HISTORY_START
inv_count = 0

while week <= HISTORY_END:
    df = generate_inventory(week, n_products=50)
    df = inject_dupes(df, rate=0.01)

    path = f"landing/inventory/{week.year}/{week.month:02d}/inventory_{week.strftime('%Y%m%d')}.csv"
    upload_to_adls(df, path)
    total_inv_rows += len(df)
    inv_count += 1

    if inv_count % 50 == 0:
        print(f"  {inv_count} files ({total_inv_rows:,} rows)...")

    week += timedelta(weeks=1)

print(f"done — {total_inv_rows:,} inventory rows in {inv_count} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Part 5 — Geo Dimension

# COMMAND ----------

STATE_REGION = {
    "Delhi": "North", "Haryana": "North", "Punjab": "North",
    "Uttar Pradesh": "North", "Rajasthan": "North", "Uttarakhand": "North",
    "Maharashtra": "West", "Gujarat": "West", "Goa": "West",
    "Madhya Pradesh": "Central", "Chhattisgarh": "Central",
    "Bihar": "East", "West Bengal": "East", "Odisha": "East", "Jharkhand": "East",
    "Karnataka": "South", "Tamil Nadu": "South", "Kerala": "South",
    "Telangana": "South", "Andhra Pradesh": "South", "Chandigarh": "North",
}

CITY_DATA = [
    ("Mumbai",          "Maharashtra",     400001, 400099, 19.076, 72.877),
    ("Delhi",           "Delhi",           110001, 110099, 28.613, 77.209),
    ("Bangalore",       "Karnataka",       560001, 560099, 12.971, 77.594),
    ("Chennai",         "Tamil Nadu",      600001, 600099, 13.082, 80.270),
    ("Hyderabad",       "Telangana",       500001, 500099, 17.385, 78.486),
    ("Kolkata",         "West Bengal",     700001, 700099, 22.572, 88.363),
    ("Pune",            "Maharashtra",     411001, 411099, 18.520, 73.856),
    ("Ahmedabad",       "Gujarat",         380001, 380099, 23.022, 72.571),
    ("Jaipur",          "Rajasthan",       302001, 302099, 26.912, 75.787),
    ("Lucknow",         "Uttar Pradesh",   226001, 226099, 26.846, 80.946),
    ("Surat",           "Gujarat",         395001, 395099, 21.170, 72.831),
    ("Bhopal",          "Madhya Pradesh",  462001, 462099, 23.259, 77.412),
    ("Patna",           "Bihar",           800001, 800099, 25.594, 85.137),
    ("Chandigarh",      "Chandigarh",      160001, 160099, 30.733, 76.779),
    ("Kochi",           "Kerala",          682001, 682099,  9.931, 76.267),
    ("Indore",          "Madhya Pradesh",  452001, 452099, 22.719, 75.857),
    ("Nagpur",          "Maharashtra",     440001, 440099, 21.145, 79.088),
    ("Vadodara",        "Gujarat",         390001, 390099, 22.307, 73.181),
    ("Varanasi",        "Uttar Pradesh",   221001, 221099, 25.317, 82.973),
    ("Agra",            "Uttar Pradesh",   282001, 282099, 27.176, 78.008),
    ("Coimbatore",      "Tamil Nadu",      641001, 641099, 11.016, 76.955),
    ("Visakhapatnam",   "Andhra Pradesh",  530001, 530099, 17.686, 83.218),
]

rows = []
for (city, state, p_start, p_end, lat, lng) in CITY_DATA:
    for pin in range(p_start, min(p_start + 20, p_end + 1)):
        rows.append({
            "pincode":   str(pin),
            "city":      city,
            "state":     state,
            "region":    STATE_REGION.get(state, "Other"),
            "latitude":  round(lat + random.uniform(-0.05, 0.05), 4),
            "longitude": round(lng + random.uniform(-0.05, 0.05), 4),
        })

geo_df = pd.DataFrame(rows)
print(f"geo dimension: {len(geo_df)} rows")

# COMMAND ----------

display(geo_df.head(10))

# COMMAND ----------

n = upload_to_adls(geo_df, "landing/geo_dimension/geo_dimension.csv")
print(f"uploaded {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Summary

# COMMAND ----------

total_time = time.time() - loop_start

print("=" * 60)
print("03c complete — historical bulk data with full cardinality")
print("=" * 60)
print()
for entity, count in totals.items():
    print(f"  {entity:15s}: {count:>12,}")
print(f"  {'inventory':15s}: {total_inv_rows:>12,}")
print(f"  {'geo dimension':15s}: {len(geo_df):>12,}")
print(f"  {'':15s}  {'─'*12}")
print(f"  {'TOTAL':15s}: {sum(totals.values()) + total_inv_rows + len(geo_df):>12,}")
print()
print(f"  time: {total_time/60:.1f} minutes")
print()
print("Cardinality rules enforced:")
print("  1.  Customer can't order before registration")
print("  2.  Failed payment → cancelled order")
print("  3.  Only delivered orders get reviews")
print("  4.  Review date after delivery date")
print("  5.  Max 2 reviews per (customer, product)")
print("  6.  Only delivered/shipped orders get refunds")
print("  7.  Refund reason matches shipment context")
print("  8.  No commission on cancelled orders")
print("  9.  Commission rate by category")
print("  10. Reduced commission on refunded orders")
print("  11. Shipment weight matches product weight")
print("  12. Reserved ≤ available in inventory")
print("  13. Full temporal chain preserved")
print()
print("Next: delete old landing/ files if needed, then Bronze ingestion")
