# Databricks notebook source
# ==============================================================
# BharatMart 360° — 03a_load_azure_sql.py
# Phase 1: Load master data into Azure SQL DB with CDC enabled
# ==============================================================
# Environment  : Databricks Premium (14-day trial)
# Connections  : pymssql  → DDL, stored procs, CDC enablement
#                spark.write.jdbc() → bulk data insert
#                spark.read.jdbc()  → row count verification
# Run          : IDEMPOTENT — initial population only
# Dirty Types  : T1 (NULL), T2 (dupe rows), T3 (price string),
#                T4 (impossible values), T5 (format chaos), T6 (outlier)
# ==============================================================

# COMMAND ----------

# MAGIC %pip install pymssql faker --quiet
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pymssql
import pandas as pd
import numpy as np
import random
import json
import re
from datetime import date, datetime
from faker import Faker

fake = Faker("en_IN")
random.seed(42)
np.random.seed(42)
print("✅ Libraries loaded")

# COMMAND ----------

# ── WIDGETS ──────────────────────────────────────────────────
dbutils.widgets.text("sql_server",   "bahartmartsql.database.windows.net", "Azure SQL Server")
dbutils.widgets.text("sql_database", "bharatmart_db", "Database name")
dbutils.widgets.text("sql_username", "veer", "SQL username")
dbutils.widgets.text("sql_password", "", "SQL password")

SQL_SERVER   = dbutils.widgets.get("sql_server")
SQL_DATABASE = dbutils.widgets.get("sql_database")
SQL_USERNAME = dbutils.widgets.get("sql_username")
SQL_PASSWORD = dbutils.widgets.get("sql_password")

# JDBC URL — MSSQL JDBC driver is pre-bundled in Databricks, no install needed
JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER}:1433;"
    f"database={SQL_DATABASE};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30"
)
JDBC_PROPS = {
    "user":     SQL_USERNAME,
    "password": SQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print(f"✅ Config — server: {SQL_SERVER} | db: {SQL_DATABASE}")

# COMMAND ----------

# ── CONNECTION HELPERS ───────────────────────────────────────

def get_conn():
    """
    pymssql — pure Python TDS, no ODBC driver required.
    Used for: CREATE TABLE, ALTER TABLE, CDC stored procs.
    """
    return pymssql.connect(
        server=SQL_SERVER,
        user=SQL_USERNAME,
        password=SQL_PASSWORD,
        database=SQL_DATABASE,
        port=1433,
        login_timeout=30,
        tds_version="7.4"
    )

def exec_sql(sql, label=""):
    """Run DDL or stored proc via pymssql. Prints result."""
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
        if label: print(f"  ✅ {label}")
    except Exception as e:
        if label: print(f"  ⚠️  {label} — {e}")

def bulk_load(df_pandas, table_name):
    """
    pandas DataFrame → Spark DataFrame → spark.write.jdbc()
    mode=append because table already exists (created via DDL above).
    JDBC bulk insert — fastest method available in Databricks.
    """
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.jdbc(
        url=JDBC_URL,
        table=f"dbo.{table_name}",
        mode="append",
        properties=JDBC_PROPS
    )

# ── TEST BOTH CONNECTIONS ────────────────────────────────────
print("Testing pymssql connection...")
try:
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT @@VERSION")
    ver = cur.fetchone()[0]
    conn.close()
    print(f"  ✅ pymssql OK — {ver[:55]}...")
except Exception as e:
    raise RuntimeError(f"❌ pymssql FAILED: {e}")

print("Testing JDBC connection...")
try:
    spark.read.jdbc(
        url=JDBC_URL,
        table="(SELECT 1 AS n) t",
        properties=JDBC_PROPS
    ).collect()
    print("  ✅ JDBC OK")
except Exception as e:
    raise RuntimeError(f"❌ JDBC FAILED: {e}")

# COMMAND ----------

# ── REFERENCE LOOKUP DATA ────────────────────────────────────

STATES = {
    "Maharashtra":"MH","Delhi":"DL","Karnataka":"KA","Tamil Nadu":"TN",
    "Uttar Pradesh":"UP","Gujarat":"GJ","West Bengal":"WB","Rajasthan":"RJ",
    "Telangana":"TS","Andhra Pradesh":"AP","Kerala":"KL","Madhya Pradesh":"MP",
    "Punjab":"PB","Haryana":"HR","Bihar":"BR","Odisha":"OD",
    "Jharkhand":"JH","Assam":"AS","Himachal Pradesh":"HP","Goa":"GA"
}
STATE_NAMES   = list(STATES.keys())
STATE_WEIGHTS = [15,12,10,9,8,8,7,6,5,5,5,4,3,3,3,2,2,2,1,1]

STATE_CODES = {
    "Maharashtra":"27","Delhi":"07","Karnataka":"29","Tamil Nadu":"33",
    "Uttar Pradesh":"09","Gujarat":"24","West Bengal":"19","Rajasthan":"08",
    "Telangana":"36","Andhra Pradesh":"37","Kerala":"32","Madhya Pradesh":"23",
    "Punjab":"03","Haryana":"06","Bihar":"10","Odisha":"21",
    "Jharkhand":"20","Assam":"18","Himachal Pradesh":"02","Goa":"30"
}

TIER1 = ["Mumbai","Delhi","Bangalore","Chennai","Hyderabad","Kolkata","Pune","Ahmedabad"]
TIER2 = ["Jaipur","Lucknow","Surat","Bhopal","Patna","Chandigarh","Kochi","Indore","Nagpur","Vadodara"]
TIER3 = ["Varanasi","Agra","Coimbatore","Madurai","Mysore","Faridabad","Ghaziabad","Dehradun","Jodhpur"]
ALL_CITIES = TIER1 + TIER2 + TIER3

# Real Indian city-to-pincode ranges
CITY_PINCODES = {
    "Mumbai": (400001,400099), "Delhi": (110001,110099),
    "Bangalore": (560001,560099), "Chennai": (600001,600099),
    "Hyderabad": (500001,500099), "Kolkata": (700001,700099),
    "Pune": (411001,411099), "Ahmedabad": (380001,380099),
    "Jaipur": (302001,302099), "Lucknow": (226001,226099),
    "Surat": (395001,395099), "Bhopal": (462001,462099),
    "Patna": (800001,800099), "Chandigarh": (160001,160099),
    "Kochi": (682001,682099), "Indore": (452001,452099),
    "Nagpur": (440001,440099), "Vadodara": (390001,390099),
    "Varanasi": (221001,221099), "Agra": (282001,282099),
    "Coimbatore": (641001,641099), "Madurai": (625001,625099),
    "Mysore": (570001,570099), "Faridabad": (121001,121099),
    "Ghaziabad": (201001,201099), "Dehradun": (248001,248099),
    "Jodhpur": (342001,342099),
}

def gen_pincode_for_city(city):
    lo, hi = CITY_PINCODES.get(city, (400001, 999999))
    return str(random.randint(lo, hi))

SELLER_PREFIXES = [
    "Sharma","Patel","Singh","Kumar","Gupta","Verma","Reddy","Nair",
    "Mehta","Shah","Joshi","Rao","Das","Mishra","Agarwal","Soni","Jain",
    "Rajesh","Suresh","Krishna","Sai","Lakshmi","Shri Ganesh",
    "New India","Royal","National","Star","Golden","Supreme","Metro",
]
SELLER_DOMAINS = [
    "Electronics","Mobiles","Fashion","Textiles","Clothing","Sarees",
    "Grocery","Kirana Store","General Store","Home Appliances","Furniture",
    "Sports","Books","Beauty","Cosmetics","Jewellers","Watches",
    "Footwear","Pharma","Kitchen","Digital","Computers",
]
SELLER_SUFFIXES = [
    "Traders","Enterprises","Store","Mart","Hub","Emporium",
    "Corner","House","Agency","& Sons","& Co","Bazaar","World",
    "Collections","Zone","Point","Center","Express",
]

def gen_seller_name():
    return f"{random.choice(SELLER_PREFIXES)} {random.choice(SELLER_DOMAINS)} {random.choice(SELLER_SUFFIXES)}"


LOYALTY_TIERS   = ["Bronze","Silver","Gold","Platinum","Diamond"]
LOYALTY_WEIGHTS = [40,30,18,9,3]

def gen_gstin(state):
    code = STATE_CODES.get(state, "27")
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    pan = (
        "".join(random.choices(letters, k=5)) +
        "".join(random.choices("0123456789", k=4)) +
        random.choice(letters)
    )
    return f"{code}{pan}1Z1"

def gen_phone():
    """Plain 10-digit mobile number starting with 7/8/9 — dirty injector will mangle formats later."""
    return f"{random.choice([7,8,9])}{random.randint(100000000,999999999)}"

print("✅ Reference data ready")

# COMMAND ----------

# ── DIRTY DATA INJECTOR ──────────────────────────────────────
# All injection is tracked so Silver verification report can match counts exactly.

class DirtyDataInjector:
    def __init__(self):
        self.report = {}

    def _log(self, table, dtype, n):
        k = f"{table}.Type{dtype}"
        self.report[k] = self.report.get(k, 0) + n

    # T1 — NULL injection
    def nullify(self, df, table, col, pct):
        n   = int(len(df) * pct)
        idx = df.sample(n=n, random_state=42).index
        df.loc[idx, col] = None
        self._log(table, 1, n)
        return df

    # T2 — Duplicate rows (same PK, appended)
    def duplicate_rows(self, df, table, pct):
        n    = int(len(df) * pct)
        dupes = df.sample(n=n, random_state=42).copy()
        result = pd.concat([df, dupes], ignore_index=True)
        self._log(table, 2, n)
        return result

    # T3 — Price stored as currency string instead of number
    def price_to_string(self, df, table, col, pct=0.30):
        """30% rows become '₹1,299.00' | rest become plain '1299.0' string"""
        n   = int(len(df) * pct)
        idx = df.sample(n=n, random_state=43).index
        def rupee_fmt(v):
            try:    return f"₹{float(v):,.2f}"
            except: return str(v)
        df[col] = df[col].astype(str)                    # all to string first
        df.loc[idx, col] = df.loc[idx, col].apply(rupee_fmt)
        self._log(table, 3, n)
        return df

    # T4 — Impossible values
    def impossible_dob(self, df, table, col):
        n1  = int(len(df) * 0.010)   # 1%  → 1900-01-01
        n2  = int(len(df) * 0.005)   # 0.5% → 2030-01-01
        i1  = df.dropna(subset=[col]).sample(n=n1, random_state=44).index
        df.loc[i1, col] = date(1900, 1, 1)
        remain = df[~df.index.isin(i1)].dropna(subset=[col])
        i2  = remain.sample(n=n2, random_state=45).index
        df.loc[i2, col] = date(2030, 1, 1)
        self._log(table, 4, n1 + n2)
        return df

    def zero_price(self, df, table, col, pct=0.003):
        n   = max(1, int(len(df) * pct))
        idx = df.sample(n=n, random_state=46).index
        df.loc[idx, col] = "0.0"
        self._log(table, 4, n)
        return df

    # T5 — Format inconsistency
    def case_chaos(self, df, table, col):
        """Mix UPPER / lower / Title case across all rows."""
        def rand_case(s):
            if not isinstance(s, str): return s
            r = random.random()
            if   r < 0.33: return s.upper()
            elif r < 0.66: return s.lower()
            else:           return s.title()
        df[col] = df[col].apply(rand_case)
        self._log(table, 5, len(df))
        return df

    def state_abbrev_mix(self, df, table, col, pct=0.35):
        """35% rows use abbreviation (MH), 10% rows ALLCAPS, rest full name."""
        n1  = int(len(df) * pct)
        i1  = df.sample(n=n1, random_state=47).index
        df.loc[i1, col] = df.loc[i1, col].apply(
            lambda s: STATES.get(s, s) if isinstance(s, str) else s
        )
        n2  = int(len(df) * 0.10)
        i2  = df[~df.index.isin(i1)].sample(n=n2, random_state=48).index
        df.loc[i2, col] = df.loc[i2, col].apply(
            lambda s: s.upper() if isinstance(s, str) else s
        )
        self._log(table, 5, n1 + n2)
        return df

    def phone_format_mix(self, df, table, col):
        """Mix +91xxxxxxxxxx / 0xxxxxxxxxx / plain 10-digit."""
        def rand_fmt(p):
            if not isinstance(p, str) or p != p: return p
            d = re.sub(r'\D', '', str(p))[-10:]
            if not d: return p
            r = random.random()
            if   r < 0.33: return f"+91{d}"
            elif r < 0.66: return f"0{d}"
            else:           return d
        df[col] = df[col].apply(rand_fmt)
        self._log(table, 5, len(df))
        return df

    def gstin_lowercase(self, df, table, col, pct=0.10):
        valid = df[col].notna()
        n     = min(int(len(df) * pct), valid.sum())
        idx   = df[valid].sample(n=n, random_state=49).index
        df.loc[idx, col] = df.loc[idx, col].str.lower()
        self._log(table, 5, n)
        return df

    # T6 — Outlier (fat-finger × 1000)
    def price_outlier(self, df, table, col, pct=0.002):
        n   = max(1, int(len(df) * pct))
        idx = df.sample(n=n, random_state=50).index
        def multiply_1000(v):
            try:
                clean = re.sub(r'[₹,]', '', str(v))
                return str(round(float(clean) * 1000, 2))
            except: return v
        df.loc[idx, col] = df.loc[idx, col].apply(multiply_1000)
        self._log(table, 6, n)
        return df

    def print_report(self):
        print("\n" + "="*55)
        print("  DIRTY DATA INJECTION REPORT")
        print("="*55)
        totals = {}
        for k, v in sorted(self.report.items()):
            tbl = k.split(".")[0]
            totals[tbl] = totals.get(tbl, 0) + v
            print(f"  {k:<38} {v:>8,}")
        print("-"*55)
        for tbl, tot in sorted(totals.items()):
            print(f"  {tbl:<38} {tot:>8,}  TOTAL")
        print("="*55)
        print("  Silver layer MUST fix/drop ALL rows above.")
        return self.report

D = DirtyDataInjector()
print("✅ DirtyDataInjector ready")

# COMMAND ----------

# -- IDEMPOTENT TABLE RESET ----------------------------------------
# DROP + CREATE ensures clean re-run without manual cleanup.
# CDC is re-enabled at the end, so this is fully safe to re-run.

print("Dropping existing tables (if any) for clean re-run...")

# Disable CDC first (must happen before DROP)
for tbl in ["customers","products","sellers","categories","brands","warehouses"]:
    exec_sql(f"""
    IF EXISTS (
        SELECT 1 FROM cdc.change_tables ct
        JOIN sys.tables t ON t.object_id = ct.source_object_id
        WHERE t.name = '{tbl}'
    )
    EXEC sys.sp_cdc_disable_table
        @source_schema = N'dbo',
        @source_name   = N'{tbl}',
        @capture_instance = N'dbo_{tbl}'
    """, f"CDC disabled on {tbl}")

# Drop tables in dependency-safe order
for tbl in ["products","customers","sellers","warehouses","brands","categories"]:
    exec_sql(f"DROP TABLE IF EXISTS dbo.{tbl}", f"Dropped {tbl}")

print("Clean slate ready")

# COMMAND ----------

# ── CREATE ALL TABLES (DDL via pymssql) ──────────────────────
# NO PRIMARY KEY constraints → allows duplicate PKs for T2 dirty data
# retail_price is VARCHAR(50) → allows "₹1,299.00" strings for T3

print("Creating tables in Azure SQL...")

exec_sql("""
CREATE TABLE dbo.categories (
    category_id      VARCHAR(20)  NOT NULL,
    category_name    VARCHAR(100) NOT NULL,
    parent_category  VARCHAR(100) NOT NULL DEFAULT '',
    level            INT          NOT NULL DEFAULT 1,
    is_active        BIT          NOT NULL DEFAULT 1,
    created_at       DATETIME     NOT NULL DEFAULT GETDATE()
)""", "categories")

exec_sql("""
CREATE TABLE dbo.brands (
    brand_id         VARCHAR(20)  NOT NULL,
    brand_name       VARCHAR(100) NOT NULL,
    country_origin   VARCHAR(50)  NOT NULL DEFAULT 'India',
    is_premium       BIT          NOT NULL DEFAULT 0,
    established_year INT,
    is_active        BIT          NOT NULL DEFAULT 1,
    created_at       DATETIME     NOT NULL DEFAULT GETDATE()
)""", "brands")

exec_sql("""
CREATE TABLE dbo.warehouses (
    warehouse_id   VARCHAR(20)  NOT NULL,
    warehouse_name VARCHAR(100) NOT NULL,
    city           VARCHAR(100) NOT NULL,
    state          VARCHAR(100) NOT NULL,
    capacity_sqft  INT          NOT NULL,
    is_active      BIT          NOT NULL DEFAULT 1,
    created_at     DATETIME     NOT NULL DEFAULT GETDATE()
)""", "warehouses")

exec_sql("""
CREATE TABLE dbo.sellers (
    seller_id    VARCHAR(20)  NOT NULL,
    seller_name  VARCHAR(200) NOT NULL,
    gstin        VARCHAR(20),
    email        VARCHAR(200) NOT NULL,
    city         VARCHAR(100) NOT NULL,
    state        VARCHAR(100) NOT NULL,
    phone        VARCHAR(20),
    rating       DECIMAL(3,1),
    joined_date  DATE         NOT NULL,
    is_active    BIT          NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL DEFAULT GETDATE()
)""", "sellers")

exec_sql("""
CREATE TABLE dbo.customers (
    customer_id       VARCHAR(20)  NOT NULL,
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    email             VARCHAR(200) NOT NULL,
    phone_number      VARCHAR(20),
    date_of_birth     DATE,
    gender            VARCHAR(10),
    city              VARCHAR(100) NOT NULL,
    state             VARCHAR(100) NOT NULL,
    pincode           VARCHAR(10)  NOT NULL,
    loyalty_tier      VARCHAR(20)  NOT NULL DEFAULT 'Bronze',
    registration_date DATE         NOT NULL,
    is_active         BIT          NOT NULL DEFAULT 1,
    created_at        DATETIME     NOT NULL DEFAULT GETDATE()
)""", "customers")

exec_sql("""
CREATE TABLE dbo.products (
    product_id     VARCHAR(20)   NOT NULL,
    product_name   VARCHAR(500)  NOT NULL,
    category_id    VARCHAR(20)   NOT NULL,
    brand_id       VARCHAR(20),
    retail_price   VARCHAR(50)   NOT NULL,
    cost_price     DECIMAL(10,2) NOT NULL,
    weight_kg      DECIMAL(8,3),
    stock_quantity INT           NOT NULL DEFAULT 0,
    unit_size      VARCHAR(30)   NOT NULL DEFAULT '',
    is_active      BIT           NOT NULL DEFAULT 1,
    created_at     DATETIME      NOT NULL DEFAULT GETDATE()
)""", "products")

print("✅ All 6 tables created")

# COMMAND ----------

# ── LOAD: CATEGORIES (50 rows, clean) ────────────────────────

print("\nLoading categories...")

cats = [
    # Level 1 — root categories
    ("CAT001","Electronics","",1),
    ("CAT002","Fashion","",1),
    ("CAT003","Home & Kitchen","",1),
    ("CAT004","Sports & Outdoors","",1),
    ("CAT005","Books & Media","",1),
    ("CAT006","Beauty & Personal Care","",1),
    ("CAT007","Grocery & Gourmet","",1),
    ("CAT008","Toys & Games","",1),
    ("CAT009","Automotive","",1),
    ("CAT010","Health & Wellness","",1),
    # Level 2 — Electronics
    ("CAT011","Smartphones","Electronics",2),
    ("CAT012","Laptops","Electronics",2),
    ("CAT013","Tablets","Electronics",2),
    ("CAT014","TVs & Displays","Electronics",2),
    ("CAT015","Audio & Headphones","Electronics",2),
    ("CAT016","Cameras","Electronics",2),
    ("CAT017","Smart Home","Electronics",2),
    ("CAT018","Accessories","Electronics",2),
    # Level 2 — Fashion
    ("CAT019","Men's Clothing","Fashion",2),
    ("CAT020","Women's Clothing","Fashion",2),
    ("CAT021","Kids' Clothing","Fashion",2),
    ("CAT022","Footwear","Fashion",2),
    ("CAT023","Watches","Fashion",2),
    ("CAT024","Bags & Luggage","Fashion",2),
    ("CAT025","Jewellery","Fashion",2),
    # Level 2 — Home & Kitchen
    ("CAT026","Furniture","Home & Kitchen",2),
    ("CAT027","Large Appliances","Home & Kitchen",2),
    ("CAT028","Kitchen Tools","Home & Kitchen",2),
    ("CAT029","Bedding & Linen","Home & Kitchen",2),
    ("CAT030","Home Decor","Home & Kitchen",2),
    # Level 2 — Sports
    ("CAT031","Cricket","Sports & Outdoors",2),
    ("CAT032","Football","Sports & Outdoors",2),
    ("CAT033","Fitness Equipment","Sports & Outdoors",2),
    ("CAT034","Outdoor & Camping","Sports & Outdoors",2),
    ("CAT035","Yoga & Meditation","Sports & Outdoors",2),
    # Level 2 — Beauty
    ("CAT036","Skincare","Beauty & Personal Care",2),
    ("CAT037","Haircare","Beauty & Personal Care",2),
    ("CAT038","Makeup","Beauty & Personal Care",2),
    ("CAT039","Fragrances","Beauty & Personal Care",2),
    # Level 2 — Grocery
    ("CAT040","Staples & Grains","Grocery & Gourmet",2),
    ("CAT041","Snacks & Namkeen","Grocery & Gourmet",2),
    ("CAT042","Beverages","Grocery & Gourmet",2),
    ("CAT043","Dairy & Eggs","Grocery & Gourmet",2),
    # Level 2 — Health
    ("CAT044","Vitamins & Supplements","Health & Wellness",2),
    ("CAT045","Ayurveda","Health & Wellness",2),
    ("CAT046","Medical Devices","Health & Wellness",2),
    # Level 2 — Books
    ("CAT047","Fiction","Books & Media",2),
    ("CAT048","Technical Books","Books & Media",2),
    ("CAT049","Children's Books","Books & Media",2),
    ("CAT050","Self Help","Books & Media",2),
]

df_cats = pd.DataFrame(cats, columns=["category_id","category_name","parent_category","level"])
df_cats["is_active"] = 1
bulk_load(df_cats, "categories")
print(f"  ✅ categories: {len(df_cats)} rows (no dirty data — reference table)")

# COMMAND ----------

# ── LOAD: BRANDS (120 rows, clean) ───────────────────────────

print("\nLoading brands...")

brands_raw = [
    # Indian brands
    ("BRD001","Tata","India",1,1868),("BRD002","Reliance","India",1,1966),
    ("BRD003","Boat","India",0,2016),("BRD004","Noise","India",0,2014),
    ("BRD005","Titan","India",1,1984),("BRD006","Amul","India",0,1946),
    ("BRD007","Dabur","India",0,1884),("BRD008","Himalaya","India",0,1930),
    ("BRD009","Marico","India",0,1987),("BRD010","Patanjali","India",0,2006),
    ("BRD011","Bata","India",0,1894),("BRD012","Peter England","India",1,1889),
    ("BRD013","Van Heusen","India",1,1881),("BRD014","Allen Solly","India",1,1744),
    ("BRD015","Wrogn","India",0,2014),("BRD016","FabIndia","India",0,1960),
    ("BRD017","Forest Essentials","India",1,2000),("BRD018","Biotique","India",0,1992),
    ("BRD019","Kama Ayurveda","India",1,2002),("BRD020","Sugar Cosmetics","India",0,2015),
    # Global brands
    ("BRD021","Apple","USA",1,1976),("BRD022","Samsung","South Korea",1,1969),
    ("BRD023","Sony","Japan",1,1946),("BRD024","LG","South Korea",1,1958),
    ("BRD025","Nike","USA",1,1964),("BRD026","Adidas","Germany",1,1949),
    ("BRD027","Puma","Germany",1,1948),("BRD028","HP","USA",1,1939),
    ("BRD029","Dell","USA",1,1984),("BRD030","Lenovo","China",1,1984),
    ("BRD031","Asus","Taiwan",1,1989),("BRD032","Microsoft","USA",1,1975),
    ("BRD033","OnePlus","China",0,2013),("BRD034","Oppo","China",0,2004),
    ("BRD035","Vivo","China",0,2009),("BRD036","Xiaomi","China",0,2010),
    ("BRD037","Philips","Netherlands",1,1891),("BRD038","Bosch","Germany",1,1886),
    ("BRD039","Whirlpool","USA",1,1911),("BRD040","Panasonic","Japan",1,1918),
    ("BRD041","Canon","Japan",1,1937),("BRD042","Nikon","Japan",1,1917),
    ("BRD043","JBL","USA",1,1946),("BRD044","Bose","USA",1,1964),
    ("BRD045","Sennheiser","Germany",1,1945),("BRD046","Logitech","Switzerland",1,1981),
    ("BRD047","Intel","USA",1,1968),("BRD048","AMD","USA",1,1969),
    ("BRD049","Nvidia","USA",1,1993),("BRD050","Qualcomm","USA",1,1985),
]
# Fill to 120 with generated brands
# BRD051-120: All real Indian/global brands
brands_raw += [
    ("BRD051","Prestige","India",0,1955),("BRD052","Hawkins","India",0,1959),
    ("BRD053","Pigeon","India",0,1996),("BRD054","Preethi","India",0,1978),
    ("BRD055","Butterfly","India",0,1986),("BRD056","Bajaj","India",0,1926),
    ("BRD057","IFB","India",1,1974),("BRD058","Voltas","India",0,1954),
    ("BRD059","Godrej","India",1,1897),("BRD060","Nilkamal","India",0,1981),
    ("BRD061","Wakefit","India",0,2016),("BRD062","Sleepwell","India",0,1972),
    ("BRD063","Havells","India",0,1958),("BRD064","Crompton","India",0,1937),
    ("BRD065","SG","India",0,1931),("BRD066","MRF","India",0,1946),
    ("BRD067","Nivia","India",0,1934),("BRD068","Cosco","India",0,1980),
    ("BRD069","Decathlon","France",0,1976),("BRD070","Boldfit","India",0,2018),
    ("BRD071","Wildcraft","India",0,1998),("BRD072","Skybags","India",0,1988),
    ("BRD073","American Tourister","USA",0,1933),
    ("BRD074","Aashirvaad","India",0,2002),("BRD075","Fortune","India",0,2000),
    ("BRD076","Saffola","India",0,1965),("BRD077","MDH","India",0,1919),
    ("BRD078","Everest","India",0,1962),("BRD079","Catch","India",0,1987),
    ("BRD080","Haldirams","India",0,1937),("BRD081","Britannia","India",0,1892),
    ("BRD082","Parle","India",0,1929),("BRD083","ITC","India",1,1910),
    ("BRD084","Mother Dairy","India",0,1974),("BRD085","Nandini","India",0,1984),
    ("BRD086","Paper Boat","India",0,2013),("BRD087","Bru","India",0,1968),
    ("BRD088","Nescafe","Switzerland",0,1938),
    ("BRD089","Lakme","India",0,1952),("BRD090","Nykaa","India",0,2012),
    ("BRD091","Maybelline","USA",0,1915),("BRD092","Colorbar","India",1,2004),
    ("BRD093","Biba","India",0,1988),("BRD094","W","India",0,1998),
    ("BRD095","Aurelia","India",0,2016),("BRD096","Global Desi","India",0,2007),
    ("BRD097","Raymond","India",1,1925),("BRD098","Manyavar","India",0,2002),
    ("BRD099","Monte Carlo","India",0,1984),("BRD100","U.S. Polo","USA",0,1890),
    ("BRD101","Woodland","India",0,1992),("BRD102","Sparx","India",0,1974),
    ("BRD103","Campus","India",0,2005),("BRD104","Liberty","India",0,1954),
    ("BRD105","Casio","Japan",0,1946),("BRD106","Fossil","USA",0,1984),
    ("BRD107","Sonata","India",0,1992),
    ("BRD108","Park Avenue","India",0,1986),("BRD109","Denver","India",0,2009),
    ("BRD110","Fogg","India",0,2011),("BRD111","Nivea","Germany",0,1882),
    ("BRD112","Dove","UK",0,1957),
    ("BRD113","Omron","Japan",1,1933),("BRD114","Dr. Morepen","India",0,1988),
    ("BRD115","Accu-Chek","Germany",1,1974),
    ("BRD116","Penguin","UK",0,1935),("BRD117","HarperCollins","USA",0,1989),
    ("BRD118","Rupa Publications","India",0,1936),("BRD119","Scholastic","USA",0,1920),
    ("BRD120","Funskool","India",0,1987),
]

df_brands = pd.DataFrame(brands_raw[:120],
    columns=["brand_id","brand_name","country_origin","is_premium","established_year"])
df_brands["is_active"] = 1
bulk_load(df_brands, "brands")
print(f"  ✅ brands: {len(df_brands)} rows (no dirty data — reference table)")

# COMMAND ----------

# ── LOAD: WAREHOUSES (20 rows, clean) ────────────────────────

print("\nLoading warehouses...")

wh_cities = [
    ("Mumbai","Maharashtra"),("Delhi","Delhi"),("Bangalore","Karnataka"),
    ("Chennai","Tamil Nadu"),("Hyderabad","Telangana"),("Kolkata","West Bengal"),
    ("Pune","Maharashtra"),("Ahmedabad","Gujarat"),("Jaipur","Rajasthan"),
    ("Lucknow","Uttar Pradesh"),("Surat","Gujarat"),("Bhopal","Madhya Pradesh"),
    ("Patna","Bihar"),("Kochi","Kerala"),("Coimbatore","Tamil Nadu"),
    ("Nagpur","Maharashtra"),("Indore","Madhya Pradesh"),("Vadodara","Gujarat"),
    ("Chandigarh","Punjab"),("Guwahati","Assam"),
]
wh_rows = []
for i,(city,state) in enumerate(wh_cities, 1):
    wh_rows.append({
        "warehouse_id":   f"WH{i:03d}",
        "warehouse_name": f"BharatMart {city} Fulfilment Centre",
        "city":           city,
        "state":          state,
        "capacity_sqft":  random.choice([50000,75000,100000,125000,150000,200000]),
        "is_active":      1,
    })

df_wh = pd.DataFrame(wh_rows)
bulk_load(df_wh, "warehouses")
print(f"  ✅ warehouses: {len(df_wh)} rows (no dirty data — reference table)")

# COMMAND ----------

# ── LOAD: SELLERS (5,000 rows + dirty) ───────────────────────

print("\nGenerating 5,000 sellers...")

rows = []
for i in range(1, 5001):
    state = random.choices(STATE_NAMES, weights=STATE_WEIGHTS)[0]
    rows.append({
        "seller_id":   f"SEL{i:05d}",
        "seller_name": gen_seller_name()[:100],
        "gstin":       gen_gstin(state),
        "email":       fake.company_email()[:100],
        "city":        random.choice(ALL_CITIES),
        "state":       state,
        "phone":       gen_phone(),
        "rating":      round(random.uniform(3.0, 5.0), 1),
        "joined_date": fake.date_between(date(2019,1,1), date(2024,12,31)),
        "is_active":   random.choices([1,0], weights=[95,5])[0],
    })

df_sel = pd.DataFrame(rows)

print("  Injecting dirty data...")
df_sel = D.nullify(df_sel,      "sellers", "phone",       0.05)   # T1  250 NULL phone
df_sel = D.nullify(df_sel,      "sellers", "gstin",       0.05)   # T1  250 NULL gstin
df_sel = D.case_chaos(df_sel,   "sellers", "seller_name")         # T5  mixed case names
df_sel = D.state_abbrev_mix(df_sel,"sellers","state")              # T5  abbrev/full mix
df_sel = D.gstin_lowercase(df_sel,"sellers","gstin")               # T5  10% lowercase GSTIN
df_sel = D.duplicate_rows(df_sel,"sellers", 0.01)                  # T2  ~50 duplicate rows

bulk_load(df_sel, "sellers")
print(f"  ✅ sellers: {len(df_sel):,} rows loaded")

# COMMAND ----------

# ── LOAD: CUSTOMERS (100,000 rows + dirty) ───────────────────

print("\nGenerating 100,000 customers (batches of 10k)...")

all_custs = []
for b in range(10):
    print(f"  Batch {b+1}/10...")
    for i in range(b*10000, (b+1)*10000):
        state = random.choices(STATE_NAMES, weights=STATE_WEIGHTS)[0]
        _cust_city = random.choice(ALL_CITIES)
        all_custs.append({
            "customer_id":       f"CUST{i+1:07d}",
            "first_name":        fake.first_name(),
            "last_name":         fake.last_name(),
            "email":             fake.email(),
            "phone_number":      gen_phone(),
            "date_of_birth":     fake.date_of_birth(minimum_age=18, maximum_age=70),
            "gender":            random.choice(["Male","Female","Other"]),
            "city":              _cust_city,
            "state":             state,
            "pincode":           gen_pincode_for_city(_cust_city),
            "loyalty_tier":      random.choices(LOYALTY_TIERS, weights=LOYALTY_WEIGHTS)[0],
            "registration_date": fake.date_between(date(2020,1,1), date(2025,12,31)),
            "is_active":         random.choices([1,0], weights=[92,8])[0],
        })

df_cust = pd.DataFrame(all_custs)

print("  Injecting dirty data...")
df_cust = D.nullify(df_cust,         "customers", "phone_number",  0.15)  # T1 15,000 NULL phone
df_cust = D.nullify(df_cust,         "customers", "date_of_birth", 0.30)  # T1 30,000 NULL DOB
df_cust = D.impossible_dob(df_cust,  "customers", "date_of_birth")        # T4 ~1,500 bad DOB
df_cust = D.case_chaos(df_cust,      "customers", "first_name")           # T5 mixed case
df_cust = D.case_chaos(df_cust,      "customers", "last_name")            # T5 mixed case
df_cust = D.state_abbrev_mix(df_cust,"customers", "state")                # T5 abbrev mix
df_cust = D.phone_format_mix(df_cust,"customers", "phone_number")         # T5 +91/0/plain
df_cust = D.duplicate_rows(df_cust,  "customers", 0.02)                   # T2 ~2,000 dupes

print(f"  Writing {len(df_cust):,} rows via JDBC...")
bulk_load(df_cust, "customers")
print(f"  ✅ customers: {len(df_cust):,} rows loaded")

# COMMAND ----------

# ── LOAD: PRODUCTS (50,000 rows + dirty) ─────────────────────

print("\nGenerating 50,000 products...")

PRODUCT_CATALOG = {
    "CAT011": {
        "products": [
            ("BRD022","Samsung Galaxy S{n} Ultra"),("BRD022","Samsung Galaxy A{n}"),
            ("BRD022","Samsung Galaxy M{n} Prime"),("BRD022","Samsung Galaxy F{n}"),
            ("BRD036","Xiaomi Redmi Note {n} Pro"),("BRD036","Xiaomi Redmi {n}C"),
            ("BRD036","Xiaomi {n}T Pro"),("BRD036","Xiaomi Poco X{n}"),
            ("BRD021","Apple iPhone {n} Pro Max"),("BRD021","Apple iPhone {n}"),
            ("BRD034","Oppo Reno {n} Pro"),("BRD034","Oppo A{n}s"),
            ("BRD035","Vivo V{n} Pro"),("BRD035","Vivo Y{n}"),
            ("BRD033","OnePlus {n}T"),("BRD033","OnePlus Nord CE {n}"),
        ],
        "n_range": (10, 17), "unit_sizes": ["128GB","256GB","512GB"],
    },
    "CAT012": {
        "products": [
            ("BRD028","HP Pavilion {n}"),("BRD028","HP Victus {n}"),("BRD028","HP Envy x360 {n}"),
            ("BRD029","Dell Inspiron {n}"),("BRD029","Dell XPS {n}"),("BRD029","Dell Vostro {n}"),
            ("BRD030","Lenovo IdeaPad Slim {n}"),("BRD030","Lenovo ThinkPad X{n}"),
            ("BRD030","Lenovo Legion {n}i"),
            ("BRD031","Asus VivoBook {n}"),("BRD031","Asus ROG Strix G{n}"),
            ("BRD021","Apple MacBook Air M{n}"),("BRD021","Apple MacBook Pro M{n}"),
        ],
        "n_range": (1, 5), "unit_sizes": ["8GB/256GB","8GB/512GB","16GB/512GB","16GB/1TB"],
    },
    "CAT013": {
        "products": [
            ("BRD021","Apple iPad {n}th Gen"),("BRD021","Apple iPad Air M{n}"),
            ("BRD022","Samsung Galaxy Tab S{n}"),("BRD022","Samsung Galaxy Tab A{n}"),
            ("BRD030","Lenovo Tab M{n}"),("BRD036","Xiaomi Redmi Pad SE"),
        ],
        "n_range": (5, 11), "unit_sizes": ["64GB","128GB","256GB"],
    },
    "CAT014": {
        "products": [
            ("BRD022","Samsung Crystal 4K Smart TV"),("BRD022","Samsung Neo QLED"),
            ("BRD024","LG OLED Evo"),("BRD024","LG NanoCell Smart TV"),
            ("BRD023","Sony Bravia XR"),("BRD023","Sony Bravia 4K"),
            ("BRD036","Xiaomi Mi TV"),("BRD036","Xiaomi Smart TV X"),
        ],
        "n_range": (1, 3), "unit_sizes": ["32 inch","43 inch","50 inch","55 inch","65 inch","75 inch"],
    },
    "CAT015": {
        "products": [
            ("BRD043","JBL Tune {n}BT"),("BRD043","JBL Flip {n}"),("BRD043","JBL Charge {n}"),
            ("BRD044","Bose QuietComfort {n}"),("BRD044","Bose SoundLink Flex"),
            ("BRD045","Sennheiser Momentum {n}"),
            ("BRD023","Sony WH-1000XM{n}"),("BRD023","Sony WF-1000XM{n}"),
            ("BRD003","boAt Rockerz {n}"),("BRD003","boAt Airdopes {n}"),("BRD003","boAt Stone {n}"),
            ("BRD004","Noise Buds VS{n}"),("BRD004","Noise ColorFit Pro {n}"),
        ],
        "n_range": (1, 7), "unit_sizes": [""],
    },
    "CAT016": {
        "products": [
            ("BRD041","Canon EOS R{n}"),("BRD041","Canon EOS {n}D"),
            ("BRD042","Nikon Z{n}"),("BRD042","Nikon D{n}00"),
            ("BRD023","Sony Alpha A{n}"),("BRD023","Sony ZV-{n}"),
        ],
        "n_range": (5, 10), "unit_sizes": ["Body Only","with 18-55mm Kit","with 24-70mm Kit"],
    },
    "CAT017": {
        "products": [
            ("BRD037","Philips Hue Smart Bulb"),("BRD037","Philips Smart LED Strip"),
            ("BRD036","Xiaomi Mi Smart Speaker"),("BRD036","Xiaomi Mi 360 Camera"),
            ("BRD063","Havells Smart Switch"),("BRD063","Havells Smart Plug"),
        ],
        "n_range": (1, 4), "unit_sizes": [""],
    },
    "CAT018": {
        "products": [
            ("BRD003","boAt USB-C Cable"),("BRD003","boAt Deuce Cable"),
            ("BRD046","Logitech MX Mouse"),("BRD046","Logitech MX Keys"),
            ("BRD036","Xiaomi Wireless Charger"),("BRD022","Samsung Fast Charger"),
            ("BRD021","Apple USB-C Adapter"),("BRD021","Apple AirTag"),
        ],
        "n_range": (1, 4), "unit_sizes": ["","1m","1.5m","2m"],
    },
    "CAT019": {
        "products": [
            ("BRD014","Allen Solly Formal Shirt"),("BRD014","Allen Solly Chinos"),
            ("BRD012","Peter England Shirt"),("BRD012","Peter England Trouser"),
            ("BRD013","Van Heusen Blazer"),("BRD013","Van Heusen Polo Tee"),
            ("BRD015","Wrogn Denim Jacket"),("BRD015","Wrogn Cargo Pants"),
            ("BRD097","Raymond Suit"),("BRD097","Raymond Formal Shirt"),
            ("BRD099","Monte Carlo Sweater"),("BRD100","U.S. Polo T-Shirt"),
            ("BRD016","FabIndia Cotton Kurta"),("BRD098","Manyavar Silk Kurta Set"),
        ],
        "n_range": (1, 5), "unit_sizes": ["S","M","L","XL","XXL"],
    },
    "CAT020": {
        "products": [
            ("BRD093","Biba Anarkali Kurta Set"),("BRD093","Biba Cotton Palazzo Set"),
            ("BRD093","Biba Straight Kurta"),("BRD093","Biba Printed Dupatta"),
            ("BRD094","W Printed Kurta"),("BRD094","W A-Line Dress"),
            ("BRD095","Aurelia Ethnic Gown"),("BRD096","Global Desi Maxi Dress"),
            ("BRD016","FabIndia Silk Saree"),("BRD016","FabIndia Chiffon Dupatta"),
        ],
        "n_range": (1, 5), "unit_sizes": ["XS","S","M","L","XL","Free Size"],
    },
    "CAT021": {
        "products": [
            ("BRD014","Allen Solly Junior Tee"),("BRD014","Allen Solly Kids Shorts"),
            ("BRD093","Biba Girls Lehenga"),("BRD093","Biba Girls Frock"),
            ("BRD016","FabIndia Kids Kurta Set"),("BRD100","U.S. Polo Kids Polo"),
        ],
        "n_range": (1, 4), "unit_sizes": ["2-3Y","4-5Y","6-7Y","8-9Y","10-12Y"],
    },
    "CAT022": {
        "products": [
            ("BRD025","Nike Air Max"),("BRD025","Nike Revolution"),("BRD025","Nike Court Vision"),
            ("BRD026","Adidas Ultraboost"),("BRD026","Adidas Samba OG"),
            ("BRD027","Puma RS-X"),("BRD027","Puma Softride"),
            ("BRD011","Bata Comfit"),("BRD011","Bata Power Sport"),
            ("BRD101","Woodland Casual Shoe"),("BRD101","Woodland Hiking Boot"),
            ("BRD103","Campus Running Shoe"),("BRD104","Liberty Formal Oxford"),
        ],
        "n_range": (1, 5), "unit_sizes": ["UK 6","UK 7","UK 8","UK 9","UK 10","UK 11"],
    },
    "CAT023": {
        "products": [
            ("BRD005","Titan Raga"),("BRD005","Titan Octane"),("BRD005","Titan Edge"),
            ("BRD107","Sonata Stardust"),("BRD107","Sonata RPM"),
            ("BRD105","Casio G-Shock"),("BRD105","Casio Edifice"),
            ("BRD106","Fossil Gen Smartwatch"),("BRD106","Fossil Grant Chronograph"),
        ],
        "n_range": (1, 6), "unit_sizes": [""],
    },
    "CAT024": {
        "products": [
            ("BRD071","Wildcraft Backpack"),("BRD071","Wildcraft Duffle Bag"),
            ("BRD072","Skybags Laptop Bag"),("BRD072","Skybags School Bag"),
            ("BRD073","American Tourister Trolley"),("BRD073","American Tourister Backpack"),
        ],
        "n_range": (1, 4), "unit_sizes": ["25L","35L","45L","55L","65L"],
    },
    "CAT025": {
        "products": [
            ("BRD005","Tanishq Gold Chain"),("BRD005","Tanishq Diamond Stud"),
            ("BRD005","Tanishq Gold Bangle"),("BRD005","Tanishq Silver Anklet"),
            ("BRD016","FabIndia Kundan Set"),("BRD016","FabIndia Oxidised Jhumka"),
        ],
        "n_range": (1, 5), "unit_sizes": [""],
    },
    "CAT026": {
        "products": [
            ("BRD059","Godrej Interio Sofa"),("BRD059","Godrej Interio Wardrobe"),
            ("BRD059","Godrej Interio Dining Table"),("BRD059","Godrej Interio Office Chair"),
            ("BRD060","Nilkamal Freedom Cabinet"),("BRD060","Nilkamal Plastic Chair"),
            ("BRD061","Wakefit Orthopaedic Mattress"),("BRD061","Wakefit Bed Frame"),
        ],
        "n_range": (1, 4), "unit_sizes": ["Single","Double","Queen","King"],
    },
    "CAT027": {
        "products": [
            ("BRD039","Whirlpool Washing Machine"),("BRD039","Whirlpool Refrigerator"),
            ("BRD022","Samsung Side-by-Side Fridge"),("BRD022","Samsung Front Load Washer"),
            ("BRD024","LG Inverter Split AC"),("BRD024","LG Smart Refrigerator"),
            ("BRD057","IFB Front Load Washer"),("BRD058","Voltas Split AC"),
        ],
        "n_range": (1, 4), "unit_sizes": ["6.5kg","7kg","8kg","190L","260L","325L","1 Ton","1.5 Ton"],
    },
    "CAT028": {
        "products": [
            ("BRD051","Prestige Svachh Cooker"),("BRD051","Prestige Omega Tawa"),
            ("BRD051","Prestige Iris Mixer Grinder"),("BRD051","Prestige Air Fryer"),
            ("BRD052","Hawkins Contura Cooker"),("BRD052","Hawkins Futura Kadhai"),
            ("BRD053","Pigeon Stovekraft Gas Hob"),("BRD053","Pigeon Handy Chopper"),
            ("BRD054","Preethi Blue Leaf Mixer"),("BRD055","Butterfly Rapid Mixer"),
        ],
        "n_range": (1, 4), "unit_sizes": ["2L","3L","5L","750W","500W","4.2L"],
    },
    "CAT029": {
        "products": [
            ("BRD062","Sleepwell Cotton Bedsheet"),("BRD062","Sleepwell Comforter Set"),
            ("BRD062","Sleepwell Ortho Mattress"),
            ("BRD016","FabIndia Block Print Bedcover"),("BRD016","FabIndia Cotton Towel Set"),
            ("BRD061","Wakefit Pillow Set"),
        ],
        "n_range": (1, 4), "unit_sizes": ["Single","Double","Queen","King"],
    },
    "CAT030": {
        "products": [
            ("BRD016","FabIndia Ceramic Vase"),("BRD016","FabIndia Brass Diya Set"),
            ("BRD016","FabIndia Jute Rug"),
            ("BRD063","Havells Decorative Fan"),("BRD063","Havells LED Table Lamp"),
            ("BRD064","Crompton Wall Clock"),("BRD037","Philips LED Fairy Lights"),
        ],
        "n_range": (1, 4), "unit_sizes": [""],
    },
    "CAT031": {
        "products": [
            ("BRD065","SG Cricket Bat"),("BRD065","SG Test Leather Ball"),
            ("BRD065","SG Batting Gloves"),("BRD065","SG Stumps Set"),
            ("BRD066","MRF Genius Grand Bat"),("BRD066","MRF Prodigy Bat"),
            ("BRD068","Cosco Cricket Ball"),
        ],
        "n_range": (1, 5), "unit_sizes": [""],
    },
    "CAT032": {
        "products": [
            ("BRD025","Nike Flight Football"),("BRD025","Nike Mercurial Boots"),
            ("BRD026","Adidas UCL Pro Ball"),("BRD026","Adidas Predator Boots"),
            ("BRD067","Nivia Shining Star Football"),("BRD067","Nivia Encounter Shin Guard"),
        ],
        "n_range": (3, 6), "unit_sizes": ["Size 4","Size 5"],
    },
    "CAT033": {
        "products": [
            ("BRD069","Decathlon Dumbbell Set"),("BRD069","Decathlon Resistance Band"),
            ("BRD069","Decathlon Kettlebell"),("BRD069","Decathlon Treadmill"),
            ("BRD070","Boldfit Pull-Up Bar"),("BRD070","Boldfit Ab Roller"),
        ],
        "n_range": (1, 4), "unit_sizes": ["5kg","10kg","15kg","20kg","25kg"],
    },
    "CAT034": {
        "products": [
            ("BRD071","Wildcraft Camping Tent"),("BRD071","Wildcraft Sleeping Bag"),
            ("BRD071","Wildcraft Rain Jacket"),
            ("BRD069","Decathlon Headlamp"),("BRD069","Decathlon Hiking Boot"),
        ],
        "n_range": (1, 4), "unit_sizes": ["2-Person","3-Person","4-Person"],
    },
    "CAT035": {
        "products": [
            ("BRD069","Decathlon Yoga Mat"),("BRD069","Decathlon Foam Roller"),
            ("BRD069","Decathlon Yoga Block"),("BRD070","Boldfit Yoga Strap"),
        ],
        "n_range": (1, 4), "unit_sizes": ["4mm","6mm","8mm"],
    },
    "CAT036": {
        "products": [
            ("BRD008","Himalaya Neem Face Wash"),("BRD008","Himalaya Purifying Scrub"),
            ("BRD008","Himalaya Moisturizing Cream"),
            ("BRD018","Biotique Vitamin C Serum"),("BRD018","Biotique Sunscreen"),
            ("BRD017","Forest Essentials Night Cream"),
            ("BRD019","Kama Ayurveda Kumkumadi Oil"),
            ("BRD111","Nivea Soft Moisturizer"),("BRD112","Dove Beauty Cream"),
        ],
        "n_range": (1, 4), "unit_sizes": ["50ml","100ml","150ml","200ml","50g","100g"],
    },
    "CAT037": {
        "products": [
            ("BRD007","Dabur Amla Hair Oil"),("BRD007","Dabur Vatika Shampoo"),
            ("BRD009","Parachute Coconut Oil"),("BRD009","Parachute Hair Serum"),
            ("BRD008","Himalaya Anti-Dandruff Shampoo"),("BRD008","Himalaya Protein Conditioner"),
            ("BRD112","Dove Hair Fall Rescue Shampoo"),
        ],
        "n_range": (1, 4), "unit_sizes": ["100ml","200ml","340ml","400ml","650ml"],
    },
    "CAT038": {
        "products": [
            ("BRD020","Sugar Matte Lipstick"),("BRD020","Sugar Ace of Face Foundation"),
            ("BRD020","Sugar Kohl of Honour Kajal"),
            ("BRD089","Lakme 9to5 Primer"),("BRD089","Lakme Absolute Gel Eyeliner"),
            ("BRD089","Lakme Enrich Lip Crayon"),
            ("BRD091","Maybelline Fit Me Foundation"),("BRD091","Maybelline Colossal Mascara"),
            ("BRD092","Colorbar Velvet Matte Lipstick"),
        ],
        "n_range": (1, 8), "unit_sizes": [""],
    },
    "CAT039": {
        "products": [
            ("BRD017","Forest Essentials Eau de Parfum"),("BRD017","Forest Essentials Body Mist"),
            ("BRD108","Park Avenue Good Morning"),("BRD109","Denver Hamilton Deo"),
            ("BRD110","Fogg Scent"),("BRD111","Nivea Men Deodorant"),
        ],
        "n_range": (1, 4), "unit_sizes": ["50ml","100ml","150ml","200ml"],
    },
    "CAT040": {
        "products": [
            ("BRD074","Aashirvaad Whole Wheat Atta"),("BRD074","Aashirvaad Multigrain Atta"),
            ("BRD001","Tata Sampann Toor Dal"),("BRD001","Tata Salt"),
            ("BRD001","Tata Sampann Chana Dal"),
            ("BRD075","Fortune Basmati Rice"),("BRD075","Fortune Sunflower Oil"),
            ("BRD076","Saffola Gold Oil"),("BRD077","MDH Chaat Masala"),("BRD078","Everest Garam Masala"),
        ],
        "n_range": (1, 3), "unit_sizes": ["500g","1kg","2kg","5kg","1L","5L"],
    },
    "CAT041": {
        "products": [
            ("BRD080","Haldirams Aloo Bhujia"),("BRD080","Haldirams Namkeen Mix"),
            ("BRD080","Haldirams Soan Papdi"),
            ("BRD081","Britannia Good Day Cookies"),("BRD081","Britannia Marie Gold"),
            ("BRD081","Britannia Jim Jam"),
            ("BRD082","Parle-G Biscuits"),("BRD082","Parle Monaco"),
            ("BRD083","ITC Bingo Mad Angles"),("BRD083","ITC Yippee Noodles"),
        ],
        "n_range": (1, 3), "unit_sizes": ["100g","150g","200g","400g","1kg"],
    },
    "CAT042": {
        "products": [
            ("BRD001","Tata Tea Gold"),("BRD001","Tata Tea Premium"),
            ("BRD001","Tata Starbucks Coffee"),
            ("BRD006","Amul Kool Milkshake"),("BRD006","Amul Lassi"),
            ("BRD087","Bru Instant Coffee"),("BRD088","Nescafe Classic"),
            ("BRD086","Paper Boat Aam Panna"),("BRD086","Paper Boat Jaljeera"),
        ],
        "n_range": (1, 3), "unit_sizes": ["100g","200g","500g","200ml","500ml","1L"],
    },
    "CAT043": {
        "products": [
            ("BRD006","Amul Gold Full Cream Milk"),("BRD006","Amul Butter"),
            ("BRD006","Amul Fresh Paneer"),("BRD006","Amul Taaza Curd"),
            ("BRD006","Amul Pure Ghee"),("BRD006","Amul Cheese Slices"),
            ("BRD084","Mother Dairy Classic Curd"),("BRD084","Mother Dairy Farm Eggs"),
        ],
        "n_range": (1, 3), "unit_sizes": ["200g","500g","1kg","200ml","500ml","1L"],
    },
    "CAT044": {
        "products": [
            ("BRD008","Himalaya Ashvagandha Wellness"),("BRD008","Himalaya Liv.52"),
            ("BRD007","Dabur Chyawanprakash"),("BRD007","Dabur Honey"),
            ("BRD010","Patanjali Multivitamin"),
        ],
        "n_range": (1, 3), "unit_sizes": ["60 Tab","120 Tab","250g","500g","1kg"],
    },
    "CAT045": {
        "products": [
            ("BRD010","Patanjali Giloy Juice"),("BRD010","Patanjali Aloe Vera Juice"),
            ("BRD010","Patanjali Tulsi Drops"),
            ("BRD007","Dabur Ashwagandha Churna"),("BRD007","Dabur Triphala Churna"),
        ],
        "n_range": (1, 3), "unit_sizes": ["100ml","200ml","500ml","60 Tab","100g"],
    },
    "CAT046": {
        "products": [
            ("BRD113","Omron Digital Thermometer"),("BRD113","Omron BP Monitor"),
            ("BRD113","Omron Nebulizer"),
            ("BRD114","Dr. Morepen Glucometer"),("BRD114","Dr. Morepen Pulse Oximeter"),
            ("BRD115","Accu-Chek Active Kit"),("BRD115","Accu-Chek Test Strips"),
        ],
        "n_range": (1, 4), "unit_sizes": [""],
    },
    "CAT047": {
        "products": [
            ("BRD116","Penguin Classics Collection"),("BRD116","Penguin India Mystery"),
            ("BRD117","HarperCollins Bestseller"),("BRD117","HarperCollins Historical Fiction"),
            ("BRD118","Rupa Modern Thriller"),
        ],
        "n_range": (1, 10), "unit_sizes": ["Paperback","Hardcover"],
    },
    "CAT048": {
        "products": [
            ("BRD116","Penguin Tech: Python Programming"),
            ("BRD117","HarperCollins Data Science Handbook"),
            ("BRD116","Penguin Tech: System Design"),
            ("BRD118","Rupa Coding Interview Prep"),
        ],
        "n_range": (1, 6), "unit_sizes": ["Paperback","Hardcover","eBook"],
    },
    "CAT049": {
        "products": [
            ("BRD116","Penguin Panchatantra Tales"),("BRD119","Scholastic Magic Tree House"),
            ("BRD117","HarperCollins Colouring Book"),("BRD117","HarperCollins Activity Book"),
        ],
        "n_range": (1, 10), "unit_sizes": ["Paperback","Hardcover"],
    },
    "CAT050": {
        "products": [
            ("BRD116","Penguin: Atomic Habits"),("BRD116","Penguin: Ikigai"),
            ("BRD117","HarperCollins: Deep Work"),("BRD117","HarperCollins: Power of Now"),
            ("BRD118","Rupa: You Can Win"),
        ],
        "n_range": (1, 5), "unit_sizes": ["Paperback","Hardcover"],
    },
}

CAT_IDS = [f"CAT{i:03d}" for i in range(11, 51)]
PRICE_RANGES = [(99,999,40),(1000,4999,35),(5000,19999,18),(20000,99999,7)]

prod_rows = []
for i in range(1, 50001):
    cat_id = random.choice(CAT_IDS)
    cat_data = PRODUCT_CATALOG[cat_id]
    brand_id, name_tmpl = random.choice(cat_data["products"])
    lo_n, hi_n = cat_data["n_range"]
    n = random.randint(lo_n, hi_n)
    pname = name_tmpl.replace("{n}", str(n))
    sizes = cat_data["unit_sizes"]
    unit_size = random.choice(sizes) if sizes and sizes != [""] else ""
    weights = [p[2] for p in PRICE_RANGES]
    lo, hi, _ = random.choices(PRICE_RANGES, weights=weights)[0]
    price = round(random.uniform(lo, hi), 2)
    cost = round(price * random.uniform(0.45, 0.70), 2)
    prod_rows.append({
        "product_id":     f"PROD{i:07d}",
        "product_name":   pname[:200],
        "category_id":    cat_id,
        "brand_id":       brand_id,
        "retail_price":   str(price),
        "cost_price":     cost,
        "weight_kg":      round(random.uniform(0.05, 30.0), 3),
        "stock_quantity": random.randint(0, 5000),
        "unit_size":      unit_size,
        "is_active":      random.choices([1,0], weights=[94,6])[0],
    })


df_prod = pd.DataFrame(prod_rows)

print("  Injecting dirty data...")
df_prod = D.nullify(df_prod,       "products", "brand_id",    0.12)  # T1 12% NULL brand
df_prod = D.nullify(df_prod,       "products", "weight_kg",   0.25)  # T1 25% NULL weight
df_prod = D.price_to_string(df_prod,"products","retail_price",0.30)  # T3 30% "₹X,XXX.XX"
df_prod = D.zero_price(df_prod,    "products", "retail_price",0.003) # T4 0.3% price="0.0"
df_prod = D.price_outlier(df_prod, "products", "retail_price",0.002) # T6 0.2% ×1000
df_prod = D.duplicate_rows(df_prod,"products", 0.01)                  # T2 ~500 dupes

print(f"  Writing {len(df_prod):,} rows via JDBC...")
bulk_load(df_prod, "products")
print(f"  ✅ products: {len(df_prod):,} rows loaded")

# COMMAND ----------

# ── ENABLE CDC (pymssql — requires db_owner role) ─────────────

print("\nEnabling CDC on database...")
exec_sql("EXEC sys.sp_cdc_enable_db", "CDC enabled on database")

CDC_TABLES = [
    "customers","products","sellers","categories","brands","warehouses"
]
for tbl in CDC_TABLES:
    exec_sql(f"""
    IF NOT EXISTS (
        SELECT 1 FROM cdc.change_tables ct
        JOIN sys.tables t ON t.object_id = ct.source_object_id
        WHERE t.name = '{tbl}'
    )
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'{tbl}',
        @role_name            = NULL,
        @supports_net_changes = 1
    """, f"CDC on {tbl}")

print("✅ CDC enabled on all 6 tables")

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS bharatmart.bronze.landing")

# COMMAND ----------

# ── VERIFY ROW COUNTS ────────────────────────────────────────

print("\nVerifying row counts in Azure SQL...")

verify_sql = """(
    SELECT 'customers'  AS tbl, COUNT(*) AS cnt FROM dbo.customers  UNION ALL
    SELECT 'products',                            COUNT(*) FROM dbo.products   UNION ALL
    SELECT 'sellers',                             COUNT(*) FROM dbo.sellers    UNION ALL
    SELECT 'categories',                          COUNT(*) FROM dbo.categories UNION ALL
    SELECT 'brands',                              COUNT(*) FROM dbo.brands     UNION ALL
    SELECT 'warehouses',                          COUNT(*) FROM dbo.warehouses
) counts"""

df_counts = spark.read.jdbc(
    url=JDBC_URL,
    table=verify_sql,
    properties=JDBC_PROPS
).toPandas()

print("\n" + "="*40)
print("  AZURE SQL — ROW COUNTS")
print("="*40)
for _, row in df_counts.iterrows():
    print(f"  {row['tbl']:<20} {int(row['cnt']):>10,}")
print("="*40)

# ── Print & save dirty data report ───────────────────────────
report = D.print_report()

report_path = "/Volumes/bharatmart/bronze/landing/injection_report_03a.json"
dbutils.fs.mkdirs("/Volumes/bharatmart/bronze/landing/")
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"\n✅ Injection report saved → {report_path}")
print("✅ 03a_load_azure_sql.py COMPLETE")
print("\nNext: Run 03b_event_producer.py  (Event Hub streaming)")
print("      Run 03c_load_adls_bulk.py   (ADLS bulk file drops)")
