# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — BharatMart 360° | Environment Setup
# MAGIC **BharatMart 360° Intelligence Platform — Synthetic Data Generation Pipeline**
# MAGIC
# MAGIC Run this notebook ONCE before anything else. It:
# MAGIC - Creates the bharatmart Unity Catalog + bronze/silver/gold/ml schemas
# MAGIC - Verifies ADLS connection via Access Connector (managed identity)
# MAGIC - Creates ADLS /landing/ folder structure for AutoLoader sources
# MAGIC - Creates ADLS /bharatmart-files/raw/ and /models/ directories
# MAGIC - Installs required Python libraries
# MAGIC - Saves all credentials to config.json in ADLS
# MAGIC - Validates the entire setup end-to-end
# MAGIC
# MAGIC **Platform**: Azure Databricks (Serverless compute)
# MAGIC **Auth**: Unity Catalog External Location + Access Connector (managed identity)
# MAGIC **Run order**: This is always FIRST. Nothing else runs before this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Widgets (Parameters)
# MAGIC Fill in ALL values before running. Everything saves to config.json at the end.

# COMMAND ----------

# ── DATABRICKS / STORAGE ──────────────────────────────────────────────────────
# Access Connector handles ADLS auth — no SAS token needed
dbutils.widgets.text("catalog_name",    "bharatmart",        "Catalog Name")
dbutils.widgets.text("adls_account",    "stbharatmartdev",   "ADLS Storage Account Name")
dbutils.widgets.text("adls_container",  "bharatmart",        "ADLS Container Name")

# ── KAGGLE ────────────────────────────────────────────────────────────────────
dbutils.widgets.text("kaggle_username", "",                  "Kaggle Username")
dbutils.widgets.text("kaggle_key",      "",                  "Kaggle API Key")

# ── AZURE SQL DATABASE (master data + CDC source) ─────────────────────────────
dbutils.widgets.text("sql_server",      "",                  "Azure SQL Server (e.g. myserver.database.windows.net)")
dbutils.widgets.text("sql_database",    "bharatmart_db",     "Azure SQL Database Name")
dbutils.widgets.text("sql_username",    "",                  "Azure SQL Username")
dbutils.widgets.text("sql_password",    "",                  "Azure SQL Password")

# ── AZURE EVENT HUB (streaming source) ───────────────────────────────────────
dbutils.widgets.text("eventhub_namespace",   "",             "Event Hub Namespace (e.g. bharatmart-eh)")
dbutils.widgets.text("eventhub_conn_str",    "",             "Event Hub Connection String (RootManageSharedAccessKey)")

# COMMAND ----------

# Read all widget values
CATALOG        = dbutils.widgets.get("catalog_name")
ADLS_ACCOUNT   = dbutils.widgets.get("adls_account")
ADLS_CONTAINER = dbutils.widgets.get("adls_container")
KAGGLE_USER    = dbutils.widgets.get("kaggle_username")
KAGGLE_KEY     = dbutils.widgets.get("kaggle_key")
SQL_SERVER     = dbutils.widgets.get("sql_server")
SQL_DATABASE   = dbutils.widgets.get("sql_database")
SQL_USER       = dbutils.widgets.get("sql_username")
SQL_PASS       = dbutils.widgets.get("sql_password")
EH_NAMESPACE   = dbutils.widgets.get("eventhub_namespace")
EH_CONN_STR    = dbutils.widgets.get("eventhub_conn_str")

# Derived paths — ALL storage is in ADLS Gen2
# IMPORTANT: On Azure Databricks Serverless, there is no local DBFS.
# All file storage uses ADLS via dbutils.fs (not os.makedirs / open / os.path)
ADLS_BASE    = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
FILES_BASE   = f"{ADLS_BASE}/bharatmart-files"   # raw kaggle data + models + config
LANDING_BASE = f"{ADLS_BASE}/landing"             # AutoLoader source folders

print("=" * 65)
print("BharatMart 360° — Environment Setup")
print("=" * 65)
print(f"  Catalog:        {CATALOG}")
print(f"  ADLS Account:   {ADLS_ACCOUNT}")
print(f"  ADLS Container: {ADLS_CONTAINER}")
print(f"  ADLS Base:      {ADLS_BASE}")
print(f"  Files Base:     {FILES_BASE}")
print(f"  Landing Base:   {LANDING_BASE}")
print(f"  Kaggle User:    {KAGGLE_USER or 'NOT SET ❌'}")
print(f"  Kaggle Key:     {'SET ✅' if KAGGLE_KEY    else 'NOT SET ❌'}")
print(f"  SQL Server:     {SQL_SERVER or 'NOT SET ❌'}")
print(f"  SQL Database:   {SQL_DATABASE}")
print(f"  SQL User:       {SQL_USER or 'NOT SET ❌'}")
print(f"  SQL Password:   {'SET ✅' if SQL_PASS      else 'NOT SET ❌'}")
print(f"  Event Hub NS:   {EH_NAMESPACE or 'NOT SET ❌'}")
print(f"  Event Hub Conn: {'SET ✅' if EH_CONN_STR   else 'NOT SET ❌'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Install Python Libraries

# COMMAND ----------

# MAGIC %pip install kaggle sdv ctgan pandas numpy scikit-learn pyarrow faker \
# MAGIC              pyodbc sqlalchemy azure-eventhub azure-storage-blob

# COMMAND ----------

# Restart Python interpreter after pip install
# This is required — new packages are not available until Python restarts
dbutils.library.restartPython()

# COMMAND ----------

# Re-read ALL widgets after restart
# Python state (all variables) is wiped when restartPython() runs
CATALOG        = dbutils.widgets.get("catalog_name")
ADLS_ACCOUNT   = dbutils.widgets.get("adls_account")
ADLS_CONTAINER = dbutils.widgets.get("adls_container")
KAGGLE_USER    = dbutils.widgets.get("kaggle_username")
KAGGLE_KEY     = dbutils.widgets.get("kaggle_key")
SQL_SERVER     = dbutils.widgets.get("sql_server")
SQL_DATABASE   = dbutils.widgets.get("sql_database")
SQL_USER       = dbutils.widgets.get("sql_username")
SQL_PASS       = dbutils.widgets.get("sql_password")
EH_NAMESPACE   = dbutils.widgets.get("eventhub_namespace")
EH_CONN_STR    = dbutils.widgets.get("eventhub_conn_str")

# Re-derive all paths
ADLS_BASE    = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
FILES_BASE   = f"{ADLS_BASE}/bharatmart-files"
LANDING_BASE = f"{ADLS_BASE}/landing"

print("Libraries installed ✅")
print(f"  ADLS Base:   {ADLS_BASE}")
print(f"  Files Base:  {FILES_BASE}")
print(f"  Landing:     {LANDING_BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Verify Library Versions

# COMMAND ----------

import ctgan, sdv, pandas, numpy, sklearn, pyarrow, faker

print("=" * 65)
print("LIBRARY VERSIONS")
print("=" * 65)
print(f"  ctgan:      {ctgan.__version__}")
print(f"  sdv:        {sdv.__version__}")
print(f"  pandas:     {pandas.__version__}")
print(f"  numpy:      {numpy.__version__}")
print(f"  sklearn:    {sklearn.__version__}")
print(f"  pyarrow:    {pyarrow.__version__}")
print(f"  faker:      {faker.VERSION}")
print()
print("All libraries loaded ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Unity Catalog Setup
# MAGIC
# MAGIC Creates the bharatmart catalog with ADLS as its managed storage location.
# MAGIC All Delta tables (bronze/silver/gold/ml) physically live in ADLS.
# MAGIC NOTE: If catalog already exists from a previous run, the IF NOT EXISTS
# MAGIC clause skips creation — this cell is always safe to re-run.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bharatmart
# MAGIC   MANAGED LOCATION 'abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/';
# MAGIC
# MAGIC USE CATALOG bharatmart;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bharatmart.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS bharatmart.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS bharatmart.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS bharatmart.ml;
# MAGIC
# MAGIC SHOW SCHEMAS IN bharatmart;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_database();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify ADLS Connection
# MAGIC
# MAGIC Auth works via Unity Catalog External Location "bharatmart-adls" which
# MAGIC uses the bharatmart-connector Access Connector (managed identity).
# MAGIC No SAS token, no spark.conf.set() — Serverless handles auth internally.
# MAGIC
# MAGIC PREREQUISITE: External Location must already be created in
# MAGIC Catalog Explorer → External Locations before running this cell.

# COMMAND ----------

try:
    files = dbutils.fs.ls(f"{ADLS_BASE}/")
    print("=" * 65)
    print("ADLS CONNECTION SUCCESS ✅")
    print(f"  Container:  {ADLS_CONTAINER} @ {ADLS_ACCOUNT}")
    print(f"  Auth:       Access Connector managed identity")
    print(f"  Top-level folders found:")
    for f in files:
        print(f"    {f.name}")
except Exception as e:
    print("ADLS CONNECTION FAILED ❌")
    print(f"  Error: {e}")
    print()
    print("  FIX: Ensure External Location 'bharatmart-adls' exists in")
    print("  Catalog → External Locations, pointing to:")
    print(f"  abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create ADLS /landing/ Folder Structure
# MAGIC
# MAGIC These 7 folders are the ONLY ones we create manually.
# MAGIC 03c_load_adls_bulk.py writes raw files here.
# MAGIC 06_bronze_autoloader_pipeline.py reads from here.
# MAGIC
# MAGIC Uses dbutils.fs.put() to write a placeholder file — this forces ADLS
# MAGIC to create the parent directory (ADLS doesn't have true empty folders).

# COMMAND ----------

LANDING_FOLDERS = [
    "reviews",        # Weekly file drop   → brz_reviews
    "inventory",      # Daily file drop    → brz_inventory
    "shipments",      # Daily file drop    → brz_shipments
    "commissions",    # Monthly file drop  → brz_commissions
    "refunds",        # Daily file drop    → brz_refunds
    "geo_dimension",  # One-time load      → brz_geo_dimension
    "date_dimension", # One-time load      → brz_date_dimension
]

print("=" * 65)
print("CREATING ADLS /landing/ FOLDERS")
print("=" * 65)

created = 0
for folder in LANDING_FOLDERS:
    placeholder = f"{LANDING_BASE}/{folder}/.keep"
    try:
        dbutils.fs.put(placeholder, "bharatmart_placeholder", overwrite=True)
        print(f"  ✅  /landing/{folder}/")
        created += 1
    except Exception as e:
        print(f"  ❌  /landing/{folder}/  →  {e}")

print()
print(f"  {created}/{len(LANDING_FOLDERS)} landing folders ready")
print()
print("  NOTE: Do NOT manually create /bronze/ /silver/ /gold/ /ml/")
print("  Unity Catalog creates those automatically when tables are written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Create ADLS /bharatmart-files/ Directories
# MAGIC
# MAGIC /raw/    = temporary Kaggle staging (DELETED after CTGAN training)
# MAGIC /models/ = permanent CTGAN .pkl files (NEVER delete)
# MAGIC
# MAGIC Uses dbutils.fs.mkdirs() — correct API for ADLS on Serverless.
# MAGIC os.makedirs() only works on the local driver /tmp/ path, not ADLS.

# COMMAND ----------

ADLS_DIRS = [
    # Kaggle raw datasets — TEMPORARY (deleted after 02_ctgan_trainer.py)
    f"{FILES_BASE}/raw/ds1_olist",
    f"{FILES_BASE}/raw/ds2_flipkart_products",
    f"{FILES_BASE}/raw/ds3_flipkart_reviews",
    f"{FILES_BASE}/raw/ds4_indian_ecommerce",
    f"{FILES_BASE}/raw/ds5_dataco",
    f"{FILES_BASE}/raw/ds7_pincode",
    f"{FILES_BASE}/raw/ds9_direct_messaging",
    f"{FILES_BASE}/raw/ds10_customer_personality",
    f"{FILES_BASE}/raw/ds11_bank_marketing",
    # CTGAN models — PERMANENT (never delete)
    f"{FILES_BASE}/models",
]

print("=" * 65)
print("CREATING ADLS /bharatmart-files/ DIRECTORIES")
print("=" * 65)

for d in ADLS_DIRS:
    try:
        dbutils.fs.mkdirs(d)
        label = d.replace(FILES_BASE, "")
        print(f"  ✅  {label}")
    except Exception as e:
        print(f"  ❌  {d}  →  {e}")

print()
print("  /raw/    → TEMPORARY. Delete after 02_ctgan_trainer.py completes.")
print("  /models/ → PERMANENT. 6 CTGAN .pkl files. Never delete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Configure Kaggle API Credentials
# MAGIC
# MAGIC Writes ~/.kaggle/kaggle.json to the local driver filesystem.
# MAGIC NOTE: ~/.kaggle/ is a LOCAL path (not ADLS) — os.makedirs and open()
# MAGIC work correctly here. This is one of the few places we use os in this script.

# COMMAND ----------

import os, json

# ~/.kaggle is LOCAL driver filesystem — os calls are correct here
kaggle_dir  = os.path.expanduser("~/.kaggle")
kaggle_file = os.path.join(kaggle_dir, "kaggle.json")

os.makedirs(kaggle_dir, exist_ok=True)

with open(kaggle_file, "w") as f:
    json.dump({"username": KAGGLE_USER, "key": KAGGLE_KEY}, f)

os.chmod(kaggle_file, 0o600)

# Test Kaggle auth
try:
    import kaggle
    kaggle.api.authenticate()
    auth_status = "✅ SUCCESS"
except Exception as e:
    auth_status = f"❌ FAILED — {e}"

print("=" * 65)
print("KAGGLE API CREDENTIALS")
print("=" * 65)
print(f"  Config file:  {kaggle_file}  (local driver, not ADLS)")
print(f"  Username:     {KAGGLE_USER}")
print(f"  Key:          {'*' * 8 if KAGGLE_KEY else 'NOT SET'}")
print(f"  Permissions:  600 ✅")
print(f"  API auth:     {auth_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Save Config to ADLS
# MAGIC
# MAGIC Saves all credentials and paths to ADLS as config.json.
# MAGIC Every downstream script (01–10) loads this file.
# MAGIC
# MAGIC IMPORTANT: Uses dbutils.fs.put() NOT open().
# MAGIC open() only works for local paths like /tmp/ or ~/.kaggle/
# MAGIC For ADLS paths, always use dbutils.fs.put() to write.

# COMMAND ----------

import json

CONFIG = {
    # Catalog
    "catalog":              CATALOG,

    # ADLS paths
    "adls_account":         ADLS_ACCOUNT,
    "adls_container":       ADLS_CONTAINER,
    "adls_base":            ADLS_BASE,
    "files_base":           FILES_BASE,
    "landing_path":         LANDING_BASE,
    "raw_base":             f"{FILES_BASE}/raw",
    "model_dir":            f"{FILES_BASE}/models",

    # Kaggle
    "kaggle_user":          KAGGLE_USER,

    # Azure SQL Database
    "sql_server":           SQL_SERVER,
    "sql_database":         SQL_DATABASE,
    "sql_username":         SQL_USER,
    "sql_password":         SQL_PASS,
    "sql_jdbc_url": (
        f"jdbc:sqlserver://{SQL_SERVER}:1433;"
        f"database={SQL_DATABASE};"
        f"encrypt=true;trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;loginTimeout=30"
    ),

    # Azure Event Hub
    "eventhub_namespace":   EH_NAMESPACE,
    "eventhub_conn_str":    EH_CONN_STR,
    "eventhub_bootstrap":   f"{EH_NAMESPACE}.servicebus.windows.net:9093",
}

config_adls_path = f"{FILES_BASE}/config.json"

# dbutils.fs.put() = correct way to write text/JSON to ADLS on Serverless
dbutils.fs.put(config_adls_path, json.dumps(CONFIG, indent=2), overwrite=True)

print("=" * 65)
print("CONFIG SAVED TO ADLS ✅")
print("=" * 65)
print(f"  Path: {config_adls_path}")
print()
print("  Load in any downstream script:")
print("    import json")
print(f"    cfg = json.loads(dbutils.fs.head('{config_adls_path}', 100000))")
print()
print("  Keys saved:")
for k, v in CONFIG.items():
    if any(s in k for s in ["password", "key", "conn_str"]):
        display = "*" * 8 if v else "NOT SET"
    else:
        display = str(v)[:80] if v else "NOT SET"
    print(f"    {k:<25s} → {display}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Full Setup Validation

# COMMAND ----------

import json

print("=" * 65)
print("BHARATMART SETUP VALIDATION")
print("=" * 65)

checks = []

# 1. Core libraries
try:
    import ctgan, sdv, pandas, numpy, faker, kaggle
    checks.append(("Core libraries (ctgan, sdv, pandas, faker)", True, ""))
except Exception as e:
    checks.append(("Core libraries", False, str(e)))

# 2. Azure libraries
try:
    import pyodbc, sqlalchemy
    from azure.eventhub import EventHubProducerClient
    from azure.storage.blob import BlobServiceClient
    checks.append(("Azure libraries (pyodbc, eventhub, blob)", True, ""))
except Exception as e:
    checks.append(("Azure libraries", False, str(e)))

# 3. Kaggle auth
try:
    kaggle.api.authenticate()
    checks.append(("Kaggle API authentication", True, ""))
except Exception as e:
    checks.append(("Kaggle API authentication", False, str(e)))

# 4. ADLS connection via dbutils.fs.ls (not os.path — ADLS path)
try:
    dbutils.fs.ls(f"{ADLS_BASE}/")
    checks.append(("ADLS connection (Access Connector)", True, ""))
except Exception as e:
    checks.append(("ADLS connection", False, str(e)))

# 5. Landing folders via dbutils.fs.ls
try:
    items = [f.name.rstrip("/") for f in dbutils.fs.ls(f"{LANDING_BASE}/")]
    expected = {"reviews","inventory","shipments","commissions",
                "refunds","geo_dimension","date_dimension"}
    found = expected.intersection(set(items))
    if len(found) == len(expected):
        checks.append(("ADLS /landing/ folders (7/7)", True, ""))
    else:
        checks.append(("ADLS /landing/ folders", False, f"Missing: {expected - found}"))
except Exception as e:
    checks.append(("ADLS /landing/ folders", False, str(e)))

# 6. Raw directories via dbutils.fs.ls
raw_names = ["ds1_olist","ds2_flipkart_products","ds3_flipkart_reviews",
             "ds4_indian_ecommerce","ds5_dataco","ds7_pincode",
             "ds9_direct_messaging","ds10_customer_personality","ds11_bank_marketing"]
try:
    existing = [f.name.rstrip("/") for f in dbutils.fs.ls(f"{FILES_BASE}/raw/")]
    found = set(raw_names).intersection(set(existing))
    if len(found) == len(raw_names):
        checks.append(("ADLS /raw/ directories (9/9)", True, ""))
    else:
        checks.append(("ADLS /raw/ directories", False, f"Missing: {set(raw_names)-found}"))
except Exception as e:
    checks.append(("ADLS /raw/ directories", False, str(e)))

# 7. Models directory via dbutils.fs.ls
try:
    dbutils.fs.ls(f"{FILES_BASE}/models/")
    checks.append(("ADLS /models/ directory", True, ""))
except Exception as e:
    checks.append(("ADLS /models/ directory", False, str(e)))

# 8. Config file via dbutils.fs.head (not open() — ADLS path)
try:
    cfg_str = dbutils.fs.head(f"{FILES_BASE}/config.json", 100000)
    cfg = json.loads(cfg_str)
    required = ["catalog","adls_base","landing_path","files_base",
                "model_dir","sql_server","sql_database","sql_jdbc_url",
                "eventhub_namespace","eventhub_bootstrap"]
    missing = [k for k in required if not cfg.get(k)]
    if not missing:
        checks.append((f"Config file in ADLS ({len(cfg)} keys)", True, ""))
    else:
        checks.append(("Config file", False, f"Missing values: {missing}"))
except Exception as e:
    checks.append(("Config file in ADLS", False, str(e)))

# 9. Unity Catalog
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    schemas = [r.databaseName for r in spark.sql("SHOW SCHEMAS").collect()]
    expected_s = {"bronze","silver","gold","ml"}
    found_s = expected_s.intersection(set(schemas))
    if len(found_s) == len(expected_s):
        checks.append(("Unity Catalog schemas (bronze/silver/gold/ml)", True, ""))
    else:
        checks.append(("Unity Catalog schemas", False, f"Missing: {expected_s - found_s}"))
except Exception as e:
    checks.append(("Unity Catalog", False, str(e)))

# 10. SQL credentials
sql_ok = all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASS])
checks.append(("Azure SQL credentials set", sql_ok,
               "" if sql_ok else "Fill sql_server / sql_username / sql_password widgets"))

# 11. Event Hub credentials
eh_ok = all([EH_NAMESPACE, EH_CONN_STR])
checks.append(("Event Hub credentials set", eh_ok,
               "" if eh_ok else "Fill eventhub_namespace / eventhub_conn_str widgets"))

# Print results
print()
all_passed = True
for name, passed, detail in checks:
    icon = "✅" if passed else "❌"
    all_passed = all_passed and passed
    print(f"  {icon}  {name}")
    if detail:
        print(f"       └─ {detail}")

print()
if all_passed:
    print("=" * 65)
    print("  ALL CHECKS PASSED ✅  Ready → 01_kaggle_downloader.py")
    print("=" * 65)
else:
    print("=" * 65)
    print("  SOME CHECKS FAILED ❌  Fix above before proceeding.")
    print("  SQL + Event Hub ❌ is OK only if widgets left blank.")
    print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Setup Complete — Next: 01_kaggle_downloader.py
# MAGIC
# MAGIC **Storage layout:**
# MAGIC ```
# MAGIC ADLS Gen2 (abfss://bharatmart@stbharatmartdev.dfs.core.windows.net/):
# MAGIC   /landing/reviews/          ← AutoLoader source (03c writes here)
# MAGIC   /landing/inventory/
# MAGIC   /landing/shipments/
# MAGIC   /landing/commissions/
# MAGIC   /landing/refunds/
# MAGIC   /landing/geo_dimension/
# MAGIC   /landing/date_dimension/
# MAGIC   /bharatmart-files/raw/     ← Kaggle staging TEMP (delete after 02)
# MAGIC   /bharatmart-files/models/  ← CTGAN .pkl files PERMANENT (never delete)
# MAGIC   /bharatmart-files/config.json
# MAGIC   /bharatmart/               ← Unity Catalog Delta tables (auto-managed)
# MAGIC
# MAGIC Azure SQL:  bahartmartsql.database.windows.net / bharatmart_db
# MAGIC Event Hub:  bharatmart-eh (Kafka surface, 5 topics)
# MAGIC ```
