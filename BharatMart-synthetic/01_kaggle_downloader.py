# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — BharatMart 360° | Kaggle Dataset Downloader
# MAGIC **BharatMart 360° Intelligence Platform**
# MAGIC
# MAGIC Pattern (zip-name safe):
# MAGIC 1. `kaggle download -p /tmp/dsX/` → downloads zip INTO /tmp/dsX/ folder
# MAGIC 2. `unzip /tmp/dsX/*.zip -d /tmp/dsX/` → wildcard unzip, no hardcoded zip name
# MAGIC 3. `shutil.copy()` → `/Volumes/bharatmart/raw_kaggle/datasets/dsX/`
# MAGIC 4. `rm -rf /tmp/dsX/` → cleanup entire folder
# MAGIC
# MAGIC Downloading to a subfolder with -p and using *.zip wildcard means we
# MAGIC never need to know the exact zip filename Kaggle saves.
# MAGIC
# MAGIC **Run order**: After 00_setup.py → Before 02_ctgan_trainer.py

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Set Kaggle Credentials + Create Volume

# COMMAND ----------

import os, shutil

os.environ['KAGGLE_USERNAME'] = 'kumarveer'
os.environ['KAGGLE_KEY']      = 'KGAT_cc9485106f1ec59cbabc9ad7a932c501'

spark.sql("CREATE SCHEMA IF NOT EXISTS bharatmart.raw_kaggle")
spark.sql("CREATE VOLUME IF NOT EXISTS bharatmart.raw_kaggle.datasets")

VOLUME_BASE = "/Volumes/bharatmart/raw_kaggle/datasets"

print(f"Volume ready:  {VOLUME_BASE}")
print(f"Volume exists: {os.path.exists(VOLUME_BASE)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — DS-1: Olist Brazilian E-Commerce

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds1_olist
# MAGIC kaggle datasets download -d olistbr/brazilian-ecommerce -p /tmp/ds1_olist
# MAGIC unzip -o /tmp/ds1_olist/*.zip -d /tmp/ds1_olist
# MAGIC ls -lh /tmp/ds1_olist/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds1_olist"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds1_olist"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-1 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds1_olist
# MAGIC echo "DS-1 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — DS-2: Flipkart Products

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds2_flipkart_products
# MAGIC kaggle datasets download -d PromptCloudHQ/flipkart-products -p /tmp/ds2_flipkart_products
# MAGIC unzip -o /tmp/ds2_flipkart_products/*.zip -d /tmp/ds2_flipkart_products
# MAGIC ls -lh /tmp/ds2_flipkart_products/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds2_flipkart_products"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds2_flipkart_products"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-2 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds2_flipkart_products
# MAGIC echo "DS-2 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — DS-3: Flipkart Reviews

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds3_flipkart_reviews
# MAGIC kaggle datasets download -d niraliivaghani/flipkart-product-customer-reviews-dataset -p /tmp/ds3_flipkart_reviews
# MAGIC unzip -o /tmp/ds3_flipkart_reviews/*.zip -d /tmp/ds3_flipkart_reviews
# MAGIC ls -lh /tmp/ds3_flipkart_reviews/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds3_flipkart_reviews"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds3_flipkart_reviews"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-3 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds3_flipkart_reviews
# MAGIC echo "DS-3 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — DS-4: Indian E-Commerce Sales

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds4_indian_ecommerce
# MAGIC kaggle datasets download -d benroshan/e-commerce-data -p /tmp/ds4_indian_ecommerce
# MAGIC unzip -o /tmp/ds4_indian_ecommerce/*.zip -d /tmp/ds4_indian_ecommerce
# MAGIC ls -lh /tmp/ds4_indian_ecommerce/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds4_indian_ecommerce"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds4_indian_ecommerce"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-4 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds4_indian_ecommerce
# MAGIC echo "DS-4 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — DS-5: DataCo Supply Chain

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds5_dataco
# MAGIC kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis -p /tmp/ds5_dataco
# MAGIC unzip -o /tmp/ds5_dataco/*.zip -d /tmp/ds5_dataco
# MAGIC ls -lh /tmp/ds5_dataco/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds5_dataco"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds5_dataco"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-5 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds5_dataco
# MAGIC echo "DS-5 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — DS-7: India Pincode Directory

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds7_pincode
# MAGIC kaggle datasets download -d datameet/pincode-dataset -p /tmp/ds7_pincode
# MAGIC unzip -o /tmp/ds7_pincode/*.zip -d /tmp/ds7_pincode
# MAGIC ls -lh /tmp/ds7_pincode/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds7_pincode"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds7_pincode"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-7 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds7_pincode
# MAGIC echo "DS-7 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — DS-8: UPI Transactions India

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds8_upi
# MAGIC kaggle datasets download -d subhamjain09/upi-data-2016-2021 -p /tmp/ds8_upi
# MAGIC unzip -o /tmp/ds8_upi/*.zip -d /tmp/ds8_upi
# MAGIC ls -lh /tmp/ds8_upi/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds8_upi"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds8_upi"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-8 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds8_upi
# MAGIC echo "DS-8 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — DS-9: E-commerce Direct Messaging

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds9_direct_messaging
# MAGIC kaggle datasets download -d mkechinov/direct-messaging -p /tmp/ds9_direct_messaging
# MAGIC unzip -o /tmp/ds9_direct_messaging/*.zip -d /tmp/ds9_direct_messaging
# MAGIC ls -lh /tmp/ds9_direct_messaging/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds9_direct_messaging"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds9_direct_messaging"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-9 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds9_direct_messaging
# MAGIC echo "DS-9 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — DS-10: Customer Personality Analysis

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds10_customer_personality
# MAGIC kaggle datasets download -d imakash3011/customer-personality-analysis -p /tmp/ds10_customer_personality
# MAGIC unzip -o /tmp/ds10_customer_personality/*.zip -d /tmp/ds10_customer_personality
# MAGIC ls -lh /tmp/ds10_customer_personality/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds10_customer_personality"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds10_customer_personality"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-10 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds10_customer_personality
# MAGIC echo "DS-10 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — DS-11: UCI Bank Marketing

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/ds11_bank_marketing
# MAGIC kaggle datasets download -d henriqueyamahata/bank-marketing -p /tmp/ds11_bank_marketing
# MAGIC unzip -o /tmp/ds11_bank_marketing/*.zip -d /tmp/ds11_bank_marketing
# MAGIC ls -lh /tmp/ds11_bank_marketing/*.csv

# COMMAND ----------

import os, shutil
src = "/tmp/ds11_bank_marketing"
dst = "/Volumes/bharatmart/raw_kaggle/datasets/ds11_bank_marketing"
os.makedirs(dst, exist_ok=True)
for f in [x for x in os.listdir(src) if x.endswith(".csv")]:
    shutil.copy(os.path.join(src, f), os.path.join(dst, f))
    print(f"  ✅  {f}")
print("\nDS-11 copied to Volume ✅")

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /tmp/ds11_bank_marketing
# MAGIC echo "DS-11 /tmp cleanup done ✅"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Verify All 10 Datasets in Volume

# COMMAND ----------

import os

VOLUME_BASE = "/Volumes/bharatmart/raw_kaggle/datasets"

EXPECTED = {
    "ds1_olist":                ["olist_orders_dataset.csv", "olist_customers_dataset.csv"],
    "ds2_flipkart_products":    ["flipkart_com-ecommerce_sample.csv"],
    "ds3_flipkart_reviews":     ["flipkart_product_review.csv"],
    "ds4_indian_ecommerce":     ["List of Orders.csv"],
    "ds5_dataco":               ["DataCoSupplyChainDataset.csv"],
    "ds7_pincode":              ["Pincode_30052019.csv"],
    "ds8_upi":                  ["UPI Dataset.csv"],
    "ds9_direct_messaging":     ["campaigns.csv"],
    "ds10_customer_personality":["marketing_campaign.csv"],
    "ds11_bank_marketing":      ["bank-additional-full.csv"],
}

print("=" * 65)
print("FINAL VERIFICATION — /Volumes/bharatmart/raw_kaggle/datasets/")
print("=" * 65)

all_ok = True
for ds_folder, key_files in EXPECTED.items():
    ds_path = os.path.join(VOLUME_BASE, ds_folder)
    if not os.path.exists(ds_path):
        print(f"  ❌  {ds_folder} — FOLDER NOT FOUND")
        all_ok = False
        continue
    all_files = os.listdir(ds_path)
    csvs      = [f for f in all_files if f.endswith(".csv")]
    size_mb   = sum(os.path.getsize(os.path.join(ds_path, f)) for f in all_files) / (1024**2)
    missing   = [f for f in key_files if f not in all_files]
    if missing:
        print(f"  ⚠️   {ds_folder:<38s} key file missing: {missing}")
        all_ok = False
    else:
        print(f"  ✅  {ds_folder:<38s} {len(csvs)} CSV  {size_mb:.1f} MB")

print()
if all_ok:
    print("ALL 10 DATASETS READY ✅  Next → 02_ctgan_trainer.py")
else:
    print("SOME DATASETS MISSING ❌  Re-run the failed step above")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚠️ CLEANUP — Run ONLY After 02_ctgan_trainer.py Completes

# COMMAND ----------

import os, shutil

REQUIRED_MODELS = [
    "model_transactions.pkl",
    "model_products.pkl",
    "model_reviews.pkl",
    "model_campaign_channel.pkl",
    "model_campaign_response.pkl",
    "model_supply_chain.pkl",
]

def cleanup_raw_kaggle():
    model_dir = "/Volumes/bharatmart/ctgan_models/models"
    print("Verifying CTGAN models before deleting raw data ...")
    all_ok = True
    for m in REQUIRED_MODELS:
        path = os.path.join(model_dir, m)
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024**2)
            print(f"  ✅  {m}  ({size_mb:.1f} MB)")
        else:
            print(f"  ❌  {m} — NOT FOUND")
            all_ok = False
    if not all_ok:
        print("\n❌ ABORT — Run 02_ctgan_trainer.py first.")
        return
    print("\nAll models confirmed. Deleting raw Kaggle data ...")
    for ds_folder in EXPECTED.keys():
        ds_path = os.path.join(VOLUME_BASE, ds_folder)
        if os.path.exists(ds_path):
            shutil.rmtree(ds_path)
            print(f"  🗑️  Deleted: {ds_folder}")
    print("\n✅ Cleanup complete.")

# Uncomment ONLY after 02_ctgan_trainer.py finishes:
# cleanup_raw_kaggle()
print("Cleanup ready. Uncomment cleanup_raw_kaggle() after training completes.")
