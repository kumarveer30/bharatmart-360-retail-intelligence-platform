# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — BharatMart 360° | CTGAN / SDV Trainer
# MAGIC **BharatMart 360° Intelligence Platform**
# MAGIC
# MAGIC Trains 6 CTGAN/SDV models on the real Kaggle datasets.
# MAGIC These .pkl files are DATA GENERATORS — not ML models.
# MAGIC
# MAGIC When Phase 1 job runs, these generators produce synthetic BharatMart data
# MAGIC that gets routed to Azure SQL, Event Hub, and ADLS landing/.
# MAGIC
# MAGIC **Reads from**:  /Volumes/bharatmart/raw_kaggle/datasets/
# MAGIC **Saves to**:    /Volumes/bharatmart/ctgan_models/models/
# MAGIC
# MAGIC Models trained:
# MAGIC   model_transactions.pkl      ← orders + order_items (Olist DS-1)
# MAGIC   model_products.pkl          ← products + categories (Flipkart DS-2)
# MAGIC   model_reviews.pkl           ← product reviews (Flipkart DS-3)
# MAGIC   model_campaign_channel.pkl  ← campaign channel data (Direct Messaging DS-9)
# MAGIC   model_campaign_response.pkl ← customer response behaviour (DS-10 + DS-11)
# MAGIC   model_supply_chain.pkl      ← supply chain + shipments (DataCo DS-5)
# MAGIC
# MAGIC Run order: After 01_kaggle_downloader.py → Before 03a/03b/03c

# COMMAND ----------

# MAGIC %pip install sdv ctgan pandas numpy scikit-learn

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Create Models Volume + Verify Raw Data Exists

# COMMAND ----------

import os

# Create the Volume where all 6 CTGAN .pkl files will be stored permanently
# This Volume is used by Phase 1 scripts (03a, 03b, 03c) to generate data
spark.sql("CREATE SCHEMA IF NOT EXISTS bharatmart.ctgan_models")
spark.sql("CREATE VOLUME IF NOT EXISTS bharatmart.ctgan_models.models")

RAW_BASE   = "/Volumes/bharatmart/raw_kaggle/datasets"
MODEL_BASE = "/Volumes/bharatmart/ctgan_models/models"

print("=" * 65)
print("BharatMart 360° — CTGAN / SDV Trainer")
print("=" * 65)
print(f"  Raw data:     {RAW_BASE}")
print(f"  Models will save to: {MODEL_BASE}")
print()
print(f"  Volume exists: {os.path.exists(MODEL_BASE)}")
print()

# Verify all raw datasets are present before starting training
REQUIRED_RAW = {
    "ds1_olist":                "olist_orders_dataset.csv",
    "ds2_flipkart_products":    "flipkart_com-ecommerce_sample.csv",
    "ds3_flipkart_reviews":     "Dataset-SA.csv",   # wildcard — any csv
    "ds5_dataco":               "DataCoSupplyChainDataset.csv",
    "ds9_direct_messaging":     "campaigns.csv",
    "ds10_customer_personality":"marketing_campaign.csv",
    "ds11_bank_marketing":      "bank-additional-full.csv",
}

print("Checking raw data availability:")
all_ok = True
for folder, key_file in REQUIRED_RAW.items():
    folder_path = os.path.join(RAW_BASE, folder)
    if not os.path.exists(folder_path):
        print(f"  ❌  {folder} — NOT FOUND")
        all_ok = False
    else:
        csvs = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if key_file and key_file not in csvs:
            # Try any csv as fallback
            print(f"  ⚠️   {folder} — {key_file} not found, using: {csvs[0] if csvs else 'NO CSV'}")
        else:
            fname = key_file if key_file else (csvs[0] if csvs else "none")
            print(f"  ✅  {folder} / {fname}")

if not all_ok:
    raise RuntimeError("Some raw datasets missing. Re-run 01_kaggle_downloader.py first.")

print()
print("All raw data ready ✅  Starting training ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Helper: Load CSV from Volume

# COMMAND ----------

# DBTITLE 1,Cell 7
import pandas as pd
import os

def load_csv(folder: str, filename: str = None, nrows: int = None) -> pd.DataFrame:
    """
    Load a CSV from /Volumes/bharatmart/raw_kaggle/datasets/{folder}/
    If filename is None, loads the first CSV found in the folder.
    nrows: limit rows for faster training on large files.
    """
    folder_path = os.path.join("/Volumes/bharatmart/raw_kaggle/datasets", folder)
    if filename:
        fpath = os.path.join(folder_path, filename)
    else:
        csvs  = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if not csvs:
            raise FileNotFoundError(f"No CSV found in {folder_path}")
        fpath = os.path.join(folder_path, csvs[0])
        print(f"  Auto-selected: {csvs[0]}")

    df = pd.read_csv(fpath, nrows=nrows,
                     sep=None, engine="python", encoding="latin1")  # sep=None handles tab-separated too
    print(f"  Loaded: {os.path.basename(fpath)} — {len(df):,} rows × {len(df.columns)} cols")
    return df


def save_model(model, model_name: str):
    """Save a trained CTGAN/SDV model to the models Volume."""
    import pickle
    path = os.path.join("/Volumes/bharatmart/ctgan_models/models", model_name)
    with open(path, "wb") as f:
        pickle.dump(model, f)
    size_mb = os.path.getsize(path) / (1024**2)
    print(f"  ✅  Saved: {model_name} ({size_mb:.1f} MB) → {path}")


def check_already_trained(model_name: str) -> bool:
    """Skip training if model already exists in Volume."""
    path = os.path.join("/Volumes/bharatmart/ctgan_models/models", model_name)
    if os.path.exists(path):
        size_mb = os.path.getsize(path) / (1024**2)
        print(f"  ⏭️  Already trained: {model_name} ({size_mb:.1f} MB) — skipping")
        return True
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — model_transactions.pkl
# MAGIC **Source**: DS-1 Olist (olist_orders + olist_order_items)
# MAGIC **Used by**: 03a_load_azure_sql.py + 03b_event_producer.py to generate orders + order_items

# COMMAND ----------

# DBTITLE 1,Cell 9
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import pandas as pd

MODEL_NAME = "model_transactions.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")

    # Load orders + items and join key columns for richer context
    orders = load_csv("ds1_olist", "olist_orders_dataset.csv", nrows=20000)
    items  = load_csv("ds1_olist", "olist_order_items_dataset.csv", nrows=20000)

    # Select columns that map to BharatMart transactions schema
    orders_slim = orders[["order_id","customer_id","order_status",
                           "order_purchase_timestamp","order_delivered_customer_date"]].copy()
    items_slim  = items[["order_id","product_id","seller_id",
                          "price","freight_value","order_item_id"]].copy()

    # Merge on order_id, take first item per order to keep 1-row-per-order structure
    df = orders_slim.merge(items_slim, on="order_id", how="left")
    df = df.drop_duplicates(subset=["order_id"])

    # Rename to BharatMart column names
    df = df.rename(columns={
        "order_purchase_timestamp":       "order_date",
        "order_delivered_customer_date":  "delivery_date",
        "freight_value":                  "shipping_cost",
    })

    # Drop nulls in key columns
    df = df.dropna(subset=["order_id","customer_id","price"])
    df = df.reset_index(drop=True)
    print(f"  Training data shape: {df.shape}")

    # Build metadata
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)

    # Train CTGAN
    synthesizer = CTGANSynthesizer(metadata, epochs=100, verbose=True)
    synthesizer.fit(df)

    save_model(synthesizer, MODEL_NAME)

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — model_products.pkl
# MAGIC **Source**: DS-2 Flipkart Products
# MAGIC **Used by**: 03a_load_azure_sql.py to populate products + categories tables

# COMMAND ----------

# Remove old thin models so check_already_trained() doesn't skip
dbutils.fs.rm("/Volumes/bharatmart/ctgan_models/models/model_products.pkl")
dbutils.fs.rm("/Volumes/bharatmart/ctgan_models/models/model_reviews.pkl")
print("Old models deleted ✅ — ready to retrain")

# COMMAND ----------

# DBTITLE 1,Cell 11
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import re

MODEL_NAME = "model_products.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")
    print("  → ENHANCED: 5 columns (retail_price, discounted_price, discount_pct, product_rating, L1_category)")

    df = load_csv("ds2_flipkart_products", "flipkart_com-ecommerce_sample.csv", nrows=20000)

    # ── Extract L1 category from product_category_tree ──────────────────
    # EDA found: ~20 L1 categories. 328 malformed rows → regex >> fallback
    cat_col = None
    for c in df.columns:
        if "category" in c.lower() and "tree" in c.lower():
            cat_col = c
            break

    if cat_col:
        def extract_l1(val):
            if val is None or str(val).strip() in ("", "nan", "None"):
                return None
            s = re.sub(r"[\[\]']", "", str(val))
            parts = s.split(">>")
            return parts[0].strip() if parts else None

        df["L1_category"] = df[cat_col].apply(extract_l1)

        # Keep top 15 L1 categories, rest → "Other" (keeps CTGAN manageable)
        top_cats = df["L1_category"].value_counts().head(15).index.tolist()
        df["L1_category"] = df["L1_category"].apply(
            lambda x: x if x in top_cats else "Other"
        )
        print(f"  L1 categories: {df['L1_category'].nunique()} unique values")
        print(f"  Top 5: {df['L1_category'].value_counts().head(5).to_dict()}")

    # ── Clean price columns ─────────────────────────────────────────────
    for col in ["retail_price", "discounted_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[^\d.]", "", regex=True),
                errors="coerce"
            )

    # ── Derive discount_pct (EDA: mean ~33%, varies by category) ────────
    if "retail_price" in df.columns and "discounted_price" in df.columns:
        df["discount_pct"] = (
            (df["retail_price"] - df["discounted_price"]) / df["retail_price"] * 100
        ).clip(0, 90).round(1)

    # ── Clean product_rating ────────────────────────────────────────────
    if "product_rating" in df.columns:
        df["product_rating"] = pd.to_numeric(
            df["product_rating"].astype(str).str.replace(r"[^\d.]", "", regex=True),
            errors="coerce"
        )

    # ── Select training columns ─────────────────────────────────────────
    keep_cols = ["retail_price", "discounted_price", "discount_pct", "product_rating", "L1_category"]
    available = [c for c in keep_cols if c in df.columns]
    df = df[available].copy()

    df = df.dropna(subset=["retail_price"])
    if "product_rating" in df.columns:
        df["product_rating"] = df["product_rating"].fillna(0)
    if "discount_pct" in df.columns:
        df["discount_pct"] = df["discount_pct"].fillna(0)
    if "L1_category" in df.columns:
        df["L1_category"] = df["L1_category"].fillna("Other")

    df = df.reset_index(drop=True)
    print(f"  Training shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Sample:\n{df.head(3).to_string()}")

    # ── Metadata with explicit types ────────────────────────────────────
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)
    if "L1_category" in df.columns:
        metadata.update_column("L1_category", sdtype="categorical")
    if "product_rating" in df.columns:
        metadata.update_column("product_rating", sdtype="numerical")

    # ── Train (150 epochs — more cols need more training) ───────────────
    synthesizer = CTGANSynthesizer(metadata, epochs=150, verbose=True)
    synthesizer.fit(df)
    save_model(synthesizer, MODEL_NAME)

    # ── Quick validation ────────────────────────────────────────────────
    sample = synthesizer.sample(100)
    print(f"\n  Validation — 100 rows:")
    if "L1_category" in sample.columns:
        print(f"  Categories: {sample['L1_category'].value_counts().head(5).to_dict()}")
    print(f"  Price: ₹{sample['retail_price'].min():.0f} – ₹{sample['retail_price'].max():.0f}")
    print(f"  Discount: {sample['discount_pct'].min():.1f}% – {sample['discount_pct'].max():.1f}%")

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — model_reviews.pkl
# MAGIC **Source**: DS-3 Flipkart Reviews (saved as Dataset-SA.csv)
# MAGIC **Used by**: 03c_load_adls_bulk.py to generate /landing/reviews/ files

# COMMAND ----------

# DBTITLE 1,Cell 13
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

MODEL_NAME = "model_reviews.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")
    print("  → ENHANCED: 3 columns (rating, text_length_bin, has_review_text)")

    df = load_csv("ds3_flipkart_reviews", filename=None, nrows=50000)
    print(f"  Raw columns: {list(df.columns)}")

    # ── Find rating column ──────────────────────────────────────────────
    # EDA found: column might be Rate, rate, Rating, rating
    rating_col = None
    for c in df.columns:
        if c.lower() in ("rate", "rating", "star", "stars"):
            rating_col = c
            break

    if rating_col is None:
        for c in df.columns:
            try:
                vals = pd.to_numeric(df[c], errors="coerce").dropna()
                if len(vals) > 100 and vals.min() >= 1 and vals.max() <= 5:
                    rating_col = c
                    break
            except:
                pass

    if rating_col:
        df["rating"] = pd.to_numeric(df[rating_col], errors="coerce")
        df = df[df["rating"].between(1, 5)]
        df["rating"] = df["rating"].astype(int)
        print(f"  Rating column: '{rating_col}' → {df['rating'].value_counts().sort_index().to_dict()}")
    else:
        print("  ⚠️ No rating column found — using default")
        df["rating"] = 4

    # ── Derive text_length_bin from review text ─────────────────────────
    # EDA found: columns may be SWAPPED. Pick the LONGER text column.
    # Calibration targets: ⭐1-2 = 40-80 words, ⭐4-5 = 10-30 words
    text_col = None
    text_candidates = [c for c in df.columns if any(
        kw in c.lower() for kw in ("review", "text", "comment", "body", "summary")
    )]

    if text_candidates:
        avg_lens = {}
        for c in text_candidates:
            avg_lens[c] = df[c].astype(str).str.len().mean()
        text_col = max(avg_lens, key=avg_lens.get)
        print(f"  Text column: '{text_col}' (avg {avg_lens[text_col]:.0f} chars)")

    if text_col:
        df["text_word_count"] = (
            df[text_col].astype(str).str.strip()
            .apply(lambda x: len(x.split()) if x and x.lower() not in ("nan", "none", "") else 0)
        )

        def bin_text_length(wc):
            if wc == 0:    return "none"     # No review text (rating only)
            elif wc <= 15: return "short"    # "Good product", "Nice quality"
            elif wc <= 50: return "medium"   # Some detail
            else:          return "long"     # Detailed (often angry)

        df["text_length_bin"] = df["text_word_count"].apply(bin_text_length)
        df["has_review_text"] = (df["text_word_count"] > 0).astype(int)

        print(f"  Text bins: {df['text_length_bin'].value_counts().to_dict()}")
        cross = pd.crosstab(df["rating"], df["text_length_bin"], normalize="index").round(2)
        print(f"  Rating × TextLen:\n{cross.to_string()}")
    else:
        df["text_length_bin"] = "medium"
        df["has_review_text"] = 1

    # ── Select and clean ────────────────────────────────────────────────
    keep_cols = ["rating", "text_length_bin", "has_review_text"]
    df = df[[c for c in keep_cols if c in df.columns]].copy()
    df = df.dropna(how="all").reset_index(drop=True)
    print(f"  Training shape: {df.shape}")

    # ── Metadata — all categorical ──────────────────────────────────────
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)
    metadata.update_column("rating", sdtype="categorical")
    metadata.update_column("text_length_bin", sdtype="categorical")
    if "has_review_text" in df.columns:
        metadata.update_column("has_review_text", sdtype="categorical")

    # ── Train ───────────────────────────────────────────────────────────
    synthesizer = CTGANSynthesizer(metadata, epochs=150, verbose=True)
    synthesizer.fit(df)
    save_model(synthesizer, MODEL_NAME)

    # ── Validation ──────────────────────────────────────────────────────
    sample = synthesizer.sample(200)
    print(f"\n  Validation — 200 rows:")
    print(f"  Ratings: {sample['rating'].value_counts().sort_index().to_dict()}")
    if "text_length_bin" in sample.columns:
        print(f"  Text bins: {sample['text_length_bin'].value_counts().to_dict()}")
        cross_val = pd.crosstab(sample["rating"], sample["text_length_bin"], normalize="index").round(2)
        print(f"  Generated Rating × TextLen:\n{cross_val.to_string()}")

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — model_campaign_channel.pkl
# MAGIC **Source**: DS-9 Direct Messaging (campaigns.csv)
# MAGIC **Used by**: 03b_event_producer.py to generate campaign_response events

# COMMAND ----------

from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

MODEL_NAME = "model_campaign_channel.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")

    # campaigns.csv is large (2GB) — sample 100K rows
    df = load_csv("ds9_direct_messaging", "campaigns.csv", nrows=100000)

    # Keep channel + engagement columns
    channel_cols = ["channel","platform","subject_length","is_open",
                    "is_click","is_unsubscribe","send_date","open_date",
                    "campaign_type","message_type"]
    available = [c for c in channel_cols if c in df.columns]
    if not available:
        available = df.columns.tolist()[:8]
    df = df[available].copy()

    df = df.dropna(how="all")
    df = df.reset_index(drop=True)
    print(f"  Training data shape: {df.shape}")
    print(f"  Columns used: {list(df.columns)}")

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)

    synthesizer = CTGANSynthesizer(metadata, epochs=100, verbose=True)
    synthesizer.fit(df)

    save_model(synthesizer, MODEL_NAME)

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — model_campaign_response.pkl
# MAGIC **Source**: DS-10 Customer Personality + DS-11 Bank Marketing
# MAGIC **Used by**: 03a_load_azure_sql.py to generate customers with response behaviour

# COMMAND ----------

# DBTITLE 1,Cell 17
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import numpy as np

MODEL_NAME = "model_campaign_response.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")

    # DS-10: customer demographics + purchase behaviour + campaign response
    df10 = load_csv("ds10_customer_personality", "marketing_campaign.csv")

    # DS-11: bank marketing — similar campaign response structure
    df11 = load_csv("ds11_bank_marketing", "bank-additional-full.csv")

    # Standardise column names and combine
    # DS-10 columns: Income, Kidhome, Teenhome, Recency, MntWines, Response etc.
    # DS-11 columns: age, job, marital, education, balance, y (subscribed) etc.

    # Common derived features
    df10_slim = pd.DataFrame({
        "age":           df10.get("Year_Birth", pd.Series(dtype='float64')).apply(
                             lambda x: 2024 - int(x) if pd.notna(x) else None),
        "income":        df10.get("Income", pd.Series(dtype='float64')),
        "recency_days":  df10.get("Recency", pd.Series(dtype='float64')),
        "total_spend":   df10.filter(like="Mnt").sum(axis=1) if any(
                             c.startswith("Mnt") for c in df10.columns) else pd.Series([np.nan] * len(df10)),
        "num_purchases": df10.filter(like="NumDeal").sum(axis=1) if any(
                             c.startswith("Num") for c in df10.columns) else pd.Series([np.nan] * len(df10)),
        "responded":     df10.get("Response", pd.Series(dtype='float64')),
    })

    df11_slim = pd.DataFrame({
        "age":           pd.to_numeric(df11.get("age", pd.Series(dtype='float64')), errors="coerce"),
        "income":        pd.to_numeric(df11.get("balance", pd.Series(dtype='float64')), errors="coerce"),
        "recency_days":  pd.to_numeric(df11.get("pdays", pd.Series(dtype='float64')).replace(-1, None),
                                       errors="coerce"),
        "total_spend":   pd.Series([np.nan] * len(df11)),
        "num_purchases": pd.to_numeric(df11.get("previous", pd.Series(dtype='float64')), errors="coerce"),
        "responded":     df11.get("y", pd.Series(dtype='object')).map({"yes": 1, "no": 0}),
    })

    df = pd.concat([df10_slim, df11_slim], ignore_index=True)
    df = df.dropna(subset=["age","income"])
    df = df.reset_index(drop=True)
    print(f"  Training data shape: {df.shape}")

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)

    synthesizer = CTGANSynthesizer(metadata, epochs=100, verbose=True)
    synthesizer.fit(df)

    save_model(synthesizer, MODEL_NAME)

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — model_supply_chain.pkl
# MAGIC **Source**: DS-5 DataCo Supply Chain
# MAGIC **Used by**: 03c_load_adls_bulk.py to generate /landing/shipments/ + /landing/inventory/ files

# COMMAND ----------

from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

MODEL_NAME = "model_supply_chain.pkl"

if not check_already_trained(MODEL_NAME):
    print(f"\nTraining {MODEL_NAME} ...")

    df = load_csv("ds5_dataco", "DataCoSupplyChainDataset.csv", nrows=50000)

    # Select supply chain relevant columns
    sc_cols = ["Days for shipping (real)","Days for shipment (scheduled)",
               "Benefit per order","Sales per customer","Delivery Status",
               "Late_delivery_risk","Category Name","Customer Segment",
               "Department Name","Market","Order Region","Order Status",
               "shipping date (DateOrders)","order date (DateOrders)"]
    available = [c for c in sc_cols if c in df.columns]
    if not available:
        available = df.columns.tolist()[:10]
    df = df[available].copy()

    df = df.dropna(how="all")
    df = df.reset_index(drop=True)
    print(f"  Training data shape: {df.shape}")
    print(f"  Columns used: {list(df.columns)}")

    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(df)

    synthesizer = CTGANSynthesizer(metadata, epochs=100, verbose=True)
    synthesizer.fit(df)

    save_model(synthesizer, MODEL_NAME)

print(f"\n{MODEL_NAME} ready ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Final Verification

# COMMAND ----------

import os

MODEL_BASE = "/Volumes/bharatmart/ctgan_models/models"

REQUIRED_MODELS = [
    "model_transactions.pkl",
    "model_products.pkl",
    "model_reviews.pkl",
    "model_campaign_channel.pkl",
    "model_campaign_response.pkl",
    "model_supply_chain.pkl",
]

print("=" * 65)
print("CTGAN MODELS — FINAL VERIFICATION")
print("=" * 65)
print(f"  Volume: {MODEL_BASE}")
print()

all_ok = True
total_mb = 0
for model_name in REQUIRED_MODELS:
    path = os.path.join(MODEL_BASE, model_name)
    if os.path.exists(path):
        size_mb = os.path.getsize(path) / (1024**2)
        total_mb += size_mb
        print(f"  ✅  {model_name:<40s}  {size_mb:.1f} MB")
    else:
        print(f"  ❌  {model_name} — NOT FOUND")
        all_ok = False

print()
print(f"  Total model storage: {total_mb:.1f} MB")
print()
if all_ok:
    print("=" * 65)
    print("  ALL 6 MODELS TRAINED ✅")
    print()
    print("  Next steps:")
    print("  1. Run cleanup cell in 01_kaggle_downloader.py")
    print("     (delete raw Kaggle data from raw_kaggle Volume)")
    print("  2. Run 03a_load_azure_sql.py  → populate Azure SQL")
    print("  3. Run 03b_event_producer.py  → start Event Hub stream")
    print("  4. Run 03c_load_adls_bulk.py  → drop files to ADLS landing/")
    print("  5. Start pipeline job (Phases 2-7)")
    print("=" * 65)
else:
    print("=" * 65)
    print("  SOME MODELS MISSING ❌  Re-run the failed step above")
    print("=" * 65)
