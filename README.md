# BharatMart 360°
### Retention Intelligence Pipeline · Databricks 14-Day AI Challenge · March 2026

> **Five published papers. One gap Springer confirmed in 2024. One end-to-end system that fills it.**

A Databricks pipeline that scores the full customer base every night and delivers a call list every morning. Each row tells the retention team who is likely to leave, what behavioral signal drove that prediction, what the customer complained about in their own words, and which three products to offer them. The pipeline is live and incremental — new customers, new orders, and new reviews flow in through real-time Event Hub streams and are scored automatically on every nightly run.

[![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-Premium-FF3621?style=flat&logo=databricks)](https://azure.microsoft.com/en-us/products/databricks)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion_Architecture-00ADD8?style=flat)](https://delta.io)
[![MLflow](https://img.shields.io/badge/MLflow-5_Registered_Models-0194E2?style=flat)](https://mlflow.org)
[![Mosaic AI](https://img.shields.io/badge/Mosaic_AI-ai__query()-8B5CF6?style=flat)](https://docs.databricks.com/en/large-language-models/ai-functions.html)

---

## Pipeline Walkthrough

[![Pipeline Demo](https://img.youtube.com/vi/lKUfQcDLOvI/0.jpg)](https://youtu.be/lKUfQcDLOvI?si=Y4WtfOM7CwDS_qDR)

> Click the thumbnail above to watch the technical pipeline walkthrough.

---

## The Problem

India's ecommerce market reached $147.3 billion in 2024. The growth numbers look strong. The retention numbers do not.

| Stat | Number | Source |
|------|--------|--------|
| Annual customer churn rate | 70–75% | Rivo / Upcounting, 2024–25 |
| Customers who buy only once | 75% | Naik et al., ResearchGate 2025 |
| Cost to acquire vs retain | 5–25× | HBR / Bain & Company |
| Revenue from repeat customers | 44% | Gorgias / Sobot, 2024–25 |

Three out of four customers who make a purchase never come back. Most platforms only detect this after the customer has already left. The 30 to 90 day window before a customer permanently stops buying — when an intervention is still possible — goes entirely undetected.

There are three specific gaps that most platforms do not address:

**Nobody knows who is about to leave.** Platforms track orders and revenue. They do not track which specific customers are silently disengaging — logging in less, abandoning carts more, ordering less frequently — before they disappear entirely.

**Every customer is treated the same.** A customer who has spent ₹69,000 across 16 orders gets the same homepage, the same discount code, and the same email campaign as someone who bought once for ₹200. These are not the same problem.

**Recommendations are generic.** Most platforms show the same bestseller list to everyone. A customer who buys running shoes and protein supplements sees the same "Trending Now" as someone buying children's textbooks.

Springer's *Electronic Commerce Research* (December 2024) confirmed there is no published system combining churn prediction, customer segmentation, and personalised recommendations in a single working pipeline for Indian ecommerce. BharatMart 360° builds one.

---

## The Output

The pipeline narrows the full customer base down to a short call list every morning.

```
92,107+  customers scored nightly (live base, grows as new customers register)
  1,139  Critical churn risk — probability above 0.75
    111  Urgent Champions — highest-value customers at Critical or High risk
     62  Call-ready — Urgent Champions who also left a specific complaint
```

Those 62 customers represent ₹2,06,085 in combined lifetime spend at risk. Recovering 25 percent of them — 16 customers — brings back approximately ₹51,522, which covers the full monthly infrastructure cost. A single retained Champions customer (average lifetime value ₹69,207) covers that cost on their own.

The call list is not just a list of names. Each row contains:
- **Who**: customer ID and segment (Champions, Loyal, or At Risk)
- **Why**: churn probability score from M1 and the top behavioral reason from SHAP analysis
- **What went wrong**: the specific complaint category extracted from their review text by M5
- **What to offer**: three personalised product recommendations from M4, ranked by value profile

---

## Architecture

Three Azure services feed raw data in. Data moves through three layers — Bronze (raw), Silver (cleaned and validated), Gold (model outputs and aggregations). Five ML models run on Silver and write to Gold. Results reach a live AIBI dashboard by 08:00 IST every morning.

![BharatMart Pipeline Architecture](Pipeline%20architecture.png)

**Stack:** Azure Databricks Premium · Azure SQL · ADLS Gen2 · Azure Event Hub · Delta Lake · Unity Catalog · LakeFlow · MLflow · Mosaic AI · Databricks AIBI · Genie Space

### Ingestion paths

Three separate ingestion patterns run in parallel because each data source has different latency requirements:

| Source | Pattern | What it carries |
|--------|---------|----------------|
| Azure Data Lake Storage | AutoLoader (batch) | Shipments, refunds, inventory, commissions |
| Azure Event Hub | Kafka streaming | Orders, sessions, cart events, campaign responses |
| Azure SQL | Change Data Capture | Customer, product, seller master data |

Bronze stores everything exactly as it arrives, including all data quality problems. Silver applies fixes. Gold runs the models and builds the dashboard tables. Every new customer who registers today is already in tomorrow's scoring run.

---

## The Data

72 months of synthetic Indian ecommerce data, January 2020 to December 2025, generated with CTGAN/SDV trained on real Kaggle datasets. The generation reflects documented Indian market patterns: Diwali shifting between October and November each year based on the actual Hindu calendar, the COVID lockdown demand dip in early 2020, UPI growing from 20% of transactions in 2020 to 72% by 2025, and gradual Tier-2 and Tier-3 city expansion.

| Table | Rows |
|-------|------|
| Customers | 92,107+ (grows nightly as new customers register) |
| Products | 50,000 |
| Orders | 1,621,695 |
| Sessions | 1,500,000+ |
| Reviews | 411,470 total, 369,230 scored |
| History | 72 months (Jan 2020 – Dec 2025) |

---

## Data Quality: 7 Problems Fixed in Silver

Seven categories of data quality problems are deliberately injected into the source layer to make the Silver cleaning testable against realistic conditions. Every fix is applied at Silver so a single correction propagates automatically to all 20 downstream Gold tables.

| Problem | Scale | Fix Applied |
|---------|-------|------------|
| Missing customer IDs | 23,552 rows | Flagged `_null_cust`, excluded from ML training |
| Duplicate rows | ~1% of records | Keep latest by `_ingested_at` timestamp |
| Mixed date formats (DD-MM-YYYY vs YYYY-MM-DD) | 175,372 rows | 6-format fallback COALESCE parse chain |
| Zero-amount orders | 9 rows | Flagged `_is_zero_amount`, kept for audit |
| Two column names for the same field across sources | All orders | `COALESCE(order_amount, subtotal)` |
| Currency mismatch in seller data | International rows | Multiply by `fx_rate` from seller table |
| Reviews and shipments with no matching order | 42,222 rows | Flagged `_is_ghost_order`, kept for sentiment scoring |

The date format problem is worth noting specifically. The string `05-03-2024` means March 5th under one assumption and May 3rd under another. This is not a cosmetic issue — it changes which orders fall in which reporting periods and affects every downstream calculation.

---

## Five Models

The five models run as a chain, not independently. M1 runs first. Each model's output feeds the next. A customer who does not appear in M1 with a high enough churn probability never reaches the call list regardless of what M2 through M5 find.

### M1: Churn Prediction (XGBoost)
> Matuszelanski & Kopczewska (2022), *JTAER*. DOI: [10.3390/jtaer17010009](https://doi.org/10.3390/jtaer17010009)

Trained on 92,107 customers using 12 behavioral features: cart abandonment rate, session depth, payment method preference, average order value, refund rate, session count, pages viewed, and others. No demographic features.

Early training returned AUC 0.9922, which flagged a data leakage problem immediately. The churn label is defined as no order in 90 days, and the number of days since last order was in the feature set. The model was simply reading the label back from the input rather than learning behavioral patterns. Removing both leaky features brought AUC to 0.6500, against the paper's benchmark of 0.6505 on Brazilian marketplace data.

SHAP analysis: `total_spent` has a SHAP value of 0.42. `avg_rating` has a SHAP value of 0.03 — a 14x difference. Customers who are about to leave stop spending before they go. They rarely complain in a review first. This pattern held on both the original Brazilian Olist dataset and BharatMart data, independently.

**Output:** `gld_churn_predictions` — one row per customer with `churn_probability`, `churn_risk_band`, and `top_churn_reason` in plain text.

---

### M2: Customer Segmentation (K-Means RFM)
> Wong, Tong & Haw (2024), *JTDE* Vol.12. DOI: [10.18080/jtde.v12n3.978](https://doi.org/10.18080/jtde.v12n3.978)

K=3 validated by three independent methods simultaneously: Elbow method, Silhouette score (0.4108 at K=3), and Calinski-Harabasz index (71,482.9 at K=3). All three methods must agree before the cluster count is accepted.

| Segment | Count | Avg lifetime spend | Avg recency |
|---------|-------|--------------------|-------------|
| Champions | 13,948 | ₹69,207 | 2 days ago |
| Loyal Customers | 46,439 | ₹2,209 | 52 days ago |
| At Risk | 31,720 | ₹565 | 66 days ago |

Champions spend 122 times more on average than At Risk customers. Combined with M1 churn probability, M2 produces a `retention_priority` column (Urgent / High / Medium / Low). Crossing M1 and M2 produces 111 Urgent Champions — the highest-value customers who are actively disengaging. Neither model can produce this group alone.

**Output:** `gld_rfm_segments` with `rfm_segment` and `retention_priority` per customer.

---

### M3: Product Recommendations (ALS Collaborative Filtering)
> Padhy et al. (2024), *MDPI Engineering Proceedings* Vol.67. DOI: [10.3390/engproc2024067050](https://doi.org/10.3390/engproc2024067050)

Recommends products based on behavioral signals rather than explicit ratings. Confidence weights: cart add = 0.5, confirmed purchase = 1.0, 4–5 star review = 2.0.

18 hyperparameter combinations tested across 3 CrossValidator folds (54 total ALS fits), directly addressing the reference paper's warning that ALS overfits without careful tuning.

- **9.5x better than random** on a 47,000-product catalog
- **97.1% customer coverage** — 87,772 customers with personalised lists; 2.9% receive popular-product fallbacks

Technical note: `recommendForAllUsers()` fails on Databricks Serverless because the output (284MB) exceeds the 268MB Spark Connect limit. The fix is numpy matrix multiplication (`U @ V.T`) in 1,000-user chunks. Scoring time: approximately 8 minutes for the full base.

**Output:** `gld_als_recommendations` — top 10 products per customer, 914,810 rows total.

---

### M4: Recommendation Re-Ranking (RFMQ Entropy Weights)
> Chen et al. (2025), *Frontiers in Big Data* Vol.8. DOI: [10.3389/fdata.2025.1680669](https://doi.org/10.3389/fdata.2025.1680669)

Takes M3's top-10 recommendations and re-orders them based on what each customer's value profile says they need right now. A Champion at Urgent risk sees high-margin products first. An At Risk customer sees familiar, lower-commitment products.

The entropy weights are calculated from the data automatically — no human input required.

| Dimension | Paper weight (outdoor sports) | BharatMart weight | Why different |
|-----------|-------------------------------|-------------------|---------------|
| R (Recency) | 0.079 | 0.003 | Most customers bought recently — very little spread |
| F (Frequency) | 0.390 | 0.174 | Moderate variation |
| M (Monetary) | 0.145 | **0.774** | Champions ₹69,207 vs At Risk ₹565 — 122x spread |
| Q (Quantity) | 0.386 | 0.049 | Basket sizes nearly identical across all segments |

India is a Monetary-first market. The entropy formula detected this automatically. The paper's dataset (UK outdoor sports retail) showed the opposite pattern because the spend spread there was much narrower.

**NDCG@10 improved +25.5%** over raw ALS output — the right product now appears at rank 1 significantly more often than before re-ranking.

**Output:** `gld_rfmq_recommendations` — same 10 products per customer, re-ordered by value profile.

---

### M5: Sentiment Analysis (Mosaic AI / LLaMA 3.3 70B)
> Roumeliotis et al. (2024), *Elsevier NLP Journal* Vol.6. DOI: [10.1016/j.nlp.2024.100056](https://doi.org/10.1016/j.nlp.2024.100056)

Runs `ai_query()` with `databricks-meta-llama-3-3-70b-instruct` against every review containing text. The prompt returns three structured outputs per review: sentiment label (positive / neutral / negative), a confidence score between 0 and 1, and an array of specific issue categories mentioned in the text — delivery delay, product mismatch, packaging damage, quality complaint, and so on.

**369,230 reviews scored. Zero parse failures.**

Two important findings:

**Hidden dissatisfaction:** 40,970 customers gave 4 or 5 stars but wrote something negative. Star ratings are structurally unreliable as satisfaction measures. Customers often click five stars because delivery was fast, then describe a product problem in the text. Rating-only analysis misses every one of these customers.

**Sentiment does not predict churn alone:** Negative reviews are distributed almost evenly across all churn risk bands — approximately 18% negative whether the customer is Critical, High, Medium, or Low risk. Unhappy customers and churning customers are not the same group. The signal is the combination: high churn probability from M1, plus high value from M2, plus a specific complaint from M5. None of the three conditions alone is enough.

**Initial historical run:** approximately 40 minutes for 369,230 reviews (one-time backfill, limited by `ai_query()` rate throttling on Free Edition). **Daily incremental scoring:** 5–10 minutes for new reviews only, appended to `gld_review_insights`.

**Output:** `gld_review_insights` — sentiment, confidence score, and `issues[]` array per review, joined to churn score and segment.

---

## Benchmark Results

One paper per model, selected before training began — not after seeing the results.

| Model | Reference Paper | Their Result | BharatMart | Notes |
|-------|----------------|-------------|------------|-------|
| M1 XGBoost | Matuszelanski & Kopczewska (2022) | AUC 0.6505 | **AUC 0.6500** | Gap of 0.0005 across two different countries |
| M2 K-Means | Wong, Tong & Haw (2024) | Silhouette 0.47 | **Silhouette 0.4108** | Dataset is 25x larger; lower score expected and documented |
| M3 ALS | Padhy et al. (2024) | RMSE 1.4485 (explicit) | **9.5x random baseline** | Different metric — implicit mode vs explicit ratings |
| M4 RFMQ | Chen et al. (2025) | F1 +81% at TOP-5 | **NDCG@10 +25.5%** | Confirmed; India Monetary-first finding documented |
| M5 Sentiment | Roumeliotis et al. (2024) | Near-zero parse failures | **0 failures / 369,230** | Production-scale validation |

---

## Dashboard

Five pages on Databricks AIBI, updated every night. No SQL required to use any of them.

| Page | What it shows |
|------|--------------|
| Retention Overview | Full funnel · current base · 111 Urgent Champions · risk distribution |
| Churn Risk | 1,139 Critical · top SHAP-derived reason per customer · risk band breakdown |
| Customer Value | Champions avg ₹2,04,620 lifetime · full segment heatmap by retention priority |
| Review Insights | 369,230 scored · 40,970 hidden dissatisfied · top complaint categories ranked |
| Action Center | 62 customers · avg churn probability 55.2% · ₹2,06,085 at risk · full call context per row |

**Genie Space** sits on top of all Gold tables for natural-language queries. Examples: *"Which customers in Delhi complained about delivery and haven't ordered in 30 days?"* or *"Show me all Champions at Critical risk who gave 5 stars but wrote a negative review."* No SQL needed.

---

## Production Setup

```
Training:    ML Personal Compute — once per month, not in nightly DAG
Scoring:     Serverless Compute — nightly at 2AM IST
DAG:         LakeFlow Jobs — Bronze → Silver → Gold → ML models in sequence
Registry:    MLflow @Production aliases (auto-loaded on each scoring run)
Pinning:     xgboost==2.0.3 · scikit-learn==1.3.2
Schema:      bharatmart.ml (all ML tables and registered models)
Dashboard:   AIBI publishes at 5AM · ready at 8AM
```

---

## Phase 2: Real-Time Alerts

The current system runs as a nightly batch. Phase 2 adds one specific change: when a high-value customer posts a negative review, the sentiment model fires immediately rather than waiting for the next 2AM batch.

```
Negative review arrives via Azure Event Hub (Kafka)
        ↓
M5 ai_query() — single call, completes in seconds
        ↓
Lookup: customer's current churn_probability (Gold table)
        ↓
Lookup: customer's segment and retention_priority (Gold table)
        ↓
If Champion + churn_probability above threshold → alert fires
        ↓
Retention team notified with full context + M4 recommendations
        ↓
Total: under 5 minutes
```

This is not a new project. Same architecture, same five models, same dashboard. The only addition is a Kafka consumer trigger configuration — no new models, no new tables, no retraining.

---

## Repository Structure

```
├── ML Models/
│   ├── ALS Reccomendation/
│   │   ├── ALS Collaborative Filtering - EDA.ipynb
│   │   ├── ALS Collaborative Filtering - Model Training.ipynb
│   │   └── Batch Scoring.ipynb
│   ├── Customer Churn Prediction/
│   │   ├── Customer Churn Prediction - EDA.ipynb
│   │   ├── Customer Churn Prediction - Feature Engineering.ipynb
│   │   ├── Customer Churn Prediction - Model Training.ipynb
│   │   ├── batch_scoring.ipynb
│   │   └── feature_engineering_prod.ipynb
│   ├── Mosaic Ai/
│   │   └── Review Sentiment Analysis.ipynb
│   ├── RFM Segmentation/
│   │   ├── Batch Scoring.ipynb
│   │   ├── FM Customer Segmentation - EDA.ipynb
│   │   ├── RFM Customer Segmentation - Clustering.ipynb
│   │   └── RFM Customer Segmentation - Model Training & Reg.ipynb
│   └── RFMQ/
│       ├── RFMQ Batch Scoring.ipynb
│       ├── RFMQ Model: Weight Matrix Build & Registration.ipynb
│       └── RFMQ Model: Weight Matrix Build & Registration (1).ipynb
├── Pipeline/
│   ├── bronze_exploration.py
│   ├── bronze_ingestion.py
│   ├── gold_layer.py
│   ├── silver_eda.py
│   ├── silver_transform.py
│   └── sql_to_bronze.py
├── BharatMart 360 Intelligence Dashboard.pdf
└── README.md
```

---

## References

| Paper | DOI |
|-------|-----|
| Matuszelanski & Kopczewska (2022). Customer Churn in Retail E-Commerce. *JTAER* | [10.3390/jtaer17010009](https://doi.org/10.3390/jtaer17010009) |
| Wong, Tong & Haw (2024). Customer Segmentation using RFM. *JTDE* Vol.12 | [10.18080/jtde.v12n3.978](https://doi.org/10.18080/jtde.v12n3.978) |
| Padhy et al. (2024). Recommendation System using ALS. *MDPI Eng. Proc.* Vol.67 | [10.3390/engproc2024067050](https://doi.org/10.3390/engproc2024067050) |
| Chen et al. (2025). Intelligent Recommendation + Customer Value Segmentation. *Frontiers Big Data* Vol.8 | [10.3389/fdata.2025.1680669](https://doi.org/10.3389/fdata.2025.1680669) |
| Roumeliotis et al. (2024). LLMs for Sentiment Analysis. *Elsevier NLP Journal* Vol.6 | [10.1016/j.nlp.2024.100056](https://doi.org/10.1016/j.nlp.2024.100056) |
| Jahan & Sanam (2024). A comprehensive framework for customer retention in e-commerce. *Electronic Commerce Research*, Springer | [10.1007/s10660-024-09936-0](https://doi.org/10.1007/s10660-024-09936-0) |
| Bain & Company / Flipkart (2025). How India Shops Online 2025 | [bain.com](https://www.bain.com/insights/how-india-shops-online-2025/) |

---

*BharatMart 360° · Databricks 14-Day AI Challenge · March 2026*
