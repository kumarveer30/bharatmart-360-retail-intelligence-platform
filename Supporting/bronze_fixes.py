# ================================================================
# BRONZE PIPELINE — 3 CELL REPLACEMENTS
# ================================================================
# REPLACE Cell 20 → FIX 1  (brz_sessions — dual source)
# REPLACE Cell 22 → FIX 2  (brz_cart_events — dual source)
# REPLACE Cell 24 → FIX 3  (brz_campaign_responses — dual source)
#
# Also update Cell 38 summary markdown — brz_sessions,
# brz_cart_events, brz_campaign_responses change from
# "Event Hub only" to "ADLS + Event Hub (append_flow)"
# ================================================================


# ================================================================
# FIX 1 — REPLACE Cell 20
# Markdown header: ### brz_sessions
# ================================================================

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_sessions",
    comment="Unified sessions — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical sessions (2020–2026)
@dp.append_flow(target="brz_sessions", name="flow_sessions_adls")
def sessions_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/sessions/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live sessions
session_schema = StructType([
    StructField("session_id",    StringType()),
    StructField("customer_id",   StringType()),
    StructField("event_type",    StringType()),
    StructField("channel",       StringType()),
    StructField("device_type",   StringType()),
    StructField("pages_viewed",  IntegerType()),
    StructField("session_start", StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_sessions", name="flow_sessions_eh")
def sessions_from_eventhub():
    opts = {**kafka_options, "subscribe": "sessions"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), session_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )


# ================================================================
# FIX 2 — REPLACE Cell 22
# Markdown header: ### brz_cart_events
# ================================================================

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_cart_events",
    comment="Unified cart events — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical cart events (2020–2026)
@dp.append_flow(target="brz_cart_events", name="flow_cart_adls")
def cart_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/cart_events/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live cart events
cart_schema = StructType([
    StructField("event_id",      StringType()),
    StructField("session_id",    StringType()),
    StructField("customer_id",   StringType()),
    StructField("product_id",    StringType()),
    StructField("action",        StringType()),
    StructField("quantity",      IntegerType()),
    StructField("unit_price",    DoubleType()),
    StructField("total_amount",  DoubleType()),
    StructField("channel",       StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_cart_events", name="flow_cart_eh")
def cart_from_eventhub():
    opts = {**kafka_options, "subscribe": "cart-events"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), cart_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )


# ================================================================
# FIX 3 — REPLACE Cell 24
# Markdown header: ### brz_campaign_responses
# ================================================================

# Step 1: empty destination table
dp.create_streaming_table(
    name="brz_campaign_responses",
    comment="Unified campaign responses — historical (ADLS CSV) + live (Event Hub JSON)",
    table_properties={"quality": "bronze"}
)

# Step 2: ADLS flow — 03c historical campaign responses (2020–2026)
@dp.append_flow(target="brz_campaign_responses", name="flow_campaign_adls")
def campaign_from_adls():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{ADLS_LANDING}/campaign_responses/")
        .withColumn("_source", lit("adls_historical"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# Step 3: Event Hub flow — 03b live campaign responses
campaign_schema = StructType([
    StructField("response_id",   StringType()),
    StructField("campaign_id",   StringType()),
    StructField("customer_id",   StringType()),
    StructField("campaign_type", StringType()),
    StructField("action",        StringType()),
    StructField("category_id",   StringType()),
    StructField("event_time",    StringType()),
])

@dp.append_flow(target="brz_campaign_responses", name="flow_campaign_eh")
def campaign_from_eventhub():
    opts = {**kafka_options, "subscribe": "campaign-responses"}
    return (spark.readStream
        .format("kafka")
        .options(**opts)
        .load()
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("data", from_json(col("value_str"), campaign_schema))
        .select("data.*")
        .withColumn("_source", lit("eventhub_live"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit("event_hub"))
    )
