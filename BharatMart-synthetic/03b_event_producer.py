# Databricks notebook source
# ==============================================================
# BharatMart 360° — 03b_event_producer.py
# Phase 1: Continuous Event Hub streaming producer
# ==============================================================
# Environment  : Databricks Premium (14-day trial)
# Connections  : azure-eventhub SDK → Event Hub (pure Python)
#                spark.read.jdbc()   → load master IDs from Azure SQL
# Run          : Continuous loop — run as a Job OR active notebook cell
#                Stop with: Interrupt Kernel / Job cancel
# Topics       : orders, cart-events, sessions,
#                payments, campaign-responses
# Event rate   : 1 order every sleep_seconds (default 2s)
# Dirty Types  : T1 (NULL), T2 (retry dupe), T3 (string timestamp),
#                T4 (zero amount), T7 (unknown FK customer_id)
# ==============================================================

# COMMAND ----------

# MAGIC %pip install azure-eventhub
# MAGIC # azure-eventhub = pure Python AMQP client, no system deps

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData

random.seed(None)   # True random for realistic stream
print("✅ Libraries loaded")

# COMMAND ----------

# ── WIDGETS ──────────────────────────────────────────────────
dbutils.widgets.text("eh_connection_string", "",
    "Event Hub connection string (Send policy)")
dbutils.widgets.text("eh_namespace", "bharatmart-eh",
    "Event Hub namespace ")
dbutils.widgets.text("sql_server",   "bahartmartsql.database.windows.net", "Azure SQL Server")
dbutils.widgets.text("sql_database", "bharatmart_db", "SQL Database")
dbutils.widgets.text("sql_username", "veer", "SQL Username")
dbutils.widgets.text("sql_password", "", "SQL Password")
dbutils.widgets.text("max_orders",   "0",
    "Max orders to send (0 = infinite loop)")
dbutils.widgets.text("sleep_seconds","2",
    "Seconds between order events")

EH_CONN_STR  = dbutils.widgets.get("eh_connection_string")
EH_NAMESPACE = dbutils.widgets.get("eh_namespace")
SQL_SERVER   = dbutils.widgets.get("sql_server")
SQL_DATABASE = dbutils.widgets.get("sql_database")
SQL_USERNAME = dbutils.widgets.get("sql_username")
SQL_PASSWORD = dbutils.widgets.get("sql_password")
MAX_ORDERS   = int(dbutils.widgets.get("max_orders"))
SLEEP_SEC    = float(dbutils.widgets.get("sleep_seconds"))

# Event Hub topic names — must match exactly what you created in Azure portal
TOPICS = {
    "orders":    "orders",
    "cart":      "cart-events",
    "sessions":  "sessions",
    "payments":  "payments",
    "campaigns": "campaign-responses",
}

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

print(f"✅ Config — EH: {EH_NAMESPACE}")
print(f"   Topics: {list(TOPICS.values())}")
print(f"   Rate: 1 order every {SLEEP_SEC}s | Max: {'∞' if MAX_ORDERS==0 else MAX_ORDERS}")

# COMMAND ----------

# ── LOAD MASTER IDs FROM AZURE SQL ───────────────────────────
# spark.read.jdbc() — JDBC is the only supported method in Databricks.
# .collect() brings IDs into driver memory as a Python list.

print("Loading master IDs from Azure SQL via JDBC...")

def load_ids(table, id_col, limit=None):
    q = f"(SELECT TOP {limit} {id_col} FROM dbo.{table} WHERE is_active=1) t" \
        if limit else \
        f"(SELECT {id_col} FROM dbo.{table} WHERE is_active=1) t"
    rows = spark.read.jdbc(
        url=JDBC_URL,
        table=q,
        properties=JDBC_PROPS
    ).collect()
    return [r[0] for r in rows]

CUSTOMER_IDS  = load_ids("customers", "customer_id", limit=80000)
PRODUCT_IDS   = load_ids("products",  "product_id",  limit=40000)
SELLER_IDS    = load_ids("sellers",   "seller_id")
CATEGORY_IDS  = load_ids("categories","category_id")
WAREHOUSE_IDS = load_ids("warehouses","warehouse_id")

print(f"  ✅ customers : {len(CUSTOMER_IDS):,}")
print(f"  ✅ products  : {len(PRODUCT_IDS):,}")
print(f"  ✅ sellers   : {len(SELLER_IDS):,}")
print(f"  ✅ categories: {len(CATEGORY_IDS):,}")
print(f"  ✅ warehouses: {len(WAREHOUSE_IDS):,}")

# ── NEW customer ID pool (Type 7 dirty — unknown FK) ─────────
# These IDs are NOT in Azure SQL → triggers new_entity_detector in Silver
NEW_CUST_IDS = [f"CUST9{i:06d}" for i in range(1, 10001)]
print(f"  ✅ new_cust_pool: {len(NEW_CUST_IDS):,} (for T7 FK violations)")

# COMMAND ----------

# ── DIRTY DATA CONTROLLER ────────────────────────────────────

class StreamDirtyController:
    """
    Controls dirty data injection into streaming events.
    Tracks exact counts for Silver layer verification.
    """
    def __init__(self):
        self.injected = {
            "T1_null_pincode": 0,
            "T2_retry_dupe":   0,
            "T3_str_timestamp":0,
            "T4_zero_amount":  0,
            "T7_unknown_fk":   0,
        }
        self.total_events = 0

    def should_inject(self, pct):
        return random.random() < pct

    def maybe_null_pincode(self, pincode):
        if self.should_inject(0.05):   # 5% NULL pincode
            self.injected["T1_null_pincode"] += 1
            return None
        return pincode

    def maybe_zero_amount(self, amount):
        if self.should_inject(0.005):  # 0.5% zero amount
            self.injected["T4_zero_amount"] += 1
            return 0.0
        return amount

    def format_timestamp(self, dt):
        """T3: ALL timestamps stored as plain string, not datetime object."""
        self.injected["T3_str_timestamp"] += 1
        return dt.strftime("%Y-%m-%d %H:%M:%S")  # string, not ISO/timestamp type

    def maybe_unknown_customer(self):
        """T7: 3% of orders use a customer_id not in Azure SQL."""
        if self.should_inject(0.03):
            self.injected["T7_unknown_fk"] += 1
            return random.choice(NEW_CUST_IDS)
        return random.choice(CUSTOMER_IDS)

    def print_stats(self, order_count):
        print(f"\n  --- Dirty stats after {order_count:,} orders ---")
        for k, v in self.injected.items():
            pct = (v / max(order_count,1)) * 100
            print(f"    {k:<28} {v:>6,}  ({pct:.1f}%)")

DC = StreamDirtyController()
print("✅ StreamDirtyController ready")

# COMMAND ----------

# ── REFERENCE DISTRIBUTIONS ──────────────────────────────────

PAYMENT_METHODS  = ["UPI","credit_card","debit_card","net_banking","COD","wallet"]
PAYMENT_WEIGHTS  = [45,20,15,8,8,4]

ORDER_STATUSES   = ["placed","confirmed","processing","shipped","delivered","cancelled"]
ORDER_WEIGHTS    = [10,25,20,25,15,5]

CHANNELS         = ["app_android","app_ios","web_mobile","web_desktop"]
CHANNEL_WEIGHTS  = [40,25,25,10]

CAMPAIGN_TYPES   = ["email","sms","push","whatsapp"]
CAMPAIGN_WEIGHTS = [35,30,25,10]

CAMPAIGN_ACTIONS = ["sent","delivered","opened","clicked","converted","unsubscribed"]

CART_ACTIONS     = ["add","remove","update_qty","move_to_wishlist","save_for_later"]
CART_WEIGHTS     = [60,15,12,8,5]

DEVICE_TYPES     = ["android","ios","desktop","tablet"]
DEVICE_WEIGHTS   = [50,30,15,5]

COUPON_CODES     = [
    "SAVE10","FLAT200","FIRST50","FESTIVE20","BHARAT15",
    None,None,None,None,None,None,None,None  # 72% no coupon
]

# Active sessions in memory: session_id → {customer_id, started_at, pages, channel}
active_sessions = {}

# COMMAND ----------

# ── EVENT BUILDERS ───────────────────────────────────────────

def new_session_id():
    return str(uuid.uuid4())

def new_order_id():
    return f"ORD{int(time.time()*1000)}{random.randint(100,999)}"

def build_session_start(customer_id, channel, now):
    session_id = new_session_id()
    active_sessions[session_id] = {
        "customer_id": customer_id,
        "started_at":  now,
        "pages":       1,
        "channel":     channel,
    }
    return session_id, {
        "session_id":    session_id,
        "customer_id":   customer_id,
        "event_type":    "session_start",
        "channel":       channel,
        "device_type":   random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0],
        "pages_viewed":  1,
        "session_start": DC.format_timestamp(now),   # T3: always a string
        "event_time":    DC.format_timestamp(now),
    }

def build_cart_event(session_id, customer_id, product_id, channel, now):
    qty    = random.randint(1, 5)
    price  = round(random.uniform(99, 49999), 2)
    amount = DC.maybe_zero_amount(qty * price)
    return {
        "event_id":      str(uuid.uuid4()),
        "session_id":    session_id,
        "customer_id":   customer_id,
        "product_id":    product_id,
        "action":        random.choices(CART_ACTIONS, weights=CART_WEIGHTS)[0],
        "quantity":      qty,
        "unit_price":    price,
        "total_amount":  amount,
        "channel":       channel,
        "event_time":    DC.format_timestamp(now),   # T3
    }

def build_order(order_id, session_id, customer_id, channel, now):
    num_items = random.choices([1,2,3,4,5], weights=[45,30,13,8,4])[0]
    items = []
    subtotal = 0.0
    for _ in range(num_items):
        pid   = random.choice(PRODUCT_IDS)
        qty   = random.randint(1, 4)
        price = round(random.uniform(99, 49999), 2)
        items.append({
            "product_id":  pid,
            "seller_id":   random.choice(SELLER_IDS),
            "quantity":    qty,
            "unit_price":  price,
            "line_total":  round(qty * price, 2),
        })
        subtotal += qty * price

    subtotal = round(subtotal, 2)
    coupon   = random.choice(COUPON_CODES)
    discount = round(subtotal * random.uniform(0.05,0.25),2) if coupon else 0.0
    delivery = 0.0 if subtotal > 499 else 49.0
    total    = DC.maybe_zero_amount(round(subtotal - discount + delivery, 2))  # T4

    pincode      = str(random.randint(110000,999999))
    pincode      = DC.maybe_null_pincode(pincode)      # T1

    deliver_days = random.randint(2, 7)
    est_delivery = DC.format_timestamp(now + timedelta(days=deliver_days))  # T3

    return {
        "order_id":           order_id,
        "session_id":         session_id,
        "customer_id":        customer_id,
        "order_items":        items,           # nested JSON array
        "order_status":       random.choices(ORDER_STATUSES, weights=ORDER_WEIGHTS)[0],
        "channel":            channel,
        "subtotal":           subtotal,
        "discount_amount":    discount,
        "delivery_charge":    delivery,
        "total_amount":       total,
        "coupon_code":        coupon,
        "delivery_pincode":   pincode,
        "warehouse_id":       random.choice(WAREHOUSE_IDS),
        "estimated_delivery": est_delivery,
        "order_timestamp":    DC.format_timestamp(now),   # T3
    }

def build_payment(order_id, customer_id, amount, now):
    method  = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
    success = random.choices([True,False], weights=[95,5])[0] # calibration: 95pct success)
    return {
        "payment_id":     f"PAY{str(uuid.uuid4())[:8].upper()}",
        "order_id":       order_id,
        "customer_id":    customer_id,
        "payment_method": method,
        "amount":         DC.maybe_zero_amount(amount),  # T4
        "status":         "success" if success else random.choice(["failed","pending"]),
        "upi_id":         f"{fake_upi()}@upi" if method=="UPI" else None,
        "payment_time":   DC.format_timestamp(now),      # T3
    }

def fake_upi():
    parts = ["user","pay","bharat","india"]
    return f"{random.choice(parts)}{random.randint(100,9999)}"

def build_campaign_response(customer_id, now):
    c_type  = random.choices(CAMPAIGN_TYPES, weights=CAMPAIGN_WEIGHTS)[0]
    action  = random.choice(CAMPAIGN_ACTIONS)
    return {
        "response_id":    str(uuid.uuid4()),
        "campaign_id":    f"CAMP{random.randint(1,500):04d}",
        "customer_id":    customer_id,
        "campaign_type":  c_type,
        "action":         action,
        "category_id":    random.choice(CATEGORY_IDS),
        "event_time":     DC.format_timestamp(now),   # T3
    }

# COMMAND ----------

# ── SEND HELPERS ─────────────────────────────────────────────

def send_event(producer_client, topic_name, payload: dict):
    """Send a single JSON event to a specific Event Hub topic."""
    batch = producer_client.create_batch(partition_key=str(random.randint(0,3)))
    batch.add(EventData(json.dumps(payload, default=str)))
    producer_client.send_batch(batch)

def send_retry_dupe(producer_client, topic_name, payload: dict):
    """T2: Send the same event twice to simulate producer retry."""
    DC.injected["T2_retry_dupe"] += 1
    send_event(producer_client, topic_name, payload)
    time.sleep(0.05)
    send_event(producer_client, topic_name, payload)

# COMMAND ----------

# ── MAIN PRODUCER LOOP ───────────────────────────────────────
# Each iteration:
#   1. Pick a customer (3% chance → unknown FK for T7)
#   2. Start/reuse a session
#   3. Generate 1-3 cart events
#   4. Generate 1 order (with nested items)
#   5. Generate 1 payment linked to order
#   6. ~50% chance → generate 1 campaign response
#   7. Sleep SLEEP_SEC seconds

print("\n" + "="*55)
print("  BharatMart Event Hub Producer — STARTING")
print("="*55)
print(f"  Topics  : {list(TOPICS.values())}")
print(f"  Rate    : 1 order / {SLEEP_SEC}s")
print(f"  Max     : {'∞' if MAX_ORDERS==0 else MAX_ORDERS} orders")
print("  Stop    : Interrupt Kernel or cancel the Job")
print("="*55 + "\n")

order_count = 0
total_events_sent = {t:0 for t in TOPICS}

try:
    # One producer per topic
    producers = {
        name: EventHubProducerClient.from_connection_string(
            conn_str=EH_CONN_STR,
            eventhub_name=topic
        )
        for name, topic in TOPICS.items()
    }

    while True:
        if MAX_ORDERS > 0 and order_count >= MAX_ORDERS:
            break

        now          = datetime.utcnow()
        customer_id  = DC.maybe_unknown_customer()   # T7: 3% unknown FK
        channel      = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

        # ── Session ───────────────────────────────────────────
        # Reuse existing session (70%) or start new one (30%)
        if active_sessions and random.random() < 0.70:
            session_id = random.choice(list(active_sessions.keys()))
        else:
            session_id, sess_evt = build_session_start(customer_id, channel, now)
            send_event(producers["sessions"], TOPICS["sessions"], sess_evt)
            total_events_sent["sessions"] += 1

        # Expire old sessions (>30 min)
        expired = [sid for sid,s in active_sessions.items()
                   if (now - s["started_at"]).total_seconds() > 1800]
        for sid in expired:
            del active_sessions[sid]

        # ── Cart events (1-3 per order) ───────────────────────
        num_cart = random.randint(1, 3)
        for _ in range(num_cart):
            cart_evt = build_cart_event(
                session_id, customer_id,
                random.choice(PRODUCT_IDS), channel, now
            )
            send_event(producers["cart"], TOPICS["cart"], cart_evt)
            total_events_sent["cart"] += 1

        # ── Order ─────────────────────────────────────────────
        order_id  = new_order_id()
        order_evt = build_order(order_id, session_id, customer_id, channel, now)

        is_retry_dupe = random.random() < 0.02   # T2: 2% retry dupe
        if is_retry_dupe:
            send_retry_dupe(producers["orders"], TOPICS["orders"], order_evt)
            total_events_sent["orders"] += 2
        else:
            send_event(producers["orders"], TOPICS["orders"], order_evt)
            total_events_sent["orders"] += 1

        # ── Payment ───────────────────────────────────────────
        pay_evt = build_payment(
            order_id, customer_id,
            order_evt["total_amount"], now
        )
        send_event(producers["payments"], TOPICS["payments"], pay_evt)
        total_events_sent["payments"] += 1

        # ── Campaign response (~50% of orders) ───────────────
        if random.random() < 0.50:
            camp_evt = build_campaign_response(customer_id, now)
            send_event(producers["campaigns"], TOPICS["campaigns"], camp_evt)
            total_events_sent["campaigns"] += 1

        order_count += 1

        # ── Progress print every 50 orders ───────────────────
        if order_count % 50 == 0:
            print(f"  [{datetime.utcnow().strftime('%H:%M:%S')}] "
                  f"Orders: {order_count:,} | "
                  f"Events sent: {sum(total_events_sent.values()):,}")

        # ── Dirty stats every 500 orders ─────────────────────
        if order_count % 500 == 0:
            DC.print_stats(order_count)

        time.sleep(SLEEP_SEC)

except KeyboardInterrupt:
    print("\n⚠️  Producer stopped by user (KeyboardInterrupt)")

finally:
    print("\nClosing Event Hub producers...")
    for name, prod in producers.items():
        try:
            prod.close()
        except:
            pass

    # ── Final summary ─────────────────────────────────────────
    print("\n" + "="*55)
    print("  FINAL SUMMARY")
    print("="*55)
    print(f"  Total orders produced   : {order_count:,}")
    print(f"  Total events sent       : {sum(total_events_sent.values()):,}")
    print("\n  Events by topic:")
    for name, count in total_events_sent.items():
        print(f"    {TOPICS[name]:<30} {count:>8,}")
    print("\n  Dirty data injected:")
    for k, v in DC.injected.items():
        print(f"    {k:<30} {v:>8,}")
    print("="*55)
    print("\n✅ 03b_event_producer.py COMPLETE")
    print("   Bronze streaming pipeline (05) will pick up all events.")

# COMMAND ----------

# MAGIC %md
# MAGIC # ── COST REMINDER ────────────────────────────────────────────
# MAGIC %md
# MAGIC ## ⚠️ Cost Control
# MAGIC - Event Hub **Standard tier** = ~₹800/month if running 24/7
# MAGIC - **Recommended:** Run in **1-2 hour bursts** to accumulate test data
# MAGIC - Stop this cell / cancel the Job when not testing
# MAGIC - The Bronze pipeline processes ALL accumulated events in each run

# COMMAND ----------


