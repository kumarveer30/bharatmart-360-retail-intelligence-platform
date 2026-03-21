# ================================================================
# 03c — Day-by-Day Replacement Cells
# ================================================================
# HOW TO APPLY (8 changes total):
#
#   REPLACE Cell 54  → CHANGE 1  (festival calendar)
#   REPLACE Cell 55  → CHANGE 2  (generate_day_orders)
#   NEW cell after Cell 80 → CHANGE 3  (derive_sessions)
#   NEW cell after CHANGE 3 → CHANGE 4  (derive_cart_events)
#   NEW cell after CHANGE 4 → CHANGE 5  (derive_campaign_responses)
#   REPLACE Cell 83  → CHANGE 6  (build days list)
#   REPLACE Cell 85  → CHANGE 7  (orders_for_day)
#   REPLACE Cell 87  → CHANGE 8  (main loop — day by day)
#
# ALSO: Delete or skip old test cells 57, 58, 59 — they call
#       generate_month_orders() which no longer exists after CHANGE 2.
#       They will error if run. Just skip them.
# ================================================================


# ================================================================
# CHANGE 1 — REPLACE Cell 54
# Markdown header: ## Festival calendar — exact dates per year
# ================================================================

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
        "diwali":          date(2026, 10, 30),
        "navratri_start":  date(2026, 10,  2),
        "dussehra":        date(2026, 10, 11),
        "dhamaka_start":   date(2026, 10, 18),
        "dhanteras":       date(2026, 10, 28),
        "holi":            date(2026,  3, 22),
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


# ================================================================
# CHANGE 2 — REPLACE Cell 55
# Markdown header: ## Day multiplier engine + generate_day_orders
# ================================================================

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


# ================================================================
# CHANGE 3 — NEW cell after Cell 80
# Markdown header: ## Derive sessions
# ================================================================

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


# ================================================================
# CHANGE 4 — NEW cell after CHANGE 3
# Markdown header: ## Derive cart events
# ================================================================

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


# ================================================================
# CHANGE 5 — NEW cell after CHANGE 4
# Markdown header: ## Derive campaign responses
# ================================================================

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


# ================================================================
# CHANGE 6 — REPLACE Cell 83
# Markdown header: ## Build days list
# ================================================================

days_to_process = []
cur = HISTORY_START
while cur <= HISTORY_END:
    days_to_process.append(cur)
    cur += timedelta(days=1)

print(f"days to process: {len(days_to_process)}")
print(f"from {days_to_process[0]} to {days_to_process[-1]}")


# ================================================================
# CHANGE 7 — REPLACE Cell 85
# Markdown header: ## Orders per day — CAGR + day multiplier
# ================================================================

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


# ================================================================
# CHANGE 8 — REPLACE Cell 87
# Markdown header: ## Run the main loop — day by day
# ================================================================

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
