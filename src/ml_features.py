from datetime import datetime, timedelta, timezone
from haversine import haversine

MERCHANT_CATEGORIES = ["electronics", "grocery", "restaurant", "travel", "clothing", "fuel", "pharmacy"]
CARD_BRAND_MAPPING = {"Visa": 0, "Mastercard": 1, "Elo": 2}
UNKNOWN_ENCODING = -1


def calculate_distance(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2))


def encode_card_type(card_type):
    return 0 if card_type == "physical" else 1


def encode_card_brand(card_brand):
    if card_brand not in CARD_BRAND_MAPPING:
        return UNKNOWN_ENCODING
    return CARD_BRAND_MAPPING[card_brand]


def encode_merchant_category(category):
    if category not in MERCHANT_CATEGORIES:
        return UNKNOWN_ENCODING
    return MERCHANT_CATEGORIES.index(category)


def parse_timestamp(ts_str):
    if isinstance(ts_str, datetime):
        return ts_str
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def calculate_features(tx, history):
    amount = tx["amount"]
    tx_time = parse_timestamp(tx["timestamp"])
    two_min_ago = tx_time - timedelta(minutes=2)
    ten_min_ago = tx_time - timedelta(minutes=10)

    recent_2min = [h for h in history if parse_timestamp(h["timestamp"]) >= two_min_ago]
    recent_10min = [h for h in history if parse_timestamp(h["timestamp"]) >= ten_min_ago]

    tx_count_2min = len(recent_2min)

    if recent_10min:
        avg_amount_10min = sum(h["amount"] for h in recent_10min) / len(recent_10min)
    else:
        avg_amount_10min = 0.0

    if history:
        last = max(history, key=lambda h: parse_timestamp(h["timestamp"]))
        distance_from_last_km = calculate_distance(
            tx["latitude"], tx["longitude"],
            last["latitude"], last["longitude"]
        )
        time_since_last_sec = (tx_time - parse_timestamp(last["timestamp"])).total_seconds()
    else:
        distance_from_last_km = 0.0
        time_since_last_sec = 0.0

    return [
        amount,
        tx_count_2min,
        avg_amount_10min,
        distance_from_last_km,
        time_since_last_sec,
        encode_merchant_category(tx.get("merchant_category", "")),
        encode_card_type(tx.get("card_type", "physical")),
        encode_card_brand(tx.get("card_brand", "Visa")),
    ]
