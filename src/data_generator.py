import random
import uuid
from datetime import datetime, timedelta, timezone

CARD_IDS = [f"card_{i:04d}" for i in range(1, 51)]
CARD_TYPES = ["physical", "digital"]
CARD_BRANDS = ["Visa", "Mastercard", "Elo"]
MERCHANT_CATEGORIES = ["electronics", "grocery", "restaurant", "travel", "clothing", "fuel", "pharmacy"]
LOCATIONS = [
    (-23.5505, -46.6333),  # São Paulo
    (-22.9068, -43.1729),  # Rio de Janeiro
    (-19.9167, -43.9345),  # Belo Horizonte
    (-25.4284, -49.2733),  # Curitiba
    (-3.7172, -38.5433),   # Fortaleza
]

_card_home_locations = {card_id: random.choice(LOCATIONS) for card_id in CARD_IDS}


def generate_transaction(card_id=None, timestamp=None, is_fraud=False):
    if card_id is None:
        card_id = random.choice(CARD_IDS)
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    if is_fraud:
        base_location = random.choice(LOCATIONS)
        amount = round(random.uniform(500, 10000), 2)
        lat_offset = random.uniform(-20, 20)
        lon_offset = random.uniform(-20, 20)
    else:
        base_location = _card_home_locations[card_id]
        amount = round(random.uniform(10, 500), 2)
        lat_offset = random.uniform(-0.05, 0.05)
        lon_offset = random.uniform(-0.05, 0.05)

    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "card_type": random.choice(CARD_TYPES),
        "card_brand": random.choice(CARD_BRANDS),
        "amount": amount,
        "timestamp": timestamp.isoformat(),
        "latitude": round(base_location[0] + lat_offset, 6),
        "longitude": round(base_location[1] + lon_offset, 6),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "is_fraud": is_fraud,
    }


def generate_historical_transactions(count=10000):
    transactions = []
    now = datetime.now(timezone.utc)
    for i in range(count):
        ts = now - timedelta(seconds=random.randint(0, 86400 * 7))
        is_fraud = random.random() < 0.10
        tx = generate_transaction(timestamp=ts, is_fraud=is_fraud)
        transactions.append(tx)
    return transactions