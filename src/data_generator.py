import random
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker

fake = Faker('pt_BR')

CARD_IDS = [fake.credit_card_number() for i in range(50)]
CARD_TYPES = ["physical", "digital"]
CARD_BRANDS = ["Visa", "Mastercard", "Elo"]
MERCHANT_CATEGORIES = ["electronics", "grocery", "restaurant", "travel", "clothing", "fuel", "pharmacy"]
LOCATIONS = [tuple(map(float, fake.location_on_land(coords_only = True))) for i in range(50)]

_card_home_locations = {card_id: random.choice(LOCATIONS) for card_id in CARD_IDS}


def generate_transaction(card_id=None, timestamp=None, is_fraud=False):
    if card_id is None:
        card_id = random.choice(CARD_IDS)
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    if is_fraud:
        in_base_loc = random.random()
        if in_base_loc < 0.2:
            base_location = _card_home_locations[card_id]
            amount = round(random.uniform(2000, 10000), 2)
            lat_offset = random.uniform(-0.05, 0.05)
            lon_offset = random.uniform(-0.05, 0.05) 
        elif in_base_loc < 0.6:
            base_location = random.choice(LOCATIONS)
            amount = round(random.uniform(10, 2000), 2)
            lat_offset = random.uniform(-20, 20)
            lon_offset = random.uniform(-20, 20)
        else:
            base_location = random.choice(LOCATIONS)
            amount = round(random.uniform(500, 10000), 2)
            lat_offset = random.uniform(-20, 20)
            lon_offset = random.uniform(-20, 20)
    else:
        base_location = _card_home_locations[card_id]
        amount = round(random.uniform(10, 2000), 2)
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