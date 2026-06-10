import pytest
from src.ml_features import (
    calculate_distance,
    encode_card_type,
    encode_card_brand,
    encode_merchant_category,
    calculate_features,
)


def test_calculate_distance_same_point():
    assert calculate_distance(0, 0, 0, 0) == 0.0


def test_calculate_distance_known_value():
    # São Paulo to Rio de Janeiro ~360 km
    dist = calculate_distance(-23.5505, -46.6333, -22.9068, -43.1729)
    assert 300 < dist < 400


def test_encode_card_type():
    assert encode_card_type("physical") == 0
    assert encode_card_type("digital") == 1


def test_encode_card_brand():
    assert encode_card_brand("Visa") == 0
    assert encode_card_brand("Mastercard") == 1
    assert encode_card_brand("Elo") == 2


def test_encode_merchant_category():
    enc = encode_merchant_category("electronics")
    assert isinstance(enc, int)
    assert enc >= 0


def test_calculate_features_returns_8_values():
    tx = {
        "amount": 100.0,
        "latitude": -23.55,
        "longitude": -46.63,
        "merchant_category": "electronics",
        "card_type": "physical",
        "card_brand": "Visa",
        "timestamp": "2026-06-09T14:30:00Z",
    }
    history = []
    features = calculate_features(tx, history)
    assert len(features) == 8


def test_calculate_features_with_history():
    tx = {
        "amount": 200.0,
        "latitude": -23.55,
        "longitude": -46.63,
        "merchant_category": "grocery",
        "card_type": "digital",
        "card_brand": "Elo",
        "timestamp": "2026-06-09T14:30:00Z",
    }
    history = [
        {"amount": 50.0, "latitude": -23.56, "longitude": -46.64, "timestamp": "2026-06-09T14:28:00Z"},
        {"amount": 75.0, "latitude": -23.57, "longitude": -46.65, "timestamp": "2026-06-09T14:29:00Z"},
    ]
    features = calculate_features(tx, history)
    assert features[1] == 2  # tx_count_2min
    assert features[2] > 0   # avg_amount_10min
