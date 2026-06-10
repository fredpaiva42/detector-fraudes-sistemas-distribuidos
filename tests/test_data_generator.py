import pytest
from src.data_generator import generate_transaction, generate_historical_transactions


def test_generate_transaction_has_required_fields():
    tx = generate_transaction()
    required = {"transaction_id", "card_id", "card_type", "card_brand",
                "amount", "timestamp", "latitude", "longitude", "merchant_category"}
    assert required.issubset(tx.keys())


def test_generate_transaction_amount_positive():
    tx = generate_transaction()
    assert tx["amount"] > 0


def test_generate_transaction_valid_card_type():
    tx = generate_transaction()
    assert tx["card_type"] in ("physical", "digital")


def test_generate_transaction_valid_card_brand():
    tx = generate_transaction()
    assert tx["card_brand"] in ("Visa", "Mastercard", "Elo")


def test_generate_historical_has_fraud_labels():
    transactions = generate_historical_transactions(100)
    assert len(transactions) == 100
    has_fraud = any(tx.get("is_fraud") for tx in transactions)
    has_legit = any(not tx.get("is_fraud") for tx in transactions)
    assert has_fraud, "Should have some fraudulent transactions"
    assert has_legit, "Should have some legitimate transactions"


def test_fraud_ratio_around_10_percent():
    transactions = generate_historical_transactions(1000)
    fraud_count = sum(1 for tx in transactions if tx.get("is_fraud"))
    ratio = fraud_count / len(transactions)
    assert 0.05 < ratio < 0.20, f"Fraud ratio {ratio:.2%} outside expected range"
