import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from confluent_kafka import Consumer, Producer
import joblib
from src.kafka_config import KAFKA_BROKER, TOPIC_TRANSACTIONS, TOPIC_ALERTS
from src.ml_features import calculate_features

MODEL_PATH = os.environ.get("MODEL_PATH", "models/fraud_model.joblib")


def parse_timestamp(ts_str):
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def run_flink_processor():
    model = joblib.load(MODEL_PATH)
    tx_history = defaultdict(list)

    consumer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "flink-fraud-processor",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_TRANSACTIONS])

    producer_conf = {"bootstrap.servers": KAFKA_BROKER}
    producer = Producer(producer_conf)

    print(f"Processor iniciado. Consumindo de {TOPIC_TRANSACTIONS}, produzindo em {TOPIC_ALERTS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erro: {msg.error()}")
                continue

            try:
                tx = json.loads(msg.value().decode("utf-8"))
                card_id = tx["card_id"]
                now = parse_timestamp(tx["timestamp"])
                ten_min_ago = now - timedelta(minutes=10)

                history = [h for h in tx_history[card_id] if parse_timestamp(h["timestamp"]) >= ten_min_ago]
                history.append(tx)
                tx_history[card_id] = history

                features = calculate_features(tx, history[:-1])
                prediction = model.predict([features])[0]
                proba = model.predict_proba([features])[0]

                is_fraud = bool(prediction == 1 or proba[1] > 0.7)
                confidence = float(proba[1])

                reasons = []
                if tx.get("amount", 0) > 500:
                    reasons.append("high_amount")
                if len(history) > 1:
                    two_min_ago = now - timedelta(minutes=2)
                    recent = [h for h in history if parse_timestamp(h["timestamp"]) >= two_min_ago]
                    if len(recent) >= 3:
                        reasons.append("rapid_succession")
                if not reasons and is_fraud:
                    reasons.append("model_flagged")

                result = {
                    "transaction_id": tx["transaction_id"],
                    "card_id": card_id,
                    "amount": tx["amount"],
                    "timestamp": tx["timestamp"],
                    "card_type": tx.get("card_type", "unknown"),
                    "card_brand": tx.get("card_brand", "unknown"),
                    "is_fraud": is_fraud,
                    "confidence": round(confidence, 4),
                    "reasons": reasons,
                }

                producer.produce(
                    TOPIC_ALERTS,
                    key=card_id.encode("utf-8"),
                    value=json.dumps(result).encode("utf-8"),
                )
                producer.poll(0)

                if is_fraud:
                    print(f"[FRAUDE] Card: {card_id} | R$ {tx['amount']:.2f} | Confiança: {confidence:.1%} | {', '.join(reasons)}")
                else:
                    print(f"[OK] Card: {card_id} | R$ {tx['amount']:.2f}")

            except Exception as e:
                print(f"Erro ao processar: {e}")
    except KeyboardInterrupt:
        print("Processor encerrado.")
    finally:
        consumer.close()
        producer.flush(timeout=5)


if __name__ == "__main__":
    run_flink_processor()
