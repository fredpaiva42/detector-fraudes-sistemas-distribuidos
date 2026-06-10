import json
import os
from datetime import datetime, timedelta, timezone
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import joblib
from src.kafka_config import KAFKA_BROKER, TOPIC_TRANSACTIONS, TOPIC_ALERTS
from src.ml_features import calculate_features

MODEL_PATH = os.environ.get("MODEL_PATH", "models/fraud_model.joblib")


def parse_timestamp(ts_str):
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


class FraudDetector(KeyedProcessFunction):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
        self.tx_history = None

    def open(self, runtime_context: RuntimeContext):
        self.model = joblib.load(self.model_path)
        self.tx_history = runtime_context.get_state(
            ValueStateDescriptor("tx_history", Types.STRING())
        )

    def process_element(self, tx_json, ctx):
        try:
            tx = json.loads(tx_json)
            history_json = self.tx_history.value()
            history = json.loads(history_json) if history_json else []

            now = parse_timestamp(tx["timestamp"])
            ten_min_ago = now - timedelta(minutes=10)
            history = [h for h in history if parse_timestamp(h["timestamp"]) >= ten_min_ago]
            history.append(tx)

            self.tx_history.update(json.dumps(history))

            features = calculate_features(tx, history[:-1])
            prediction = self.model.predict([features])[0]
            proba = self.model.predict_proba([features])[0]

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
                "card_id": tx["card_id"],
                "amount": tx["amount"],
                "timestamp": tx["timestamp"],
                "card_type": tx.get("card_type", "unknown"),
                "card_brand": tx.get("card_brand", "unknown"),
                "is_fraud": is_fraud,
                "confidence": round(confidence, 4),
                "reasons": reasons,
            }
            yield json.dumps(result)
        except Exception as e:
            yield json.dumps({"error": str(e)})


def run_flink_processor():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000)

    kafka_consumer = FlinkKafkaConsumer(
        topics=TOPIC_TRANSACTIONS,
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": KAFKA_BROKER, "group.id": "flink-fraud-processor"},
    )

    kafka_producer = FlinkKafkaProducer(
        topic=TOPIC_ALERTS,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BROKER},
    )

    ds = env.add_source(kafka_consumer)
    ds = ds.key_by(lambda x: json.loads(x).get("card_id", ""))
    ds = ds.process(FraudDetector(MODEL_PATH), output_type=Types.STRING())
    ds.add_sink(kafka_producer)

    env.execute("Fraud Detector")


if __name__ == "__main__":
    run_flink_processor()
