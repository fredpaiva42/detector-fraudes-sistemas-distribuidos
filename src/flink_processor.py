import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import DeliveryGuarantee
import joblib
from src.kafka_config import (
    KAFKA_BROKER, TOPIC_TRANSACTIONS, TOPIC_ALERTS, TOPIC_DLQ,
    FLINK_PARALLELISM, FRAUD_CONFIDENCE_THRESHOLD,
)
from src.ml_features import calculate_features, calculate_distance, parse_timestamp

MODEL_PATH = os.environ.get("MODEL_PATH", "models/fraud_model.joblib")

JAR_DIR = os.environ.get("FLINK_JAR_DIR", str(Path(__file__).resolve().parent.parent / "jars"))

JAR_FILES = [
    f"file://{JAR_DIR}/flink-connector-kafka-5.0.0-2.2.jar",
    f"file://{JAR_DIR}/kafka-clients-3.7.2.jar",
    f"file://{JAR_DIR}/flink-connector-base-2.2.1.jar",
]


class FraudDetector(KeyedProcessFunction):
    def __init__(self, model_path, confidence_threshold=0.7):
        self.model_path = model_path
        self.confidence_threshold = confidence_threshold
        self.model = None
        self.tx_history = None

    def open(self, runtime_context: RuntimeContext):
        self.model = joblib.load(self.model_path)
        self.tx_history = runtime_context.get_state(
            ValueStateDescriptor("tx_history", Types.STRING())
        )

    def _build_reasons(self, tx, history, proba_fraud):
        reasons = []
        if tx.get("amount", 0) > 500:
            reasons.append("high_amount")
        if len(history) > 1:
            now = parse_timestamp(tx["timestamp"])
            two_min_ago = now - timedelta(minutes=2)
            recent = [h for h in history if parse_timestamp(h["timestamp"]) >= two_min_ago]
            if len(recent) >= 3:
                reasons.append("rapid_succession")
        last = max(history, key=lambda h: parse_timestamp(h["timestamp"])) if history else None
        if last is not None:
            dist = calculate_distance(
                tx["latitude"], tx["longitude"],
                last["latitude"], last["longitude"],
            )
            if dist > 100:
                reasons.append("impossible_distance")
        if not reasons and proba_fraud > 0.5:
            reasons.append("model_flagged")
        return reasons

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

            proba_fraud = float(proba[1])
            is_fraud = proba_fraud > self.confidence_threshold

            reasons = self._build_reasons(tx, history[:-1], proba_fraud)

            result = {
                "transaction_id": tx["transaction_id"],
                "card_id": tx["card_id"],
                "amount": tx["amount"],
                "timestamp": tx["timestamp"],
                "card_type": tx.get("card_type", "unknown"),
                "card_brand": tx.get("card_brand", "unknown"),
                "is_fraud": is_fraud,
                "confidence": round(proba_fraud, 4),
                "reasons": reasons,
            }

            status = "FRAUDE" if is_fraud else "OK"
            reasons_str = ", ".join(reasons) if reasons else "nenhum"
            print(f"[{status}] Card: {tx['card_id']} | R$ {tx['amount']:.2f} | Confiança: {proba_fraud:.1%} | {reasons_str}")

            yield json.dumps(result)
        except Exception as e:
            print(f"Erro ao processar: {e}", flush=True)
            yield json.dumps({"error": str(e), "raw": tx_json, "topic": TOPIC_DLQ})


def create_topics_if_needed():
    import confluent_kafka.admin
    from src.kafka_config import TOPIC_TRANSACTIONS, TOPIC_ALERTS, TOPIC_DLQ, TOPIC_PARTITIONS, TOPIC_REPLICATION_FACTOR

    admin = confluent_kafka.admin.AdminClient({"bootstrap.servers": KAFKA_BROKER})
    existing = set(admin.list_topics().topics.keys())
    topics_to_create = []
    for topic in [TOPIC_TRANSACTIONS, TOPIC_ALERTS, TOPIC_DLQ]:
        if topic not in existing:
            topics_to_create.append(
                confluent_kafka.admin.NewTopic(
                    topic,
                    num_partitions=TOPIC_PARTITIONS,
                    replication_factor=TOPIC_REPLICATION_FACTOR,
                )
            )
    if topics_to_create:
        print(f"Criando tópicos: {[t.topic for t in topics_to_create]}", flush=True)
        fs = admin.create_topics(topics_to_create)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Tópico criado: {topic}", flush=True)
            except Exception as e:
                print(f"Erro ao criar tópico {topic}: {e}", flush=True)


def run_flink_processor():
    print("=== INICIANDO FLINK PROCESSOR ===", flush=True)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PARALLELISM)
    env.add_jars(*JAR_FILES)
    env.enable_checkpointing(30000)

    print(f"JARs adicionados: {JAR_FILES}", flush=True)
    print(f"Parallelism: {env.get_parallelism()}", flush=True)
    print(f"Confidence threshold: {FRAUD_CONFIDENCE_THRESHOLD}", flush=True)
    print("Criando KafkaSource...", flush=True)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_TRANSACTIONS)
        .set_group_id("flink-fraud-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    print("KafkaSource criado. Construindo pipeline...", flush=True)

    ds = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-transactions",
    )
    ds = ds.key_by(lambda x: json.loads(x).get("card_id", ""))
    ds = ds.process(
        FraudDetector(MODEL_PATH, confidence_threshold=FRAUD_CONFIDENCE_THRESHOLD),
        output_type=Types.STRING(),
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_ALERTS)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    ds.sink_to(kafka_sink)

    print("Pipeline construído com KafkaSink. Executando env.execute()...", flush=True)

    env.execute("Fraud Detector")

    print("=== FIM FLINK PROCESSOR ===", flush=True)


if __name__ == "__main__":
    run_flink_processor()