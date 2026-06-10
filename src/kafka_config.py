import os

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC_TRANSACTIONS = "transacoes-raw"
TOPIC_ALERTS = "alertas-fraude"
TOPIC_DLQ = "alertas-fraude-dlq"
TOPIC_PARTITIONS = int(os.environ.get("TOPIC_PARTITIONS", "3"))
TOPIC_REPLICATION_FACTOR = int(os.environ.get("TOPIC_REPLICATION_FACTOR", "1"))

FLINK_PARALLELISM = int(os.environ.get("FLINK_PARALLELISM", "3"))
FRAUD_CONFIDENCE_THRESHOLD = float(os.environ.get("FRAUD_CONFIDENCE_THRESHOLD", "0.7"))