import json
import time
import random
from confluent_kafka import Producer
from src.kafka_config import KAFKA_BROKER, TOPIC_TRANSACTIONS
from src.data_generator import generate_transaction


def delivery_callback(err, msg):
    if err:
        print(f"Erro na entrega: {err}")


def run_producer(rate_per_sec=10):
    conf = {"bootstrap.servers": KAFKA_BROKER}
    producer = Producer(conf)
    interval = 1.0 / rate_per_sec

    print(f"Producer iniciado. Enviando {rate_per_sec} transações/seg para {TOPIC_TRANSACTIONS}")

    try:
        while True:
            is_fraud = random.random() < 0.10
            tx = generate_transaction(is_fraud=is_fraud)
            producer.produce(
                TOPIC_TRANSACTIONS,
                key=tx["card_id"].encode("utf-8"),
                value=json.dumps(tx).encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Producer encerrado.")
    finally:
        producer.flush(timeout=5)


if __name__ == "__main__":
    run_producer()
