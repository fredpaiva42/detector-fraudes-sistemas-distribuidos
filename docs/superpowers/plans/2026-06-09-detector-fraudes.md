# Detector de Fraudes em Tempo Real — Plano de Implementação

> **Para workers agênticos:** Use superpowers:subagent-driven-development (recomendado) ou superpowers:executing-plans para implementar task por task. Steps usam syntaxe checkbox (`- [ ]`) para tracking.

**Goal:** Construir um sistema de detecção de fraude em tempo real com Kafka, PyFlink e Random Forest, totalmente containerizado com Docker Compose.

**Architecture:** Producer gera transações fake → Kafka (`transacoes-raw`) → PyFlink processa com estado por cartão e classifica com modelo ML → Kafka (`alertas-fraude`) → Consumer exibe no terminal. Treinamento do modelo é executado separadamente.

**Tech Stack:** Python 3.12+, uv, confluent-kafka, apache-flink, scikit-learn, joblib, haversine, Docker Compose

---

## File Map

| Arquivo | Responsabilidade |
|---------|-----------------|
| `pyproject.toml` | Dependências e metadata do projeto |
| `.python-version` | Versão do Python |
| `.gitignore` | Arquivos ignorados pelo git |
| `Dockerfile` | Imagem Docker para os serviços Python |
| `docker-compose.yml` | Orquestração de todos os serviços |
| `src/kafka_config.py` | Configurações Kafka (tópicos, brokers) |
| `src/data_generator.py` | Gerador de transações fake |
| `src/ml_features.py` | Cálculo de features para o modelo |
| `src/producer.py` | Publica transações no Kafka |
| `src/train_model.py` | Treina e salva modelo Random Forest |
| `src/flink_processor.py` | Processamento stateful + inferência ML |
| `src/alert_consumer.py` | Consome alertas e exibe no terminal |
| `tests/test_data_generator.py` | Testes do gerador de transações |
| `tests/test_ml_features.py` | Testes do cálculo de features |
| `tests/test_train_model.py` | Testes do pipeline de treinamento |
| `README.md` | Documentação com justificativas técnicas |

---

### Task 1: Scaffolding do Projeto

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `.gitignore`

- [ ] **Step 1: Criar pyproject.toml**

```toml
[project]
name = "detector-fraudes"
version = "0.1.0"
description = "Detecção de fraude em tempo real com Kafka, PyFlink e Random Forest"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "confluent-kafka>=2.3.0",
    "apache-flink>=1.18.0",
    "scikit-learn>=1.4.0",
    "joblib>=1.3.0",
    "haversine>=2.8.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src"]
```

- [ ] **Step 2: Criar .python-version**

```
3.12
```

- [ ] **Step 3: Criar .gitignore**

```
.venv/
__pycache__/
*.pyc
.superpowers/
models/*.joblib
.env
```

- [ ] **Step 4: Inicializar projeto com uv**

```bash
cd /home/fredm/sistemas-distribuidos/detector-fraudes
uv sync
```

Expected: `uv.lock` criado, `.venv/` com dependências instaladas.

- [ ] **Step 5: Commit**

```bash
git init
git add pyproject.toml .python-version .gitignore uv.lock
git commit -m "chore: project scaffolding with uv"
```

---

### Task 2: Configuração Kafka

**Files:**
- Create: `src/kafka_config.py`

- [ ] **Step 1: Criar kafka_config.py**

```python
KAFKA_BROKER = "kafka:9092"
TOPIC_TRANSACTIONS = "transacoes-raw"
TOPIC_ALERTS = "alertas-fraude"
```

- [ ] **Step 2: Commit**

```bash
git add src/kafka_config.py
git commit -m "feat: add kafka configuration module"
```

---

### Task 3: Gerador de Transações

**Files:**
- Create: `src/data_generator.py`
- Create: `tests/test_data_generator.py`

- [ ] **Step 1: Escrever teste que falha**

```python
# tests/test_data_generator.py
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
```

- [ ] **Step 2: Rodar teste para verificar que falha**

```bash
uv run pytest tests/test_data_generator.py -v
```

Expected: FAIL com "ModuleNotFoundError: No module named 'src.data_generator'"

- [ ] **Step 3: Implementar data_generator.py**

```python
# src/data_generator.py
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


def generate_transaction(card_id=None, timestamp=None, is_fraud=False):
    if card_id is None:
        card_id = random.choice(CARD_IDS)
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    base_location = random.choice(LOCATIONS)

    if is_fraud:
        amount = round(random.uniform(500, 10000), 2)
        lat_offset = random.uniform(-20, 20)
        lon_offset = random.uniform(-20, 20)
    else:
        amount = round(random.uniform(10, 500), 2)
        lat_offset = random.uniform(-0.1, 0.1)
        lon_offset = random.uniform(-0.1, 0.1)

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
```

- [ ] **Step 4: Rodar teste para verificar que passa**

```bash
uv run pytest tests/test_data_generator.py -v
```

Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/data_generator.py tests/test_data_generator.py
git commit -m "feat: data generator with fraud patterns"
```

---

### Task 4: Cálculo de Features

**Files:**
- Create: `src/ml_features.py`
- Create: `tests/test_ml_features.py`

- [ ] **Step 1: Escrever teste que falha**

```python
# tests/test_ml_features.py
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
        {"amount": 75.0, "latitude": -23.57, "longitude": -46.65, "timestamp": "2026-06-09T14:25:00Z"},
    ]
    features = calculate_features(tx, history)
    assert features[1] == 2  # tx_count_2min
    assert features[2] > 0   # avg_amount_10min
```

- [ ] **Step 2: Rodar teste para verificar que falha**

```bash
uv run pytest tests/test_ml_features.py -v
```

Expected: FAIL com "ModuleNotFoundError"

- [ ] **Step 3: Implementar ml_features.py**

```python
# src/ml_features.py
from datetime import datetime, timedelta, timezone
from haversine import haversine

MERCHANT_CATEGORIES = ["electronics", "grocery", "restaurant", "travel", "clothing", "fuel", "pharmacy"]


def calculate_distance(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2))


def encode_card_type(card_type):
    return 0 if card_type == "physical" else 1


def encode_card_brand(card_brand):
    mapping = {"Visa": 0, "Mastercard": 1, "Elo": 2}
    return mapping.get(card_brand, 0)


def encode_merchant_category(category):
    return MERCHANT_CATEGORIES.index(category) if category in MERCHANT_CATEGORIES else 0


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
```

- [ ] **Step 4: Rodar teste para verificar que passa**

```bash
uv run pytest tests/test_ml_features.py -v
```

Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/ml_features.py tests/test_ml_features.py
git commit -m "feat: ML feature calculation module"
```

---

### Task 5: Pipeline de Treinamento ML

**Files:**
- Create: `src/train_model.py`
- Create: `tests/test_train_model.py`

- [ ] **Step 1: Escrever teste que falha**

```python
# tests/test_train_model.py
import os
import pytest
import joblib
from src.train_model import train_model


def test_train_model_creates_joblib_file(tmp_path):
    model_path = tmp_path / "fraud_model.joblib"
    metrics = train_model(n_samples=500, model_path=str(model_path))
    assert model_path.exists()


def test_train_model_returns_metrics(tmp_path):
    model_path = tmp_path / "fraud_model.joblib"
    metrics = train_model(n_samples=500, model_path=str(model_path))
    assert "f1_score" in metrics
    assert "precision" in metrics
    assert "recall" in metrics
    assert metrics["f1_score"] > 0


def test_train_model_f1_above_threshold(tmp_path):
    model_path = tmp_path / "fraud_model.joblib"
    metrics = train_model(n_samples=1000, model_path=str(model_path))
    assert metrics["f1_score"] > 0.5, f"F1 {metrics['f1_score']:.2f} too low"


def test_train_model_loadable(tmp_path):
    model_path = tmp_path / "fraud_model.joblib"
    train_model(n_samples=500, model_path=str(model_path))
    model = joblib.load(model_path)
    assert hasattr(model, "predict")
```

- [ ] **Step 2: Rodar teste para verificar que falha**

```bash
uv run pytest tests/test_train_model.py -v
```

Expected: FAIL com "ModuleNotFoundError"

- [ ] **Step 3: Implementar train_model.py**

```python
# src/train_model.py
import os
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, precision_score, recall_score, confusion_matrix
from src.data_generator import generate_historical_transactions
from src.ml_features import calculate_features


def train_model(n_samples=10000, model_path="models/fraud_model.joblib"):
    transactions = generate_historical_transactions(n_samples)

    X = []
    y = []
    history_map = {}

    for tx in transactions:
        card_id = tx["card_id"]
        history = history_map.get(card_id, [])
        features = calculate_features(tx, history)
        X.append(features)
        y.append(1 if tx.get("is_fraud") else 0)
        history_map.setdefault(card_id, []).append(tx)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    metrics = {
        "f1_score": round(f1_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
    }

    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    joblib.dump(model, model_path)

    return metrics


if __name__ == "__main__":
    metrics = train_model()
    print(f"Modelo treinado e salvo em models/fraud_model.joblib")
    print(f"F1-Score: {metrics['f1_score']}")
    print(f"Precision: {metrics['precision']}")
    print(f"Recall: {metrics['recall']}")
    print(f"Confusion Matrix: {metrics['confusion_matrix']}")
```

- [ ] **Step 4: Rodar teste para verificar que passa**

```bash
uv run pytest tests/test_train_model.py -v
```

Expected: PASS (4 tests)

- [ ] **Step 5: Treinar modelo real**

```bash
uv run python -m src.train_model
```

Expected: Modelo salvo em `models/fraud_model.joblib` com F1 > 0.5

- [ ] **Step 6: Commit**

```bash
git add src/train_model.py tests/test_train_model.py models/
git commit -m "feat: ML training pipeline with Random Forest"
```

---

### Task 6: Producer Kafka

**Files:**
- Create: `src/producer.py`

- [ ] **Step 1: Implementar producer.py**

```python
# src/producer.py
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
```

- [ ] **Step 2: Commit**

```bash
git add src/producer.py
git commit -m "feat: Kafka producer for fake transactions"
```

---

### Task 7: PyFlink Processor

**Files:**
- Create: `src/flink_processor.py`

- [ ] **Step 1: Implementar flink_processor.py**

Este módulo usa PyFlink DataStream API com `KeyedProcessFunction` para manter estado real por `card_id`. O estado armazena as transações recentes de cada cartão em uma janela de 10 minutos, permitindo calcular features como `tx_count_2min` e `avg_amount_10min`.

```python
# src/flink_processor.py
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
```

- [ ] **Step 2: Commit**

```bash
git add src/flink_processor.py
git commit -m "feat: PyFlink stateful processor with ML inference"
```

---

### Task 8: Consumer de Alertas

**Files:**
- Create: `src/alert_consumer.py`

- [ ] **Step 1: Implementar alert_consumer.py**

```python
# src/alert_consumer.py
import json
from confluent_kafka import Consumer
from src.kafka_config import KAFKA_BROKER, TOPIC_ALERTS

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


def format_alert(alert):
    if alert.get("is_fraud"):
        color = RED
        status = "FRAUDE"
    else:
        color = GREEN
        status = "APROVADO"

    confidence = alert.get("confidence", 0)
    reasons = ", ".join(alert.get("reasons", []))

    return (
        f"{color}{BOLD}[{status}]{RESET} "
        f"Card: {alert.get('card_id', '?')} | "
        f"Tipo: {alert.get('card_type', '?')} | "
        f"Bandeira: {alert.get('card_brand', '?')} | "
        f"Valor: R$ {alert.get('amount', 0):.2f} | "
        f"Confiança: {confidence:.1%} | "
        f"Motivos: {reasons or 'nenhum'} | "
        f"Timestamp: {alert.get('timestamp', '?')}"
    )


def run_alert_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "fraud-alert-display",
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_ALERTS])

    print(f"{YELLOW}Consumer de alertas iniciado. Monitorando {TOPIC_ALERTS}...{RESET}")
    print("-" * 100)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erro: {msg.error()}")
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))
                print(format_alert(alert))
            except json.JSONDecodeError:
                print(f"Mensagem inválida: {msg.value()}")
    except KeyboardInterrupt:
        print("Consumer encerrado.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_alert_consumer()
```

- [ ] **Step 2: Commit**

```bash
git add src/alert_consumer.py
git commit -m "feat: alert consumer with colored terminal output"
```

---

### Task 9: Docker Compose

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`

- [ ] **Step 1: Criar Dockerfile**

```dockerfile
FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY src/ ./src/
COPY models/ ./models/

ENV PYTHONPATH=/app
ENV MODEL_PATH=/app/models/fraud_model.joblib
```

- [ ] **Step 2: Criar docker-compose.yml**

```yaml
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - fraud-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - fraud-net

  flink-jobmanager:
    image: flink:1.18-java17
    command: jobmanager
    ports:
      - "8081:8081"
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
    networks:
      - fraud-net

  flink-taskmanager:
    image: flink:1.18-java17
    command: taskmanager
    depends_on:
      - flink-jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
    networks:
      - fraud-net

  producer:
    build: .
    command: python -m src.producer
    depends_on:
      - kafka
    environment:
      PYTHONPATH: /app
    networks:
      - fraud-net

  flink-processor:
    build: .
    command: python -m src.flink_processor
    depends_on:
      - kafka
      - flink-jobmanager
      - flink-taskmanager
    environment:
      PYTHONPATH: /app
      MODEL_PATH: /app/models/fraud_model.joblib
    volumes:
      - ./models:/app/models
    networks:
      - fraud-net

  alert-consumer:
    build: .
    command: python -m src.alert_consumer
    depends_on:
      - kafka
    environment:
      PYTHONPATH: /app
    networks:
      - fraud-net

  train-model:
    build: .
    command: python -m src.train_model
    volumes:
      - ./models:/app/models
    environment:
      PYTHONPATH: /app
    networks:
      - fraud-net

networks:
  fraud-net:
    driver: bridge
```

- [ ] **Step 3: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "feat: Docker Compose setup for all services"
```

---

### Task 10: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Criar README.md**

```markdown
# Detector de Fraudes em Tempo Real

Sistema de detecção de fraude em transações de cartão de crédito usando Kafka, PyFlink e Random Forest.

## Arquitetura

```
Producer → Kafka (transacoes-raw) → PyFlink (stateful + ML) → Kafka (alertas-fraude) → Consumer
```

## Justificativas Técnicas

### Por que PyFlink?
- Processamento por evento (não micro-batch), ideal para latência baixa
- API Python nativa, compatível com o ecossistema do projeto
- Stateful processing built-in com fault-tolerance via checkpoints
- Mais moderno e acadêmico que Spark Streaming para stream processing

### Por que Docker Compose?
- Reprodutibilidade: `docker compose up` levanta o sistema inteiro
- Qualquer colega ou professor consegue rodar sem configurar ambiente
- Isola dependências (Kafka, Zookeeper, Flink) do sistema host
- Demonstra conhecimento de containerização

### Por que Random Forest?
- Robusto a overfitting (ensemble de árvores)
- Lida bem com features heterogêneas (numéricas + categóricas)
- Fácil de explicar e justificar na defesa do trabalho
- Rápido de treinar para protótipos

## Como Rodar

### Pré-requisitos
- Docker e Docker Compose
- Python 3.12+ e uv (para desenvolvimento local)

### Com Docker Compose

```bash
# 1. Treinar o modelo (executar uma vez)
docker compose run --rm train-model

# 2. Subir o pipeline completo
docker compose up

# 3. Ver alertas (em outro terminal)
docker compose logs -f alert-consumer

# 4. Parar tudo
docker compose down
```

### Desenvolvimento Local

```bash
# Instalar dependências
uv sync

# Treinar modelo
uv run python -m src.train_model

# Iniciar producer (terminal 1)
uv run python -m src.producer

# Iniciar consumer de alertas (terminal 2)
uv run python -m src.alert_consumer
```

## Estrutura do Projeto

```
src/
├── kafka_config.py       # Configurações Kafka
├── data_generator.py     # Gerador de transações fake
├── ml_features.py        # Cálculo de features para ML
├── producer.py           # Publica transações no Kafka
├── train_model.py        # Treina modelo Random Forest
├── flink_processor.py    # Processamento stateful + inferência
└── alert_consumer.py     # Exibe alertas no terminal
```

## Testes

```bash
uv run pytest tests/ -v
```

## Features do Modelo

| Feature | Descrição |
|---------|-----------|
| amount | Valor da transação |
| tx_count_2min | Transações nos últimos 2 min |
| avg_amount_10min | Valor médio nos últimos 10 min |
| distance_from_last_km | Distância da última transação |
| time_since_last_sec | Tempo desde última transação |
| merchant_category_encoded | Categoria do comerciante |
| card_type_encoded | Físico ou digital |
| card_brand_encoded | Visa/Mastercard/Elo |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README with architecture and justifications"
```

---

## Resumo de Commits

1. `chore: project scaffolding with uv`
2. `feat: add kafka configuration module`
3. `feat: data generator with fraud patterns`
4. `feat: ML feature calculation module`
5. `feat: ML training pipeline with Random Forest`
6. `feat: Kafka producer for fake transactions`
7. `feat: PyFlink stateful processor with ML inference`
8. `feat: alert consumer with colored terminal output`
9. `feat: Docker Compose setup for all services`
10. `docs: README with architecture and justifications`
