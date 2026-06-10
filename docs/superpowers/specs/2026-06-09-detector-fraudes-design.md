# Detector de Fraudes em Tempo Real

## Contexto

Trabalho de graduação da disciplina de Sistemas Distribuídos. O projeto processa transações de cartão de crédito em tempo real, mantendo estado por cartão para identificar anomalias usando machine learning.

**Objetivo:** Demonstrar conceitos de sistemas distribuídos — mensageria (Kafka), processamento de streams com estado (PyFlink), e pipeline de ML integrado.

## Justificativa das Escolhas Técnicas

### Por que PyFlink?
- Processamento por evento (não micro-batch), ideal para latência baixa
- API Python nativa, compatível com o ecossistema do projeto (uv, Python 3.12+)
- Stateful processing built-in com fault-tolerance via checkpoints
- Mais moderno e acadêmico que Spark Streaming para stream processing

### Por que Docker Compose?
- Reprodutibilidade: `docker compose up` levanta o sistema inteiro
- Qualquer colega ou professor consegue rodar sem configurar ambiente
- Isola dependências (Kafka, Zookeeper, Flink) do sistema host
- Demonstra conhecimento de containerização, relevante para a disciplina

### Por que Random Forest?
- Robusto a overfitting (ensemble de árvores)
- Lida bem com features heterogêneas (numéricas + categóricas)
- Fácil de explicar e justificar na defesa do trabalho
- Rápido de treinar para protótipos
- Boa precisão para dados tabulares sem necessidade de tuning extensivo

## Arquitetura

```
┌─────────────┐     ┌───────────────┐     ┌──────────────────┐     ┌───────────────┐     ┌───────────────┐
│  Producer    │────▶│  Kafka        │────▶│  PyFlink         │────▶│  Kafka        │────▶│  Consumer     │
│  (gerador    │     │  tópico:      │     │  (stateful +     │     │  tópico:      │     │  de Alertas   │
│  de fake     │     │  transacoes-  │     │  ML inference)   │     │  alertas-     │     │  (terminal    │
│  txns)       │     │  raw          │     │                  │     │  fraude       │     │  colorido)    │
└─────────────┘     └───────────────┘     └──────────────────┘     └───────────────┘     └───────────────┘
                                                                     ▲
                                                                     │
                                              ┌──────────────────────┘
                                              │
                                     ┌────────────────┐
                                     │  Script de     │
                                     │  Treinamento   │
                                     │  (fora do      │
                                     │  pipeline,     │
                                     │  gera .joblib) │
                                     └────────────────┘
```

**Fluxo:**
1. Producer gera transações fake e publica em `transacoes-raw` (key: `card_id`)
2. PyFlink consome, mantém estado por cartão, calcula features, classifica com modelo ML
3. Resultado vai para `alertas-fraude`
4. Consumer exibe alertas no terminal com cores (verde=aprovado, vermelho=suspeito)
5. Treinamento é executado separadamente para gerar o modelo `.joblib`

## Componentes

### 1. Producer (`src/producer.py`)
- Gera transações fake com distribuição realista
- ~10% marcadas como fraudulentas (valores altos, frequência alta, localização incompatível)
- Publica no Kafka com `card_id` como key (garante partição consistente)
- Velocidade configurável (5-20 transações/segundo)

### 2. PyFlink Processor (`src/flink_processor.py`)
- Consome de `transacoes-raw`
- Keyed State por `card_id` — cada cartão tem seu próprio estado
- Janela de tempo: mantém buffer das últimas transações por cartão
- Calcula features em tempo real a cada nova transação
- Carrega modelo `.joblib` e classifica
- Produz resultado em `alertas-fraude`

### 3. Consumer de Alertas (`src/alert_consumer.py`)
- Consome de `alertas-fraude`
- Terminal colorido: verde para aprovado, vermelho para suspeito
- Mostra: timestamp, card_id, card_type, card_brand, valor, motivo(s) da flag, confiança

### 4. Treinamento ML (`src/train_model.py`)
- Gera 10.000 transações históricas com labels
- Pré-processa features, split 80/20 treino/teste
- Treina `RandomForestClassifier` (n_estimators=100)
- Avalia: precision, recall, F1-score, confusion matrix
- Salva como `models/fraud_model.joblib`

### 5. Módulos Compartilhados
- `src/data_generator.py` — gerador fake usado pelo producer e pelo treinamento
- `src/ml_features.py` — cálculo de features reutilizável (PyFlink + treinamento)
- `src/kafka_config.py` — configurações Kafka (tópicos, brokers)

## Schema dos Dados

### Transação (`transacoes-raw`)
```json
{
  "transaction_id": "uuid",
  "card_id": "card_0042",
  "card_type": "physical",
  "card_brand": "Visa",
  "amount": 1250.00,
  "timestamp": "2026-06-09T14:30:00Z",
  "latitude": -23.5505,
  "longitude": -46.6333,
  "merchant_category": "electronics"
}
```

### Alerta (`alertas-fraude`)
```json
{
  "transaction_id": "uuid",
  "card_id": "card_0042",
  "amount": 1250.00,
  "timestamp": "2026-06-09T14:30:00Z",
  "card_type": "physical",
  "card_brand": "Visa",
  "is_fraud": true,
  "confidence": 0.87,
  "reasons": ["high_amount", "rapid_succession", "location_anomaly"]
}
```

### Features para o Modelo (8 features)
| Feature | Tipo | Descrição |
|---------|------|-----------|
| `amount` | float | Valor da transação |
| `tx_count_2min` | int | Transações nos últimos 2 minutos |
| `avg_amount_10min` | float | Valor médio nos últimos 10 minutos |
| `distance_from_last_km` | float | Distância geográfica da última transação |
| `time_since_last_sec` | float | Tempo desde a última transação |
| `merchant_category_encoded` | int | Categoria do comerciante (label encoding) |
| `card_type_encoded` | int | 0=physical, 1=digital |
| `card_brand_encoded` | int | 0=Visa, 1=Mastercard, 2=Elo |

## Processamento Stateful (PyFlink)

1. Consume de `transacoes-raw` como tabela Flink
2. Keyed State por `card_id` com `MapState` para buffer de transações
3. A cada nova transação:
   - `tx_count_2min`: contagem de txns nos últimos 2 min
   - `avg_amount_10min`: média dos valores na janela
   - `distance_from_last_km`: haversine entre coordenadas
   - `time_since_last_sec`: diferença de timestamps
   - Codifica variáveis categóricas
4. Inferência: `model.predict()` + `model.predict_proba()`
5. Se `predict == 1` OU `predict_proba[1] > 0.7` → suspeita
6. Produz em `alertas-fraude`

**Fault-tolerance:** Checkpoints a cada 30s. Modelo carregado uma vez no início do job.

## Pipeline de ML

### Treinamento
1. Gera 10.000 transações históricas (mesmo gerador do producer)
2. ~10% marcadas como fraude por regras (valor > threshold, frequência alta, distância > X km)
3. Calcula as 8 features para cada transação
4. Split 80/20 treino/teste
5. `RandomForestClassifier(n_estimators=100)`
6. Avalia: precision, recall, F1-score, confusion matrix
7. Salva `models/fraud_model.joblib`

### Inferência (PyFlink)
- Modelo carregado uma vez via UDF
- Classifica cada transação processada
- Threshold de confiança: 0.7

## Docker Compose

### Serviços
| Serviço | Descrição | Portas |
|---------|-----------|--------|
| `zookeeper` | Gerencia cluster Kafka | 2181 |
| `kafka` | Broker Kafka | 9092 |
| `flink-jobmanager` | Coordena jobs Flink | 8081 (UI) |
| `flink-taskmanager` | Executa tasks Flink | — |
| `producer` | Gera transações | — |
| `flink-processor` | Stateful + ML | — |
| `alert-consumer` | Exibe alertas | — |
| `train-model` | Treina modelo (run once) | — |

### Volumes
- `./models:/app/models` — modelo `.joblib` compartilhado
- `./src:/app/src` — código fonte (dev)

### Rede
- `fraud-net` — rede Docker interna
- Kafka acessível como `kafka:9092`

### Comandos
```bash
# Treinar modelo (executar uma vez)
docker compose run --rm train-model

# Subir pipeline completo
docker compose up

# Ver alertas
docker compose logs -f alert-consumer

# Parar tudo
docker compose down
```

## Estrutura de Arquivos

```
detector-fraudes/
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── .python-version
├── Dockerfile
├── README.md
├── .gitignore
├── models/
│   └── fraud_model.joblib
├── src/
│   ├── producer.py
│   ├── flink_processor.py
│   ├── alert_consumer.py
│   ├── train_model.py
│   ├── data_generator.py
│   ├── ml_features.py
│   └── kafka_config.py
└── docs/
    └── superpowers/
        └── specs/
            └── 2026-06-09-detector-fraudes-design.md
```

## Dependências Python

```
confluent-kafka   # Cliente Kafka
apache-flink      # PyFlink
scikit-learn      # Random Forest
joblib            # Serialização do modelo
haversine         # Cálculo de distância geográfica
```

## Tratamento de Erros

- **Producer:** retry com backoff se Kafka indisponível
- **PyFlink:** checkpoint a cada 30s, restart on failure
- **Consumer:** commit manual de offsets
- **Modelo ML:** fallback para regras heurísticas se modelo não carregado

## Testes

- `test_data_generator.py` — gerador produz transações válidas com % de fraude esperada
- `test_ml_features.py` — cálculo de features correto
- `test_train_model.py` — treinamento roda e salva modelo

Não testa: integração com Kafka real, PyFlink (framework externo).

## Critérios de Sucesso

1. `docker compose up` levanta todo o sistema
2. Transações fluem: producer → Kafka → PyFlink → Kafka → consumer
3. Alertas aparecem no terminal com cores
4. Modelo treinado com F1-score > 0.8
5. README explica as justificativas técnicas
