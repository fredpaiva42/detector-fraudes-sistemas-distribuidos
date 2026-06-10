# Detecção de Fraude em Tempo Real (Kafka + Stateful Processing)

Processamento de dados em movimento com manutenção de estado por cartão para identificar anomalias em milissegundos.

## O Cenário

Um fluxo constante de transações de cartão de crédito avaliado em tempo real. O sistema decide se uma compra é legítima ou fraudulenta antes mesmo de o cliente sair do caixa.

## Arquitetura

```
                          Kafka                           Kafka
Producer ──pub──▶ transacoes-raw ──▶ PyFlink ──▶ alertas-fraude ──▶ Alert Consumer
  (gera tx)         (particionado       (stateful         (resultado        (exibe no
                     por card_id)        + ML)             + motivos)         terminal)
```

### Ingestão e Buffer (Kafka)

- **Producer** gera transações fake e publica no tópico `transacoes-raw`
- **Particionamento por chave**: mensagens usam `card_id` como chave, garantindo que todas as transações de um mesmo cartão caiam na mesma partição — essencial para manter a ordem cronológica por usuário
- **Desacoplamento**: mesmo que o processador fique lento ou caia, as transações ficam seguras e ordenadas na fila

### Processamento de Estado (PyFlink)

O `flink_processor` consome do Kafka e mantém estado por cartão:

- **Janela deslizante de 10 minutos**: histórico recente por `card_id`
- **Métricas em tempo real**: transações nos últimos 2 min, valor médio, distância geográfica, tempo desde última transação
- **Keyed State**: Flink mantém o histórico em `ValueState` com checkpoint a cada 30s

### Inteligência e Decisão (ML)

Features calculadas em tempo real alimentam um **Random Forest** (`class_weight='balanced'`):

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

### Feedback Loop (Kafka Egress)

Resultado publicado no tópico `alertas-fraude`. Outros sistemas podem ouvir para tomar ações (SMS, bloqueio de cartão). Erros vão para `alertas-fraude-dlq`.

## Motivos de Fraude

Transações fraudulentas recebem motivos explicativos:

| Motivo | Condição |
|--------|----------|
| `valor_alto` | Valor > R$ 500 |
| `transacoes_rapidas` | 3+ transações em 2 min |
| `deslocamento_impossivel` | Distância > 100 km da última tx em < 2h |
| `modelo_suspeito` | Modelo detectou fraude sem outros sinais claros |

## Justificativas Técnicas

### Por que PyFlink?

- Processamento por evento (não micro-batch), ideal para latência baixa
- API Python nativa, compatível com o ecossistema do projeto
- Stateful processing built-in com fault-tolerance via checkpoints
- `key_by(card_id)` garante que todas as tx de um cartão vão para a mesma instância, permitindo estado consistente

### Integração PyFlink + Kafka

O conector Kafka do Flink mudou significativamente entre versões. Para PyFlink 2.2.1 (Flink 2.2):

- **Conector JAR correto**: `flink-connector-kafka-5.0.0-2.2.jar`
- **JARs adicionais**: `kafka-clients-3.7.2.jar` e `flink-connector-base-2.2.1.jar`
- **Carregar JARs via `env.add_jars()`**: o método `Configuration.set_string("pipeline.jars", ...)` não carrega os JARs no classloader da JVM
- **Particionamento explícito**: tópicos criados com 3 partições via `topic-init`, paralelismo do Flink configurável via `FLINK_PARALLELISM`

### Por que Docker Compose?

- `docker compose up` levanta o sistema inteiro
- Qualquer colega ou professor consegue rodar sem configurar ambiente
- Isola dependências (Kafka, Zookeeper) do sistema host

### Por que Random Forest?

- Robusto a overfitting (ensemble de árvores)
- `class_weight='balanced'` compensa o desbalanceamento (~10% fraude)
- Lida bem com features heterogêneas (numéricas + categóricas)
- Fácil de explicar e justificar na defesa do trabalho

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

# Baixar JARs do conector Kafka (necessário para rodar o Flink localmente)
mkdir -p jars && \
  wget -q -O jars/flink-connector-kafka-5.0.0-2.2.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar" && \
  wget -q -O jars/kafka-clients-3.7.2.jar "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.2/kafka-clients-3.7.2.jar" && \
  wget -q -O jars/flink-connector-base-2.2.1.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/2.2.1/flink-connector-base-2.2.1.jar"

# Treinar modelo
uv run python -m src.train_model

# Iniciar producer (terminal 1)
uv run python -m src.producer

# Iniciar consumer de alertas (terminal 2)
uv run python -m src.alert_consumer
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_BROKER` | `kafka:9092` | Endereço do broker Kafka |
| `FLINK_PARALLELISM` | `3` | Paralelismo do Flink (deve corresponder ao nº de partições) |
| `FRAUD_CONFIDENCE_THRESHOLD` | `0.7` | Threshold de confiança para classificar fraude |
| `MODEL_PATH` | `models/fraud_model.joblib` | Caminho do modelo treinado |
| `FLINK_JAR_DIR` | `/opt/flink/usrlib` | Diretório com JARs do conector Kafka (fallback local: `jars/`) |
| `TOPIC_PARTITIONS` | `3` | Número de partições ao criar tópicos |
| `TOPIC_REPLICATION_FACTOR` | `1` | Fator de replicação (1 para single-node) |

## Estrutura do Projeto

```
src/
├── __init__.py            # Pacote Python
├── kafka_config.py        # Configurações Kafka e variáveis de ambiente
├── data_generator.py      # Gerador de transações (cartões com localização estável)
├── ml_features.py         # Cálculo de features + encoding + distância haversine
├── producer.py            # Publica transações no Kafka (key_by card_id)
├── train_model.py          # Treina Random Forest (class_weight='balanced')
├── flink_processor.py      # Processamento stateful + inferência ML
└── alert_consumer.py       # Exibe alertas formatados no terminal

tests/
├── test_data_generator.py  # Testes do gerador
├── test_ml_features.py     # Testes de features e encoding
└── test_train_model.py     # Testes de treino e métricas

models/
└── fraud_model.joblib      # Modelo treinado

jars/                        # Apenas para dev local (não commitado)
├── flink-connector-kafka-5.0.0-2.2.jar
├── flink-connector-base-2.2.1.jar
└── kafka-clients-3.7.2.jar
```

## Testes

```bash
uv run pytest tests/ -v
```

## Tópicos Kafka

| Tópico | Partições | Descrição |
|--------|-----------|-----------|
| `transacoes-raw` | 3 | Transações de entrada (key: card_id) |
| `alertas-fraude` | 3 | Resultados da análise (fraude/ok + motivos) |
| `alertas-fraude-dlq` | 1 | Dead-letter queue para erros de processamento |