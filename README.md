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

### Integração PyFlink + Kafka: notas técnicas

O conector Kafka do Flink mudou significativamente entre versões. O `FlinkKafkaConsumer` (legacy) foi removido no conector 3.x+, substituído pela nova API `KafkaSource`/`KafkaSink`. Para o PyFlink 2.2.1 (Flink 2.2), é necessário:

- **Conector JAR correto**: `flink-connector-kafka-5.0.0-2.2.jar` (a antiga artifact `_2.12` não existe mais)
- **JARs adicionais**: `kafka-clients-3.7.2.jar` e `flink-connector-base-2.2.1.jar`
- **Carregar JARs via `env.add_jars()`**: o método `Configuration.set_string("pipeline.jars", ...)` não carrega os JARs no classloader da JVM; `add_jars()` faz ambos
- **Paralelismo 1**: para Kafka de nó único, o paralelismo padrão (20) sobrecarrega o broker

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
