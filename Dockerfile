FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless wget && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

RUN mkdir -p /opt/flink/usrlib && \
    wget -q -O /opt/flink/usrlib/flink-connector-kafka-5.0.0-2.2.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar" && \
    wget -q -O /opt/flink/usrlib/kafka-clients-3.7.2.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.2/kafka-clients-3.7.2.jar" && \
    wget -q -O /opt/flink/usrlib/flink-connector-base-2.2.1.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/2.2.1/flink-connector-base-2.2.1.jar"

COPY src/ ./src/
COPY models/ ./models/
COPY jars/ ./jars/

ENV PYTHONPATH=/app
ENV MODEL_PATH=/app/models/fraud_model.joblib
ENV FLINK_JAR_DIR=/opt/flink/usrlib
