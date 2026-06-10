FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY src/ ./src/
COPY models/ ./models/

ENV PYTHONPATH=/app
ENV MODEL_PATH=/app/models/fraud_model.joblib
