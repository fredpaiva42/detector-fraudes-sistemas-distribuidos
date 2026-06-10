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
