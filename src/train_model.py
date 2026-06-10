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
