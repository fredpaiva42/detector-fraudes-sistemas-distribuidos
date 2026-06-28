import os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from xgboost import XGBClassifier
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import f1_score, precision_score, recall_score, confusion_matrix
from src.data_generator import generate_historical_transactions
from src.ml_features import calculate_features


def train_model(n_samples=10000, model_path="models/fraud_model.joblib"):
    transactions = generate_historical_transactions(n_samples)

    X = []
    y = []
    history_map = {}
    cards = []

    for tx in transactions:
        card_id = tx["card_id"]
        history = history_map.get(card_id, [])
        features = calculate_features(tx, history)
        X.append(features)
        y.append(1 if tx.get("is_fraud") else 0)
        history_map.setdefault(card_id, []).append(tx)
        cards.append(card_id)

    X = np.array(X)
    y = np.array(y)

    gss = GroupShuffleSplit(n_splits=1, test_size=0.30, random_state=42)

    indices_treino, indices_teste = next(gss.split(X, y, groups=cards))

    X_train, X_test = X[indices_treino], X[indices_teste]
    y_train, y_test = y[indices_treino], y[indices_teste]
    
    model_knn = KNeighborsClassifier() 
    weight = ['uniform', 'distance']
    n_neigh = [5, 10, 20]
    print("Realizando Grid Search para o modelo de K-Nearest Neighbors")
    gridSearch_knn = GridSearchCV(estimator=model_knn,param_grid={'weights':weight,'n_neighbors':n_neigh},cv=4)
    gridSearch_knn.fit(X_train,y_train)
    optimal_knn = gridSearch_knn.best_estimator_

    model_xgb = XGBClassifier(random_state=42)
    n_estim_xgb = [50, 100, 300]
    learn_rate = [0.01, 0.1, 0.3, 0.5]
    max_depth_xgb = [10, 100, 200]
    print("Realizando Grid Search para o modelo de XGBoost")
    gridSearch_xgb = GridSearchCV(estimator=model_xgb,param_grid={'n_estimators':n_estim_xgb,'learning_rate':learn_rate,'max_depth':max_depth_xgb},cv=4)
    gridSearch_xgb.fit(X_train,y_train)
    optimal_xgb = gridSearch_xgb.best_estimator_


    model_rf = RandomForestClassifier(random_state=42, class_weight="balanced")
    n_estim_rf = [100, 1000]
    max_depth_rf = [80, 100]
    max_feat = [3, 5]
    print("Realizando Grid Search para o modelo de Random Forest")
    gridSearch_rf = GridSearchCV(estimator=model_rf,param_grid={'n_estimators':n_estim_rf,'max_features':max_feat,'max_depth':max_depth_rf},cv=4)
    gridSearch_rf.fit(X_train,y_train)
    optimal_rf = gridSearch_rf.best_estimator_

    y_pred_knn = optimal_knn.predict(X_test)
    y_pred_xgb = optimal_xgb.predict(X_test)
    y_pred_rf = optimal_rf.predict(X_test)
    metrics_knn = {
        "f1_score": round(f1_score(y_test, y_pred_knn), 4),
        "precision": round(precision_score(y_test, y_pred_knn), 4),
        "recall": round(recall_score(y_test, y_pred_knn), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred_knn).tolist(),
    }

    metrics_xgb = {
        "f1_score": round(f1_score(y_test, y_pred_xgb), 4),
        "precision": round(precision_score(y_test, y_pred_xgb), 4),
        "recall": round(recall_score(y_test, y_pred_xgb), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred_xgb).tolist(),
    }

    metrics_rf = {
        "f1_score": round(f1_score(y_test, y_pred_rf), 4),
        "precision": round(precision_score(y_test, y_pred_rf), 4),
        "recall": round(recall_score(y_test, y_pred_rf), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred_rf).tolist(),
    }

    metrics = {}
    model = None

    print(f"XGB: {metrics_xgb['f1_score']}")
    print(f"KNN: {metrics_knn['f1_score']}")
    print(f"RF: {metrics_rf['f1_score']}")

    if(metrics_xgb['f1_score'] >= metrics_rf['f1_score']):
        if(metrics_xgb['f1_score'] >= metrics_knn['f1_score']):
            metrics = metrics_xgb
            model = optimal_xgb
        else:
            metrics = metrics_knn
            model = optimal_knn
    elif(metrics_knn['f1_score'] >= metrics_rf['f1_score']):
        metrics = metrics_knn
        model = optimal_knn
    else:
        metrics = metrics_rf
        model = optimal_rf

    print(f"Modelo escolhido: {model.__class__.__name__}")
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
