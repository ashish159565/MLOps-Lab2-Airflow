from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import joblib
import os
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

CLEAN_PATH = "/opt/airflow/working_data/cleaned/iris_clean.csv"
MODEL_PATH = "/opt/airflow/working_data/model/iris_model.pkl"

def split_data():
    df = pd.read_csv(CLEAN_PATH)
    X = df.drop(columns=["species"])
    y = df["species"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    os.makedirs("/opt/airflow/working_data/model", exist_ok=True)
    X_train.to_csv("/opt/airflow/working_data/model/X_train.csv", index=False)
    y_train.to_csv("/opt/airflow/working_data/model/y_train.csv", index=False)
    X_test.to_csv("/opt/airflow/working_data/model/X_test.csv", index=False)
    y_test.to_csv("/opt/airflow/working_data/model/y_test.csv", index=False)
    print("✅ Data split and saved to /model folder.")

def train_model():
    X_train = pd.read_csv("/opt/airflow/working_data/model/X_train.csv")
    y_train = pd.read_csv("/opt/airflow/working_data/model/y_train.csv").values.ravel()
    model = LogisticRegression(max_iter=200)
    model.fit(X_train, y_train)
    joblib.dump(model, MODEL_PATH)
    print("✅ Model trained and saved.")

def evaluate_model():
    model = joblib.load(MODEL_PATH)
    X_test = pd.read_csv("/opt/airflow/working_data/model/X_test.csv")
    y_test = pd.read_csv("/opt/airflow/working_data/model/y_test.csv").values.ravel()
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"✅ Model accuracy: {acc:.3f}")

def register_model():
    print("✅ Model registered to local store (simulated).")

with DAG(
    "dag_3_train",
    start_date=datetime(2025,10,6),
    schedule_interval=None,
    catchup=False,
    default_args={"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)},
) as dag:
    t1 = PythonOperator(task_id="split_data", python_callable=split_data)
    t2 = PythonOperator(task_id="train_model", python_callable=train_model)
    t3 = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)
    t4 = PythonOperator(task_id="register_model", python_callable=register_model)
    t5 = TriggerDagRunOperator(task_id="trigger_deploy", trigger_dag_id="dag_4_deploy")

    t1 >> t2 >> t3 >> t4 >> t5
