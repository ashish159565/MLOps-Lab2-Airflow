from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests

URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
RAW_PATH = "/opt/airflow/working_data/raw/iris.csv"

def download_data():
    os.makedirs("/opt/airflow/working_data/raw", exist_ok=True)
    r = requests.get(URL)
    with open(RAW_PATH, "w") as f:
        f.write(r.text)
    print("✅ Downloaded raw Iris dataset.")

def validate_data():
    df = pd.read_csv(RAW_PATH, header=None)
    if df.shape[0] < 100:
        raise ValueError("Dataset too small — possible download error")
    print(f"✅ Validated dataset with {df.shape[0]} rows.")

def add_headers():
    df = pd.read_csv(RAW_PATH, header=None)
    df.columns = ["sepal_length","sepal_width","petal_length","petal_width","species"]
    df.to_csv(RAW_PATH, index=False)
    print("✅ Added headers and saved final raw dataset.")

with DAG(
    "dag_1_extract",
    start_date=datetime(2025,10,6),
    schedule_interval=None,
    catchup=False,
    default_args={"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)},
) as dag:
    t1 = PythonOperator(task_id="download_data", python_callable=download_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="add_headers", python_callable=add_headers)
    t4 = TriggerDagRunOperator(task_id="trigger_transform", trigger_dag_id="dag_2_transform")

    t1 >> t2 >> t3 >> t4
