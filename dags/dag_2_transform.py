from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import os

RAW_PATH = "/opt/airflow/working_data/raw/iris.csv"
CLEAN_PATH = "/opt/airflow/working_data/cleaned/iris_clean.csv"

def load_data():
    os.makedirs("/opt/airflow/working_data/cleaned", exist_ok=True)
    df = pd.read_csv(RAW_PATH)
    print(f"✅ Loaded raw data with shape {df.shape}")
    return df.to_json()

def clean_missing():
    df = pd.read_csv(RAW_PATH)
    df = df.dropna()
    df.to_csv(CLEAN_PATH, index=False)
    print("✅ Cleaned missing values.")

def encode_species():
    df = pd.read_csv(CLEAN_PATH)
    df["species"] = df["species"].astype("category").cat.codes
    df.to_csv(CLEAN_PATH, index=False)
    print("✅ Encoded species as numeric labels.")

def normalize_features():
    df = pd.read_csv(CLEAN_PATH)
    num_cols = ["sepal_length","sepal_width","petal_length","petal_width"]
    df[num_cols] = (df[num_cols] - df[num_cols].min()) / (df[num_cols].max() - df[num_cols].min())
    df.to_csv(CLEAN_PATH, index=False)
    print("✅ Normalized numeric columns.")

with DAG(
    "dag_2_transform",
    start_date=datetime(2025,10,6),
    schedule_interval=None,
    catchup=False,
    default_args={"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)},
) as dag:
    t1 = PythonOperator(task_id="load_data", python_callable=load_data)
    t2 = PythonOperator(task_id="clean_missing", python_callable=clean_missing)
    t3 = PythonOperator(task_id="encode_species", python_callable=encode_species)
    t4 = PythonOperator(task_id="normalize_features", python_callable=normalize_features)
    t5 = TriggerDagRunOperator(task_id="trigger_train", trigger_dag_id="dag_3_train")

    t1 >> t2 >> t3 >> t4 >> t5
