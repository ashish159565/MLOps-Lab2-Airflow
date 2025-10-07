from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import joblib
import pandas as pd
import json

MODEL_PATH = "/opt/airflow/working_data/model/iris_model.pkl"

def load_model():
    joblib.load(MODEL_PATH)
    print("✅ Model loaded successfully.")

def predict_sample():
    model = joblib.load(MODEL_PATH)
    sample = pd.DataFrame({
        "sepal_length":[5.8],
        "sepal_width":[2.7],
        "petal_length":[5.1],
        "petal_width":[1.9]
    })
    pred = model.predict(sample)[0]
    with open("/opt/airflow/working_data/model/prediction.json", "w") as f:
        json.dump({"predicted_label": int(pred)}, f)
    print(f"✅ Made prediction: {pred}")

def log_result():
    with open("/opt/airflow/working_data/model/prediction.json") as f:
        result = json.load(f)
    print(f"✅ Logged prediction result: {result}")

with DAG(
    "dag_4_deploy",
    start_date=datetime(2025,10,6),
    schedule_interval=None,
    catchup=False,
    default_args={"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)},
) as dag:
    t1 = PythonOperator(task_id="load_model", python_callable=load_model)
    t2 = PythonOperator(task_id="predict_sample", python_callable=predict_sample)
    t3 = PythonOperator(task_id="log_result", python_callable=log_result)
    t1 >> t2 >> t3
