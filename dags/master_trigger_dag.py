from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    "master_trigger_dag",
    start_date=datetime(2025,10,6),
    schedule_interval=None,
    catchup=False,
) as dag:
    start_pipeline = TriggerDagRunOperator(task_id="start_extract", trigger_dag_id="dag_1_extract")
    start_pipeline
