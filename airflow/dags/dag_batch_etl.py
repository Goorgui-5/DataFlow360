# airflow/dags/dag_batch_etl.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from batch_etl import run as batch_run


default_args = {
    "owner": "dataflow360",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="batch_inplay_aggregation",
    description="Agrège les matchs IN_PLAY par compétition et stocke dans Postgres",
    start_date=datetime(2025, 8, 1),
    schedule_interval="*/2 * * * *", # toutes les 2 minutes 
    catchup=False,
    default_args=default_args,
    tags=["batch", "football", "postgres"],
) as dag:

    aggregate_inplay = PythonOperator(
        task_id="aggregate_inplay_counts",
        python_callable=batch_run,
    )

    aggregate_inplay
