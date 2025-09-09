# airflow/dags/etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Import du script batch_etl.py
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(DAGS_DIR)

from batch_etl import run 

# ================================
# Config du DAG
# ================================
default_args = {
    "owner": "dataflow360",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_inplay_counts",
    description="DAG pour calculer et stocker les agrégats de matchs IN_PLAY",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval="*/5 * * * *",  
    catchup=False,
    tags=["etl", "dataflow360"],
) as dag:

    # ================================
    # Tâche Python
    # ================================
    etl_task = PythonOperator(
        task_id="run_batch_etl",
        python_callable=run
    )

    etl_task
