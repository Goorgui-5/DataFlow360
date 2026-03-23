from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

log = logging.getLogger("dataflow360_dag")

PROJECT_PATH = "/home/goorgui/Bureau/ODC_DEV_DATA/PROJET_DATAFLOW360/DataFLOW360/generator/streaming_foot"

default_args = {
    "owner": "serigne",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="pipeline_dataflow360",
    default_args=default_args,
    description="Pipeline complet DataFlow360 : Kafka -> Postgres -> DW",
    schedule="0 * * * *",
    catchup=False,
    tags=["football", "dataflow360", "ETL"],
)

def check_postgres():
    import psycopg2
    conn = psycopg2.connect(host="localhost", port=5432, database="dataflow360", user="postgres", password="nnbbvv")
    conn.close()
    log.info("PostgreSQL OK")

def check_kafka():
    from kafka import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="airflow_check")
    admin.list_topics()
    admin.close()
    log.info("Kafka OK")

def run_load_dw():
    import sys
    sys.path.insert(0, PROJECT_PATH)
    from load_dw import run
    run()

def run_quality_checks():
    import psycopg2
    conn = psycopg2.connect(host="localhost", port=5432, database="dataflow360", user="postgres", password="nnbbvv")
    cur = conn.cursor()
    for table in ["dw.dim_equipe", "dw.dim_competition", "dw.dim_date", "dw.fait_match"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        n = cur.fetchone()[0]
        log.info(f"{table} : {n} lignes")
    cur.close()
    conn.close()

t1 = PythonOperator(task_id="check_postgres",    python_callable=check_postgres,    dag=dag)
t2 = PythonOperator(task_id="check_kafka",       python_callable=check_kafka,       dag=dag)
t3 = BashOperator(  task_id="run_kafka_producer", bash_command=f"cd {PROJECT_PATH} && timeout 90 python streaming_api_producer.py || true", dag=dag)
t4 = BashOperator(  task_id="run_kafka_consumer", bash_command=f"cd {PROJECT_PATH} && timeout 60 python kafka_to_postgres.py || true",      dag=dag)
t5 = PythonOperator(task_id="load_dw",           python_callable=run_load_dw,       dag=dag)
t6 = PythonOperator(task_id="quality_check",     python_callable=run_quality_checks, dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
