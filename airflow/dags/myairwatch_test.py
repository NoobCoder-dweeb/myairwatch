# import BashOperator and DAG
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="myairwatch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test = BashOperator(
        task_id="test_local_pipeline", bash_command="echo 'Pipeline placeholder works'"
    )
