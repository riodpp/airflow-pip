from airflow.decorators import (
    dag,
    task,
)
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["example"],
)
def airflow_dbt_dag():
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="mkdir dbt && cp -r ~/Documents/DataEngineerProject/dbt-intro/dbt-intro ./dbt && pwd",
    )

    dbt_run

airflow_dbt_dag()