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
    citybike_trips_bronze = BashOperator(
        task_id="citybike_trips_bronze",
        bash_command="cd ~/dbt-intro && dbt run -s citybike_trips_bronze",
    )

    citybike_trips_silver = BashOperator(
        task_id="citybike_trips_silver",
        bash_command="cd ~/dbt-intro && dbt run -s citybike_trips_silver",
    )

    citybike_trips_gold = BashOperator(
        task_id="citybike_trips_gold",
        bash_command="cd ~/dbt-intro && dbt run -s citybike_trips_gold",
    )

    my_fourth_dbt_model = BashOperator(
        task_id="my_fourth_dbt_model_task",
        bash_command="cd ~/dbt-intro && dbt run -s my_fourth_dbt_model",
    )

    citybike_trips_bronze >> citybike_trips_silver >> citybike_trips_gold >> my_fourth_dbt_model


airflow_dbt_dag()