import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator

with DAG(
    dag_id="my_dag",
    start_date=pendulum.datetime(2023, 12, 8),
    schedule="30 15 * * *",
    catchup=False
    
) as dag:
    
    task1 = BashOperator(
        task_id="log_task", bash_command="bash $HOME/test/test_schedule.sh ")
    
    task1