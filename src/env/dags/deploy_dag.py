from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

with DAG(
    dag_id="deploy",
    start_date=(datetime.utcnow() - timedelta(days=1)),
    schedule="10 00 * * *",
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id='deploy_crontab',
        bash_command='crontab $HOME/develop/mycrypto/src/env/crontab '
    )
    
    task2 = BashOperator(
        task_id='deploy_bashrc',
        bash_command='rsync -av $HOME/develop/mycrypto/src/env/.bashrc $HOME/ '
    )
    
    task1 >> task2