from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 


default_args = {
    'owner': 'fedor',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    
}


with DAG(
    dag_id='ch_md_tables_cleanup',
    default_args=default_args,
    description='Cleans CH tables',
    start_date=(datetime.utcnow() - timedelta(days=1)),
    schedule_interval='05 0 * * *', 
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='ch_md_tables_clean',
        bash_command=(
            "python ~/develop/mycrypto/src/python_scripts/infra/"
            "cleanup_ch.py "
            "--days 5"
        )
    )

    task1
    