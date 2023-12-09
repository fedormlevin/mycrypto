from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

EXCHANGE = 'binance'
STOP_AFTER = 100
ENDPOINT = 'wss://stream.binance.us:9443/ws'
BATCH_SIZE = 100

default_args = {
    'depends_on_past': False,
    'catchup': False,
    'start_date': (datetime.utcnow() - timedelta(days=1)),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'schedule': "10 00 * * *"
}

with DAG(
    dag_id=f"{EXCHANGE}_streams",
    start_date=(datetime.utcnow() - timedelta(days=1)),
    schedule="10 00 * * *",
    catchup=False
) as dag:
    
    task1 = BashOperator(
        task_id='consume_order_book_stream',
        bash_command=f'python $HOME/develop/mycrypto/src/python_scripts/{EXCHANGE}'
        f'{EXCHANGE}_clickhouse.py '
        '--table binance_ticker_order_book_stream '
        f'--endpoint {ENDPOINT} '
        f'-b {BATCH_SIZE} '
        f'--log-name {EXCHANGE}_order_book '
        f'--stop-after {STOP_AFTER} '
    )
    
    task2 = BashOperator(
        task_id='dummy_task',
        bash_command='echo second stream '
    )
    