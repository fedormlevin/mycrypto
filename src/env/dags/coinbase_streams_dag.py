from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

EXCHANGE = 'coinbase'
STOP_AFTER = 6000
ENDPOINT = 'wss://advanced-trade-ws.coinbase.com'
BATCH_SIZE = 100
SCHEDULE="11 00 * * *",

default_args = {
    'depends_on_past': False,
    'catchup': False,
    'start_date': (datetime.utcnow() - timedelta(days=1)),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    dag_id=f"{EXCHANGE}_streams",
    default_args=default_args,
    description=f'{EXCHANGE} market data feeds',
    schedule=SCHEDULE
) as dag:
    
    task1 = BashOperator(
        task_id='consume_trade_stream',
        bash_command=f'python $HOME/develop/mycrypto/src/python_scripts/{EXCHANGE}/'
        f'{EXCHANGE}_clickhouse.py '
        '--table coinbase_market_trades_stream '
        f'--endpoint {ENDPOINT} '
        f'-b {BATCH_SIZE} '
        f'--log-name {EXCHANGE}_trades '
        f'--stop-after {STOP_AFTER} '
    )
    
    task2 = BashOperator(
        task_id='dummy_task',
        bash_command='echo second stream '
    )
    