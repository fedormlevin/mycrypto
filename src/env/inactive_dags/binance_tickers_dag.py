from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 


default_args = {
    'owner': 'fedor',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    
}

STOP = 100


with DAG(
    dag_id='binance_orders',
    default_args=default_args,
    description='The Trade Streams push raw trade information; each trade has a unique buyer and seller',
    start_date=(datetime.utcnow() - timedelta(days=1)),
    schedule_interval='05 0 * * *', 
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='binance_orders_feed',
        bash_command=(
            "python /Users/fedorlevin/workspace/mycrypto/src/python_scripts/binance/"
            "binance_clickhouse.py "
            "--table binance_symbol_ticker_stream "
            "--endpoint wss://stream.binance.us:9443/ws "
            "-b 2 "
            "--log-name binance_tickers "
            f"--stop-after {STOP}"
        )
    )

    task1
    