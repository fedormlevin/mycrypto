from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator 


default_args = {
    'owner': 'fedor',
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    
}

STOP=100

with DAG(
    dag_id='coinbase_trades',
    default_args=default_args,
    description='The Trade Streams push raw trade information; each trade has a unique buyer and seller',
    start_date=(datetime.utcnow() - timedelta(days=1)),
    schedule_interval='05 0 * * *', 
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='coinbase_trades_feed',
        bash_command=(
            "python /Users/fedorlevin/develop/mycrypto/src/python_scripts/coinbase/"
            "coinbase_clickhouse.py "
            "--table coinbase_market_trades_stream "
            "--endpoint wss://advanced-trade-ws.coinbase.com "
            "-b 2 "
            "--log-name coinbase_trades "
            f"--stop-after {STOP}"
        )
    )

    task1
    