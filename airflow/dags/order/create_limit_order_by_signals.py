from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import order
from datetime import timedelta
import os

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'postgres_stocks'

STRATEGY_WEIGHTS = {
    'sma': 0.7,
    'bollinger': 0.3,
}
THRESHOLD = 0.5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='20 1 * * *',
) as dag:

    create_limit_order_aapl = PythonOperator(
        task_id='create_limit_order_aapl',
        python_callable=order.create_limit_order_by_signals,
        op_kwargs={
            'connector': CONN_ID,
            'ticker': 'AAPL',
            'weights': STRATEGY_WEIGHTS,
            'threshold': THRESHOLD,
        }
    )
