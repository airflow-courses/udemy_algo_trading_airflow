from airflow import DAG
import os
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from utils.load_utils import load_dataframe_to_table, get_db_url
from utils.strategies import bollinger_strategy
from utils.constants import Constants

CONNECTION_ID = 'postgres_staging'
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DSN = get_db_url(CONNECTION_ID)

SMA = 30
DEV = 2

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    # 'email': ['dwh@advgroup.ru'],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='15 1 * * *'
) as dag:

    load_signal_bollinger_aapl = PythonOperator(
        task_id='load_signal_bollinger_aapl',
        python_callable=load_dataframe_to_table,
        op_kwargs={
            'dns': DSN,
            'df': bollinger_strategy(
                DSN, Constants.AAPL_TABLE_NAME,
                SMA, DEV
            ).assign(figi=Constants.AAPL_FIGI),
            'table_name': Constants.SIGNAL_TABLE_NAME
        }
    )