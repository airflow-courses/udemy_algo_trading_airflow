from airflow import DAG
import os
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from utils.load_utils import load_dataframe_to_table, get_close_price_by_date, get_db_url
from utils.constants import Constants

CONNECTION_ID = 'postgres_staging'
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DNS = get_db_url(CONNECTION_ID)

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
    schedule_interval='* 1 * * *',
) as dag:

    update_close_price_aapl = PythonOperator(
        task_id='update_close_price_aapl',
        python_callable=load_dataframe_to_table,
        op_kwargs={
            'dns': DNS,
            'df': get_close_price_by_date(Constants.AAPL_FIGI, datetime.now()),
            'table_name': Constants.AAPL_TABLE_NAME,
        }
    )

