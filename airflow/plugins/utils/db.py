import psycopg2  # метод copy в postgres csv прямо в бд так быстрее
from io import StringIO  # для имитации csv
import csv
from airflow.hooks.base_hook import BaseHook
import pandas as pd


def _get_db_url(connector: str) -> str:
    connection = BaseHook.get_connection(connector)

    return f'user={connection.login} password={connection.password} host={connection.host} ' \
           f'port={connection.port} dbname={connection.schema}'


def load_df_to_db(connector: str, df: pd.DataFrame, table_name: str) -> None:
    buffer = StringIO()
    df.to_csv(buffer, index=False, sep='|', na_rep='NUL', quoting=csv.QUOTE_MINIMAL,
              header=False, float_format='%.8f', doublequote=False, escapechar='\\')
    buffer.seek(0)  # буфер будет ссылаться на посдежнюю позицию, поэтому нужно вернуться на начало
    copy_query = f"""
        COPY {table_name}({','.join(df.columns)})
        FROM STDIN
        DELIMITER '|'
        NULL 'NUL'
    """
    conn = psycopg2.connect(_get_db_url(connector))
    # чтобы коннект автоматом закрылся после запроса
    with conn.cursor() as cursor:
        cursor.copy_expert(copy_query, buffer)
    conn.commit()
    conn.close()


def get_date_from_price_table(connector: str, table_name: str, filter_: str = None) -> pd.DataFrame:
    query = f"""
        SELECT time,
               open,
               high,
               low,
               close,
               volume
        FROM {table_name}
        {filter_}
    """
    with psycopg2.connect(dsn=_get_db_url(connector)) as conn:
        data = pd.read_sql(query, conn)

    return data


def get_date_from_signal_table(connector: str, filter_: str) -> pd.DataFrame:
    query = f"""
            SELECT time,
                   position,
                   strategy_type,
                   ticker
            FROM signal
            {filter_}
        """
    with psycopg2.connect(dsn=_get_db_url(connector)) as conn:
        data = pd.read_sql(query, conn)

    return data


def get_last_price_from_price_table(connector: str, table_name: str) -> float:
    data = get_date_from_price_table(
        connector,
        table_name,
        f"WHERE time = (SELECT max(time) FROM {table_name})"
    )
    data = data.sort_values(by='time', ascending=False)

    return data.iloc[0]['close']
