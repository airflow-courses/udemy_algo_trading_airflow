from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
from io import StringIO
import csv
import tinvest
from configparser import ConfigParser
import os
from datetime import date, datetime, timedelta


config = ConfigParser(os.environ)
config.read('/usr/local/airflow/tinkoff.cfg')
token = config.get('core', 'TOKEN_TINKOFF')
TOKEN = config['TOKEN_TINKOFF'] if not token else token

USE_SANDBOX = bool(config['core']['USE_SANDBOX'])


def get_db_url(connector: str, for_alchemy=False, driver='postgresql+psycopg2'):
    connection = BaseHook.get_connection(connector)
    password = connection.password
    host = connection.host
    login = connection.login
    schema = connection.schema
    port = connection.port
    if for_alchemy:
        return f'{driver}://{login}:{password}@{host}/{schema}'
    return f'user={login} password={password} host={host} port={port} dbname={schema}'


def get_data_from_table(dns: str, table_name: str, query: str = None) -> pd.DataFrame:
    """
    Параметры:
    table_name: str - Название таблицы в формате строки

    Функция подключается к БД и забирает данные из таблицы,
    возвращая их в виде pd.DataFrame
    """
    conn = psycopg2.connect(dsn=dns)
    if query is None:
        query = f'SELECT * FROM {table_name}'
    try:
        df = pd.read_sql(query.format(table_name, table_name), conn)
    except:
        df = None
    conn.close()

    return df


def load_dataframe_to_table(dns: str, df: pd.DataFrame, table_name: str) -> None:
    """
    Параметры:
    df: pd.DataFrame - Данные для загрузки
    table_name: str - Название таблицы для загрузки данных

    Функция подключается к staging и копирует данные через буффер,
    который представлен в виде CSV-файла
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False, sep='|', na_rep='NUL', quoting=csv.QUOTE_MINIMAL,
              header=False, float_format='%.8f', doublequote=False, escapechar='\\')
    buffer.seek(0)
    copy_query = f"""
                COPY {table_name}({', '.join(df.columns)})
                FROM STDIN
                DELIMITER '|'
                NULL 'NUL'
    """
    conn = psycopg2.connect(dsn=dns)
    with conn.cursor() as cursor:
        cursor.copy_expert(copy_query, buffer)
    conn.commit()
    conn.close()


def truncate_table(dns: str, table_name: str) -> None:
    """
    Параметры:
    table_name: str - Название таблицы для очистки

    Функция очищает таблицу
    """
    query = f'TRUNCATE TABLE {table_name}'
    conn = psycopg2.connect(dsn=dns)
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    conn.close()


def load_stock_prices_by_ticker(figi: str) -> pd.DataFrame:
    client = tinvest.SyncClient(TOKEN, use_sandbox=USE_SANDBOX)

    now = date.today()
    candles = client.get_market_candles(
        figi,
        datetime.strptime((now - timedelta(days=365)).strftime('%Y-%m-%d'), '%Y-%m-%d'),
        datetime.strptime(now.strftime('%Y-%m-%d'), '%Y-%m-%d'),
        tinvest.schemas.CandleResolution.day
    )

    return pd.DataFrame(
        ((candle['time'], candle['c']) for candle in candles.dict()['payload']['candles']),
        columns=('time', 'close_price')
    )


def get_close_price_by_date(figi: str, date_: datetime) -> pd.DataFrame:
    client = tinvest.SyncClient(TOKEN, use_sandbox=USE_SANDBOX)

    candle = client.get_market_candles(
        figi,
        datetime.strptime((date_ - timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d'),
        datetime.strptime(date_.strftime('%Y-%m-%d'), '%Y-%m-%d'),
        tinvest.schemas.CandleResolution.day
    ).dict()['payload']['candles'][-1]

    return pd.DataFrame(
        [[
            candle['time'],
            candle['c']
        ]],
        columns=('time', 'close_price')
    )


def get_close_price_by_period(figi: str, from_: datetime, to_: datetime) -> pd.DataFrame:
    client = tinvest.SyncClient(TOKEN, use_sandbox=USE_SANDBOX)

    candles = client.get_market_candles(
        figi,
        from_,
        to_,
        tinvest.schemas.CandleResolution.day
    ).dict()['payload']['candles']

    return pd.DataFrame(
        ((candle['time'], candle['c']) for candle in candles.dict()['payload']['candles']),
        columns=('time', 'close_price')
    )


def get_prices_from_db(dsn: str, table_name: str, limit: int) -> pd.DataFrame:
    query = f"""
        SELECT time, close_price
        FROM {table_name}
        ORDER BY time DESC
        LIMIT {limit}
    """
    with psycopg2.connect(dsn=dsn) as conn:
        return pd.read_sql(query, conn).sort_values('time')


def create_name_cols(cols):
    new_cols = []
    for col in cols:
        new_cols.append('_'.join([c.lower() for c in reversed(col) if c]))
    return new_cols


def prepare_col_name(col_name) -> str:
    return col_name.lower() \
            .replace(' ', '_') \
            .replace(')', '') \
            .replace('(', '') \
            .replace('&', '_and_') \
            .replace('\\', '_') \
            .replace('/', '_') \
            .replace('-', '') \
            .replace('+', '') \
            .replace('___', '_') \
            .replace('__', '_') \
            .replace('%', 'perc')
