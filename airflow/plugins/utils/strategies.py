import pandas as pd
from utils import load_utils
import numpy as np


def sma_strategy(dsn: str, input_table_name: str, sma_short: int, sma_long: int) -> pd.DataFrame:
    data = load_utils.get_prices_from_db(dsn, input_table_name, sma_long + 5)
    data['sma_short'] = data['close_price'].rolling(sma_short).mean()
    data['sma_long'] = data['close_price'].rolling(sma_long).mean()

    data = data.dropna()
    data['position'] = np.where(data['sma_short'] > data['sma_long'], 1, -1)

    return data.tail(1)[['time', 'position']].assign(strategy_type='cross_sma')


def bollinger_strategy(dsn: str, input_table_name: str, sma: int, dev: int) -> pd.DataFrame:
    data = load_utils.get_prices_from_db(dsn, input_table_name, limit=sma + 5)
    data['sma'] = data['close_price'].rolling(sma).mean()
    data_dev = data['close_price'].rolling(sma).std() * dev
    data['lower'] = data['sma'] - data_dev
    data['upper'] = data['sma'] + data_dev

    data = data.dropna()
    data['distance'] = data['close_price'] - data['sma']
    data['position'] = np.where(data['close_price'] < data['lower'], 1, np.nan)
    data['position'] = np.where(data['close_price'] > data['upper'], -1, data['position'])
    data['position'] = np.where(data['distance'] * data['distance'].shift(1) < 0, 0, data['position'])
    data['position'] = data['position'].ffill().fillna(0)
    data['position'] = data['position'].astype('int8')

    return data.tail(1)[['time', 'position']].assign(strategy_type='bollinger')
