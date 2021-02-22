import pandas as pd
import numpy as np
from typing import Callable
from utils import db


def apply_strategy(
        connector: str,
        source_table_name: str,
        ticker: str,
        strategy_func: Callable,
        op_kwargs: dict
) -> None:
    data = db.get_data_from_price_table(connector, source_table_name)
    signal = strategy_func(data, **op_kwargs).assign(ticker=ticker)
    db.load_df_to_db(connector, signal, 'signal')


def cross_sma_strategy(
        data: pd.DataFrame,
        sma_short: int,
        sma_long: int
) -> pd.DataFrame:
    data['sma_short'] = data['close'].rolling(sma_short).mean()
    data['sma_long'] = data['close'].rolling(sma_long).mean()

    data['position'] = np.where(data['sma_short'] > data['sma_long'], 1, -1)
    data['strategy_type'] = 'sma'

    return data[['time', 'position']].tail(1).assign(strategy_type='sma')
