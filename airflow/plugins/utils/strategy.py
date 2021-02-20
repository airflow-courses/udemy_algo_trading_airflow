import pandas as pd
import numpy as np
from utils import db


def cross_sma_strategy(
        data: pd.DataFrame,
        sma_short: int,
        sma_long: int,
) -> pd.DataFrame:
    data['sma_short'] = data['close'].rolling(sma_short).mean()
    data['sma_long'] = data['close'].rolling(sma_long).mean()

    data['position'] = np.where(data['sma_short'] > data['sma_long'], 1, -1)

    return data[['time', 'position']].tail(1).assign(strategy_type='sma')
