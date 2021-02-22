from utils import db
import pandas as pd
import numpy as np
from utils import tinkoff
import math


def _get_last_price_from_table(connector: str, table_name: str) -> float:
    data = db.get_data_from_price_table(
        connector,
        table_name,
        f"""
            WHERE time = (SELECT max(time) FROM {table_name})
        """
    )
    data = data.sort_values(by='time', ascending=False)

    return data.iloc[0]['close']


def create_limit_order_by_signals(
        connector: str,
        ticker: str,
        weights: dict,
        threshold: float = 0.5
) -> None:
    signals = db.get_data_from_signal_table(
        connector,
        f"WHERE time = (SELECT max(time) FROM signal) AND lower(ticker) = lower('{ticker}')",
    )
    signals['weights'] = signals['strategy_type'].map(weights)
    signals['weighted_position'] = signals['weights'] * signals['position']
    position = tinkoff.get_position_by_ticker(ticker)
    last_price = _get_last_price_from_table(connector, ticker.lower())

    if signals['weighted_position'].sum() > threshold:
        if not position:
            tinkoff.create_limit_order_by_figi(
                tinkoff.get_figi_by_ticker(ticker),
                int(tinkoff.get_current_balance('USD') * 0.1 // last_price),
                math.ceil(last_price * 100) / 100.0,
                'Buy'
            )

    elif signals['weighted_position'].sum() < -threshold:
        if position:
            tinkoff.create_limit_order_by_figi(
                position.figi,
                position.lots,
                math.floor(last_price * 100) / 100.0,
                'Sell'
            )

