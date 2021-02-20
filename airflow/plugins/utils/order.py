from utils import db
import pandas as pd
import numpy as np


def create_limit_order_by_signals(connector: str, weights: dict, threshold: float = 0.5) -> None:
    signals = db.get_data_from_table(
        connector,
        'signal',
        'WHERE time = (SELECT max(time) FROM signal)',
    )
    signals['weights'] = signals['strategy_type'].map(weights)
    signals['weighted_position'] = signals['weights'] * signals['position']
    if signals['weighted_position'].sum() < -threshold:
        # check portfolio for short position by this ticker
        # sell
        ...
    elif signals['weighted_position'] > threshold:
        # check portfolio for long position by this ticker
        # buy
        ...
