import tinvest
import pandas as pd
from configparser import ConfigParser
from datetime import datetime, timedelta
from typing import Optional


def _get_api_params_from_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read('/usr/local/airflow/tinkoff.cfg')

    return {
        'token': config_parser.get('core', 'TOKEN_TINKOFF'),
        'use_sandbox': config_parser.get('core', 'USE_SANDBOX')
    }


def get_figi_from_ticker(ticker: str) -> str:
    client = tinvest.SyncClient(**_get_api_params_from_config())
    ticker_data = client.get_market_search_by_ticker(ticker)
    return ticker_data.payload.instruments[0].figi


def get_data_by_ticker_and_period(
        ticker: str,
        period_in_days: int = 365,
        freq: tinvest.CandleResolution = tinvest.CandleResolution.day
) -> pd.DataFrame:
    client = tinvest.SyncClient(**_get_api_params_from_config())
    figi = get_figi_from_ticker(ticker)

    raw_data = client.get_market_candles(
        figi,
        datetime.now() - timedelta(days=period_in_days),
        datetime.now() - timedelta(days=1),
        freq,
    )

    return pd.DataFrame(
        data=(
            (
                candle.time,
                candle.o,
                candle.h,
                candle.l,
                candle.c,
                candle.v,
            ) for candle in raw_data.payload.candles
        ),
        columns=(
            'time',
            'open',
            'high',
            'low',
            'close',
            'volume',
        )
    )


def get_position_by_ticker(ticker: str) -> Optional[tinvest.PortfolioPosition]:
    client = tinvest.SyncClient(**_get_api_params_from_config())
    positions = client.get_portfolio().payload.positions

    filtered_positions = list(filter(lambda x: x.ticker.lower() == ticker.lower(), positions))
    if len(filtered_positions) == 0:
        return None
    return filtered_positions[0]


def create_limit_order_by_figi(
        figi: str,
        lots: int,
        price: float,
        op_type: str = 'Buy'
) -> None:
    if op_type not in ('Buy', 'Sell'):
        raise ValueError('Operation type must be Sell or Buy with upper-case first letter')
    client = tinvest.SyncClient(**_get_api_params_from_config())
    client.post_orders_limit_order(
        figi=figi,
        body=tinvest.schemas.LimitOrderRequest(
            lots=lots,
            operation=tinvest.schemas.OperationType(value=op_type),
            price=price
        )
    )


def get_current_balance(currency_type: str) -> float:
    client = tinvest.SyncClient(**_get_api_params_from_config())
    currencies = client.get_portfolio_currencies().payload.currencies

    filtered_currencies = list(
        filter(lambda x: x.currency.lower() == currency_type.lower(), currencies)
    )
    if len(filtered_currencies) == 0:
        return 0.0
    return float(filtered_currencies[0].balance)











