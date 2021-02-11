import tinvest
import pandas as pd
from configparser import ConfigParser
from datetime import datetime, timedelta


def _get_api_params_from_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read('/usr/local/airflow/tinkoff.cfg')

    return {
        'token': config_parser.get('core', 'TOKEN_TINKOFF'),
        'use_sandbox': config_parser.get('core', 'USE_SANDBOX')
    }


def get_data_by_ticker_and_period(
        ticker: str,
        period_in_days: int = 365,
        freq: tinvest.CandleResolution = tinvest.CandleResolution.day
) -> pd.DataFrame:
    client = tinvest.SyncClient(**_get_api_params_from_config())
    ticker_data = client.get_market_search_by_ticker(ticker)
    figi = ticker_data.payload.instruments[0].figi

    raw_data = client.get_market_candles(
        figi,
        datetime.now() - timedelta(days=period_in_days),
        datetime.now(),
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