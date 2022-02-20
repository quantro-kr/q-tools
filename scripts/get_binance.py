import calendar
import json

import fire
import os
import pandas as pd
import requests

from datetime import datetime, timedelta, timezone
from time import sleep

from os.path import expanduser
from pytz import UTC


def ts2dt(ts):
    return datetime.fromtimestamp(ts / 1000, timezone.utc)


def dt2ts(date):
    return int(calendar.timegm(date.timetuple()) * 1000 + date.microsecond / 1000)


def get_interval(interval):
    for iv in ['1m', '1h', '4h', '6h', '1d']:
        if pd.Timedelta(interval) == pd.Timedelta(iv):
            return iv
    raise ValueError("unknown interval")


def fetch_klines(symbol, startTime: datetime, endTime: datetime = None, interval='1d', market='SPOT', debug=True,
                 limit=None):
    if market == 'SPOT':
        endpoint = 'https://api.binance.com/api/v3/klines'
        limit = 1000 if limit is None else limit
    elif market == 'UMFUTURES':
        endpoint = 'https://fapi.binance.com/fapi/v1/klines'
        limit = 1500 if limit is None else limit

    endTime = datetime.today().astimezone(timezone.utc) if endTime is None else endTime

    params = {
        "symbol": symbol,
        "interval": interval if interval != '1min' else '1m',
        "startTime": dt2ts(startTime),
        "endTime": dt2ts(endTime),
        "limit": limit
    }

    next = startTime

    print(f'Start downloading {symbol}...')
    while next.timestamp() <= endTime.timestamp():
        resp = requests.get(endpoint, params=params)
        result = json.loads(resp.text)
        if debug:
            print(
                f"fetch_klines({ts2dt(params['startTime'])}-{ts2dt(params['endTime']) if 'endTime' in params else ''}):[{market}]{params} ")
        if "code" in result:
            print("retry...", result)
            sleep(10)
        elif result:
            for it in result:
                current = ts2dt(it[0])
                if current.timestamp() > endTime.timestamp():
                    break
                yield it
            next = current + pd.Timedelta(interval)
            params['startTime'] = dt2ts(next)
        else:
            print(f"skip as no data")


def spot_klines(symbol, interval='1d', startTime: datetime = None, endTime: datetime = None, limit=500, debug=True):
    return fetch_klines(symbol, startTime, endTime, interval, 'SPOT', debug, limit)


def futures_klines(symbol, interval='1d', startTime: datetime = None, endTime: datetime = None, limit=1500, debug=True):
    return fetch_klines(symbol, startTime, endTime, interval, 'UMFUTURES', debug, limit)


class Main(object):
    """
    바이낸스 현/선물 OHLCV 데이터를 csv 형태로 저장한다.

    실행 :

    python scripts/get_binance.py download_csv --market=futures --symbol=btcusdt --interval=1d --start_time=2021-01-01 --out=download
        => 2020년 부터 오늘까지의 BTCUSDT 선물 일봉 데이터를 download/futures/1d/BTCUSDT.csv 에 저장

    python scripts/get_binance.py download_csv --market=futures --symbol=all --interval=1d --start_time=2021-01-01 --end_time=2021-01-31 --out=download
        => 2021 년 사이의 모든 USDT 선물의 일봉 데이터를 download/futures/1d/*.csv 에 저장
    """

    @staticmethod
    def download_csv(market='futures', symbol='btcusdt', start_time='2018-01-01', end_time=None, interval='1d',
                     out='download', currency='usdt'):
        """
        :param market: 현물은 spot, 선물은 futures
        :param symbol: 전체 다운로드시 ALL, 개별 다운로드시 해당 심볼(BTCUSDT, ETHUSDT 등)
        :param start_time: 다운로드 시작일을 YYYY-MM-DD 형식으로 지정 (예: 2020-01-01)
        :param end_time: 다운로드 종료일을 YYYY-MM-DD 형식으로 지정. 미지정시 오늘 날짜
        :param interval: 데이터 간격. 1d|6h|4h|1h|1m
        :param out: 다운로드 폴더. 지정된 폴더 이하로 <market>/<interval>/<symbol.csv> 형식으로 저장됨
        :param currency: usdt, usd 등 기준 화폐
        """
        symbol = symbol.upper()
        currency = currency.upper()
        market = market.lower()

        if symbol == 'ALL':
            for symbol in Main._get_symbols(market, currency):
                Main._download_csv(symbol, start_time, end_time, interval, out, market)
        else:
            Main._download_csv(symbol, start_time, end_time, interval, out, market)

    @staticmethod
    def _download_csv(symbol='btcusdt', start_time='20180101', end_time=None, interval='1d', out='download',
                      market='futures'):
        def _datetime_str(t):
            dt = ts2dt(int(t))
            return dt.strftime('%Y-%m-%d') if interval == '1d' else dt.strftime('%Y-%m-%d %H:%M:%S')

        out = os.path.join(out, market, interval)
        os.makedirs(out, exist_ok=True)
        names = ['date', 'open', 'high', 'low', 'close', 'volume']
        csv_path = expanduser(f'{out}/{symbol}.csv')

        date_fmt = '%Y-%m-%d' if interval == '1d' else '%Y-%m-%d %H:%M:%S'

        df0 = pd.DataFrame([], columns=names)
        if os.path.exists(csv_path):
            df0 = pd.read_csv(csv_path, usecols=names, dtype=str)
            print(f"Loaded {csv_path} :\n{df0.head(1)}\n...\n{df0.tail(1)}")
            last_dt = datetime.strptime(df0.iloc[-1][0], date_fmt)
            start_dt = datetime.strptime(str(start_time), '%Y-%m-%d')
            if (start_dt - last_dt).days > 1:
                raise ValueError(f'exists missing period: csv_last_dt={last_dt}, start_dt={start_dt}')
            else:
                start_time = max(start_dt, last_dt + timedelta(days=1)).strftime('%Y-%m-%d')

        start_time = UTC.localize(datetime.strptime(str(start_time), "%Y-%m-%d"))
        if end_time is None:
            end_time = datetime.today().astimezone(timezone.utc) - timedelta(minutes=1)
        else:
            end_time = UTC.localize(datetime.strptime(str(end_time), "%Y-%m-%d")) - timedelta(minutes=1)

        if market == 'futures':
            data = list(futures_klines(symbol, startTime=start_time, endTime=end_time, interval=interval))
        elif market == 'spot':
            data = list(spot_klines(symbol, startTime=start_time, endTime=end_time, interval=interval))
        else:
            raise ValueError(f'market({market}) should be "futures" or "spot".')

        if data and len(data) > 0:
            df = pd.DataFrame(data, dtype=str)
            df = df.drop(df.columns[len(names):], axis=1)
            df.columns = names
            df['date'] = df['date'].map(_datetime_str)
            print(f"Fetch data :\n{df.head(1)}\n...\n{df.tail(1)}")
            df = pd.concat([df0, df], ignore_index=True, axis=0)
            df.to_csv(csv_path, index=False)
        else:
            print(f'skip as no new data since {start_time}')

    @staticmethod
    def _get_symbols(market, currency):
        if market == 'futures':
            url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        else:
            url = 'https://api.binance.com/api/v3/exchangeInfo'
        result = json.loads(requests.get(url).text)
        if 'symbols' not in result:
            print('error: result=',result)
            return []
        symbols = result['symbols']
        return [it['symbol'] for it in symbols if it['symbol'].endswith(currency)]


if __name__ == '__main__':
    fire.Fire(Main)
