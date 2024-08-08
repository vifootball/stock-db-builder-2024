from typing import Union, Optional
import pandas as pd
import yfinance as yf
import datetime as dt
import pandas_datareader.data as web # FRED Data


def get_history_from_yf(symbol: str) -> Optional[pd.DataFrame]:
    # get raw data    
    # history = yf.Ticker(symbol).history(period='max').reset_index(drop=False) # date가 index
    history = yf.Ticker(symbol).history(period='100000mo')
    if history.empty: # 데이터프레임 비어있으면 "max" 파라미터로 다시 받아온다
        history = yf.Ticker(symbol).history(period='max')

    history = history.reset_index(drop=False) # date가 index
    
    if len(history) > 50:
    # 데이터가 어느정도 있으면서 최근까지 업데이트 되는 종목만 수집하고자 함 
    # 정상데이터가 아니라면, 데이터가 아예 없거나 과거 데이터만 있음
        today = dt.datetime.today()
        last_traded_day = history['Date'].max().replace(tzinfo=None)
        days_from_last_traded = (today - last_traded_day)
        if days_from_last_traded < pd.Timedelta('50 days'):
            # table handling
            history['symbol'] = symbol.upper()
            history['Date'] = history['Date'].dt.strftime('%Y-%m-%d')
            history = history.rename(columns={
                'symbol' : 'symbol',
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Dividends': 'dividend',
                'Stock Splits': 'stock_split',
                'Capital Gains': 'capital_gain'
            })
            history = history[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split']]
    
        else:
            history = None
    else:
        history = None
    return history


def get_history_from_fred(symbol: str) -> Union[pd.DataFrame, None]:
    try:
        # get raw data
        start, end = (dt.datetime(1800, 1, 1), dt.datetime.today())
        history = web.DataReader(symbol, 'fred', start, end)#.asfreq(freq='1d', method='ffill').reset_index(drop=False)
        history = history.reset_index()
        history.rename(columns={
            f'{symbol}': 'close',
            'DATE': 'date'
        }, inplace=True)
        history['symbol'] = symbol.upper()
        history['date'] = history['date'].dt.strftime('%Y-%m-%d')            

        header = pd.DataFrame(columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'stock_split'])
        history = pd.concat([header, history])
    except:
        history = None
    return history