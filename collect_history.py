import pandas as pd
import os
from constants import *
from history import *
from preprocess_history import *
import ray
import time
import warnings
from tqdm import tqdm

warnings.filterwarnings('ignore', category=FutureWarning)


# LOAD SYMBOLS

# LOAD SYMBOLS - ETF
etf_master = pd.read_csv(os.path.join('downloads', 'master', 'etf_master.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE][:100]
enumerated_etf_symbols = list(enumerate(etf_symbols)) # ray에러 방지용으로 반복문마다 실행순서를 부여하고 이에 따라 약간의 delay를 주는데, 그 순서를 enumerate로 가져옴

# LOAD SYMBOLS - CURRENCY
currency_master = pd.read_csv(os.path.join('downloads', 'master', 'currency_master.csv'))
currency_symbols = currency_master['symbol'].to_list()
enumerated_currency_symbols = list(enumerate(currency_symbols))

# LOAD SYMBOLS - INDEX FOR YAHOO
indices_master_yahoo = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_yahoo.csv'))
indices_symbols_yahoo = indices_master_yahoo['symbol'].to_list()

indices_master_investpy = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_investpy.csv'))
indices_symbols_investpy = indices_master_investpy['symbol'].to_list()

indices_master_fd = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_fd.csv'))
indices_symbols_fd = indices_master_fd['symbol'].to_list()

indices_symbols_general = list(set(indices_symbols_yahoo + indices_symbols_investpy + indices_symbols_fd))
enumerated_indices_symbols_general = list(enumerate(indices_symbols_general))

# LOAD SYMBOLS - INDEX FOR FRED
indices_master_fred = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_fred.csv'))
indices_symbols_fred = list(enumerate(indices_master_fred['symbol'].to_list()))


#### USING RAY
@ray.remote
def collect_history(order, symbol, method='yf', put_dirpath='./downloads/history/etf'):
    warnings.filterwarnings('ignore', category=FutureWarning)
    time.sleep(0.3)
    time.sleep((order % 8) * 0.08) # num_cpus: 8  # delay: 0.12 seconds
    
    if method == 'yf':
        history = transform_history(get_history_from_yf(symbol))
    elif method == 'fred':
        history = transform_history(get_history_from_fred(symbol))
    else:
        raise ValueError("method must be one of ['yf', 'fred']")
    
    if history is not None:
        os.makedirs(put_dirpath, exist_ok=True)
        history.to_csv(f'{put_dirpath}/{symbol}_history.csv', index=False)
        print(f"[INFO] [{order}] {symbol}: Succesfully collected history.")
    else:
        print(f"[WARNING] [{order}] {symbol}: Failed to collect history.")


#### USING FOR-LOOP
def collect_etf_history_from_yf_with_for_loop(etf_symbols):
    for symbol in tqdm(etf_symbols, mininterval=0.5, total=len(etf_symbols)):
        history = transform_history(get_history_from_yf(symbol))
        if history is not None:
            os.makedirs('downloads/history/etf', exist_ok=True)
            history.to_csv(f'downloads/history/etf/{symbol}_history.csv', index=False)

