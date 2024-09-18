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

