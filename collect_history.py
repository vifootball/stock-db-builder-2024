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


#### sybol list 선언

etf_master = pd.read_csv(os.path.join('downloads', 'etf_masters.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE]
enumerated_etf_symbols = list(enumerate(etf_symbols)) # ray에러 방지용으로 반복문마다 실행순서를 부여하고 이에 따라 약간의 delay를 주는데, 그 순서를 enumerate로 가져옴

# currency_master = pd.read_csv(os.path.join('downloads', 'master_symbols', 'masters_currency.csv'))
# currency_symbols = list(enumerate(currency_master['symbol'].to_list()))

# indices_master_yahoo = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_yahoo.csv'))
# indices_symbols_yahoo = list(enumerate(indices_master_yahoo['symbol'].to_list()))

# indices_master_investpy = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_investpy.csv'))
# indices_symbols_investpy = list(enumerate(indices_master_investpy['symbol'].to_list()))

# indices_master_fd = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_fd.csv'))
# indices_symbols_fd = list(enumerate(indices_master_investpy['symbol'].to_list()))

# indices_master_fred = pd.read_csv(os.path.join('downloads', 'masters_indices', 'masters_indices_fred.csv'))
# indices_symbols_fred = list(enumerate(indices_master_fred['symbol'].to_list()))

#### ray

@ray.remote
def collect_etf_history_from_yf(order, symbol):
    warnings.filterwarnings('ignore', category=FutureWarning)
    time.sleep(0.3)
    time.sleep((order % 8) * 0.08) # num_cpus: 8  # delay: 0.12 seconds
    history = transform_history(get_history_from_yf(symbol))
    if history is not None:
        os.makedirs('downloads/history/etf', exist_ok=True)
        history.to_csv(f'downloads/history/etf/{symbol}_history.csv', index=False)
        print(f"[INFO] [{order}] {symbol}: Succesfully collected history.")
    else:
        print(f"[WARNING] [{order}] {symbol}: Failed to collect history.")


#### for - loop
        
def collect_etf_history_from_yf_with_for_loop(etf_symbols):
    for symbol in tqdm(etf_symbols, mininterval=0.5, total=len(etf_symbols)):
        history = transform_history(get_history_from_yf(symbol))
        if history is not None:
            os.makedirs('downloads/history/etf', exist_ok=True)
            history.to_csv(f'downloads/history/etf/{symbol}_history.csv', index=False)

if __name__ == '__main__':

    ####### Collect History with Ray ########
    ray.init(ignore_reinit_error=True,  num_cpus=8) 
    
    tasks_collect_etf_history_from_yf = [collect_etf_history_from_yf.remote(order, symbol) for order, symbol in enumerated_etf_symbols[:88]]
    ray.get(tasks_collect_etf_history_from_yf)
    
    # tasks_collect_currency_history_from_yf = [collect_currency_history_from_yf.remote(order, symbol) for order, symbol in currency_symbols[:]]
    # ray.get(tasks_collect_currency_history_from_yf)

    # tasks_collect_indices_investpy_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_investpy[:]]
    # ray.get(tasks_collect_indices_investpy_history_from_yf)
    
    # tasks_collect_indices_yahoo_history_from_yf = [collect_indices_history_from_yf.remote(order, symbol) for order, symbol in indices_symbols_yahoo[:]]
    # ray.get(tasks_collect_indices_yahoo_history_from_yf)
    
    # tasks_collect_indices_history_from_fred = [collect_indices_history_from_fred.remote(order,symbol) for order, symbol in indices_symbols_fred[:]]
    # ray.get(tasks_collect_indices_history_from_fred)

    ray.shutdown()

    ####### Collect History with for-loop ########
    # collect_etf_history_from_yf_with_for_loop(etf_symbols[:30])
