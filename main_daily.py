from etf import *
from currency import *
from index import *
import warnings
from tqdm import tqdm
from history import *
from collect_history import *
from preprocess_history import *
import ray

# (v) LOAD SYMBOLS
# (v) DEFINE FUNCTIONS
# (v) COLLECT HISTORY
# ( ) COLLECT GRADES
# ( ) MAKE CHUNKS

# LOAD SYMBOLS

# LOAD SYMBOLS - ETF
etf_master = pd.read_csv(os.path.join('downloads', 'etf_masters.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE][:100]
enumerated_etf_symbols = list(enumerate(etf_symbols)) # ray에러 방지용으로 반복문마다 실행순서를 부여하고 이에 따라 약간의 delay를 주는데, 그 순서를 enumerate로 가져옴

# LOAD SYMBOLS - CURRENCY
currency_master = pd.read_csv(os.path.join('downloads', 'currency_master.csv'))
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
indices_symbols_fred = indices_master_fred['symbol'].to_list()
enumerated_indices_symbols_fred = list(enumerate(indices_symbols_fred))


if __name__ == '__main__':
    
    ray.init(ignore_reinit_error=True,  num_cpus=8) 

    # COLLECT HISTORY - ETF
    tasks_collect_etf_history_from_yf = [collect_history.remote(order, symbol, method='yf') for order, symbol in enumerated_etf_symbols[:10]]
    ray.get(tasks_collect_etf_history_from_yf)

    # COLLECT HISTORY - CURRENCY
    tasks_collect_currency_history_from_yf = [collect_history.remote(order, symbol, method='yf') for order, symbol in enumerated_currency_symbols[:]]
    ray.get(tasks_collect_currency_history_from_yf)

    # COLLECT HISTORY - INDEX FOR YAHOO
    tasks_collect_index_history_from_yf = [collect_history.remote(order, symbol, method='yf') for order, symbol in enumerated_indices_symbols_general[:]]
    ray.get(tasks_collect_index_history_from_yf)

    # COLLECT HISTORY - INDEX FOR FRED
    tasks_collect_index_history_from_fred = [collect_history.remote(order, symbol) for order, symbol in enumerated_indices_symbols_fred[:]]
    ray.get(tasks_collect_index_history_from_fred)

    ray.shutdown()

