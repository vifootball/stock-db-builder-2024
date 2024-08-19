from etf import *
from currency import *
from index import *

# LOAD SYMBOLS
etf_master = pd.read_csv(os.path.join('downloads', 'etf_masters.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE]
enumerated_etf_symbols = list(enumerate(etf_symbols)) # ray에러 방지용으로 반복문마다 실행순서를 부여하고 이에 따라 약간의 delay를 주는데, 그 순서를 enumerate로 가져옴

currency_master = pd.read_csv(os.path.join('downloads', 'currency_master.csv'))
currency_symbols = list(enumerate(currency_master['symbol'].to_list()))

indices_master_yahoo = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_yahoo.csv'))
indices_symbols_yahoo = indices_master_yahoo['symbol'].to_list()

indices_master_investpy = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_investpy.csv'))
indices_symbols_investpy = indices_master_investpy['symbol'].to_list()

indices_master_fd = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_fd.csv'))
indices_symbols_fd = indices_master_fd['symbol'].to_list()

indices_symbols_general = list(enumerate((set(indices_symbols_yahoo + indices_symbols_investpy + indices_symbols_fd))))

indices_master_fred = pd.read_csv(os.path.join('downloads', 'index_master', 'index_master_fred.csv'))
indices_symbols_fred = list(enumerate(indices_master_fred['symbol'].to_list()))

print(indices_symbols_general)