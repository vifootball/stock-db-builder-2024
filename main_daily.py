from etf import *
from currency import *
from index import *
from grade import *
from utils import *
import warnings
from tqdm import tqdm
from history import *
from collect_history import *
from preprocess_history import *
from postgresql_helper import *
import ray

# path setting for Cron: 다음 명령어로 체크 print(os.path.abspath('.'))
os.chdir('/Users/chungdongwook/dongwook-src/stock-db-builder-2024')

# 테스트할 때 복붙 해서 실행하기

# MAIN PROCESS
# [0] LOAD SYMBOLS
# --
# [1] COLLECT HISOTRY
# [2] MAKE HISTORY CHUNKS
# [3] COLLECT GRADES
# [4] LOAD FILES TO DB

# LOAD SYMBOLS

# LOAD SYMBOLS - ETF
etf_master = pd.read_csv(os.path.join('downloads', 'master', 'etf_master.csv'))
etf_symbols = etf_master['symbol'].to_list()
etf_symbols = [x for x in etf_symbols if x not in Etfs.EXCLUDE][:]
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
indices_symbols_fred = indices_master_fred['symbol'].to_list()
enumerated_indices_symbols_fred = list(enumerate(indices_symbols_fred))


if __name__ == '__main__':
    
    # COLLECT HISTORY
    ray.init(ignore_reinit_error=True,  num_cpus=8) 
    
    # COLLECT HISTORY - ETF    
    dirpath_history_etf = './downloads/history/etf'
    tasks_collect_etf_history_from_yf = [collect_history.remote(order, symbol, method='yf', put_dirpath=dirpath_history_etf) for order, symbol in enumerated_etf_symbols[:]]
    ray.get(tasks_collect_etf_history_from_yf)

    # COLLECT HISTORY - CURRENCY
    dirpath_history_currency = './downloads/history/currency'
    tasks_collect_currency_history_from_yf = [collect_history.remote(order, symbol, method='yf', put_dirpath=dirpath_history_currency) for order, symbol in enumerated_currency_symbols[:]]
    ray.get(tasks_collect_currency_history_from_yf)

    # COLLECT HISTORY - INDEX - YAHOO
    dirpath_history_index_yahoo = './downloads/history/index_yahoo'
    tasks_collect_index_history_from_yf = [collect_history.remote(order, symbol, method='yf', put_dirpath=dirpath_history_index_yahoo) for order, symbol in enumerated_indices_symbols_general[:]]
    ray.get(tasks_collect_index_history_from_yf)

    # COLLECT HISTORY - INDEX - FRED
    dirpath_history_index_fred = './downloads/history/index_fred'
    tasks_collect_index_history_from_fred = [collect_history.remote(order, symbol, method='fred', put_dirpath=dirpath_history_index_fred) for order, symbol in enumerated_indices_symbols_fred[:]]
    ray.get(tasks_collect_index_history_from_fred)

    ray.shutdown()


    # MAKE CHUNKS

    save_dfs_by_chunk( # ETF CHUNK
        get_dirpath='./downloads/history/etf',
        put_dirpath='./downloads/history/chunks',
        prefix_chunk='etf_chunk'
    )

    save_dfs_by_chunk( # CURRENCY CHUNK
        get_dirpath='./downloads/history/currency',
        put_dirpath='./downloads/history/chunks',
        prefix_chunk='currency_chunk'
    )

    save_dfs_by_chunk( # INDEX - YAHOO CHUNK
        get_dirpath='./downloads/history/index_yahoo',
        put_dirpath='./downloads/history/chunks',
        prefix_chunk='index_fred_chunk'
    )

    save_dfs_by_chunk( # INDEX - FRED CHUNK
        get_dirpath='./downloads/history/index_fred',
        put_dirpath='./downloads/history/chunks',
        prefix_chunk='index_yahoo_chunk'
    )


    # COLLECT GRADES
    collect_grades()


    # LOAD FILES TO DB

    # COPY FILES TO DB: DW_L1_HISTORY
    copy_csv_files_to_db(
        csv_dir_path = os.path.join("downloads", "history", "chunks"),
        table_name = "DW_L1_HISTORY",
        create_table_query = CREATE_TABLE_DW_L1_HISTORY
    )

    # COPY FILE TO DB: DM_GRADE
    copy_csv_file_to_db(
        csv_file_path = os.path.join("downloads", "etf_grades.csv"),
        table_name = 'DM_GRADE',
        create_table_query = CREATE_TABLE_DM_GRADE
    )

    # COPY FILE TO DB: DM_GRADE_PIVOT
    copy_csv_file_to_db(
        csv_file_path = os.path.join("downloads", "etf_grades_pivotted.csv"),
        table_name = 'DM_GRADE_PIVOT',
        create_table_query = CREATE_TABLE_DM_GRADE_PIVOT
    )