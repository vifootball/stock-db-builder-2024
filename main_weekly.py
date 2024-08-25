from etf import *
from grade import *
from utils import *
import warnings
from tqdm import tqdm
from history import *
from collect_history import *
from preprocess_history import *


# MAIN PROCESS
# (1) (v) COLLECT HOLDINGS
# (2) (v) CONCAT HOLDINGS
# (3) ( ) LOAD FILES TO DB

if __name__ == '__main__':

    # COLLECT HOLDINGS
    collect_etf_holdings()
    
    
    # CONCAT HOLDINGS
    concatenated_etf_holdings = concat_csv_files_in_dir(get_dirpath='./downloads/etf_holdings')
    concatenated_etf_holdings.to_csv('./downloads/etf_holdings.csv', index=False)
    
    
    # LOAD FILES TO DB